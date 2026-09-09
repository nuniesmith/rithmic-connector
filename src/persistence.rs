// =============================================================================
// persistence.rs — futures market data → QuestDB.
//
// The read path aggregates trades into 1-minute [`FuturesCandle`]s and hands
// each completed bar to a [`CandleSink`]. Two sinks exist:
//
//   * [`StubCandleSink`]     — logs what it *would* write. Used when the
//                              capability gate is not Ready (idle connector) and
//                              in tests that don't want a socket.
//   * [`QuestDbCandleSink`]  — the real writer: serialises each candle to a
//                              QuestDB InfluxDB Line Protocol (ILP) line and
//                              ships it over TCP (:9009) to the `candles_futures`
//                              table, mirroring how janus's candle sink writes
//                              `candles_crypto`.
//
// TABLE (auto-created by the first ILP write; verified live against QuestDB
// 9.x, 2026-07 — a scratch table created from a line of this exact shape came
// back with columns/types identical to `candles_crypto`):
//
//   candles_futures
//     symbol    SYMBOL        -- venue-tagged, e.g. `rithmic:MESU6`  (ILP tag)
//     exchange  SYMBOL        -- e.g. `CME`                          (ILP tag)
//     interval  SYMBOL        -- e.g. `1m`                           (ILP tag)
//     open      DOUBLE        -- (ILP float field)
//     high      DOUBLE
//     low       DOUBLE
//     close     DOUBLE
//     volume    DOUBLE        -- written as a bare number so ILP makes it DOUBLE
//     timestamp TIMESTAMP     -- designated timestamp, µs (ILP appends ns → µs)
//   TIMESTAMP(timestamp) PARTITION BY DAY WAL
//   DEDUP UPSERT KEYS(timestamp, symbol, exchange, interval)
//
// The DEDUP clause mirrors fks #177 (candles_crypto) — re-ingesting a row with
// the same (timestamp, symbol, exchange, interval) upserts instead of appending
// a duplicate. ILP auto-creates the table WITHOUT dedup, so the DEDUP is applied
// by the idempotent migration
// infrastructure/config/questdb/migrations/002_candles_futures_dedup.sql (see
// that file / the README runbook). The designated timestamp MUST be one of the
// keys — it is (`timestamp`).
//
// The `symbol`/`exchange`/`interval`/`open..volume`/`timestamp` shape is
// deliberately identical to `candles_crypto` so the SAME fks-web chart reader
// (src/web/src/hooks.server.ts) can query `candles_futures` unchanged.
//
// DOCTRINE: read-only market data only. The order plant is never a source here.
// DOM/depth is a separate, higher-volume table (`book_events_futures`) and is
// out of scope for this PR (spike Phase 3).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// The QuestDB table every futures candle is written to.
pub const CANDLES_TABLE: &str = "candles_futures";

/// A single 1-minute futures candle the connector persists.
#[derive(Debug, Clone)]
pub struct FuturesCandle {
    /// Venue-tagged symbol, e.g. `rithmic:MESU6`.
    pub symbol: String,
    /// The exchange the instrument trades on, e.g. `CME`.
    pub exchange: String,
    /// Bar interval label, e.g. `1m`.
    pub interval: String,
    /// Epoch-millisecond timestamp of the bar's open.
    pub ts_ms: i64,
    /// OHLC.
    pub open: f64,
    /// OHLC.
    pub high: f64,
    /// OHLC.
    pub low: f64,
    /// OHLC.
    pub close: f64,
    /// Aggregated traded volume within the bar.
    pub volume: u64,
}

/// The persistence seam. Implementors take a completed candle and get it durable
/// (or log it). Errors are the sink's concern (buffering, reconnect, metrics)
/// and must NEVER propagate back far enough to kill ingestion — hence the
/// infallible signature.
pub trait CandleSink: Send + Sync {
    /// Persist one completed candle. Non-blocking: must not await or block the
    /// caller's (read-loop) task.
    fn write_candle(&self, candle: &FuturesCandle);
}

/// Observable persistence counters, shared with `/status`. Cheap atomics so the
/// writer task updates them without locks.
#[derive(Debug, Default)]
pub struct PersistMetrics {
    candles_persisted: AtomicU64,
    write_errors: AtomicU64,
}

impl PersistMetrics {
    /// Candles successfully written to QuestDB since start.
    pub fn candles_persisted(&self) -> u64 {
        self.candles_persisted.load(Ordering::Relaxed)
    }

    /// Persistence errors (connect/write failures, dropped lines) since start.
    pub fn write_errors(&self) -> u64 {
        self.write_errors.load(Ordering::Relaxed)
    }

    fn inc_persisted(&self) {
        self.candles_persisted.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_errors(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Where to reach QuestDB's ILP TCP ingestion endpoint (:9009 by default).
#[derive(Debug, Clone)]
pub struct QuestDbConfig {
    /// ILP host — defaults to the in-network `fks_questdb`.
    pub host: String,
    /// ILP TCP port — defaults to `9009`.
    pub port: u16,
}

impl Default for QuestDbConfig {
    fn default() -> Self {
        Self {
            host: "fks_questdb".to_string(),
            port: 9009,
        }
    }
}

impl QuestDbConfig {
    /// `host:port`, ready for `TcpStream::connect`.
    pub fn ilp_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// No-op sink that logs what a real writer *would* persist. Used when the
/// connector is gated off (idle) and in tests that don't want a socket.
#[derive(Debug, Default, Clone)]
pub struct StubCandleSink;

impl CandleSink for StubCandleSink {
    fn write_candle(&self, candle: &FuturesCandle) {
        info!(
            target: "rithmic_connector::persistence",
            line = %format_ilp_line(candle),
            "STUB candles_futures write (no QuestDB writer wired — connector idle or test)"
        );
    }
}

/// The real sink: formats each candle to a QuestDB ILP line and ships it to
/// `candles_futures` over TCP. Writes are handed to a background task over an
/// unbounded channel so `write_candle` never blocks the read loop; the task owns
/// the socket, reconnects on failure, and counts every outcome.
#[derive(Debug, Clone)]
pub struct QuestDbCandleSink {
    tx: mpsc::UnboundedSender<String>,
    metrics: Arc<PersistMetrics>,
}

impl QuestDbCandleSink {
    /// Build a sink writing to `cfg`, spawning the background ILP writer task.
    /// The socket connects lazily on the first candle, so a gated-off connector
    /// that never produces a candle opens no connection.
    pub fn connect(cfg: QuestDbConfig, metrics: Arc<PersistMetrics>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(writer_loop(cfg, rx, metrics.clone()));
        Self { tx, metrics }
    }

    /// The shared persistence counters (also surfaced on `/status`).
    pub fn metrics(&self) -> Arc<PersistMetrics> {
        self.metrics.clone()
    }
}

impl CandleSink for QuestDbCandleSink {
    fn write_candle(&self, candle: &FuturesCandle) {
        let line = format_ilp_line(candle);
        if self.tx.send(line).is_err() {
            // The writer task is gone (should only happen at shutdown). Count it
            // and log — never panic on the read loop.
            self.metrics.inc_errors();
            error!(
                target: "rithmic_connector::persistence",
                symbol = %candle.symbol,
                "candles_futures write dropped — ILP writer task is gone"
            );
        }
    }
}

/// Background task: owns the ILP TCP socket, drains the channel, and writes each
/// line. Reconnects on failure. Best-effort (at-most-once) delivery, matching
/// janus's TCP ILP sink — a line lost to a transient socket failure is counted
/// as an error and dropped, never retried indefinitely (which would stall the
/// channel and starve the read loop).
async fn writer_loop(
    cfg: QuestDbConfig,
    mut rx: mpsc::UnboundedReceiver<String>,
    metrics: Arc<PersistMetrics>,
) {
    let addr = cfg.ilp_addr();
    let mut stream: Option<TcpStream> = None;

    while let Some(line) = rx.recv().await {
        // (Re)connect lazily.
        if stream.is_none() {
            match TcpStream::connect(&addr).await {
                Ok(s) => {
                    info!(target: "rithmic_connector::persistence", %addr, "connected to QuestDB ILP");
                    stream = Some(s);
                }
                Err(e) => {
                    metrics.inc_errors();
                    warn!(target: "rithmic_connector::persistence", %addr, error = %e, "QuestDB ILP connect failed — dropping candle line");
                    continue;
                }
            }
        }

        // ILP lines are newline-terminated.
        let bytes = format!("{line}\n");
        let s = stream.as_mut().expect("stream is Some");
        if let Err(e) = s.write_all(bytes.as_bytes()).await {
            metrics.inc_errors();
            warn!(target: "rithmic_connector::persistence", %addr, error = %e, "QuestDB ILP write failed — dropping connection, will reconnect");
            stream = None; // force reconnect on the next line
            continue;
        }
        metrics.inc_persisted();
        debug!(target: "rithmic_connector::persistence", %line, "candles_futures ILP write");
    }

    if let Some(mut s) = stream.take() {
        let _ = s.shutdown().await;
    }
    debug!(target: "rithmic_connector::persistence", "ILP writer loop exited (channel closed)");
}

/// Serialise one candle to a QuestDB ILP line (WITHOUT the trailing newline).
///
/// Shape (verified live against `candles_crypto`'s auto-created schema):
///
/// ```text
/// candles_futures,symbol=rithmic:MESU6,exchange=CME,interval=1m open=5000,high=5001,low=4999.5,close=5000.5,volume=42 1720000000000000000
/// ```
///
/// - `symbol` / `exchange` / `interval` are ILP **tags** → SYMBOL columns.
/// - `open..volume` are bare-number ILP **fields** → DOUBLE columns (an `i`
///   suffix would make them LONG; we never emit one, so `volume` is DOUBLE and
///   the fks-web chart reader can query it like `candles_crypto`).
/// - the trailing integer is the designated `timestamp` in **nanoseconds**
///   (`ts_ms * 1_000_000`); QuestDB stores it as µs.
pub fn format_ilp_line(c: &FuturesCandle) -> String {
    format!(
        "{table},symbol={sym},exchange={exch},interval={iv} open={o},high={h},low={l},close={cl},volume={v} {ts_ns}",
        table = CANDLES_TABLE,
        sym = escape_tag(&c.symbol),
        exch = escape_tag(&c.exchange),
        iv = escape_tag(&c.interval),
        o = fmt_f64(c.open),
        h = fmt_f64(c.high),
        l = fmt_f64(c.low),
        cl = fmt_f64(c.close),
        v = fmt_f64(c.volume as f64),
        ts_ns = c.ts_ms * 1_000_000,
    )
}

/// Escape an ILP tag value: space, comma and equals must be backslash-escaped.
/// (Colon — as in `rithmic:MESU6` — is NOT special and is left as-is.)
fn escape_tag(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for ch in v.chars() {
        if matches!(ch, ' ' | ',' | '=') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Format an f64 as a bare ILP field value (shortest round-trip repr; QuestDB
/// parses a suffix-less number as DOUBLE).
fn fmt_f64(x: f64) -> String {
    // `{}` gives Rust's shortest round-tripping decimal: 5000.0 → "5000",
    // 4999.5 → "4999.5". Non-finite values would be invalid ILP; clamp them to 0
    // rather than emit `inf`/`NaN` (which QuestDB rejects) and poison the batch.
    if x.is_finite() {
        format!("{x}")
    } else {
        "0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> FuturesCandle {
        FuturesCandle {
            symbol: "rithmic:MESU6".to_string(),
            exchange: "CME".to_string(),
            interval: "1m".to_string(),
            ts_ms: 1_720_000_000_000,
            open: 5000.0,
            high: 5001.0,
            low: 4999.5,
            close: 5000.5,
            volume: 42,
        }
    }

    #[test]
    fn ilp_line_has_the_exact_expected_shape() {
        // This exact string was validated against a live QuestDB (:9009) scratch
        // table: it auto-created columns identical to candles_crypto
        // (symbol/exchange/interval SYMBOL; open/high/low/close/volume DOUBLE;
        // timestamp TIMESTAMP designated) and round-tripped volume=42.0 as DOUBLE
        // and timestamp as 1720000000000000 µs.
        let line = format_ilp_line(&sample());
        assert_eq!(
            line,
            "candles_futures,symbol=rithmic:MESU6,exchange=CME,interval=1m \
             open=5000,high=5001,low=4999.5,close=5000.5,volume=42 1720000000000000000"
        );
    }

    #[test]
    fn timestamp_is_nanoseconds() {
        let line = format_ilp_line(&sample());
        // ts_ms 1_720_000_000_000 → ns 1_720_000_000_000_000_000 as the last token.
        let last = line.rsplit(' ').next().unwrap();
        assert_eq!(last, "1720000000000000000");
    }

    #[test]
    fn volume_is_a_double_field_not_an_integer() {
        // No `i` suffix ⇒ QuestDB creates a DOUBLE column (matches candles_crypto).
        let line = format_ilp_line(&sample());
        assert!(line.contains("volume=42 "));
        assert!(!line.contains("volume=42i"));
    }

    #[test]
    fn tag_values_escape_spaces_commas_and_equals() {
        let c = FuturesCandle {
            symbol: "a b,c=d".to_string(),
            ..sample()
        };
        let line = format_ilp_line(&c);
        assert!(line.contains(r"symbol=a\ b\,c\=d,exchange=CME"));
    }

    #[test]
    fn stub_sink_accepts_a_candle_through_the_trait() {
        let sink: Box<dyn CandleSink> = Box::new(StubCandleSink);
        sink.write_candle(&sample());
    }

    #[tokio::test]
    async fn questdb_sink_ships_the_exact_ilp_line_over_tcp() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        // Stand up a throwaway TCP listener standing in for QuestDB's ILP port —
        // no live QuestDB, no live Rithmic. Assert the bytes on the wire are
        // exactly the ILP line + newline.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics = Arc::new(PersistMetrics::default());
        let sink = QuestDbCandleSink::connect(
            QuestDbConfig {
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            metrics.clone(),
        );

        sink.write_candle(&sample());

        let (mut socket, _) = listener.accept().await.unwrap();
        // Read one line.
        let mut buf = Vec::new();
        // The writer sends one line then idles; read until we have a newline.
        loop {
            let mut chunk = [0u8; 256];
            let n = socket.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.contains(&b'\n') {
                break;
            }
        }
        let got = String::from_utf8(buf).unwrap();
        assert_eq!(
            got,
            "candles_futures,symbol=rithmic:MESU6,exchange=CME,interval=1m \
             open=5000,high=5001,low=4999.5,close=5000.5,volume=42 1720000000000000000\n"
        );

        // The persisted counter must reflect the successful write.
        // Give the writer task a beat to update the atomic after write_all.
        for _ in 0..50 {
            if metrics.candles_persisted() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(metrics.candles_persisted(), 1);
        assert_eq!(metrics.write_errors(), 0);
    }
}
