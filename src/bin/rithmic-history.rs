// rithmic-history — one-shot historical bar fetch, for reports and for
// checking what the market actually did against what a chart claimed.
//
// Reads the same spawner-injected credential as the connector. Writes CSV to
// stdout by default; `--questdb` additionally persists into `candles_futures`,
// which OVERWRITES any live-aggregated rows for the same
// (timestamp, symbol, exchange, interval) and is therefore opt-in.
//
//   rithmic-history --symbol GC:COMEX --start 2026-09-03T12:00:00Z \
//                   --end 2026-09-03T17:00:00Z --period 5

use anyhow::{Context, Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};

use rithmic_connector::config::RithmicConfig;
use rithmic_connector::history::{HistoryRequest, fetch};
use rithmic_connector::persistence::{
    CandleSink, PersistMetrics, QuestDbCandleSink, QuestDbConfig,
};

const USAGE: &str = "\
rithmic-history — fetch closed historical bars from Rithmic

USAGE:
  rithmic-history --symbol SYM:EXCHANGE --start RFC3339 --end RFC3339 [--period N] [--questdb]

OPTIONS:
  --symbol SYM:EXCHANGE   e.g. GC:COMEX, NQ:CME, ES:CME  (required)
  --start  RFC3339        window start, e.g. 2026-09-03T12:00:00Z  (required)
  --end    RFC3339        window end (exclusive)                   (required)
  --period N              minutes per bar (default 1)
  --questdb               ALSO write into candles_futures. This overwrites
                          live-aggregated rows for the same key — off by default.
";

struct Args {
    symbol: String,
    exchange: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    period_min: i32,
    questdb: bool,
}

fn parse_args() -> Result<Args> {
    let mut symbol = None;
    let mut start = None;
    let mut end = None;
    let mut period_min = 1_i32;
    let mut questdb = false;

    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--questdb" => questdb = true,
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            "--symbol" | "--start" | "--end" | "--period" => {
                let v = it
                    .next()
                    .with_context(|| format!("{flag} requires a value"))?;
                match flag.as_str() {
                    "--symbol" => symbol = Some(v),
                    "--start" => start = Some(v),
                    "--end" => end = Some(v),
                    _ => {
                        period_min = v.parse().with_context(|| format!("bad --period: {v}"))?;
                    }
                }
            }
            other => bail!("unknown argument {other}\n\n{USAGE}"),
        }
    }

    let symbol = symbol.context("--symbol is required")?;
    let (sym, exch) = symbol
        .split_once(':')
        .context("--symbol must be SYMBOL:EXCHANGE, e.g. GC:COMEX")?;

    let parse_ts = |label: &str, raw: Option<String>| -> Result<DateTime<Utc>> {
        let raw = raw.with_context(|| format!("--{label} is required"))?;
        Ok(DateTime::parse_from_rfc3339(&raw)
            .with_context(|| format!("--{label} must be RFC3339, got {raw}"))?
            .with_timezone(&Utc))
    };

    Ok(Args {
        symbol: sym.trim().to_string(),
        exchange: exch.trim().to_string(),
        start: parse_ts("start", start)?,
        end: parse_ts("end", end)?,
        period_min,
        questdb,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = parse_args()?;
    let config = RithmicConfig::from_env();

    let req = HistoryRequest {
        symbol: args.symbol.clone(),
        exchange: args.exchange.clone(),
        period_min: args.period_min,
        start_sec: args.start.timestamp(),
        end_sec: args.end.timestamp(),
    };
    // Validate before connecting so a bad window costs no session.
    req.validate()?;

    let bars = fetch(&config, &req).await?;

    if bars.is_empty() {
        // Not an error: a closed market returns nothing. Say so on stderr so a
        // caller piping stdout to a file gets an empty file and a clear reason,
        // rather than an empty file and silence.
        eprintln!(
            "rithmic-history: no bars for {}:{} between {} and {} — \
             market closed for that window, or the contract has no history there",
            args.symbol,
            args.exchange,
            args.start.to_rfc3339_opts(SecondsFormat::Secs, true),
            args.end.to_rfc3339_opts(SecondsFormat::Secs, true),
        );
    }

    println!("timestamp,symbol,exchange,interval,open,high,low,close,volume,num_trades");
    let interval = req.interval_label();
    let venue_symbol = format!("{}:{}", rithmic_connector::VENUE, args.symbol);
    for b in &bars {
        let ts = DateTime::from_timestamp_millis(b.ts_ms)
            .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true))
            .unwrap_or_else(|| b.ts_ms.to_string());
        println!(
            "{ts},{venue_symbol},{},{interval},{},{},{},{},{},{}",
            args.exchange, b.open, b.high, b.low, b.close, b.volume, b.num_trades
        );
    }

    if args.questdb && !bars.is_empty() {
        let sink = QuestDbCandleSink::connect(
            QuestDbConfig {
                host: std::env::var("QUESTDB_HOST").unwrap_or_else(|_| "questdb".into()),
                port: 9009,
            },
            std::sync::Arc::new(PersistMetrics::default()),
        );
        for b in &bars {
            sink.write_candle(&b.to_candle(&venue_symbol, &args.exchange, &interval));
        }
        eprintln!(
            "rithmic-history: wrote {} bars into candles_futures as {venue_symbol}/{interval} \
             (this REPLACED any live-aggregated rows on the same keys)",
            bars.len()
        );
    }

    eprintln!("rithmic-history: {} bars", bars.len());
    Ok(())
}
