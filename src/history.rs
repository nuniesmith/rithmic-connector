// history.rs — historical time-bar retrieval over Rithmic's HISTORY plant.
//
// WHY THIS EXISTS. The connector subscribes to live market data, which means
// its knowledge of the market begins the moment it connects. On 2026-09-03 the
// operator traded a full ORB session from TradingView while the connector was
// down on a credential problem; when it came back there was simply no way to
// ask what the market had done, because nothing had been recorded. A live feed
// cannot answer a question about the past, and "run it earlier next time" is
// not an answer — it makes every historical question contingent on having
// already guessed which day would matter.
//
// The vendor crate has shipped `RithmicHistoryPlant` all along. This module is
// the connector's use of it: a bounded, read-only request for closed bars over
// an explicit time window.
//
// TWO DELIBERATE CHOICES, both about not corrupting the record:
//
//   1. Bars returned here are NOT written to `candles_futures` by default.
//      That table is deduplicated on (timestamp, symbol, exchange, interval),
//      so a backfill does not duplicate — it OVERWRITES. Live rows are
//      aggregated from our own tick stream; history rows are Rithmic's own
//      construction. They are not the same measurement, and the table has no
//      provenance column to tell them apart afterwards. Silently replacing one
//      with the other would make every edge measured on that table
//      unreproducible, which is the exact failure mode already recorded once
//      on the Gate-B sample. Persisting is therefore opt-in and loud.
//
//   2. The request is capped. Rithmic bills and rate-limits history, and an
//      unbounded window is how a debugging session turns into a support
//      ticket. A window that exceeds the cap is refused, not silently
//      truncated — a truncated series that still looks like a series is worse
//      than an error, because the gap is invisible in the output.

use anyhow::{Context, Result, bail};

use crate::config::RithmicConfig;
use crate::persistence::FuturesCandle;

/// Largest window a single call may request, in bars.
///
/// Rithmic returns replay bars as a multi-response stream; this bounds both the
/// wire volume and the memory held. 20k covers ~2 weeks of 1-minute bars on a
/// 23-hour session, which is far more than any single report needs.
pub const MAX_BARS: i64 = 20_000;

/// A closed historical bar, as Rithmic reported it.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryBar {
    /// Bar open time, epoch milliseconds (UTC).
    pub ts_ms: i64,
    /// First trade price in the bar.
    pub open: f64,
    /// Highest trade price in the bar.
    pub high: f64,
    /// Lowest trade price in the bar.
    pub low: f64,
    /// Last trade price in the bar.
    pub close: f64,
    /// Contracts traded in the bar.
    pub volume: u64,
    /// Number of trades in the bar. Carried because a bar with a wide range and
    /// almost no trades is a liquidity artefact, not a move — the distinction
    /// matters when a range gate is measured off this data.
    pub num_trades: u64,
}

impl HistoryBar {
    /// Render as the persistence layer's candle type.
    ///
    /// `interval` is passed in rather than derived so the caller has to state
    /// what it is writing — the interval is part of the dedup key, so getting
    /// it wrong overwrites a different series.
    #[must_use]
    pub fn to_candle(&self, symbol: &str, exchange: &str, interval: &str) -> FuturesCandle {
        FuturesCandle {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            interval: interval.to_string(),
            ts_ms: self.ts_ms,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        }
    }
}

/// What a caller asks for. Validated by [`HistoryRequest::validate`] before any
/// connection is opened, so a bad window costs nothing.
#[derive(Debug, Clone)]
pub struct HistoryRequest {
    /// Bare Rithmic symbol, e.g. `GC`. Rithmic resolves the front month.
    pub symbol: String,
    /// Exchange code, e.g. `COMEX`.
    pub exchange: String,
    /// Minutes per bar.
    pub period_min: i32,
    /// Window start, epoch seconds (inclusive).
    pub start_sec: i64,
    /// Window end, epoch seconds (exclusive).
    pub end_sec: i64,
}

impl HistoryRequest {
    /// Reject a request that cannot produce a trustworthy series.
    ///
    /// Every branch here refuses rather than repairs. Clamping an over-long
    /// window would return a series whose right edge is silently not the edge
    /// the caller asked for.
    pub fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty() {
            bail!("symbol is empty");
        }
        if self.exchange.trim().is_empty() {
            bail!("exchange is empty (Rithmic requires one, e.g. GC:COMEX)");
        }
        if self.period_min <= 0 {
            bail!("period_min must be positive, got {}", self.period_min);
        }
        if self.end_sec <= self.start_sec {
            bail!(
                "end ({}) must be after start ({})",
                self.end_sec,
                self.start_sec
            );
        }
        let span_sec = self.end_sec - self.start_sec;
        let bars = span_sec / (i64::from(self.period_min) * 60);
        if bars > MAX_BARS {
            bail!(
                "window spans ~{bars} bars of {}m, over the {MAX_BARS} cap — \
                 narrow the window or widen the period. Refusing rather than \
                 truncating: a short series that still looks complete is worse \
                 than an error.",
                self.period_min
            );
        }
        Ok(())
    }

    /// The interval label this request writes under, e.g. `5m`.
    #[must_use]
    pub fn interval_label(&self) -> String {
        format!("{}m", self.period_min)
    }
}

/// Fetch closed time bars for one instrument over one window.
///
/// Opens its own history-plant session. This is a THIRD plant on the same
/// credential (the connector already runs ticker + PnL concurrently), which
/// Rithmic permits — the one-session-per-credential limit that bites this
/// platform is per plant type, and is why the operator's R|Trader Pro contends
/// with the live feed but a history request does not.
#[cfg(feature = "live-connect")]
pub async fn fetch(config: &RithmicConfig, req: &HistoryRequest) -> Result<Vec<HistoryBar>> {
    use rithmic_rs::{ConnectStrategy, RithmicHistoryPlant, rti::request_time_bar_replay::BarType};

    req.validate()?;

    let rr_config = crate::connector::build_data_rr_config(config)
        .context("building the Rithmic config for the history plant")?;

    let plant = RithmicHistoryPlant::connect(&rr_config, ConnectStrategy::Retry)
        .await
        .map_err(|e| anyhow::anyhow!("history plant connect failed: {e}"))?;
    let handle = plant.get_handle();
    handle
        .login()
        .await
        .map_err(|e| anyhow::anyhow!("history plant login failed: {e}"))?;

    let responses = handle
        .load_time_bars(
            req.symbol.clone(),
            req.exchange.clone(),
            BarType::MinuteBar,
            req.period_min,
            i32::try_from(req.start_sec).context("start time out of range for Rithmic (i32)")?,
            i32::try_from(req.end_sec).context("end time out of range for Rithmic (i32)")?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("time-bar replay failed: {e}"));

    // Disconnect on BOTH paths. A history plant left open holds a session that
    // the next request — or the operator's terminal — then contends with.
    let _ = handle.disconnect().await;

    let bars = decode_bars(&responses?);
    Ok(bars)
}

/// Pull the bar-bearing responses out of a replay stream.
///
/// Kept separate from the transport so it is testable without a connection.
/// Responses that carry no OHLC are acknowledgements and end-of-stream
/// markers, not bars, and are skipped rather than materialised as zero-priced
/// rows — a zero-priced bar in a price series is indistinguishable from a real
/// collapse to zero once it leaves this function.
#[cfg(feature = "live-connect")]
fn decode_bars(responses: &[rithmic_rs::RithmicResponse]) -> Vec<HistoryBar> {
    use rithmic_rs::rti::messages::RithmicMessage;

    let mut out = Vec::new();
    for resp in responses {
        let RithmicMessage::ResponseTimeBarReplay(bar) = &resp.message else {
            continue;
        };
        let (Some(marker), Some(open), Some(high), Some(low), Some(close)) = (
            bar.marker,
            bar.open_price,
            bar.high_price,
            bar.low_price,
            bar.close_price,
        ) else {
            continue;
        };
        out.push(HistoryBar {
            ts_ms: i64::from(marker) * 1_000,
            open,
            high,
            low,
            close,
            volume: bar.volume.unwrap_or(0),
            num_trades: bar.num_trades.unwrap_or(0),
        });
    }
    out.sort_by_key(|b| b.ts_ms);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(period_min: i32, span_sec: i64) -> HistoryRequest {
        HistoryRequest {
            symbol: "GC".into(),
            exchange: "COMEX".into(),
            period_min,
            start_sec: 1_756_900_000,
            end_sec: 1_756_900_000 + span_sec,
        }
    }

    #[test]
    fn a_normal_session_window_is_accepted() {
        // The case this module exists for: one RTH session of 1-minute bars.
        assert!(req(1, 8 * 3600).validate().is_ok());
        assert_eq!(req(5, 3600).interval_label(), "5m");
    }

    #[test]
    fn an_oversized_window_is_refused_not_truncated() {
        // 1-minute bars across a year is ~525k bars. The failure must be an
        // error, because a truncated series still looks like a series.
        let e = req(1, 365 * 24 * 3600).validate().unwrap_err().to_string();
        assert!(e.contains("over the"), "{e}");
        assert!(e.contains("Refusing rather than"), "{e}");
    }

    #[test]
    fn a_backwards_or_empty_window_is_refused() {
        let mut r = req(1, 3600);
        std::mem::swap(&mut r.start_sec, &mut r.end_sec);
        assert!(r.validate().is_err());

        let mut same = req(1, 0);
        same.end_sec = same.start_sec;
        assert!(same.validate().is_err());
    }

    #[test]
    fn an_exchange_is_required() {
        // Rithmic fails the subscribe on a mismatched venue, and GC/COMEX vs
        // NQ/CME is exactly the split this platform runs across.
        let mut r = req(1, 3600);
        r.exchange = "  ".into();
        assert!(r.validate().unwrap_err().to_string().contains("exchange"));
    }

    #[test]
    fn to_candle_carries_the_stated_interval() {
        // The interval is part of the dedup key on candles_futures, so it is
        // the caller's explicit input rather than something inferred here.
        let bar = HistoryBar {
            ts_ms: 1_756_900_000_000,
            open: 4483.9,
            high: 4520.0,
            low: 4480.1,
            close: 4519.2,
            volume: 1234,
            num_trades: 56,
        };
        let c = bar.to_candle("rithmic:GC", "COMEX", "5m");
        assert_eq!(c.interval, "5m");
        assert_eq!(c.symbol, "rithmic:GC");
        assert_eq!(c.ts_ms, 1_756_900_000_000);
        assert!((c.high - 4520.0).abs() < f64::EPSILON);
    }
}
