// =============================================================================
// aggregator.rs — minimal trade → 1-minute candle bucketing.
//
// Foundation-grade: enough to wire the read path (trade tick) through to the
// persistence seam (`CandleSink`) end-to-end, so the plumbing is exercised.
// The production aggregator (multiple intervals, session boundaries, VWAP,
// gap handling) is Phase-1 spike work — see RITHMIC_INTEGRATION_SPIKE.md §4.
// =============================================================================

use std::sync::Arc;

use crate::persistence::{CandleSink, FuturesCandle};

const MINUTE_MS: i64 = 60_000;

/// Accumulates trades into the current 1-minute bucket and flushes a completed
/// [`FuturesCandle`] to the sink when the minute rolls over.
pub struct CandleAggregator {
    symbol: String,
    exchange: String,
    sink: Arc<dyn CandleSink>,
    current: Option<Bucket>,
}

struct Bucket {
    minute_start_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
}

impl CandleAggregator {
    /// Create an aggregator for one venue-tagged symbol on `exchange`, writing to
    /// `sink`. `symbol`/`exchange` populate the matching `candles_futures`
    /// columns on every emitted bar.
    pub fn new(
        symbol: impl Into<String>,
        exchange: impl Into<String>,
        sink: Arc<dyn CandleSink>,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            exchange: exchange.into(),
            sink,
            current: None,
        }
    }

    /// Feed one trade. Flushes the previous bar to the sink when a new minute
    /// begins. Returns `true` if a completed candle was flushed.
    pub fn on_trade(&mut self, ts_ms: i64, price: f64, size: u64) -> bool {
        let minute_start = (ts_ms / MINUTE_MS) * MINUTE_MS;

        match &mut self.current {
            Some(bucket) if bucket.minute_start_ms == minute_start => {
                bucket.high = bucket.high.max(price);
                bucket.low = bucket.low.min(price);
                bucket.close = price;
                bucket.volume += size;
                false
            }
            _ => {
                let flushed = self.flush();
                self.current = Some(Bucket {
                    minute_start_ms: minute_start,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: size,
                });
                flushed
            }
        }
    }

    /// Emit the current bucket (if any) to the sink. Called on minute rollover
    /// and at shutdown. Returns `true` if a candle was written.
    pub fn flush(&mut self) -> bool {
        if let Some(bucket) = self.current.take() {
            self.sink.write_candle(&FuturesCandle {
                symbol: self.symbol.clone(),
                exchange: self.exchange.clone(),
                interval: "1m".to_string(),
                ts_ms: bucket.minute_start_ms,
                open: bucket.open,
                high: bucket.high,
                low: bucket.low,
                close: bucket.close,
                volume: bucket.volume,
            });
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::CandleSink;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        candles: Mutex<Vec<FuturesCandle>>,
    }
    impl CandleSink for RecordingSink {
        fn write_candle(&self, candle: &FuturesCandle) {
            self.candles.lock().unwrap().push(candle.clone());
        }
    }

    #[test]
    fn emitted_candle_carries_symbol_and_exchange() {
        let sink = Arc::new(RecordingSink::default());
        let mut agg = CandleAggregator::new("rithmic:MESU6", "CME", sink.clone());
        agg.on_trade(0, 100.0, 1);
        agg.flush();
        let candles = sink.candles.lock().unwrap();
        assert_eq!(candles[0].symbol, "rithmic:MESU6");
        assert_eq!(candles[0].exchange, "CME");
        assert_eq!(candles[0].interval, "1m");
    }

    #[test]
    fn aggregates_within_a_minute_and_flushes_on_rollover() {
        let sink = Arc::new(RecordingSink::default());
        let mut agg = CandleAggregator::new("rithmic:MES", "CME", sink.clone());

        // Three trades in minute 0.
        assert!(!agg.on_trade(0, 5000.0, 1));
        assert!(!agg.on_trade(10_000, 5002.0, 2));
        assert!(!agg.on_trade(20_000, 4999.0, 3));
        // First trade in minute 1 flushes minute 0.
        assert!(agg.on_trade(60_000, 5001.0, 1));

        let candles = sink.candles.lock().unwrap();
        assert_eq!(candles.len(), 1);
        let c = &candles[0];
        assert_eq!(c.ts_ms, 0);
        assert_eq!(c.open, 5000.0);
        assert_eq!(c.high, 5002.0);
        assert_eq!(c.low, 4999.0);
        assert_eq!(c.close, 4999.0);
        assert_eq!(c.volume, 6);
    }

    /// REGRESSION (2026-08-24, first live GC connect). A zero price must never
    /// reach the aggregator, because `low.min(0.0)` is 0.0 and there is no way
    /// to recover the real low afterwards. The caller
    /// (`connector.rs`, RithmicMessage::LastTrade) is what filters absent /
    /// non-positive prices; this test pins the CONSEQUENCE so the reason that
    /// filter exists is visible from the aggregator side too.
    ///
    /// Live symptom this reproduces — the very first bar written to
    /// candles_futures was `open 4698.1  high 4698.5  low 0.0  close 0.0`,
    /// because 41.5% of Rithmic's LastTrade messages for GC carried no price
    /// and `unwrap_or(0.0)` coerced them into real ticks.
    #[test]
    fn a_zero_price_tick_destroys_the_low_and_close() {
        let sink = Arc::new(RecordingSink::default());
        let mut agg = CandleAggregator::new("rithmic:GC", "COMEX", sink.clone());

        agg.on_trade(0, 4698.1, 1);
        agg.on_trade(1_000, 4698.5, 1);
        agg.on_trade(2_000, 0.0, 0); // the message that must be filtered upstream
        agg.flush();

        let candles = sink.candles.lock().unwrap();
        let c = &candles[0];
        assert_eq!(c.low, 0.0, "documents the corruption a 0.0 tick causes");
        assert_eq!(c.close, 0.0, "and that it also lands in close");
        assert_eq!(
            c.high, 4698.5,
            "high survives, which is why the bug reads as a plausible bar"
        );
    }

    /// The same sequence WITHOUT the zero tick — what the connector now feeds
    /// the aggregator after filtering — produces an honest bar.
    #[test]
    fn filtered_stream_produces_an_honest_bar() {
        let sink = Arc::new(RecordingSink::default());
        let mut agg = CandleAggregator::new("rithmic:GC", "COMEX", sink.clone());

        agg.on_trade(0, 4698.1, 1);
        agg.on_trade(1_000, 4698.5, 1);
        agg.on_trade(2_000, 4698.3, 2);
        agg.flush();

        let candles = sink.candles.lock().unwrap();
        let c = &candles[0];
        assert_eq!(c.open, 4698.1);
        assert_eq!(c.high, 4698.5);
        assert_eq!(c.low, 4698.1, "low is a REAL traded price, never 0");
        assert_eq!(c.close, 4698.3);
        assert!(
            c.low > 0.0,
            "a gold bar can never legitimately print a 0 low"
        );
    }

    #[test]
    fn final_flush_emits_open_bucket() {
        let sink = Arc::new(RecordingSink::default());
        let mut agg = CandleAggregator::new("rithmic:MES", "CME", sink.clone());
        agg.on_trade(0, 100.0, 5);
        assert!(agg.flush());
        assert!(!agg.flush(), "second flush has nothing to emit");
        assert_eq!(sink.candles.lock().unwrap().len(), 1);
    }
}
