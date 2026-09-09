// =============================================================================
// Integration test — the read path end-to-end WITHOUT live Rithmic:
//
//   synthetic trade stream → CandleAggregator → QuestDbCandleSink → (stand-in
//   TCP listener acting as QuestDB's ILP port) → assert the exact ILP bytes.
//
// This proves the persistence path is correct without a paid Rithmic account or
// a live QuestDB: we feed the aggregator the same shape of trades the connector
// feeds it from a `RithmicMessage::LastTrade`, and confirm complete
// `candles_futures` ILP lines land on the wire.
// =============================================================================

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use rithmic_connector::aggregator::CandleAggregator;
use rithmic_connector::persistence::{PersistMetrics, QuestDbCandleSink, QuestDbConfig};

/// Read from `socket` until we have `want` newline-terminated lines.
async fn read_lines(socket: &mut tokio::net::TcpStream, want: usize) -> Vec<String> {
    let mut buf = Vec::new();
    loop {
        if buf.iter().filter(|&&b| b == b'\n').count() >= want {
            break;
        }
        let mut chunk = [0u8; 512];
        let n = socket.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8(buf)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

#[tokio::test]
async fn synthetic_trade_stream_persists_candles_futures_ilp() {
    // Stand-in for QuestDB's ILP TCP port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let metrics = Arc::new(PersistMetrics::default());
    let sink = Arc::new(QuestDbCandleSink::connect(
        QuestDbConfig {
            host: addr.ip().to_string(),
            port: addr.port(),
        },
        metrics.clone(),
    ));

    let mut agg = CandleAggregator::new("rithmic:MESU6", "CME", sink);

    // A synthetic stream of trades spanning three 1-minute buckets. Each new
    // minute flushes the previous completed candle to the sink → the wire.
    // Minute 0 @ ts 0..: trades → O=5000 H=5002 L=4999 C=4999 V=6
    agg.on_trade(0, 5000.0, 1);
    agg.on_trade(10_000, 5002.0, 2);
    agg.on_trade(20_000, 4999.0, 3);
    // Minute 1 @ ts 60_000: flushes minute 0. O=5001 H=5005 L=5001 C=5005 V=4
    agg.on_trade(60_000, 5001.0, 1);
    agg.on_trade(90_000, 5005.0, 3);
    // Minute 2 @ ts 120_000: flushes minute 1. O=5010 ... V=7
    agg.on_trade(120_000, 5010.0, 7);
    // Final flush emits minute 2.
    agg.flush();

    let (mut socket, _) = listener.accept().await.unwrap();
    let lines = read_lines(&mut socket, 3).await;

    assert_eq!(
        lines.len(),
        3,
        "three completed candles expected: {lines:?}"
    );
    // Minute 0 → designated ts in ns = 0.
    assert_eq!(
        lines[0],
        "candles_futures,symbol=rithmic:MESU6,exchange=CME,interval=1m \
         open=5000,high=5002,low=4999,close=4999,volume=6 0"
    );
    // Minute 1 → ts 60_000 ms = 60_000_000_000 ns.
    assert_eq!(
        lines[1],
        "candles_futures,symbol=rithmic:MESU6,exchange=CME,interval=1m \
         open=5001,high=5005,low=5001,close=5005,volume=4 60000000000"
    );
    // Minute 2 → ts 120_000 ms = 120_000_000_000 ns.
    assert_eq!(
        lines[2],
        "candles_futures,symbol=rithmic:MESU6,exchange=CME,interval=1m \
         open=5010,high=5010,low=5010,close=5010,volume=7 120000000000"
    );

    // Metrics reflect three durable writes, no errors.
    for _ in 0..100 {
        if metrics.candles_persisted() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(metrics.candles_persisted(), 3);
    assert_eq!(metrics.write_errors(), 0);
}

#[tokio::test]
async fn write_after_questdb_gone_is_counted_not_panicked() {
    // Bind, capture the addr, then DROP the listener so connects fail: the sink
    // must count the error and keep the read loop alive (no panic, no crash).
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }; // listener dropped here → port refuses connections

    let metrics = Arc::new(PersistMetrics::default());
    let sink = Arc::new(QuestDbCandleSink::connect(
        QuestDbConfig {
            host: addr.ip().to_string(),
            port: addr.port(),
        },
        metrics.clone(),
    ));

    let mut agg = CandleAggregator::new("rithmic:MES", "CME", sink);
    agg.on_trade(0, 100.0, 1);
    agg.flush(); // emits one candle → connect fails in the writer task

    for _ in 0..100 {
        if metrics.write_errors() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        metrics.write_errors(),
        1,
        "the failed write must be counted"
    );
    assert_eq!(metrics.candles_persisted(), 0);
}
