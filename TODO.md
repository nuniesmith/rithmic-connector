# rithmic-connector — TODO

Foundation is in place (compiles, gated, connectable, health surface). The
follow-on work below is all **blocked on the access gate** (a paid Rithmic
account + dev-kit credentials + a test-system login — spike §5.2), except where
noted.

## Phase 1 (the spike proper — needs test creds)
- [ ] Validate connect/login/subscribe against the **Rithmic Test** system with
      real credentials. Confirm the `rithmic-rs` login/subscribe dance works.
- [ ] Own a **reconnect/resubscribe loop** — `rithmic-rs` leaves reconnection to
      the caller. Currently the stream loop exits on a connection issue.
- [x] Replace the persistence STUB with a real QuestDB **ILP writer** to
      `candles_futures` (`QuestDbCandleSink`, TCP :9009, mirrors janus
      `candle_sink.rs`). ILP line shape unit-tested + validated live; DEDUP
      migration `002_candles_futures_dedup.sql` added. Still to reconcile with
      janus's P11 multi-asset schema if it diverges.
- [ ] Trade timestamps: use the exchange/Rithmic tick timestamp
      (`rithmic_to_unix_nanos`) instead of `Utc::now()` for candle bucketing.
- [ ] **Measure the DOM firehose** (subscribe order book for a few CME symbols;
      record message rate + ILP absorption). The spike's headline deliverable.

## Phase 2 (read-only positions / history)
- [x] Subscribe **PnL plant** for current positions — shipped as `GET
      /positions` (read-only snapshot+subscribe into an in-memory book; fks
      #183, before the crate moved home). Live wire path still unverified
      (needs paid creds).
- [ ] Read order/fill **reports** read-only (resolve whether this needs the
      order plant's report stream WITHOUT enabling order entry — spike §5.3).
- [ ] Register in janus's source-status API (`GET /api/data/sources`).

## Phase 3 (DOM / book)
- [ ] `book_events_futures` schema (top-N vs full depth — decided by the Phase-1
      measurement).

## Not blocked on access
- [ ] Prometheus `/metrics` endpoint (currently `/status` JSON only).
- [ ] Front-month roll handling in the `rithmic:SYMBOL` namespace (spike §8).

## NEVER (doctrine)
- The **order plant is never opened for writes**. No execution path. Not later,
  not with a two-key arm. See spike §4 "Phase ∞".
