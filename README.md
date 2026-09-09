# rithmic-connector

The credential-gated, **READ-ONLY** Rithmic futures market-data connector for
the FKS platform. Design notes:
`docs/architecture/RITHMIC_INTEGRATION_SPIKE.md` in the public
[fks](https://github.com/nuniesmith/fks) repo.

## Verifying the read-only claim

This repository exists to be read. If you are a broker, prop firm or data vendor
checking what this software does to an account, the whole answer is here — there
is no other component that touches the broker connection.

**It cannot place an order.** Not "is configured not to" — the capability is not
in the source. `tests/readonly_doctrine.rs` scans every `.rs` file in `src/` and
**fails the build** if it finds `.place_order(`, `.cancel_order(`,
`.exit_position(`, `RithmicOrderPlant::` or any of the other order-entry calls
the vendor crate exposes. Run it yourself:

```bash
cargo test --test readonly_doctrine
```

The test exists because the vendor crate `rithmic-rs` re-exports the full
order-entry API and it is linked into this binary whether or not it is called.
Human review is not a barrier against a one-line edit; a failing build is.

What it does instead: opens the Ticker plant for top-of-book market data, and
the PnL plant read-only to report position and account balance at
`GET /positions`. `GET /status` reports `order_plant_open` and `read_only`
explicitly.

Rithmic's own certification step requires an order-plant login. That is
deliberately **not** in this crate — it lives as a separate throwaway binary so
this repository's claim stays true of the artifact that runs against a live
account.

> **Doctrine, non-negotiable:** read-only by construction. This crate opens
> **only** the Ticker plant of Rithmic's R|Protocol API. The **order plant
> (execution) is never opened** — there is no order-entry code path here or
> anywhere in this crate. See the fks repo's root `CLAUDE.md` "no autonomous
> execution" and spike §4 "Phase ∞ — Autonomous execution: NEVER".

## What this is (and is not)

This is a **compiling, connectable, gated foundation** — not the full DOM /
positions integration (that is follow-on, spike Phases 2–3).

| Area | Status in this PR |
|------|-------------------|
| `rithmic-rs` dependency (R\|Protocol client) | **Real.** v2.0.0, compiled against its actual API. |
| Config from env + capability gate | **Real + unit-tested.** |
| Connect + login + subscribe **one** instrument (Ticker plant) | **Real** (`live-connect` feature) — untested against a live gateway (needs paid creds). |
| `/health` + `/status` server (:9091) | **Real.** |
| Trade → 1-minute candle bucketing | **Minimal but real** — wires the read path to the sink. |
| Persistence to `candles_futures` (QuestDB ILP) | **Real + unit-tested.** ILP line formatting is byte-for-byte tested and validated live against QuestDB; the aggregator→sink flow is tested end-to-end over a stand-in TCP socket. Not yet exercised against a *live Rithmic* trade stream (needs paid creds). |
| Read-only positions/PnL (PnL plant → `/positions`) | **Real + unit-tested.** Opens the PnL plant read-only (account triple required), snapshots + subscribes, maps updates into an in-memory book served at `GET /positions`. The message→snapshot mapping is unit-tested against the vendor protobuf types; the **live wire path is unverified** (needs paid creds). |
| DOM / `book_events_futures`, order history | **Out of scope** (follow-on). |
| Reconnect/resubscribe loop | **Not yet** — `rithmic-rs` leaves reconnection to the caller; the loop exits on a connection issue. Follow-on. |

## The capability gate

The connector touches Rithmic **only** when it is BOTH enabled AND credentialed.
`RithmicConfig::gate()` returns:

- `Disabled` — `RITHMIC_ENABLED` is false (the **default**). Clean no-op.
- `MissingCredentials { missing }` — enabled but URL/user/password absent. Clean
  no-op, logs exactly which fields are missing (never their values).
- `Ready` — the **only** state that permits a network connection.

The `/health` + `/status` server runs regardless, so the platform always sees
the connector's state (including `read_only: true`, `order_plant_open: false`).

## Environment variables

| Var | Default | Meaning |
|-----|---------|---------|
| `RITHMIC_ENABLED` | `false` | Master runtime gate. Nothing connects unless true. |
| `RITHMIC_ENV` | `demo` | `demo` \| `test` \| `live`. |
| `RITHMIC_GATEWAY_URL` | *(empty)* | Primary `wss://…` gateway (issued by Rithmic after onboarding). |
| `RITHMIC_GATEWAY_ALT_URL` | = primary | Beta/alt URL for the vendor crate's alternating reconnect. |
| `RITHMIC_SYSTEM_NAME` | per-env default | e.g. `Rithmic Test`. Third secret slot. |
| `RITHMIC_USER` | *(empty)* | Login user — **injected by the spawner secret store** (broker slot 1 / `api_key`). |
| `RITHMIC_PASSWORD` | *(empty)* | Login password — spawner slot 2 / `api_secret`. |
| `RITHMIC_APP_NAME` | `josm:fks-rithmic-connector` | App name registered with Rithmic. The `josm:` prefix is assigned by them and identifies this application — do not drop it. |
| `RITHMIC_APP_VERSION` | crate version | App version string. |
| `RITHMIC_INSTRUMENT` | `MES` | The single instrument to subscribe. |
| `RITHMIC_EXCHANGE` | `CME` | Its exchange (becomes the `exchange` column). |
| `RITHMIC_ACCOUNT_ID` | *(empty)* | Trading account id for the read-only positions/PnL subscription. Optional — market data flows without it. |
| `RITHMIC_FCM_ID` | *(empty)* | Futures Commission Merchant id (positions). Optional. |
| `RITHMIC_IB_ID` | *(empty)* | Introducing Broker id (positions). Optional. |
| `QUESTDB_ILP_HOST` | `fks_questdb` | QuestDB ILP host for `candles_futures` writes. |
| `QUESTDB_ILP_PORT` | `9009` | QuestDB ILP TCP port. |
| `QUESTDB_ILP_URL` | *(empty)* | Overrides host+port; `host:port`, optional `tcp://`/`http://` prefix (scheme stripped). |
| `RITHMIC_HEALTH_HOST` / `RITHMIC_HEALTH_PORT` | `0.0.0.0` / `9091` | Health/status bind. |

Credentials map onto the existing spawner 3-slot **broker** secret exactly as
crypto bots receive exchange keys: `user → api_key`, `password → api_secret`,
`system → api_passphrase` (spike §2.1).

## Persistence — `candles_futures` (QuestDB ILP)

Completed 1-minute candles are written to QuestDB's `candles_futures` table over
the InfluxDB Line Protocol (ILP) on TCP **:9009**, mirroring how the janus candle
sink writes `candles_crypto`. Writes are handed to a background task over a
channel, so `write_candle` never blocks the read loop; the task owns the socket,
reconnects on failure, and counts every outcome (`candles_persisted` /
`persist_errors`, both surfaced on `/status`). Persistence is **best-effort /
at-most-once** — a line lost to a transient socket failure is counted as an
error and dropped, never retried indefinitely (which would stall the read loop).
This matches janus's TCP ILP sink; DEDUP (below) makes any resulting re-ingest
idempotent.

**Separate namespace.** Futures get their **own** table (`candles_futures`), not
`candles_crypto` — conflating instrument classes corrupts series. Symbols are
venue-tagged (`rithmic:MESU6`).

**Table DDL** (auto-created by the first ILP write; the column set is *identical*
to `candles_crypto` so the same fks-web chart reader queries it unchanged):

```sql
CREATE TABLE IF NOT EXISTS candles_futures (
    symbol    SYMBOL CAPACITY 256 CACHE,   -- venue-tagged, e.g. rithmic:MESU6 (ILP tag)
    exchange  SYMBOL CAPACITY 256 CACHE,   -- e.g. CME                        (ILP tag)
    interval  SYMBOL CAPACITY 256 CACHE,   -- e.g. 1m                         (ILP tag)
    open      DOUBLE,
    high      DOUBLE,
    low       DOUBLE,
    close     DOUBLE,
    volume    DOUBLE,                       -- bare-number ILP field ⇒ DOUBLE (never `i`)
    timestamp TIMESTAMP                     -- designated, µs (ILP appends ns → µs)
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(timestamp, symbol, exchange, interval);
```

**DEDUP keys:** `(timestamp, symbol, exchange, interval)` — mirrors fks #177 for
`candles_crypto`. Re-ingesting a row with the same key upserts instead of
appending a duplicate. ILP auto-creates the table *without* dedup, so on a
running deploy apply the idempotent migration
[`infrastructure/config/questdb/migrations/002_candles_futures_dedup.sql`](../../infrastructure/config/questdb/migrations/002_candles_futures_dedup.sql)
(`ALTER TABLE candles_futures DEDUP ENABLE UPSERT KEYS(...)`).

## Positions / PnL — `GET /positions` (read-only)

When the account triple (`RITHMIC_ACCOUNT_ID` / `RITHMIC_FCM_ID` / `RITHMIC_IB_ID`)
is present **and** the gate is `Ready`, the connector opens a **second** read-only
plant — the **PnL plant** — concurrently with the market-data ticker. It logs in,
pulls a position snapshot, subscribes to live updates, and maintains an in-memory
book of open positions keyed by symbol (a position going flat is dropped). Market
data flows with or without the account triple; only this reader is gated on it.

The book is served as JSON at `GET /positions` on the health port (`:9091`):

```jsonc
{
  "account": "ACCT1",
  "count": 1,
  "positions": [
    { "symbol": "ESU6", "exchange": "CME", "product_code": "ES",
      "net_quantity": -2, "direction": "short", "avg_open_price": 5012.25,
      "open_pnl": -37.5, "day_pnl": -40.0, "buy_qty": 0, "sell_qty": 2,
      "open_quantity": 2, "account_id": "ACCT1" }
  ],
  "account_summary": { "account_id": "ACCT1", "net_quantity": -2,
                       "open_pnl": -37.5, "day_pnl": -40.0, "account_balance": 52000.0 },
  "read_only": true,
  "order_plant_open": false
}
```

`/status` also carries a `positions_tracked` count. **Doctrine:** this reads the
PnL plant *only* — `RithmicOrderPlant` is never imported or opened anywhere in the
crate, and both `/positions` and `/status` surface `order_plant_open: false`. The
message→snapshot mapping (`map_instrument_update` / `map_account_update`) is pure
and unit-tested against the vendor protobuf types; the **live wire path is
unverified** and needs a paid Rithmic account to confirm.

**Exact ILP line shape** (unit-tested byte-for-byte, and validated live against
QuestDB :9009 via a scratch table whose columns/types came back identical to
`candles_crypto`):

```text
candles_futures,symbol=rithmic:MESU6,exchange=CME,interval=1m open=5000,high=5001,low=4999.5,close=5000.5,volume=42 1720000000000000000
```

## How to go live (supply creds → verify candles_futures fills)

The connector is **doubly gated**: it must be built/run under the `rithmic`
compose profile (not in the default `up`) AND pass the runtime capability gate
(`RITHMIC_ENABLED=true` + credentials). Steps:

1. **Onboard with Rithmic** (the long pole — a paid account, dev-kit
   credentials, and a gateway URL). Until this exists there is nothing to test.
2. **Store the broker credentials** via the spawner secret store / Settings, in
   the existing 3-slot broker shape: `user → RITHMIC_USER`,
   `password → RITHMIC_PASSWORD`, `system → RITHMIC_SYSTEM_NAME` (spike §2.1).
   Never put them in `docker-compose.yml` or commit them.
3. **Set the runtime env** (via `.env` / the spawner): `RITHMIC_ENABLED=true`,
   `RITHMIC_ENV=test` (or `demo`/`live`), `RITHMIC_GATEWAY_URL=wss://…`,
   `RITHMIC_INSTRUMENT` / `RITHMIC_EXCHANGE` for the contract you want.
   `QUESTDB_ILP_HOST`/`PORT` default to the in-network `fks_questdb:9009` — leave
   them unless QuestDB is elsewhere. For the **read-only positions/PnL** feed,
   also set `RITHMIC_ACCOUNT_ID` / `RITHMIC_FCM_ID` / `RITHMIC_IB_ID`; omit them
   and only market data flows.
4. **Spawn under the profile:**
   ```bash
   docker compose --profile rithmic build rithmic-connector
   docker compose --profile rithmic up -d rithmic-connector
   ```
5. **Confirm the gate opened** (should show `connected: true`, `read_only: true`,
   `order_plant_open: false`):
   ```bash
   curl -s http://127.0.0.1:9092/status | jq
   ```
6. **Enable dedup** once the table has been auto-created by the first candle
   (idempotent; see migration 002):
   ```bash
   curl -G 'http://localhost:9000/exec' \
     --data-urlencode "query=ALTER TABLE candles_futures DEDUP ENABLE UPSERT KEYS(timestamp, symbol, exchange, interval);"
   ```
7. **Verify candles are filling** (during market hours):
   ```bash
   curl -G 'http://localhost:9000/exec' \
     --data-urlencode "query=SELECT symbol, count(), max(timestamp) FROM candles_futures;"
   ```
   `candles_persisted` on `/status` should climb in lockstep; `persist_errors`
   should stay at 0. The fks-web chart reader can then query `candles_futures`
   with the same code path as `candles_crypto`.
8. **Verify positions** (if the account triple is set): `GET /positions` lists
   open positions and account P&L, and `/status` carries `positions_tracked`:
   ```bash
   curl -s http://127.0.0.1:9092/positions | jq
   ```

To **stand down**: `RITHMIC_ENABLED=false` (or stop the profile) → the connector
returns to a clean, logged no-op; nothing connects.

## Build & test

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test                       # default (live-connect) — compiles rithmic-rs
cargo test --no-default-features # skeleton only (no native-tls toolchain needed)
cargo check --workspace --locked
```

The `live-connect` feature (default on) pulls in `rithmic-rs` + `native-tls`
(openssl). Turn it off to build the gate/config/health skeleton on a minimal
image. Tests **never** open a live Rithmic connection — that requires a paid
account (spike §5.2).

## rithmic-rs viability (the spike's #1 risk, resolved)

`rithmic-rs` **2.0.0** (crates.io, MIT OR Apache-2.0, published 2026-04-21, rust
1.85, not yanked) is **viable and was adopted**. It compiles cleanly and its
real API is a close match to the spike's assumptions: actor-per-plant with
`RithmicTickerPlant` / `HistoryPlant` / `PnlPlant` / `OrderPlant`, a
`RithmicConfig` builder, `connect(&config, ConnectStrategy)` → `get_handle()` →
`login()` / `subscribe(symbol, exchange)`, and a `broadcast::Receiver` of typed
`RithmicMessage` updates (`LastTrade`, `BestBidOffer`, …). One adaptation: the
crate's own `RithmicConfig::from_env` uses a different env scheme
(`RITHMIC_DEMO_USER`, …) than the spawner-injected `RITHMIC_USER`/`RITHMIC_PASSWORD`,
so we build its config via the **builder** instead. Documented, not a blocker.

The residual risk is unchanged from the spike and is **not** about the crate:
**access + conformance** (a paid Rithmic account, dev-kit credentials, and
Rithmic's certification for production URLs) gate everything downstream, on
Rithmic's timeline.
