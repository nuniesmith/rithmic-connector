// =============================================================================
// connector.rs — the read-only Rithmic connector.
//
// DOCTRINE (non-negotiable): READ-ONLY. This module opens ONLY the Ticker plant
// (`RithmicTickerPlant`) and subscribes market data. It NEVER imports or opens
// `RithmicOrderPlant` — there is no order-entry code path here or anywhere in
// this crate. See CLAUDE.md "no autonomous execution" and the spike doc §4
// "Phase ∞ — Autonomous execution: NEVER".
//
// Lifecycle:
//   * gate = Disabled            → log why, no-op (health server stays up).
//   * gate = MissingCredentials  → log which fields, no-op.
//   * gate = Ready               → open the Ticker plant, log received data,
//                                   bucket trades → the persistence stub.
// =============================================================================

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::config::{GateDecision, RithmicConfig};
use crate::persistence::CandleSink;
use crate::positions::PositionsBook;
use crate::state::ConnectorState;

/// The venue tag applied to every symbol this connector emits (`rithmic:SYMBOL`).
pub const VENUE: &str = "rithmic";

/// Run the connector to completion (or until the gated no-op returns).
///
/// This is the single entry point the binary calls. It consults the capability
/// gate first and only ever opens a Rithmic session when `Ready`.
pub async fn run(
    config: RithmicConfig,
    state: Arc<ConnectorState>,
    sink: Arc<dyn CandleSink>,
    positions: PositionsBook,
) -> anyhow::Result<()> {
    match config.gate() {
        GateDecision::Disabled => {
            info!(reason = %GateDecision::Disabled.reason(), "connector idle (gate: disabled)");
            Ok(())
        }
        GateDecision::MissingCredentials { missing } => {
            let decision = GateDecision::MissingCredentials { missing };
            warn!(reason = %decision.reason(), "connector idle (gate: missing credentials)");
            Ok(())
        }
        GateDecision::Ready => {
            info!(
                environment = ?config.environment,
                symbol = %format!("{VENUE}:{}", config.instrument),
                exchange = %config.exchange,
                positions_read = config.positions_ready(),
                "capability gate READY — opening the read-only Ticker plant"
            );
            run_ready(config, state, sink, positions).await
        }
    }
}

/// Build the vendor crate's config from our env-driven [`RithmicConfig`]. We
/// build programmatically (not `RrConfig::from_env`) because the crate's own env
/// scheme differs from the spawner-injected RITHMIC_USER/RITHMIC_PASSWORD.
#[cfg(feature = "live-connect")]
fn build_rr_config(config: &RithmicConfig) -> anyhow::Result<rithmic_rs::RithmicConfig> {
    use rithmic_rs::{RithmicConfig as RrConfig, RithmicEnv};

    use crate::config::RithmicEnvironment;

    let env = match config.environment {
        RithmicEnvironment::Demo => RithmicEnv::Demo,
        RithmicEnvironment::Test => RithmicEnv::Test,
        RithmicEnvironment::Live => RithmicEnv::Live,
    };

    RrConfig::builder(env)
        .url(config.gateway_url.clone())
        .beta_url(config.gateway_alt_url.clone())
        .user(config.user.clone())
        .password(config.password.clone())
        .system_name(config.system_name.clone())
        .app_name(config.app_name.clone())
        .app_version(config.app_version.clone())
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build Rithmic config: {e}"))
}

/// The credential the MARKET-DATA session should use: the dedicated data login
/// when one is configured, otherwise the primary.
///
/// The split exists because Rithmic allows one session per credential, and the
/// operator's own terminal needs the trading login during exactly the hours the
/// platform needs data. With two logins the two never contend; with one, this
/// returns the primary and the old behaviour is unchanged.
///
/// A second prop-firm ACCOUNT does not help — the firm issues one login and
/// varies only the account number — so the data credential has to come from a
/// different FCM.
#[cfg(feature = "live-connect")]
pub(crate) fn build_data_rr_config(
    config: &RithmicConfig,
) -> anyhow::Result<rithmic_rs::RithmicConfig> {
    use rithmic_rs::{RithmicConfig as RrConfig, RithmicEnv};

    use crate::config::RithmicEnvironment;

    let Some(cred) = config.data.as_ref() else {
        return build_rr_config(config);
    };
    let env = match config.environment {
        RithmicEnvironment::Demo => RithmicEnv::Demo,
        RithmicEnvironment::Test => RithmicEnv::Test,
        RithmicEnvironment::Live => RithmicEnv::Live,
    };
    RrConfig::builder(env)
        .url(cred.gateway_url.clone())
        .beta_url(cred.gateway_alt_url.clone())
        .user(cred.user.clone())
        .password(cred.password.clone())
        .system_name(cred.system_name.clone())
        .app_name(config.app_name.clone())
        .app_version(config.app_version.clone())
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build Rithmic data config: {e}"))
}

/// The `Ready` path. Split out so the gate handling above stays legible.
/// Supervise the PnL/positions reader: run one, and reconnect when it drops.
///
/// WHY THIS EXISTS. The ticker plant has had a supervisor since 2026-08-24; the
/// PnL reader never did. It was spawned detached, and when its stream ended the
/// task simply finished — nothing restarted it until the *market-data* session
/// happened to cycle, which can be hours or never.
///
/// The resulting failure mode is the worst shape available: market data keeps
/// flowing, every dashboard looks healthy, and the account state silently stops
/// updating. On 2026-08-31 the PnL stream reset and the operator traded an
/// entire session with no position, no day P&L and no trailing-drawdown
/// tracking, while the candles ticked along perfectly. The evaluation was lost
/// that day.
///
/// Unlike the ticker supervisor this one does NOT treat a fast death as a stolen
/// session. Both plants ride the same credential, so if the session is being
/// taken the ticker supervisor already detects it and stops the whole connector;
/// duplicating that judgement here would just race it.
#[cfg(feature = "live-connect")]
async fn supervise_pnl(
    config: rithmic_rs::RithmicConfig,
    account: rithmic_rs::RithmicAccount,
    book: PositionsBook,
    state: Arc<ConnectorState>,
) {
    const BACKOFF_START_SECS: u64 = 5;
    const BACKOFF_MAX_SECS: u64 = 60;
    /// A reader that stayed up this long counts as healthy, so a stable stream
    /// that drops once an hour reconnects promptly instead of inheriting an
    /// escalated delay.
    const HEALTHY_SECS: u64 = 120;

    /// How often to re-check the kill switch while halted.
    const KILL_POLL_SECS: u64 = 3;

    let kill = crate::killswitch::KillSwitch::from_env();
    let mut backoff = BACKOFF_START_SECS;
    let mut announced_kill = false;
    loop {
        // The PnL reader ALWAYS rides the trading login, so it always yields to
        // the kill switch — that login is the one the operator's terminal wants.
        // Market data may keep running on a separate credential; these are now
        // independent decisions rather than one switch for both.
        if kill.is_killed() {
            if !announced_kill {
                warn!(
                    "kill switch SET — releasing the PnL/positions session; account state is STALE \
                     until resumed"
                );
                announced_kill = true;
            }
            state.set_pnl_connected(false);
            tokio::time::sleep(std::time::Duration::from_secs(KILL_POLL_SECS)).await;
            continue;
        }
        if announced_kill {
            info!("kill switch cleared — resuming PnL/positions");
            announced_kill = false;
            backoff = BACKOFF_START_SECS;
        }

        let started = std::time::Instant::now();
        state.set_pnl_connected(true);
        match crate::positions::run_pnl(config.clone(), account.clone(), book.clone()).await {
            Ok(()) => warn!("PnL/positions stream closed — reconnecting"),
            Err(e) => warn!(error = %e, "PnL/positions reader failed — reconnecting"),
        }
        // Down BEFORE the sleep, so the blind window is observable for its whole
        // duration rather than only at the instant of failure.
        state.set_pnl_connected(false);
        let lasted = started.elapsed().as_secs();
        if lasted >= HEALTHY_SECS {
            backoff = BACKOFF_START_SECS;
        }
        warn!(
            lasted_secs = lasted,
            reconnect_in_secs = backoff,
            "account state is STALE until this reconnects — market data is unaffected"
        );
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(BACKOFF_MAX_SECS);
    }
}

/// Supervise the market-data session: run one, and reconnect when it drops.
///
/// WHY THIS EXISTS. `rithmic-rs` states plainly that reconnection is the
/// caller's job, and until now the caller did not do it: any dropped stream
/// ended the task with `Ok(())`, the process exited 0, and because that is a
/// SUCCESS exit `restart: on-failure` did not bring it back either. The feed
/// simply stopped, silently, until someone noticed.
///
/// That is not a rare edge. Measured 2026-08-24 on the live paper account,
/// sessions are dropped periodically with a `permission denied` close frame
/// regardless of instrument: 19 minutes, then 10.6 minutes, then again. A feed
/// that dies every ~10-20 minutes and never returns is not a feed.
///
/// THE DISTINCTION THAT MATTERS. A LOGIN failure is a configuration problem —
/// bad credentials, wrong system name, an unentitled exchange — and retrying it
/// is precisely the login hammer that gets a brokerage account locked (observed
/// earlier the same day: 9 attempts in 25 seconds against a bad system name).
/// So a session that never establishes is FATAL, while a session that
/// established and later dropped is retried with backoff.
#[cfg(feature = "live-connect")]
async fn run_ready(
    config: RithmicConfig,
    state: Arc<ConnectorState>,
    sink: Arc<dyn CandleSink>,
    positions: PositionsBook,
) -> anyhow::Result<()> {
    // Backoff between reconnects, capped. Reset after a session that lasted
    // long enough to count as healthy, so a stable feed that drops once an hour
    // always reconnects promptly instead of inheriting an escalated delay.
    const BACKOFF_START_SECS: u64 = 5;
    const BACKOFF_MAX_SECS: u64 = 60;
    const HEALTHY_SESSION_SECS: u64 = 120;
    /// A session that dies faster than this was almost certainly TAKEN, not
    /// dropped — Rithmic allows one session per credential, so a competing
    /// login (the operator's own R|Trader Pro phone app, typically) revokes
    /// ours within seconds.
    const STOLEN_SESSION_SECS: u64 = 90;
    /// How many consecutive stolen-looking sessions before we stop trying.
    const MAX_STOLEN_IN_A_ROW: u32 = 3;
    /// How often to re-check the kill switch while halted. Short enough that a
    /// resume from the UI feels immediate, long enough to be free.
    const KILL_POLL_SECS: u64 = 3;

    let kill = crate::killswitch::KillSwitch::from_env();
    let mut backoff = BACKOFF_START_SECS;
    let mut stolen_in_a_row: u32 = 0;
    let mut announced_kill = false;
    loop {
        // Honour the operator kill switch BEFORE opening a session. Polled in
        // the supervisor rather than checked once at startup so a resume takes
        // effect without a container restart — and so a kill set while we were
        // connected stops the NEXT reconnect rather than racing it.
        // The kill switch exists to hand the SESSION back to the operator's own
        // terminal. With a dedicated data credential that session is not the one
        // they need — the data login contends with nothing — so halting market
        // data would surrender the feed for no benefit and re-create the very
        // blackout the second credential was bought to eliminate.
        //
        // The kill still applies to the PnL reader, which does ride the trading
        // login. See `supervise_pnl`.
        let kill_applies_to_market_data = config.data.is_none();
        if kill.is_killed() && kill_applies_to_market_data {
            if !announced_kill {
                warn!(
                    path = %kill.path().display(),
                    "kill switch SET — holding off Rithmic; the session is free for another login"
                );
                announced_kill = true;
            }
            state.set_connected(false);
            state.set_killed(true);
            tokio::time::sleep(std::time::Duration::from_secs(KILL_POLL_SECS)).await;
            continue;
        }
        if announced_kill {
            info!("kill switch cleared — resuming Rithmic");
            announced_kill = false;
            // A resume is an explicit operator action, so start from a clean
            // backoff rather than inheriting an escalated one.
            backoff = BACKOFF_START_SECS;
            stolen_in_a_row = 0;
        }
        state.set_killed(false);

        let started = std::time::Instant::now();
        match run_session(&config, &state, &sink, &positions).await {
            Ok(()) => {
                let lasted = started.elapsed().as_secs();
                if lasted >= HEALTHY_SESSION_SECS {
                    backoff = BACKOFF_START_SECS;
                }

                // STOP rather than fight over the credential. Reconnecting into
                // a competing login is a LOGIN WAR: every reconnect revokes the
                // other party's session, they reconnect and revoke ours, and it
                // repeats indefinitely. Observed live 2026-08-24 — the operator
                // was logged out of their trading phone app every few seconds by
                // this connector. Retrying is correct for a network drop and
                // actively harmful here, and the two are told apart by how fast
                // the session died.
                if lasted < STOLEN_SESSION_SECS {
                    stolen_in_a_row += 1;
                    if stolen_in_a_row >= MAX_STOLEN_IN_A_ROW {
                        state.set_connected(false);
                        return Err(anyhow::anyhow!(
                            "session revoked {MAX_STOLEN_IN_A_ROW}x in a row after <{STOLEN_SESSION_SECS}s \
                             — another login is using these credentials (Rithmic allows ONE session per \
                             credential). Stopping instead of fighting over it: reconnecting would keep \
                             logging the other party out. Close the other session (e.g. R|Trader Pro), or \
                             get a SECOND Rithmic API user for the platform, then restart the connector."
                        ));
                    }
                } else {
                    stolen_in_a_row = 0;
                }

                warn!(
                    lasted_secs = lasted,
                    reconnect_in_secs = backoff,
                    stolen_in_a_row,
                    "market-data session ended — reconnecting"
                );
            }
            Err(e) => {
                // Never established (connect/login/subscribe). Retrying a
                // rejected login is the hammer loop; surface it and stop.
                state.set_connected(false);
                return Err(e);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(BACKOFF_MAX_SECS);
    }
}

/// One market-data session: connect, login, subscribe, stream until it drops.
///
/// `Ok(())` means the session was ESTABLISHED and later ended (retryable).
/// `Err` means it never established (fatal — see [`run_ready`]).
#[cfg(feature = "live-connect")]
async fn run_session(
    config: &RithmicConfig,
    state: &Arc<ConnectorState>,
    sink: &Arc<dyn CandleSink>,
    positions: &PositionsBook,
) -> anyhow::Result<()> {
    use rithmic_rs::{ConnectStrategy, RithmicTickerPlant, rti::messages::RithmicMessage};

    use crate::aggregator::CandleAggregator;

    // The ticker plant runs on the DATA credential; the PnL reader below keeps
    // the primary. When only one login exists these are the same config and
    // nothing changes.
    let rr_config = build_data_rr_config(config)?;

    // Read-only positions reader (PnL plant). Runs concurrently with the ticker
    // ONLY when the account triple is configured; market data flows regardless.
    // Spawned as a detached task so a positions-side reconnect can't stall the
    // market-data loop, and vice versa. Never opens the order plant.
    let pnl_task = if config.positions_ready() {
        let account = rithmic_rs::RithmicAccount {
            account_id: config.account_id.clone(),
            fcm_id: config.fcm_id.clone(),
            ib_id: config.ib_id.clone(),
        };
        let pnl_config = build_rr_config(config)?;
        let book = positions.clone();
        let pnl_state = state.clone();
        info!(account = %config.account_id, "starting read-only PnL/positions reader");
        Some(tokio::spawn(async move {
            supervise_pnl(pnl_config, account, book, pnl_state).await;
        }))
    } else {
        info!(
            "positions reader disabled (set RITHMIC_ACCOUNT_ID/FCM_ID/IB_ID to enable); \
             market data still flows"
        );
        None
    };

    // Ticker plant ONLY. Read-only by construction.
    let ticker_plant = RithmicTickerPlant::connect(&rr_config, ConnectStrategy::Retry)
        .await
        .map_err(|e| anyhow::anyhow!("ticker plant connect failed: {e}"))?;
    let mut handle = ticker_plant.get_handle();

    handle
        .login()
        .await
        .map_err(|e| anyhow::anyhow!("ticker plant login failed: {e}"))?;
    state.set_connected(true);
    info!("logged in to Rithmic ticker plant");

    // Subscribe EVERY configured instrument on this ONE session. Rithmic
    // permits only one concurrent session per credential (measured 2026-08-24:
    // a second connector logging in killed the first with `permission denied`),
    // so one-container-per-instrument is not an option — the single session has
    // to carry them all.
    let instruments = config.instruments();
    if instruments.is_empty() {
        return Err(anyhow::anyhow!(
            "no instruments configured — set RITHMIC_INSTRUMENT (e.g. \"GC:COMEX,NQ:CME,ES:CME\")"
        ));
    }

    // One aggregator per symbol, keyed by the BARE Rithmic symbol so incoming
    // messages (whose `symbol` field is bare, e.g. "GC") route in O(1). The
    // venue-tagged form ("rithmic:GC") is what the aggregator writes to
    // candles_futures, so both spellings are needed and must not be confused.
    let kill_check = crate::killswitch::KillSwitch::from_env();
    let mut aggregators: HashMap<String, CandleAggregator> = HashMap::new();
    for (sym, exch) in &instruments {
        handle
            .subscribe(sym, exch)
            .await
            .map_err(|e| anyhow::anyhow!("market-data subscribe failed for {sym}/{exch}: {e}"))?;
        let venue_symbol = format!("{VENUE}:{sym}");
        info!(symbol = %venue_symbol, exchange = %exch, "subscribed to market data (read-only)");
        aggregators.insert(
            sym.clone(),
            CandleAggregator::new(venue_symbol, exch.clone(), sink.clone()),
        );
    }
    state.set_subscribed(instruments.len());

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                if let Some(err) = &update.error {
                    if err.is_connection_issue() {
                        warn!(error = %err, "connection issue on ticker stream — stopping loop");
                        break;
                    }
                    continue;
                }
                // Mid-session kill: drop the session promptly so the operator
                // can log in from their phone without waiting for a natural
                // disconnect. Checked on the message path (cheap: a stat) so a
                // busy feed reacts within one message.
                if kill_check.is_killed() {
                    warn!("kill switch set mid-session — releasing the Rithmic session");
                    break;
                }
                state.inc_messages();

                match update.message {
                    RithmicMessage::LastTrade(trade) => {
                        // A LastTrade WITHOUT a trade_price is not a trade at
                        // this price — it is not a trade at all. Rithmic sends
                        // LastTrade updates with the price field ABSENT (volume
                        // / session bookkeeping); measured live on GC at the
                        // 2026-08-24 first connect, 176 of 424 LastTrade
                        // messages (41.5%) had no price AND no size.
                        //
                        // `unwrap_or(0.0)` turned every one of those into a
                        // genuine 0.0 tick, and CandleAggregator faithfully did
                        // `low = low.min(0.0)` -> 0.0, plus `close = 0.0`
                        // whenever such a message landed last in a bucket. The
                        // first bar ever written proved it:
                        //   open 4698.1  high 4698.5  low 0.0  close 0.0
                        //
                        // That is not a cosmetic defect here: ORB is built
                        // ENTIRELY on the opening range's high/low, so a
                        // zero-floored low silently maximises every computed
                        // range and every position size derived from it
                        // (`orb::size_position` divides by stop distance).
                        // Skip the update instead of coercing absent to zero.
                        let Some(price) = trade.trade_price.filter(|p| *p > 0.0) else {
                            continue;
                        };
                        // Route by the message's OWN symbol. With several
                        // instruments on one session the stream is interleaved,
                        // so trusting a single configured symbol would file
                        // every venue's trades into one bucket — silently
                        // blending GC, NQ and ES into one nonsense candle.
                        // An unknown symbol is dropped rather than guessed at.
                        let Some(sym) = trade.symbol.as_deref() else {
                            continue;
                        };
                        let Some(aggregator) = aggregators.get_mut(sym) else {
                            continue;
                        };
                        let size = trade.trade_size.unwrap_or(0) as u64;
                        debug!(symbol = %sym, price, size, "trade");
                        // Bucket into 1m bars; the stub sink logs the write.
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        if aggregator.on_trade(now_ms, price, size) {
                            state.inc_candles();
                        }
                    }
                    RithmicMessage::BestBidOffer(bbo) => {
                        debug!(
                            symbol = bbo.symbol.as_deref().unwrap_or("?"),
                            bid = bbo.bid_price.unwrap_or(0.0),
                            ask = bbo.ask_price.unwrap_or(0.0),
                            "bbo"
                        );
                    }
                    _ => {}
                }
            }
            Err(e) => {
                warn!(error = %e, "ticker subscription channel closed — stopping loop");
                break;
            }
        }
    }

    // Flush any partial bar and shut the session down cleanly.
    // Flush EVERY symbol's partial bar, not just one — with several
    // instruments on a session, returning early here would silently discard
    // the in-progress bar of every other symbol.
    for aggregator in aggregators.values_mut() {
        if aggregator.flush() {
            state.inc_candles();
        }
    }
    state.set_connected(false);
    let _ = handle.disconnect().await;
    info!("ticker plant disconnected");

    // Tear the positions reader down with the market-data loop.
    if let Some(task) = pnl_task {
        task.abort();
    }
    Ok(())
}

/// Fallback `Ready` path when the crate is built without the `live-connect`
/// feature: there is no client compiled in, so we log and idle instead of
/// pretending to connect.
#[cfg(not(feature = "live-connect"))]
async fn run_ready(
    _config: RithmicConfig,
    _state: Arc<ConnectorState>,
    _sink: Arc<dyn CandleSink>,
    _positions: PositionsBook,
) -> anyhow::Result<()> {
    warn!(
        "gate is READY but the crate was built without the `live-connect` feature — \
         no Rithmic client available; rebuild with default features to connect"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RithmicConfig;
    use crate::persistence::StubCandleSink;

    /// A disabled connector must be a clean no-op — it returns Ok without ever
    /// attempting a connection. (No live Rithmic connection is exercised in
    /// tests; that needs real creds + a paid account.)
    #[tokio::test]
    async fn disabled_connector_is_a_noop() {
        let cfg = RithmicConfig::from_getter(|_| None); // enabled=false
        let state = ConnectorState::new(cfg.gate(), "rithmic:MES");
        let sink = Arc::new(StubCandleSink);
        run(cfg, state.clone(), sink, PositionsBook::new())
            .await
            .unwrap();
        assert!(!state.snapshot().connected);
        assert_eq!(state.snapshot().messages_received, 0);
    }

    /// Enabled-but-uncredentialed is also a clean no-op (missing-creds gate).
    #[tokio::test]
    async fn missing_credentials_is_a_noop() {
        let cfg =
            RithmicConfig::from_getter(|k| (k == "RITHMIC_ENABLED").then(|| "true".to_string()));
        assert!(matches!(
            cfg.gate(),
            GateDecision::MissingCredentials { .. }
        ));
        let state = ConnectorState::new(cfg.gate(), "rithmic:MES");
        run(
            cfg,
            state.clone(),
            Arc::new(StubCandleSink),
            PositionsBook::new(),
        )
        .await
        .unwrap();
        assert!(!state.snapshot().connected);
    }
}
