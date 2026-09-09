// =============================================================================
// main.rs — rithmic-connector binary entry point.
//
// Boots the /health + /status server (always) and the connector task (which
// no-ops cleanly when the capability gate is not Ready). Shuts down on SIGINT.
// =============================================================================

use std::sync::Arc;

use anyhow::Context;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use rithmic_connector::config::RithmicConfig;
use rithmic_connector::connector;
use rithmic_connector::health;
use rithmic_connector::persistence::{CandleSink, QuestDbCandleSink, StubCandleSink};
use rithmic_connector::positions::PositionsBook;
use rithmic_connector::state::ConnectorState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = RithmicConfig::from_env();
    let gate = config.gate();
    info!(
        service = "rithmic-connector",
        version = env!("CARGO_PKG_VERSION"),
        enabled = config.enabled,
        environment = ?config.environment,
        gate = ?gate,
        "starting (READ-ONLY; order plant is never opened)"
    );
    // Log the gate decision plainly so operators see exactly why we did/didn't
    // connect — this is the capability gate visible in the logs.
    info!(reason = %gate.reason(), "capability gate decision");

    let state = ConnectorState::new(gate, format!("{}:{}", connector::VENUE, config.instrument));

    // Read-only positions book, shared between the PnL reader and the HTTP
    // server. Empty until the (optional) positions subscription fills it.
    //
    // The trailing floor per TakeProfit Trader Rule 3: it trails the highest
    // END-OF-DAY balance and CAPS at the starting balance.
    //
    // This was previously computed from intraday high-water equity including
    // open P&L and with no cap, on the theory that the broker's static
    // `min_account_balance` was wrong. It was not — an end-of-day floor
    // correctly does not move on an intraday peak, and the figure that model
    // produced (150,175 on a 150,000 account) is one the rule cannot reach.
    // We agreed with the broker in the end; `floor_disagreement` should now
    // sit at ~0 and a persistent non-zero value means one of us has drifted.
    let positions = PositionsBook::with_trailing_dd(config.trailing_dd);
    // The end-of-day mark is ACCOUNT history, not process state. Restoring it
    // matters more than it looks: without it a mid-week restart reverts the
    // floor to the starting balance, which is LOWER than the real one and so
    // overstates headroom — the direction that loses an account.
    //
    // The three outcomes are handled separately ON PURPOSE. Seeding the floor
    // is only correct when we know there is no history to miss; doing it when
    // history exists but cannot be read is precisely how a too-low floor gets
    // published with full confidence.
    let eod_path = std::path::PathBuf::from(
        std::env::var("RITHMIC_EOD_FILE")
            .unwrap_or_else(|_| rithmic_connector::eod::DEFAULT_EOD_FILE.to_string()),
    );
    let mut eod_usable = true;
    match rithmic_connector::eod::load(&eod_path, &config.account_id, config.starting_equity) {
        rithmic_connector::eod::Loaded::Restored(state) => {
            if config.starting_equity > 0.0 {
                positions.seed_starting_balance(config.starting_equity);
            }
            positions.restore_high_water_eod(state.high_water_eod);
            info!(
                mark = state.high_water_eod,
                account = %state.account_id,
                trade_date = %state.trade_date,
                observed_at = %state.observed_at,
                "restored end-of-day high-water balance"
            );
        }
        rithmic_connector::eod::Loaded::Absent => {
            // No history: the floor genuinely starts at the starting balance.
            if config.starting_equity > 0.0 {
                positions.seed_starting_balance(config.starting_equity);
                info!(
                    equity = config.starting_equity,
                    "no end-of-day history — seeding the floor at the starting balance"
                );
            }
        }
        rithmic_connector::eod::Loaded::Rejected { reason } => {
            // Deliberately do NOT seed. With no starting balance the risk block
            // is absent from /positions, orb-briefing reads that as `Unseeded`
            // and publishes geometry with NO SIZE. A briefing that refuses to
            // size sends the operator to look; one that sizes against a floor
            // we cannot justify does not.
            eod_usable = false;
            error!(
                path = %eod_path.display(),
                reason = %reason,
                "END-OF-DAY MARK REFUSED — not seeding a floor. The account risk \
                 state will report UNSEEDED and no size will be published until \
                 this is resolved. If the account was reset, remove the state \
                 file; if this is a different account, point RITHMIC_EOD_FILE at \
                 its own path."
            );
        }
    }
    if eod_usable {
        tokio::spawn(rithmic_connector::eod::run(
            positions.clone(),
            eod_path.clone(),
            config.account_id.clone(),
            config.starting_equity,
        ));
    } else {
        // Rolling over would OVERWRITE the mark we just refused to trust with
        // one derived from an unseeded book, quietly destroying the evidence
        // needed to work out what happened.
        error!("end-of-day rollover NOT started — refusing to overwrite a mark that was rejected");
    }
    // Say which credential each plant is on. A split that silently failed to
    // take effect looks identical to one that worked until the next blackout,
    // and this line is the cheapest way to tell them apart at a glance.
    match config.data.as_ref() {
        Some(_) => info!(
            "credentials: SPLIT — market data on the dedicated data login, \
             PnL/positions on the trading login. The kill switch releases only \
             the trading session; market data keeps running."
        ),
        None => info!(
            "credentials: SINGLE — both plants share one login, so the operator's \
             own terminal contends with the platform and the kill switch stops \
             market data too. Set RITHMIC_DATA_USER/PASSWORD to split them."
        ),
    }
    if config.trailing_dd <= 0.0 {
        warn!(
            "RITHMIC_TRAILING_DD unset — no trailing floor will be computed. \
             The broker's min_account_balance is NOT a substitute; see T12."
        );
    }

    // Health/status server — always up, independent of the Rithmic session.
    let health_host = config.health_host.clone();
    let health_port = config.health_port;
    let health_state = state.clone();
    let health_positions = positions.clone();
    let health_task = tokio::spawn(async move {
        if let Err(e) =
            health::serve(&health_host, health_port, health_state, health_positions).await
        {
            tracing::error!(error = %e, "health server exited with error");
        }
    });

    // Persistence sink. Only stand up the real QuestDB ILP writer when the gate
    // is Ready (i.e. candles will actually flow); otherwise the connector is a
    // clean no-op and the stub sink just logs. The QuestDB writer connects
    // lazily on the first candle and shares the state's persistence counters so
    // `/status` reflects what reached QuestDB.
    let sink: Arc<dyn CandleSink> = if state.gate().is_ready() {
        info!(
            questdb = %config.questdb.ilp_addr(),
            "persisting candles_futures to QuestDB over ILP"
        );
        Arc::new(QuestDbCandleSink::connect(
            config.questdb.clone(),
            state.persist_metrics(),
        ))
    } else {
        Arc::new(StubCandleSink)
    };
    let connector_task = tokio::spawn(connector::run(config, state, sink, positions));

    // Set when the connector ends badly, so the exit status can say so.
    let mut failure: Option<anyhow::Error> = None;

    // Wait for shutdown or the connector completing.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received — shutting down");
        }
        res = connector_task => {
            match res {
                Ok(Ok(())) => info!("connector task finished"),
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "connector task failed");
                    failure = Some(anyhow::anyhow!("connector task failed: {e}"));
                }
                Err(e) => {
                    tracing::error!(error = %e, "connector task panicked");
                    failure = Some(anyhow::anyhow!("connector task panicked: {e}"));
                }
            }
        }
    }

    health_task.abort();

    // A fatal error must not look like a clean shutdown.
    //
    // These two branches used to log and then fall through to `Ok(())`, so the
    // process exited 0 on a failed login. Docker's `restart: on-failure` reads 0
    // as success and never restarts — which is how a ticker-plant login refused
    // at 2026-09-05 07:13Z left the connector dead for two and a half days,
    // writing nothing to candles_futures, while `docker ps -a` reported the
    // reassuring "Exited (0)". The operator believed the feed was up because the
    // credentials were his and nothing said otherwise.
    //
    // This does not by itself survive a session held by the operator's own
    // terminal: `permission denied` means someone else has the login, and three
    // retries in ten seconds will all be refused. What it does is make the exit
    // status honest, so the restart policy, the health probe and anything
    // watching container state can tell "stopped on purpose" from "died".
    if let Some(err) = failure {
        return Err(err);
    }
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info,rithmic_connector=debug"))
        .context("building tracing filter")
        .unwrap();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
