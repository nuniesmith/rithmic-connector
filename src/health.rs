// =============================================================================
// health.rs — /health + /status + /positions server (FKS bot contract, :9091).
//
// Mirrors the spawner/bot health surface so the platform can see connector
// state whether or not Rithmic is reachable:
//   GET /health    → 200 {"status":"ok"}   (cheap liveness for Docker/Prometheus)
//   GET /status    → connector snapshot     (gate, connected, counters, invariants)
//   GET /positions → read-only positions book (open positions + account P&L)
// =============================================================================

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde_json::json;
use tracing::info;

use crate::killswitch::KillSwitch;
use crate::positions::PositionsBook;
use crate::state::ConnectorState;

/// Combined server state: the connector snapshot source + the positions book.
#[derive(Clone)]
pub struct HealthState {
    /// Shared connector state (gate, counters, connection status).
    pub conn: Arc<ConnectorState>,
    /// Read-only positions book fed by the PnL plant.
    pub positions: PositionsBook,
    /// Durable operator kill switch (see `killswitch.rs`).
    pub kill: KillSwitch,
}

/// Guard for the MUTATING kill/resume routes.
///
/// GET /health, /status and /positions stay open — they are read-only and the
/// platform scrapes them. Kill and resume CHANGE BEHAVIOUR, so they take the
/// same `X-Internal-Token` every other money-adjacent route on this platform
/// uses (nginx sets it; the webui reaches the connector through nginx).
///
/// FAIL-CLOSED: with `NGINX_INTERNAL_TOKEN` unset or empty the routes are
/// REFUSED outright rather than left open. An unset token is a misconfiguration,
/// and defaulting it to "no auth required" on a route that can hand the Rithmic
/// session to someone else is not a default worth having.
fn authorized(headers: &HeaderMap) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let expected = std::env::var("NGINX_INTERNAL_TOKEN").unwrap_or_default();
    if expected.trim().is_empty() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "NGINX_INTERNAL_TOKEN is not set — kill/resume disabled (fail-closed)"
            })),
        ));
    }
    let got = headers
        .get("x-internal-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    // Constant-time compare, matching the spawner's auth: a byte-by-byte
    // shortcut leaks the token through timing.
    let (a, b) = (expected.as_bytes(), got.as_bytes());
    let ok = a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0;
    if ok {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "bad or missing X-Internal-Token"})),
        ))
    }
}

/// Build the axum router for the health/status/positions surface.
pub fn router(conn: Arc<ConnectorState>, positions: PositionsBook) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/positions", get(positions_handler))
        .route("/metrics", get(metrics))
        .route("/kill", post(kill_handler))
        .route("/resume", post(resume_handler))
        .with_state(HealthState {
            conn,
            positions,
            kill: KillSwitch::from_env(),
        })
}

/// Prometheus exposition. Exists so the two failure shapes that cost the
/// 2026-08-31 evaluation become alertable rather than merely visible in a JSON
/// blob nobody was watching:
///
/// * `rithmic_account_state_blind` — market data flowing while the PnL plant is
///   down. That asymmetry is the dangerous one: every dashboard reads healthy
///   and the account is untracked.
/// * `rithmic_trailing_headroom_usd` — dollars above OUR computed floor, not the
///   broker's. The broker's field reported ~$4,500 of room with ~$170 left.
/// * `rithmic_floor_disagreement_usd` — how far the broker's number sits from
///   ours. Non-zero means one of us is wrong about the account-ending rule, and
///   that is worth knowing before it matters rather than after.
async fn metrics(State(st): State<HealthState>) -> String {
    let snap = st.conn.snapshot();
    let mut out = String::new();
    let mut g = |name: &str, help: &str, v: f64| {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
        ));
    };
    g(
        "rithmic_connected",
        "1 when the market-data session is open.",
        u8::from(snap.connected).into(),
    );
    g(
        "rithmic_pnl_connected",
        "1 when the read-only PnL/positions subscription is live.",
        u8::from(st.conn.pnl_connected()).into(),
    );
    g(
        "rithmic_account_state_blind",
        "1 when market data is flowing but account state is NOT. The 2026-08-31 shape.",
        u8::from(st.conn.account_state_blind()).into(),
    );
    // -1 distinguishes "never received one" from "received one just now"; a 0
    // age for a stream that has never delivered would read as perfectly fresh.
    g(
        "rithmic_account_state_age_seconds",
        "Seconds since the last account/position update; -1 before the first.",
        st.positions.age_secs().map_or(-1.0, |s| s as f64),
    );
    if let Some(r) = st.positions.risk() {
        g(
            "rithmic_trailing_headroom_usd",
            "Equity minus OUR computed trailing floor. Negative means breached.",
            r.headroom,
        );
        // Exposed directly rather than left to be inferred from floor + dd,
        // because the high-water mark is IN-MEMORY and only ever ratchets up.
        // After a prop-account reset it must fall back to the starting equity,
        // and the only way to see that it has is to look at it.
        g(
            "rithmic_trailing_high_water_usd",
            "Highest END-OF-DAY balance (realised). Trails per TPT Rule 3; re-seeds from RITHMIC_STARTING_EQUITY on restart.",
            r.high_water_eod,
        );
        g(
            "rithmic_trailing_floor_usd",
            "The trailing floor computed from our own high-water equity.",
            r.computed_min_balance,
        );
        g(
            "rithmic_floor_disagreement_usd",
            "Broker's reported floor minus ours. Non-zero means one of us is wrong.",
            r.floor_disagreement,
        );
    }
    out
}

/// Cheap liveness probe. Always 200 while the process is up — it does NOT
/// depend on the Rithmic session (a gated-off connector is still healthy).
async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "service": "rithmic-connector" }))
}

/// Full connector snapshot: gate decision, connection state, counters, and the
/// read-only/order-plant invariants. Includes the count of tracked positions.
async fn status(State(state): State<HealthState>) -> Json<serde_json::Value> {
    let mut snap = json!(state.conn.snapshot());
    if let Some(obj) = snap.as_object_mut() {
        obj.insert("positions_tracked".into(), json!(state.positions.len()));
        // Distinguish "operator turned it off" from "it broke" — `connected:
        // false` alone cannot, and that ambiguity is exactly what makes an
        // operator poke at a system that is behaving correctly.
        if let Some(detail) = state.kill.details() {
            obj.insert("kill_detail".into(), json!(detail.trim()));
        }
    }
    Json(snap)
}

/// The read-only positions book: open positions + optional account P&L summary.
/// The `read_only`/`order_plant_open` invariants are surfaced in the payload.
async fn positions_handler(State(state): State<HealthState>) -> Json<serde_json::Value> {
    Json(json!(state.positions.view()))
}

/// Bind and serve the health surface until the process exits.
pub async fn serve(
    host: &str,
    port: u16,
    conn: Arc<ConnectorState>,
    positions: PositionsBook,
) -> anyhow::Result<()> {
    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(%addr, "health/status server listening (GET /health, /status, /positions)");
    axum::serve(listener, router(conn, positions)).await?;
    Ok(())
}

/// POST /kill — release the Rithmic session so another login (the operator's
/// phone app) can use the single-session credential.
///
/// Durable: the sentinel survives a container restart, so a killed connector
/// that is restarted comes back KILLED. Only an explicit resume clears it.
async fn kill_handler(
    State(st): State<HealthState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    authorized(&headers)?;
    let reason = "operator kill via API";
    match st.kill.kill(reason) {
        Ok(()) => {
            info!("kill switch set via API");
            Ok(Json(json!({
                "killed": true,
                "detail": "Rithmic session released; another login may now use these credentials",
                "sentinel": st.kill.path().display().to_string(),
            })))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("could not set the kill switch: {e}")})),
        )),
    }
}

/// POST /resume — clear the kill switch; the supervisor reconnects within a few
/// seconds. Errors if the operator's other session is still holding the
/// credential are surfaced by the supervisor, not here.
async fn resume_handler(
    State(st): State<HealthState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    authorized(&headers)?;
    match st.kill.resume() {
        Ok(()) => {
            info!("kill switch cleared via API");
            Ok(Json(json!({
                "killed": false,
                "detail": "reconnecting — close any other Rithmic login first, or this session will be revoked",
            })))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("could not clear the kill switch: {e}")})),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GateDecision;

    #[tokio::test]
    async fn router_builds_with_state() {
        // Smoke test: the router assembles with a gated-off state. Full HTTP
        // integration is left to the platform's health checks.
        let state = ConnectorState::new(GateDecision::Disabled, "rithmic:MES");
        let _ = router(state, PositionsBook::new());
    }

    #[tokio::test]
    async fn health_is_ok() {
        let Json(v) = health().await;
        assert_eq!(v["status"], "ok");
    }

    #[tokio::test]
    async fn positions_handler_reports_book() {
        use crate::positions::PositionSnapshot;
        let book = PositionsBook::new();
        book.upsert(PositionSnapshot {
            symbol: "ESU6".into(),
            exchange: "CME".into(),
            product_code: "ES".into(),
            net_quantity: 1,
            direction: "long",
            avg_open_price: 5000.0,
            open_pnl: 10.0,
            day_pnl: 12.0,
            buy_qty: 1,
            sell_qty: 0,
            open_quantity: 1,
            account_id: "ACCT1".into(),
        });
        let state = HealthState {
            kill: KillSwitch::with_path(std::env::temp_dir().join("fks-test-nokill")),
            conn: ConnectorState::new(GateDecision::Ready, "rithmic:ES"),
            positions: book,
        };
        let Json(v) = positions_handler(State(state)).await;
        assert_eq!(v["count"], 1);
        assert_eq!(v["read_only"], true);
        assert_eq!(v["order_plant_open"], false);
        assert_eq!(v["positions"][0]["symbol"], "ESU6");
    }

    #[tokio::test]
    async fn status_includes_positions_tracked() {
        let state = HealthState {
            kill: KillSwitch::with_path(std::env::temp_dir().join("fks-test-nokill")),
            conn: ConnectorState::new(GateDecision::Ready, "rithmic:ES"),
            positions: PositionsBook::new(),
        };
        let Json(v) = status(State(state)).await;
        assert_eq!(v["positions_tracked"], 0);
        // Doctrine invariant still present on /status.
        assert_eq!(v["order_plant_open"], false);
    }
}
