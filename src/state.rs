// =============================================================================
// state.rs — shared, observable connector state.
//
// A cheap snapshot the /status endpoint (and later Prometheus) reads. Atomics
// so the connector task can update it without locks on the hot path.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::config::GateDecision;
use crate::persistence::PersistMetrics;

/// Live connector state, shared between the connector task and the HTTP server.
#[derive(Debug)]
pub struct ConnectorState {
    /// The gate decision computed at startup (drives the whole lifecycle).
    gate: GateDecision,
    /// The instrument this connector is configured to watch (e.g. `rithmic:MES`).
    symbol: String,
    /// Whether an authenticated Rithmic session is currently open.
    connected: AtomicBool,
    /// Count of market-data updates received since start.
    messages_received: AtomicU64,
    /// Count of candles handed to the persistence sink since start.
    candles_written: AtomicU64,
    /// How many instruments are subscribed on the current session. Surfaced
    /// because ONE session now carries several symbols (Rithmic allows only one
    /// concurrent session per credential), so "connected" alone no longer tells
    /// you whether every configured instrument is actually being received.
    subscribed_instruments: AtomicU64,
    /// Whether the operator kill switch is currently holding the session off.
    /// Distinct from `connected: false`, which could equally mean a network
    /// problem — an operator looking at /status needs to tell "I turned it off"
    /// apart from "it broke".
    killed: AtomicBool,
    /// Persistence counters (candles durable / write errors). Shared with the
    /// QuestDB sink so `/status` reflects what actually reached QuestDB.
    persist: Arc<PersistMetrics>,
    /// Whether the PnL/positions plant currently has a live subscription.
    ///
    /// Tracked SEPARATELY from `connected` because the dangerous state is the
    /// asymmetric one. On 2026-08-31 the ticker plant reconnected and market
    /// data flowed perfectly all day while the PnL reader stayed dead; every
    /// dashboard was green, and the operator traded a session with no position,
    /// no day P&L and no trailing-drawdown tracking. A single "connected" flag
    /// cannot express that, and it is precisely the state worth paging on.
    pnl_connected: AtomicBool,
}

impl ConnectorState {
    /// Construct from the startup gate decision and configured symbol.
    pub fn new(gate: GateDecision, symbol: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            gate,
            symbol: symbol.into(),
            connected: AtomicBool::new(false),
            messages_received: AtomicU64::new(0),
            candles_written: AtomicU64::new(0),
            subscribed_instruments: AtomicU64::new(0),
            killed: AtomicBool::new(false),
            persist: Arc::new(PersistMetrics::default()),
            pnl_connected: AtomicBool::new(false),
        })
    }

    /// Mark the PnL/positions subscription up or down.
    pub fn set_pnl_connected(&self, up: bool) {
        self.pnl_connected.store(up, Ordering::Relaxed);
    }

    /// Whether the PnL subscription is up.
    pub fn pnl_connected(&self) -> bool {
        self.pnl_connected.load(Ordering::Relaxed)
    }

    /// True when market data is flowing but account state is not — the exact
    /// shape of the 2026-08-31 failure, and the one worth alerting on.
    pub fn account_state_blind(&self) -> bool {
        self.connected.load(Ordering::Relaxed) && !self.pnl_connected.load(Ordering::Relaxed)
    }

    /// The shared persistence counters — hand this to the [`crate::persistence::QuestDbCandleSink`]
    /// so its successes/errors surface on `/status`.
    pub fn persist_metrics(&self) -> Arc<PersistMetrics> {
        self.persist.clone()
    }

    /// Mark the Rithmic session as up/down.
    pub fn set_connected(&self, up: bool) {
        self.connected.store(up, Ordering::Relaxed);
    }

    /// Increment the received-message counter.
    pub fn inc_messages(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record how many instruments were subscribed on this session.
    pub fn set_subscribed(&self, n: usize) {
        self.subscribed_instruments
            .store(n as u64, Ordering::Relaxed);
    }

    /// Record whether the operator kill switch is holding the session off.
    pub fn set_killed(&self, killed: bool) {
        self.killed.store(killed, Ordering::Relaxed);
    }

    /// Increment the candles-written counter.
    pub fn inc_candles(&self) {
        self.candles_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Gate decision recorded at startup.
    pub fn gate(&self) -> &GateDecision {
        &self.gate
    }

    /// Render an observable snapshot for the `/status` endpoint.
    pub fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            gate: self.gate.clone(),
            gate_reason: self.gate.reason(),
            symbol: self.symbol.clone(),
            connected: self.connected.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            candles_written: self.candles_written.load(Ordering::Relaxed),
            subscribed_instruments: self.subscribed_instruments.load(Ordering::Relaxed),
            killed: self.killed.load(Ordering::Relaxed),
            candles_persisted: self.persist.candles_persisted(),
            persist_errors: self.persist.write_errors(),
            read_only: true,
            order_plant_open: false,
        }
    }
}

/// JSON-serialisable status payload. `read_only`/`order_plant_open` are constant
/// invariants surfaced deliberately: the order plant is never opened.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StatusSnapshot {
    /// The startup gate decision.
    pub gate: GateDecision,
    /// Human-readable gate reason.
    pub gate_reason: String,
    /// Configured instrument symbol.
    pub symbol: String,
    /// Whether an authenticated session is currently open.
    pub connected: bool,
    /// Market-data updates received since start.
    pub messages_received: u64,
    /// Candles handed to the persistence sink since start.
    pub candles_written: u64,
    /// Instruments subscribed on the current session.
    pub subscribed_instruments: u64,
    /// True when the operator kill switch is holding the session off — so a
    /// disconnected connector is distinguishable from a deliberately-halted one.
    pub killed: bool,
    /// Candles successfully written to QuestDB (`candles_futures`) since start.
    pub candles_persisted: u64,
    /// Persistence errors (ILP connect/write failures, dropped lines) since start.
    pub persist_errors: u64,
    /// Always true — this connector is read-only by construction.
    pub read_only: bool,
    /// Always false — the order plant is never opened (doctrine invariant).
    pub order_plant_open: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_updates_and_invariants() {
        let state = ConnectorState::new(GateDecision::Ready, "rithmic:MES");
        state.set_connected(true);
        state.inc_messages();
        state.inc_messages();
        state.inc_candles();

        let snap = state.snapshot();
        assert!(snap.connected);
        assert_eq!(snap.messages_received, 2);
        assert_eq!(snap.candles_written, 1);
        assert_eq!(snap.symbol, "rithmic:MES");
        // Doctrine invariants must always hold.
        assert!(snap.read_only);
        assert!(!snap.order_plant_open);
    }

    #[test]
    fn blindness_is_the_asymmetric_state_not_either_plant_alone() {
        // 2026-08-31: the ticker reconnected and candles flowed all day while
        // the PnL reader stayed dead. Neither flag alone expresses that — it is
        // the COMBINATION that means "every dashboard is green and the account
        // is untracked", so that is what gets alerted on.
        let state = ConnectorState::new(GateDecision::Ready, "rithmic:GC");

        // Nothing up: not blind, just off. Paging here would cry wolf every
        // night after the close.
        assert!(!state.account_state_blind());

        // The dangerous shape.
        state.set_connected(true);
        assert!(state.account_state_blind());

        // Both up: healthy.
        state.set_pnl_connected(true);
        assert!(!state.account_state_blind());

        // PnL dies while market data continues — back to blind.
        state.set_pnl_connected(false);
        assert!(state.account_state_blind());
    }
}
