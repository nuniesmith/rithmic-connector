// =============================================================================
// rithmic-connector — library root.
//
// Foundation / skeleton of the credential-gated, READ-ONLY Rithmic futures
// market-data connector. Design: docs/architecture/RITHMIC_INTEGRATION_SPIKE.md
// (roadmap P12). This crate is a sidecar: it speaks Rithmic's R|Protocol via
// the community `rithmic-rs` crate, subscribes read-only plants, and (later)
// writes to `candles_futures`.
//
// DOCTRINE: read-only by construction. The ORDER plant is never opened; there
// is no order-entry code path anywhere in this crate.
// =============================================================================

#![warn(unsafe_code)]
#![warn(missing_docs)]

//! Read-only Rithmic connector foundation. See the crate-level module docs and
//! `RITHMIC_INTEGRATION_SPIKE.md` for the full design and the honest list of
//! what is real versus stubbed in this PR.

/// Minimal trade → 1-minute candle bucketing (wires the read path to the sink).
pub mod aggregator;
/// Environment-driven configuration and the capability gate.
pub mod config;
/// The read-only connector lifecycle (Ticker plant only).
pub mod connector;
/// End-of-day rollover for the trailing-drawdown floor (TPT Rule 3).
pub mod eod;
/// Library error surface.
pub mod error;
/// `/health` + `/status` HTTP server (FKS bot contract, :9091).
pub mod health;
/// Historical bar retrieval over Rithmic's history plant — the read path for
/// questions about the past, which a live subscription cannot answer.
pub mod history;
pub mod killswitch;
/// Persistence — `candles_futures` writes over QuestDB ILP (+ the logging stub).
pub mod persistence;
/// Read-only positions book fed by the PnL plant (+ the `/positions` surface).
pub mod positions;
/// Shared, observable connector state for the status surface.
pub mod state;

pub use config::{GateDecision, RithmicConfig, RithmicEnvironment};
pub use connector::{VENUE, run};
pub use error::ConnectorError;
pub use persistence::{
    CANDLES_TABLE, CandleSink, FuturesCandle, PersistMetrics, QuestDbCandleSink, QuestDbConfig,
    StubCandleSink, format_ilp_line,
};
pub use positions::{AccountSummary, PositionSnapshot, PositionsBook, PositionsView};
pub use state::{ConnectorState, StatusSnapshot};
