// =============================================================================
// error.rs — library error surface (thiserror, per FKS convention).
// =============================================================================

/// Errors surfaced by the connector library.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// The connector was asked to connect but the capability gate is not `Ready`.
    /// Carries the human-readable gate reason.
    #[error("capability gate not satisfied: {0}")]
    Gated(String),

    /// The `live-connect` feature is not compiled in, so no client exists.
    #[error("built without the `live-connect` feature — no Rithmic client available")]
    NoClient,

    /// A failure from the underlying Rithmic client / transport.
    #[error("rithmic client error: {0}")]
    Client(String),
}
