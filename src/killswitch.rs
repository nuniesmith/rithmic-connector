//! Operator kill switch for the Rithmic session.
//!
//! # Why this exists
//!
//! Rithmic permits exactly ONE session per credential. With a single login, the
//! platform and the operator's own R|Trader Pro app cannot both be connected —
//! each revokes the other. Observed live on 2026-08-24: the connector logged
//! the operator out of their trading phone app every few seconds.
//!
//! Until a second Rithmic API user exists, the operator needs to hand the
//! session back and forth deliberately. This is that control: kill before
//! picking up the phone, resume afterwards.
//!
//! # Why a FILE, not just an in-memory flag
//!
//! The sentinel must survive a container restart. An in-memory flag would be
//! cleared by any restart — Docker restart policy, a host reboot, a redeploy —
//! and the connector would silently reconnect and steal the session back while
//! the operator was mid-trade on their phone. A kill switch that forgets is
//! worse than none, because it is trusted.
//!
//! Same reasoning as the funding bot's durable sentinel
//! (`fks-state/bots/crypto-futures/src/futures/kill.rs`): a killed process that
//! is respawned must come back KILLED, and only an explicit operator resume may
//! clear it.
//!
//! # Semantics
//!
//! Presence of the file = killed. Contents are informational only (a timestamp
//! and reason), never parsed for control flow — a truncated or garbled file
//! still reads as "killed", which is the safe direction.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

/// Default sentinel path. Lives under the state mount so it is durable across
/// container recreation.
pub const DEFAULT_KILL_FILE: &str = "/state/rithmic.kill";

/// Durable kill-switch sentinel.
#[derive(Clone, Debug)]
pub struct KillSwitch {
    path: PathBuf,
}

impl KillSwitch {
    /// Read the sentinel path from `RITHMIC_KILL_FILE`, falling back to
    /// [`DEFAULT_KILL_FILE`].
    pub fn from_env() -> Self {
        let path = std::env::var("RITHMIC_KILL_FILE")
            .ok()
            .filter(|p| !p.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_KILL_FILE.to_string());
        Self {
            path: PathBuf::from(path),
        }
    }

    /// Construct with an explicit sentinel path (tests, or an override).
    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Filesystem path of the sentinel (shown in API responses and logs).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// True when the operator has killed the session.
    ///
    /// An unreadable-but-present file still counts as killed: this must fail
    /// TOWARD halted, never toward "connect anyway".
    pub fn is_killed(&self) -> bool {
        self.path.exists()
    }

    /// Trip the switch. Idempotent — killing an already-killed session is fine
    /// and does not overwrite the original reason/timestamp, so the record of
    /// WHEN the operator first killed it survives repeated clicks.
    pub fn kill(&self, reason: &str) -> std::io::Result<()> {
        if self.is_killed() {
            info!(path = %self.path.display(), "kill switch already set — no change");
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = format!(
            "killed_at={}\nreason={}\n",
            chrono::Utc::now().to_rfc3339(),
            reason.replace('\n', " ")
        );
        std::fs::write(&self.path, body)?;
        warn!(path = %self.path.display(), reason, "KILL SWITCH SET — Rithmic session released");
        Ok(())
    }

    /// Clear the switch so the supervisor may reconnect. Idempotent.
    pub fn resume(&self) -> std::io::Result<()> {
        if !self.is_killed() {
            info!("kill switch already clear — no change");
            return Ok(());
        }
        std::fs::remove_file(&self.path)?;
        info!(path = %self.path.display(), "kill switch CLEARED — reconnecting");
        Ok(())
    }

    /// Informational contents, for `/status`. `None` when not killed.
    pub fn details(&self) -> Option<String> {
        std::fs::read_to_string(&self.path).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("fks-killswitch-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p.push("rithmic.kill");
        p
    }

    #[test]
    fn absent_file_is_not_killed() {
        let k = KillSwitch::with_path(tmp("absent"));
        assert!(!k.is_killed());
    }

    #[test]
    fn kill_then_resume_round_trips_and_creates_missing_dirs() {
        let k = KillSwitch::with_path(tmp("roundtrip"));
        assert!(!k.is_killed());
        k.kill("operator using phone").unwrap();
        assert!(k.is_killed(), "parent dir must be created, not error");
        assert!(k.details().unwrap().contains("operator using phone"));
        k.resume().unwrap();
        assert!(!k.is_killed());
    }

    /// Both operations are idempotent so a double-click in the UI cannot error
    /// or, worse, leave the switch in the opposite state.
    #[test]
    fn kill_and_resume_are_idempotent() {
        let k = KillSwitch::with_path(tmp("idem"));
        k.kill("first").unwrap();
        k.kill("second").unwrap();
        assert!(
            k.details().unwrap().contains("first"),
            "the ORIGINAL kill reason/time must survive repeated kills"
        );
        k.resume().unwrap();
        k.resume().unwrap();
        assert!(!k.is_killed());
    }
}
