//! End-of-day rollover for the trailing-drawdown floor.
//!
//! WHY THIS EXISTS. TakeProfit Trader's Rule 3 trails the highest **end-of-day**
//! balance — not intraday equity, and not open P&L. That makes the daily close
//! a load-bearing event: it is the only moment the floor is allowed to move up.
//!
//! Getting this wrong is dangerous in BOTH directions, which is why it is a
//! module rather than a line in a loop:
//!
//! * Advance on intraday equity (what the connector did until 2026-09-02) and
//!   the floor climbs ABOVE the real one. Headroom is understated — annoying,
//!   but safe.
//! * Never advance at all and the floor stays at its seed forever. After the
//!   first winning close the real floor has risen and ours has not, so headroom
//!   is OVERSTATED. That is the direction that loses an account.
//!
//! So the mark must both advance at the close and SURVIVE A RESTART: it is
//! account history, not process state. A connector that restarts on Thursday
//! and silently reverts to Monday's floor is reporting money that is not there.

use std::path::{Path, PathBuf};

use chrono::{Datelike, TimeZone, Utc, Weekday};
use chrono_tz::America::New_York;
use tracing::{info, warn};

use crate::positions::PositionsBook;

/// Where the mark is persisted. Same `/state` volume as the kill switch.
pub const DEFAULT_EOD_FILE: &str = "/state/rithmic.eod";

/// CME settlement, ET. The futures trading day ends here, so this is the
/// balance the firm's rule reads.
const EOD_HOUR: u32 = 17;
const EOD_MINUTE: u32 = 0;

/// The only drawdown rule this connector implements.
///
/// Persisted with the mark so a state written under a different rule is
/// REJECTED rather than reinterpreted. TPT PRO accounts trail intraday on
/// realised *and* unrealised gains, which is a different calculation entirely —
/// silently reading a PRO mark as a Test mark would produce a confident,
/// wrong floor.
pub const RULE_MODE_TEST_EOD: &str = "test_eod";

/// What was persisted at the last close.
///
/// WHY THIS IS A RECORD AND NOT A NUMBER. A bare float carries no way to tell
/// whose money it describes. It survives an account reset, a switch to a second
/// account, and a change of starting balance, and in every one of those cases
/// it is silently applied to an account that never earned it. The fields here
/// exist so the mark can be REFUSED, which is the only safe response to a mark
/// that cannot be shown to belong to this account.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EodState {
    /// Schema version. An unknown version is rejected, not guessed at.
    pub version: u8,
    /// The account this mark belongs to.
    pub account_id: String,
    /// The drawdown rule it was computed under.
    pub rule_mode: String,
    /// The account's original starting balance at the time of writing. A change
    /// here means a different account or a reset, so the mark cannot carry over.
    pub starting_balance: f64,
    /// The end-of-day high-water balance itself.
    pub high_water_eod: f64,
    /// Trade date (ET) of the close that produced it, `YYYY-MM-DD`.
    pub trade_date: String,
    /// When it was written, RFC3339 UTC.
    pub observed_at: String,
    /// Trade date (ET) of the most recent close this connector actually
    /// OBSERVED, whether or not the mark moved.
    ///
    /// Separate from `trade_date` — which records the last ADVANCE — because
    /// the two answer different questions. A mark may legitimately sit
    /// unchanged for many sessions, so an old `trade_date` is not evidence of
    /// a gap. An old `last_close_verified` is: it means closes happened that
    /// we did not see, and any one of them could have settled higher and
    /// raised the real floor above ours. That is the direction that
    /// OVERSTATES headroom.
    pub last_close_verified: String,
}

/// What `load` concluded. Three outcomes, deliberately not two.
#[derive(Debug, Clone, PartialEq)]
pub enum Loaded {
    /// The mark verifiably belongs to this account.
    Restored(EodState),
    /// No file. Normal on a genuinely fresh account.
    Absent,
    /// A mark exists but could not be shown to belong here.
    ///
    /// This is NOT the same as `Absent`, and the difference is the whole point.
    /// Absent means "no history yet", so seeding the floor at the starting
    /// balance is correct. Rejected means "there is history and we cannot read
    /// it" — seeding from the starting balance would then claim a floor LOWER
    /// than the real one, which OVERSTATES headroom. That is the direction that
    /// loses an account, so the caller must decline to claim a floor at all.
    Rejected {
        /// Why it could not be trusted, for the operator to act on.
        reason: String,
    },
}

/// Read the persisted mark and prove it belongs to this account.
///
/// Every mismatch is a rejection rather than a repair. There is no sensible way
/// to adapt one account's high-water balance to another's, and a plausible
/// number is exactly what makes a wrong floor dangerous.
#[must_use]
pub fn load(path: &Path, account_id: &str, starting_balance: f64) -> Loaded {
    // R2: ONLY a genuinely missing file is `Absent`. Every other read error —
    // a directory at the path, invalid UTF-8, a permission denial, an I/O
    // fault — used to take this branch too, and `Absent` authorises seeding a
    // fresh floor. "I could not read the file" is not evidence of a new
    // account; it is evidence of nothing, and the two must not share an answer.
    //
    // A missing file is itself weaker evidence than it looks (a lost volume or
    // a wrong path also produce it), which is why the caller treats even
    // `Absent` as authorising a seed only when a starting balance is
    // configured.
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Loaded::Absent,
        Err(e) => {
            return Loaded::Rejected {
                reason: format!(
                    "state file unreadable ({e}) — refusing to treat this as a fresh account"
                ),
            };
        }
    };
    let state: EodState = match serde_json::from_str(raw.trim()) {
        Ok(s) => s,
        Err(e) => {
            // Includes the pre-2026-09-04 format, which was a bare number.
            // A legacy mark cannot be attributed to an account, so it is
            // refused rather than adopted.
            return Loaded::Rejected {
                reason: format!("unreadable ({e})"),
            };
        }
    };
    if state.version != 2 {
        return Loaded::Rejected {
            reason: format!(
                "unknown schema version {} (this connector writes 2; a v1 record \
                 predates last_close_verified and cannot prove continuity)",
                state.version
            ),
        };
    }
    if state.rule_mode != RULE_MODE_TEST_EOD {
        return Loaded::Rejected {
            reason: format!(
                "written under rule mode '{}', this connector implements '{RULE_MODE_TEST_EOD}'",
                state.rule_mode
            ),
        };
    }
    if state.account_id != account_id {
        return Loaded::Rejected {
            reason: format!(
                "belongs to account '{}', configured account is '{account_id}'",
                state.account_id
            ),
        };
    }
    if (state.starting_balance - starting_balance).abs() > 0.01 {
        return Loaded::Rejected {
            reason: format!(
                "written at starting balance {}, configured is {starting_balance} — \
                 an account reset or a different account",
                state.starting_balance
            ),
        };
    }
    // R3: the dates are part of the record's provenance, so a record whose
    // dates are not dates has no provenance. `trade_date = "not-a-date"` was
    // previously accepted and restored.
    if chrono::NaiveDate::parse_from_str(&state.trade_date, "%Y-%m-%d").is_err() {
        return Loaded::Rejected {
            reason: format!("trade_date '{}' is not a date", state.trade_date),
        };
    }
    if chrono::DateTime::parse_from_rfc3339(&state.observed_at).is_err() {
        return Loaded::Rejected {
            reason: format!(
                "observed_at '{}' is not an RFC3339 timestamp",
                state.observed_at
            ),
        };
    }
    // CONTINUITY. A mark is only usable if we saw every close since it was
    // taken. Sol's R3: record the last verified close separately, and refuse
    // when continuity cannot be established.
    //
    // Deliberately NOT an age cutoff on the mark itself — a high-water balance
    // may legitimately sit unchanged for weeks, and rejecting it for being old
    // would refuse perfectly good state. What matters is whether any close went
    // UNOBSERVED, because an unobserved close may have settled higher and moved
    // the real floor above ours.
    match chrono::NaiveDate::parse_from_str(&state.last_close_verified, "%Y-%m-%d") {
        Err(_) => {
            return Loaded::Rejected {
                reason: format!(
                    "last_close_verified '{}' is not a date",
                    state.last_close_verified
                ),
            };
        }
        Ok(verified) => {
            let expected = most_recent_close_date(Utc::now());
            if verified < expected {
                return Loaded::Rejected {
                    reason: format!(
                        "continuity gap: last observed close was {verified}, but {expected} \
                         has since settled. Closes we did not see may have raised the real \
                         floor above this mark, so it cannot be trusted"
                    ),
                };
            }
        }
    }

    if !state.high_water_eod.is_finite() || state.high_water_eod <= 0.0 {
        return Loaded::Rejected {
            reason: format!("implausible mark {}", state.high_water_eod),
        };
    }
    Loaded::Restored(state)
}

/// Persist the mark as a full record, temp-file-and-rename so a crash mid-write
/// cannot leave a truncated document that `load` would then have to interpret.
pub fn store(path: &Path, state: &EodState) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let body = serde_json::to_string_pretty(state)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let tmp = path.with_extension("eod.tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)
}

/// Build the record for a mark taken now.
#[must_use]
pub fn state_for(
    account_id: &str,
    starting_balance: f64,
    high_water_eod: f64,
    last_close_verified: String,
) -> EodState {
    let now = Utc::now();
    EodState {
        version: 2,
        account_id: account_id.to_string(),
        rule_mode: RULE_MODE_TEST_EOD.to_string(),
        starting_balance,
        high_water_eod,
        trade_date: now
            .with_timezone(&New_York)
            .date_naive()
            .format("%Y-%m-%d")
            .to_string(),
        observed_at: now.to_rfc3339(),
        last_close_verified,
    }
}

/// The trade date (ET) of the most recent weekday close that has already passed.
///
/// Weekends are skipped for the same reason `secs_until_next_close` skips them:
/// there is no settlement to account for.
#[must_use]
pub fn most_recent_close_date(now: chrono::DateTime<Utc>) -> chrono::NaiveDate {
    let et = now.with_timezone(&New_York);
    let mut day = et.date_naive();
    // Today only counts once its close has actually passed.
    // Compare whole times rather than hour/minute separately: with EOD_MINUTE
    // at 0 the minute half is always true, and clippy is right that such a
    // comparison hides its own intent.
    let today_close_passed = chrono::NaiveTime::from_hms_opt(EOD_HOUR, EOD_MINUTE, 0)
        .is_some_and(|close| et.time() >= close);
    if !today_close_passed {
        day = day.pred_opt().unwrap_or(day);
    }
    while matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
        day = day.pred_opt().unwrap_or(day);
    }
    day
}

/// Seconds from `now` until the next weekday CME settlement in ET.
///
/// Weekends are skipped: there is no settlement to roll, and rolling a stale
/// Friday balance twice would be harmless but misleading in the logs. Holidays
/// are deliberately NOT modelled — a holiday roll folds in an unchanged balance,
/// which is a no-op by construction (`roll_end_of_day` only advances on a
/// higher number).
#[must_use]
pub fn secs_until_next_close(now: chrono::DateTime<Utc>) -> u64 {
    let et = now.with_timezone(&New_York);
    let mut day = et.date_naive();
    loop {
        if !matches!(day.weekday(), Weekday::Sat | Weekday::Sun)
            && let Some(naive) = day.and_hms_opt(EOD_HOUR, EOD_MINUTE, 0)
            // A DST transition can make a wall-clock time ambiguous or
            // non-existent; take the earliest valid instant rather than
            // unwrapping into a panic once a year.
            && let Some(target) = New_York.from_local_datetime(&naive).earliest()
        {
            let delta = target.with_timezone(&Utc) - now;
            if delta.num_seconds() > 0 {
                return u64::try_from(delta.num_seconds()).unwrap_or(0);
            }
        }
        day = day.succ_opt().unwrap_or(day);
    }
}

/// Run the rollover forever: sleep until the next close, fold the realised
/// balance into the mark, persist it.
pub async fn run(book: PositionsBook, path: PathBuf, account_id: String, starting_balance: f64) {
    loop {
        let wait = secs_until_next_close(Utc::now());
        tokio::time::sleep(std::time::Duration::from_secs(wait)).await;

        let closed = most_recent_close_date(Utc::now())
            .format("%Y-%m-%d")
            .to_string();
        match book.roll_end_of_day() {
            // Persist on EVERY observed close, advancing or not. Writing only on
            // an advance meant the file recorded the last time the mark MOVED and
            // never the last time a close was SEEN — so a connector that ran for
            // a fortnight of flat closes looked, on restart, exactly like one
            // that had been switched off for a fortnight.
            crate::positions::RollOutcome::Verified { mark, advanced } => {
                let state = state_for(&account_id, starting_balance, mark, closed.clone());
                if let Err(e) = store(&path, &state) {
                    // Not fatal, but it means a restart loses this close and
                    // reverts to a LOWER floor — the overstating direction.
                    warn!(error = %e, path = %path.display(),
                          "close {closed} observed (mark {mark}) but could NOT be persisted");
                } else if advanced {
                    info!("end-of-day rollover {closed}: high-water balance now {mark}");
                } else {
                    info!("end-of-day rollover {closed}: observed, no advance (mark stays {mark})");
                }
            }
            // The close happened and we could not see it. Nothing is written,
            // so `last_close_verified` stays behind and the next load refuses —
            // which is the point.
            crate::positions::RollOutcome::NotObserved => {
                warn!(
                    close = %closed,
                    "END-OF-DAY CLOSE NOT OBSERVED — no account balance available. \
                     The floor may have moved without us; continuity is broken until \
                     a close is seen again."
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> chrono::DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("eodtest-{name}-{}.json", std::process::id()));
        p
    }

    fn sample() -> EodState {
        state_for(
            "TEST0000001",
            150_000.0,
            152_300.0,
            most_recent_close_date(Utc::now())
                .format("%Y-%m-%d")
                .to_string(),
        )
    }

    #[test]
    fn a_mark_from_this_account_round_trips() {
        let p = tmp("roundtrip");
        store(&p, &sample()).unwrap();
        match load(&p, "TEST0000001", 150_000.0) {
            Loaded::Restored(s) => {
                assert!((s.high_water_eod - 152_300.0).abs() < f64::EPSILON);
                assert_eq!(s.rule_mode, RULE_MODE_TEST_EOD);
            }
            other => panic!("expected Restored, got {other:?}"),
        }
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn a_mark_from_a_different_account_is_refused() {
        // The failure this record exists to prevent: a bare number survives an
        // account switch and is applied to money that never earned it.
        let p = tmp("otheracct");
        store(&p, &sample()).unwrap();
        let got = load(&p, "TPT9999999", 150_000.0);
        assert!(matches!(got, Loaded::Rejected { .. }), "{got:?}");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn a_reset_account_is_refused_via_the_starting_balance() {
        // A reset keeps the account id but restores the starting balance. The
        // old mark is then not merely stale, it is unreachable under the rule.
        let p = tmp("reset");
        store(&p, &sample()).unwrap();
        let got = load(&p, "TEST0000001", 100_000.0);
        assert!(matches!(got, Loaded::Rejected { .. }), "{got:?}");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn the_legacy_bare_number_format_is_refused_not_adopted() {
        // Before 2026-09-04 the file held just "152300". It cannot be attributed
        // to an account, so it must not become a floor.
        let p = tmp("legacy");
        std::fs::write(&p, "152300").unwrap();
        let got = load(&p, "TEST0000001", 150_000.0);
        assert!(matches!(got, Loaded::Rejected { .. }), "{got:?}");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn a_mark_from_another_rule_mode_is_refused() {
        // PRO accounts trail intraday on unrealised gains — a different
        // calculation. Reading one as a Test mark yields a confident wrong floor.
        let p = tmp("rulemode");
        let mut s = sample();
        s.rule_mode = "pro_intraday".into();
        store(&p, &s).unwrap();
        let got = load(&p, "TEST0000001", 150_000.0);
        assert!(matches!(got, Loaded::Rejected { .. }), "{got:?}");
        let _ = std::fs::remove_file(&p);
    }

    /// R2: a file we cannot READ is not evidence of a new account.
    #[test]
    fn an_unreadable_path_is_refused_not_treated_as_a_fresh_account() {
        // A DIRECTORY where the state file should be. `read_to_string` fails
        // with something other than NotFound, which previously fell into the
        // same `Absent` branch as "no file" — and Absent authorises seeding a
        // fresh floor, which UNDERSTATES the floor and overstates headroom.
        let dir = std::env::temp_dir().join(format!("eod-isdir-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let got = load(&dir, "TEST0000001", 150_000.0);
        assert!(
            matches!(got, Loaded::Rejected { .. }),
            "a directory at the state path must be REFUSED, got {got:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_utf8_is_refused_not_treated_as_a_fresh_account() {
        let p = tmp("badutf8");
        std::fs::write(&p, [0xff, 0xfe, 0x00, 0x01]).unwrap();
        assert!(matches!(
            load(&p, "TEST0000001", 150_000.0),
            Loaded::Rejected { .. }
        ));
        let _ = std::fs::remove_file(&p);
    }

    /// R3: the dates are the record's provenance; a record whose dates are not
    /// dates has none.
    #[test]
    fn nonsense_dates_are_refused() {
        let p = tmp("baddates");
        let mut s = sample();
        s.trade_date = "not-a-date".into();
        store(&p, &s).unwrap();
        assert!(matches!(
            load(&p, "TEST0000001", 150_000.0),
            Loaded::Rejected { .. }
        ));

        let mut s2 = sample();
        s2.observed_at = "not-a-timestamp".into();
        store(&p, &s2).unwrap();
        assert!(matches!(
            load(&p, "TEST0000001", 150_000.0),
            Loaded::Rejected { .. }
        ));
        let _ = std::fs::remove_file(&p);
    }

    /// R3: a close we did not see may have raised the real floor above our mark.
    #[test]
    fn a_missed_close_breaks_continuity_and_is_refused() {
        let p = tmp("continuity");
        let mut s = sample();
        s.last_close_verified = "2020-01-02".into(); // many closes ago
        store(&p, &s).unwrap();
        let got = load(&p, "TEST0000001", 150_000.0);
        match got {
            Loaded::Rejected { ref reason } => {
                assert!(reason.contains("continuity"), "{reason}")
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
        let _ = std::fs::remove_file(&p);
    }

    /// ...but an OLD MARK with current continuity is fine. A high-water balance
    /// may legitimately sit unchanged for weeks; that is not a defect.
    #[test]
    fn an_old_mark_with_unbroken_continuity_is_accepted() {
        let p = tmp("oldmark");
        let mut s = sample();
        s.trade_date = "2026-07-01".into(); // the mark last MOVED in July
        // ...but every close since has been observed.
        s.last_close_verified = most_recent_close_date(Utc::now())
            .format("%Y-%m-%d")
            .to_string();
        store(&p, &s).unwrap();
        assert!(
            matches!(load(&p, "TEST0000001", 150_000.0), Loaded::Restored(_)),
            "an unchanged mark is not a stale mark"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn a_missing_file_is_absent_not_rejected() {
        // The distinction the caller branches on: Absent means "no history, so
        // seeding at the starting balance is correct"; Rejected means "history
        // exists and cannot be read, so claim no floor at all".
        let p = tmp("missing");
        let _ = std::fs::remove_file(&p);
        assert_eq!(load(&p, "TEST0000001", 150_000.0), Loaded::Absent);
    }

    #[test]
    fn waits_until_the_same_days_close_when_it_is_still_ahead() {
        // 2026-09-02 is a Wednesday. 12:00Z = 08:00 ET, close is 17:00 ET.
        let secs = secs_until_next_close(utc("2026-09-02T12:00:00Z"));
        assert_eq!(secs, 9 * 3600, "08:00 ET -> 17:00 ET is nine hours");
    }

    #[test]
    fn rolls_to_the_next_weekday_once_todays_close_has_passed() {
        // 22:00Z Wednesday = 18:00 ET, an hour after the close.
        let secs = secs_until_next_close(utc("2026-09-02T22:00:00Z"));
        assert_eq!(secs, 23 * 3600, "next stop is Thursday 17:00 ET");
    }

    #[test]
    fn skips_the_weekend() {
        // Saturday 2026-09-05 12:00Z — the next close is Monday, not tomorrow.
        let secs = secs_until_next_close(utc("2026-09-05T12:00:00Z"));
        let days = secs / 86_400;
        assert!((2..=3).contains(&days), "expected ~2 days, got {days}s");
    }

    #[test]
    fn a_malformed_mark_is_refused_rather_than_trusted() {
        let dir = std::env::temp_dir().join("fks_eod_test_malformed");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("rithmic.eod");

        std::fs::write(&p, "not-a-number").unwrap();
        assert!(matches!(
            load(&p, "TEST0000001", 150_000.0),
            Loaded::Rejected { .. }
        ));

        // A zero or negative mark is equally untrustworthy: it would put the
        // floor below zero and report imaginary headroom. Note this is now
        // REJECTED, not ignored — under the old bare-number format an
        // unreadable file silently became "no history", which is the one
        // interpretation that lets a too-low floor through.
        let mut bad = sample();
        bad.high_water_eod = 0.0;
        store(&p, &bad).unwrap();
        assert!(matches!(
            load(&p, "TEST0000001", 150_000.0),
            Loaded::Rejected { .. }
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_stored_mark_round_trips_through_the_file() {
        let dir = std::env::temp_dir().join("fks_eod_test_roundtrip2");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("rithmic.eod");
        store(
            &p,
            &state_for(
                "TEST0000001",
                150_000.0,
                154_675.0,
                most_recent_close_date(Utc::now())
                    .format("%Y-%m-%d")
                    .to_string(),
            ),
        )
        .unwrap();
        match load(&p, "TEST0000001", 150_000.0) {
            Loaded::Restored(s) => assert!((s.high_water_eod - 154_675.0).abs() < f64::EPSILON),
            other => panic!("expected Restored, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
