// =============================================================================
// positions.rs — read-only positions book fed by the Rithmic PnL plant.
//
// DOCTRINE (non-negotiable): READ-ONLY. This module opens ONLY the PnL plant
// (`RithmicPnlPlant`) to *observe* positions and account P&L. It NEVER imports
// or opens `RithmicOrderPlant`; there is no order-entry path here. See the
// crate CLAUDE.md "no autonomous execution" and connector.rs doctrine header.
//
// Two halves:
//   * `PositionSnapshot` / `PositionsBook` — feature-independent, serde-friendly
//     state the /positions endpoint reads. Compiles with or without live-connect.
//   * `run_pnl` + `map_*` — the live PnL subscription + the pure mapping from the
//     vendor crate's protobuf updates into `PositionSnapshot`. Behind
//     `live-connect` because they reference `rithmic_rs` types. The mapping is
//     pure and unit-tested (default builds include the feature).
// =============================================================================

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// A single open futures position, as last reported by Rithmic. Serialised by
/// the `/positions` endpoint. All monetary/qty fields are best-effort: Rithmic
/// sends many of them as optional strings, so absent values become 0.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct PositionSnapshot {
    /// Instrument symbol (e.g. `ESU6`). Namespaced `rithmic:` upstream in the UI.
    pub symbol: String,
    /// Exchange the instrument trades on (e.g. `CME`).
    pub exchange: String,
    /// Product code (e.g. `ES`).
    pub product_code: String,
    /// Signed net position quantity (>0 long, <0 short).
    pub net_quantity: i32,
    /// `long` / `short` / `flat`, derived from `net_quantity`.
    pub direction: &'static str,
    /// Average open fill price for the position.
    pub avg_open_price: f64,
    /// Open-position P&L (mark-to-market on the open quantity).
    pub open_pnl: f64,
    /// Day P&L (realised + unrealised for the session).
    pub day_pnl: f64,
    /// Filled buy quantity for the session.
    pub buy_qty: i32,
    /// Filled sell quantity for the session.
    pub sell_qty: i32,
    /// Open-position quantity (absolute).
    pub open_quantity: i32,
    /// Owning account id.
    pub account_id: String,
}

/// Account-level P&L summary (optional; populated from AccountPnLPositionUpdate).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct AccountSummary {
    /// Account id this summary belongs to.
    pub account_id: String,
    /// Net position quantity across the account.
    pub net_quantity: i32,
    /// Open-position P&L across the account.
    pub open_pnl: f64,
    /// Day P&L across the account.
    pub day_pnl: f64,
    /// Account balance, if reported.
    pub account_balance: f64,
    /// Settled cash as reported. Kept because `account_balance` and
    /// `cash_on_hand` diverged on 2026-08-31 and neither alone reconciled with
    /// `day_pnl` — surfacing both lets a human see which one moved.
    pub cash_on_hand: f64,
    /// The minimum-account-balance the BROKER reports.
    ///
    /// Kept for COMPARISON, not because it is wrong. On 2026-08-31 this sat at
    /// 145,500 all session while the account peaked intraday at 154,675, and we
    /// concluded the broker was broken. It was not: TPT's floor trails the
    /// highest END-OF-DAY balance, so an intraday peak correctly does not move
    /// it. We were the ones with the wrong model (corrected 2026-09-02).
    ///
    /// [`RiskState::floor_disagreement`] now exists to watch for the two
    /// drifting apart, which should be ~0 and is a signal about US as much as
    /// about the broker.
    pub reported_min_balance: f64,
}

/// The trailing-drawdown state, computed HERE rather than read from the broker.
///
/// TPT Rule 3 has two halves that use DIFFERENT inputs, and conflating them is
/// what made this module wrong until 2026-09-02:
///
///   TRAILS   on the highest END-OF-DAY balance — realised only, no open P&L —
///            and STOPS once the floor reaches the original starting balance.
///   BREACHES at any time, on realised OR unrealised loss.
///
/// It is the one rule that can end an account outright, so it is computed here
/// rather than inferred — but computed to the ACTUAL rule. The earlier version
/// trailed intraday equity with no cap and produced floors the firm can never
/// reach (150,175 on a 150,000 account).
///
/// The principle that motivated the original still holds, and now cuts the
/// other way: a risk number that is wrong is strictly worse than one that is
/// missing, because a missing number is visibly missing and a wrong one gets
/// acted upon.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct RiskState {
    /// Equity as we compute it: balance plus open P&L. The floor does not TRAIL
    /// on unrealised money, but it can be BREACHED by it, so this is the figure
    /// headroom is measured from.
    pub equity: f64,
    /// Highest END-OF-DAY balance observed, realised only. This is what the
    /// rule trails — NOT intraday equity, and NOT open P&L.
    pub high_water_eod: f64,
    /// Our operative floor: `min(high_water_eod - trailing_dd,
    /// starting_balance)`. The cap is not decoration — without it the floor
    /// climbs past a level the firm's rule can never reach.
    pub computed_min_balance: f64,
    /// `equity - computed_min_balance`. Negative means the rule is breached.
    pub headroom: f64,
    /// The configured trailing drawdown this floor is derived from.
    pub trailing_dd: f64,
    /// True once the floor has reached the starting balance and stopped
    /// trailing. From here the floor is fixed for the life of the account.
    pub floor_capped: bool,
    /// Signed difference between what the broker reports and what we compute.
    /// Should be ~0. A persistent non-zero value means one of us has the rule
    /// wrong — and on 2026-08-31 that was us.
    pub floor_disagreement: f64,
    /// True once the starting balance is known; until then the floor is not
    /// meaningful and must not be rendered as authoritative.
    pub seeded: bool,
}

/// The observable positions snapshot returned by `GET /positions`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PositionsView {
    /// Owning account id (empty until a position/account update arrives).
    pub account: String,
    /// Number of currently-open positions.
    pub count: usize,
    /// The open positions, ordered by symbol.
    pub positions: Vec<PositionSnapshot>,
    /// Account-level P&L summary, if one has been received.
    pub account_summary: Option<AccountSummary>,
    /// Trailing-drawdown state computed from our own high-water mark. `None`
    /// until the first account update seeds it.
    pub risk: Option<RiskState>,
    /// Always true — this book is fed by the read-only PnL plant.
    pub read_only: bool,
    /// Always false — the order plant is never opened (doctrine invariant).
    pub order_plant_open: bool,
    /// Seconds since the last position/account update; `None` before the first.
    ///
    /// Published so a CONSUMER can tell a live snapshot from a frozen one. The
    /// briefing sizes trades off `risk` above, and a PnL reader that has
    /// silently died looks identical to a quiet market without this — which is
    /// exactly the pair that went unnoticed for a full session on 2026-08-31.
    pub age_secs: Option<u64>,
}

/// Shared, thread-safe book of open positions. Cheap to clone (Arc inside): the
/// PnL task holds one clone and mutates it; the HTTP server holds another and
/// reads it. Keyed by symbol; a position going flat is removed.
#[derive(Debug, Clone, Default)]
pub struct PositionsBook {
    inner: Arc<Mutex<BookInner>>,
}

#[derive(Debug, Default)]
struct BookInner {
    positions: BTreeMap<String, PositionSnapshot>,
    account: Option<AccountSummary>,
    /// Highest END-OF-DAY balance, realised only. Monotonic — never lowered by
    /// a losing day, which is the point of a *trailing* drawdown.
    ///
    /// It advances ONLY via `roll_end_of_day`. It deliberately does NOT move on
    /// intraday equity or open P&L: TPT's Rule 3 trails the end-of-day balance,
    /// so an intraday peak that is given back before the close never raises the
    /// floor. Advancing here on every update (as this did until 2026-09-02)
    /// produces a floor strictly above the real one.
    high_water_eod: Option<f64>,
    /// The account's original starting balance. The floor CAPS here: once the
    /// minimum balance reaches it, the rule stops trailing entirely.
    starting_balance: Option<f64>,
    trailing_dd: f64,
    /// When the last position/account update landed. Tracked HERE rather than
    /// in the connector state because the book is the thing every update flows
    /// through — a timestamp kept anywhere else has to be threaded through the
    /// PnL task and will eventually be forgotten at one call site, which is
    /// exactly what happened on the first attempt at this.
    last_update: Option<std::time::Instant>,
}

/// What happened at an end-of-day rollover.
///
/// `Option<f64>` used to carry this, and it conflated two opposite facts:
/// "the close was observed and the mark did not move" with "no account
/// balance was available, so the close was not observed at all". Only the
/// first means the day is accounted for. Continuity — knowing that no close
/// was missed — depends entirely on telling them apart.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RollOutcome {
    /// A balance was available, so this close IS accounted for.
    Verified {
        /// The high-water mark after folding this close in.
        mark: f64,
        /// Whether the mark actually moved up.
        advanced: bool,
    },
    /// No balance was available. The close was NOT accounted for, and the
    /// account may have settled higher without us seeing it.
    NotObserved,
}

impl PositionsBook {
    /// A fresh, empty book with no trailing-drawdown rule configured.
    pub fn new() -> Self {
        Self::default()
    }

    /// A book that computes a trailing floor from `trailing_dd` dollars below
    /// the high-water equity it observes.
    pub fn with_trailing_dd(trailing_dd: f64) -> Self {
        let book = Self::default();
        book.inner
            .lock()
            .expect("positions book poisoned")
            .trailing_dd = trailing_dd;
        book
    }

    /// Declare the account's original starting balance.
    ///
    /// This is two things at once, and both are required before the floor means
    /// anything: the seed for the end-of-day high-water mark, and the CAP the
    /// floor may never climb past.
    pub fn seed_starting_balance(&self, balance: f64) {
        let mut inner = self.inner.lock().expect("positions book poisoned");
        inner.starting_balance = Some(balance);
        inner.high_water_eod = Some(inner.high_water_eod.map_or(balance, |hw| hw.max(balance)));
    }

    /// Restore an end-of-day high-water mark carried over from a previous
    /// process. The mark is account history, not process state — losing it on a
    /// restart silently reverts the floor to the starting balance.
    pub fn restore_high_water_eod(&self, balance: f64) {
        let mut inner = self.inner.lock().expect("positions book poisoned");
        inner.high_water_eod = Some(inner.high_water_eod.map_or(balance, |hw| hw.max(balance)));
    }

    /// Close the trading day: fold the REALISED balance into the end-of-day
    /// high-water mark.
    ///
    /// Realised only, and only here. This is the single place the floor is
    /// allowed to move up, which is what makes the rule an end-of-day rule.
    /// Returns the new mark when it advanced, so the caller can persist it.
    pub fn roll_end_of_day(&self) -> RollOutcome {
        let mut inner = self.inner.lock().expect("positions book poisoned");
        let Some(account) = inner.account.as_ref() else {
            return RollOutcome::NotObserved;
        };
        let balance = account.account_balance;
        // A zero balance means "not reported yet", not "the account is empty".
        if balance == 0.0 {
            return RollOutcome::NotObserved;
        }
        let advanced = !matches!(inner.high_water_eod, Some(hw) if balance <= hw);
        inner.high_water_eod = Some(inner.high_water_eod.map_or(balance, |hw| hw.max(balance)));
        RollOutcome::Verified {
            mark: inner.high_water_eod.unwrap_or(balance),
            advanced,
        }
    }

    /// Insert/update a position. A flat position (net 0 and no open qty) is
    /// removed instead — the book tracks *open* positions only.
    pub fn upsert(&self, snap: PositionSnapshot) {
        let mut inner = self.inner.lock().expect("positions book poisoned");
        inner.last_update = Some(std::time::Instant::now());
        if snap.net_quantity == 0 && snap.open_quantity == 0 {
            inner.positions.remove(&snap.symbol);
        } else {
            inner.positions.insert(snap.symbol.clone(), snap);
        }
    }

    /// Record the latest account-level P&L summary.
    ///
    /// This deliberately does NOT touch the high-water mark. Until 2026-09-02 it
    /// advanced it on `balance + open_pnl` on every update, which conflated the
    /// two halves of the rule: the floor TRAILS on end-of-day realised balance,
    /// but is BREACHED at any time by realised or unrealised loss. Only the
    /// second half involves open P&L, and it is applied in `risk_from` where
    /// headroom is measured. See `roll_end_of_day` for the first half.
    pub fn set_account(&self, summary: AccountSummary) {
        let mut inner = self.inner.lock().expect("positions book poisoned");
        inner.last_update = Some(std::time::Instant::now());
        inner.account = Some(summary);
    }

    /// Seconds since the last position/account update; `None` before the first.
    /// `None` and "zero seconds" must stay distinguishable — a stream that has
    /// never delivered anything is not a fresh one.
    pub fn age_secs(&self) -> Option<u64> {
        self.inner
            .lock()
            .expect("positions book poisoned")
            .last_update
            .map(|t| t.elapsed().as_secs())
    }

    /// The trailing-drawdown state, or `None` before the first real update.
    pub fn risk(&self) -> Option<RiskState> {
        let inner = self.inner.lock().expect("positions book poisoned");
        Self::risk_from(&inner)
    }

    fn risk_from(inner: &BookInner) -> Option<RiskState> {
        // No configured drawdown means no floor to compute. Returning a
        // zero-width floor here (as this did until 2026-09-02) makes headroom
        // equal open P&L, so any losing tick reads as a breach and pages the
        // operator over nothing.
        if inner.trailing_dd <= 0.0 {
            return None;
        }
        let account = inner.account.as_ref()?;
        let starting_balance = inner.starting_balance?;
        let high_water_eod = inner.high_water_eod?;

        // Breaches count unrealised money; trailing does not.
        let equity = account.account_balance + account.open_pnl;

        let trailed = high_water_eod - inner.trailing_dd;
        let computed_min_balance = trailed.min(starting_balance);
        Some(RiskState {
            equity,
            high_water_eod,
            computed_min_balance,
            headroom: equity - computed_min_balance,
            trailing_dd: inner.trailing_dd,
            floor_capped: trailed >= starting_balance,
            floor_disagreement: account.reported_min_balance - computed_min_balance,
            seeded: true,
        })
    }

    /// Number of open positions currently tracked.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("positions book poisoned")
            .positions
            .len()
    }

    /// Whether the book has no open positions.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Render an observable view for the `/positions` endpoint.
    pub fn view(&self) -> PositionsView {
        let inner = self.inner.lock().expect("positions book poisoned");
        let positions: Vec<PositionSnapshot> = inner.positions.values().cloned().collect();
        // Prefer the account summary's id; fall back to any position's account.
        let account = inner
            .account
            .as_ref()
            .map(|a| a.account_id.clone())
            .or_else(|| positions.first().map(|p| p.account_id.clone()))
            .unwrap_or_default();
        PositionsView {
            account,
            count: positions.len(),
            positions,
            risk: Self::risk_from(&inner),
            account_summary: inner.account.clone(),
            age_secs: inner.last_update.map(|t| t.elapsed().as_secs()),
            read_only: true,
            order_plant_open: false,
        }
    }
}

/// Direction label from a signed net quantity.
pub fn direction_of(net_quantity: i32) -> &'static str {
    if net_quantity > 0 {
        "long"
    } else if net_quantity < 0 {
        "short"
    } else {
        "flat"
    }
}

/// Parse an optional Rithmic string-encoded number into an f64 (0.0 on
/// absent/garbage). Rithmic sends several P&L fields as strings.
pub fn parse_opt_f64(s: &Option<String>) -> f64 {
    s.as_deref()
        .map(str::trim)
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0)
}

// ── Live PnL subscription (behind live-connect) ──────────────────────────────

/// Map a Rithmic per-instrument P&L update into a [`PositionSnapshot`].
///
/// Returns `None` when the update carries no symbol (nothing we can key on).
/// Pure — unit-tested without any network.
#[cfg(feature = "live-connect")]
pub fn map_instrument_update(
    u: &rithmic_rs::rti::InstrumentPnLPositionUpdate,
) -> Option<PositionSnapshot> {
    let symbol = u.symbol.clone().filter(|s| !s.is_empty())?;
    let net_quantity = u.net_quantity.unwrap_or(0);
    Some(PositionSnapshot {
        symbol,
        exchange: u.exchange.clone().unwrap_or_default(),
        product_code: u.product_code.clone().unwrap_or_default(),
        net_quantity,
        direction: direction_of(net_quantity),
        avg_open_price: u.avg_open_fill_price.unwrap_or(0.0),
        open_pnl: parse_opt_f64(&u.open_position_pnl),
        day_pnl: u.day_pnl.unwrap_or(0.0),
        buy_qty: u.buy_qty.unwrap_or(0),
        sell_qty: u.sell_qty.unwrap_or(0),
        open_quantity: u.open_position_quantity.unwrap_or(0),
        account_id: u.account_id.clone().unwrap_or_default(),
    })
}

/// Map a Rithmic account-level P&L update into an [`AccountSummary`].
#[cfg(feature = "live-connect")]
pub fn map_account_update(a: &rithmic_rs::rti::AccountPnLPositionUpdate) -> AccountSummary {
    AccountSummary {
        account_id: a.account_id.clone().unwrap_or_default(),
        net_quantity: a.net_quantity.unwrap_or(0),
        open_pnl: parse_opt_f64(&a.open_position_pnl),
        day_pnl: parse_opt_f64(&a.day_pnl),
        account_balance: parse_opt_f64(&a.account_balance),
        cash_on_hand: parse_opt_f64(&a.cash_on_hand),
        reported_min_balance: parse_opt_f64(&a.min_account_balance),
    }
}

/// Run the read-only PnL subscription until the stream closes. Opens the PnL
/// plant, logs in, pulls a position snapshot, subscribes to live updates, and
/// feeds every position/account update into `book`.
///
/// This is a secondary reader within the connector's `Ready` state: it runs only
/// when the account triple (account_id/fcm_id/ib_id) is configured. Market data
/// (the ticker plant) runs independently of it.
#[cfg(feature = "live-connect")]
pub async fn run_pnl(
    rr_config: rithmic_rs::RithmicConfig,
    account: rithmic_rs::RithmicAccount,
    book: PositionsBook,
) -> anyhow::Result<()> {
    use rithmic_rs::{ConnectStrategy, RithmicPnlPlant, rti::messages::RithmicMessage};
    use tokio::sync::broadcast::error::RecvError;
    use tracing::{info, warn};

    let plant = RithmicPnlPlant::connect(&rr_config, ConnectStrategy::Retry)
        .await
        .map_err(|e| anyhow::anyhow!("pnl plant connect failed: {e}"))?;
    let handle = plant.get_handle(&account);

    handle
        .login()
        .await
        .map_err(|e| anyhow::anyhow!("pnl plant login failed: {e}"))?;
    info!(account = %account.account_id, "logged in to Rithmic PnL plant (read-only)");

    // Pull a one-off snapshot, then subscribe to live updates. Snapshot rows and
    // subscription updates both arrive on `subscription_receiver` as
    // Instrument/Account PnL messages, so the loop below handles them uniformly.
    if let Err(e) = handle.pnl_position_snapshots().await {
        warn!(error = %e, "pnl position snapshot request failed (continuing with live updates)");
    }
    handle
        .subscribe_pnl_updates()
        .await
        .map_err(|e| anyhow::anyhow!("pnl subscribe failed: {e}"))?;
    info!("subscribed to Rithmic PnL updates (read-only)");

    let mut receiver = handle.subscription_receiver.resubscribe();

    // Diagnostics for a stream that has never produced a row.
    //
    // `/positions` has reported account:"" and count:0 since this reader was
    // built, while login and subscribe both succeed and the snapshot request
    // returns Ok. The loop below could not say WHICH of the two possible causes
    // it was, because both were silent: a non-connection error was `continue`d
    // without a log, and any message variant we do not map hit `_ => {}` and
    // vanished. "Receiving nothing" and "receiving something unrecognised" are
    // completely different bugs and looked identical from outside.
    //
    // Counted locally rather than on ConnectorState: this is the only consumer,
    // and a periodic summary keeps a chatty stream from flooding the log.
    let mut n_total: u64 = 0;
    let mut n_instrument: u64 = 0;
    let mut n_account: u64 = 0;
    let mut n_errors: u64 = 0;
    let mut n_unhandled: u64 = 0;
    // First few of each kind are logged in full; the rest only in the summary.
    const VERBOSE_FIRST: u64 = 5;
    const REPORT_EVERY: std::time::Duration = std::time::Duration::from_secs(60);
    let mut last_report = std::time::Instant::now();

    loop {
        if last_report.elapsed() >= REPORT_EVERY {
            // Logged even when everything is zero. A flat account legitimately
            // produces no INSTRUMENT rows, so silence is not itself a fault —
            // but silence that is never reported is indistinguishable from a
            // reader that died, which is the failure mode this whole file keeps
            // reproducing.
            info!(
                total = n_total,
                instrument_updates = n_instrument,
                account_updates = n_account,
                errors = n_errors,
                unhandled = n_unhandled,
                "PnL stream summary (last 60s window is cumulative since login)"
            );
            last_report = std::time::Instant::now();
        }
        match receiver.recv().await {
            Ok(resp) => {
                n_total += 1;
                if let Some(err) = &resp.error {
                    if err.is_connection_issue() {
                        warn!(error = %err, "connection issue on PnL stream — stopping loop");
                        break;
                    }
                    n_errors += 1;
                    // A rejection from Rithmic — "account not found", a
                    // permission problem — arrives here. Discarding it silently
                    // is why the cause of an empty book was never visible.
                    if n_errors <= VERBOSE_FIRST {
                        warn!(
                            error = %err,
                            n_errors,
                            "PnL stream returned an error (not a connection issue)"
                        );
                    }
                    continue;
                }
                match resp.message {
                    RithmicMessage::InstrumentPnLPositionUpdate(u) => {
                        n_instrument += 1;
                        if let Some(snap) = map_instrument_update(&u) {
                            info!(
                                symbol = %snap.symbol,
                                net = snap.net_quantity,
                                dir = snap.direction,
                                "position update"
                            );
                            book.upsert(snap);
                        }
                    }
                    RithmicMessage::AccountPnLPositionUpdate(a) => {
                        n_account += 1;
                        book.set_account(map_account_update(&a));
                    }
                    ref other => {
                        n_unhandled += 1;
                        // Truncated: enough to identify the variant without
                        // dumping a full account payload into the log.
                        if n_unhandled <= VERBOSE_FIRST {
                            let dbg = format!("{other:?}");
                            info!(
                                variant = &dbg[..dbg.len().min(160)],
                                n_unhandled, "PnL stream message not mapped by this reader"
                            );
                        }
                    }
                }
            }
            Err(RecvError::Lagged(n)) => {
                warn!(
                    skipped = n,
                    "PnL subscription lagged — some updates dropped"
                );
            }
            Err(RecvError::Closed) => {
                warn!("PnL subscription channel closed — stopping loop");
                break;
            }
        }
    }

    let _ = handle.disconnect().await;
    info!("pnl plant disconnected");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_from_net_quantity() {
        assert_eq!(direction_of(3), "long");
        assert_eq!(direction_of(-2), "short");
        assert_eq!(direction_of(0), "flat");
    }

    #[test]
    fn parse_opt_f64_is_lenient() {
        assert_eq!(parse_opt_f64(&Some("123.5".to_string())), 123.5);
        assert_eq!(parse_opt_f64(&Some(" -4.0 ".to_string())), -4.0);
        assert_eq!(parse_opt_f64(&Some("".to_string())), 0.0);
        assert_eq!(parse_opt_f64(&Some("n/a".to_string())), 0.0);
        assert_eq!(parse_opt_f64(&None), 0.0);
    }

    fn sample(symbol: &str, net: i32, open_qty: i32) -> PositionSnapshot {
        PositionSnapshot {
            symbol: symbol.to_string(),
            exchange: "CME".to_string(),
            product_code: "ES".to_string(),
            net_quantity: net,
            direction: direction_of(net),
            avg_open_price: 5000.0,
            open_pnl: 12.5,
            day_pnl: 20.0,
            buy_qty: 1,
            sell_qty: 0,
            open_quantity: open_qty,
            account_id: "ACCT1".to_string(),
        }
    }

    #[test]
    fn book_upserts_and_orders_by_symbol() {
        let book = PositionsBook::new();
        book.upsert(sample("MNQU6", 1, 1));
        book.upsert(sample("ESU6", 2, 2));
        let view = book.view();
        assert_eq!(view.count, 2);
        // BTreeMap → deterministic symbol order.
        assert_eq!(view.positions[0].symbol, "ESU6");
        assert_eq!(view.positions[1].symbol, "MNQU6");
        assert_eq!(view.account, "ACCT1");
        assert!(view.read_only);
        assert!(!view.order_plant_open);
    }

    #[test]
    fn book_updates_existing_symbol_in_place() {
        let book = PositionsBook::new();
        book.upsert(sample("ESU6", 1, 1));
        book.upsert(sample("ESU6", 3, 3));
        let view = book.view();
        assert_eq!(view.count, 1);
        assert_eq!(view.positions[0].net_quantity, 3);
    }

    #[test]
    fn book_removes_position_when_flat() {
        let book = PositionsBook::new();
        book.upsert(sample("ESU6", 2, 2));
        assert_eq!(book.len(), 1);
        // Net 0 + no open qty ⇒ closed ⇒ removed.
        book.upsert(sample("ESU6", 0, 0));
        assert!(book.is_empty());
        assert_eq!(book.view().count, 0);
    }

    #[test]
    fn account_summary_populates_view_and_account_id() {
        let book = PositionsBook::new();
        book.set_account(AccountSummary {
            account_id: "ACCT9".to_string(),
            net_quantity: 1,
            open_pnl: 5.0,
            day_pnl: 7.5,
            account_balance: 10_000.0,
            cash_on_hand: 10_000.0,
            reported_min_balance: 9_000.0,
        });
        let view = book.view();
        assert_eq!(view.account, "ACCT9");
        assert_eq!(view.account_summary.unwrap().account_balance, 10_000.0);
    }

    fn acct(balance: f64, open_pnl: f64, reported_min: f64) -> AccountSummary {
        AccountSummary {
            account_id: "TPT".to_string(),
            net_quantity: 0,
            open_pnl,
            day_pnl: 0.0,
            account_balance: balance,
            cash_on_hand: balance,
            reported_min_balance: reported_min,
        }
    }

    #[test]
    fn an_unseen_stream_is_not_a_fresh_one() {
        // "Never delivered" and "delivered a moment ago" must stay
        // distinguishable, or a stream that has produced nothing since boot
        // reads as perfectly current and the staleness alert never fires.
        //
        // Tracked on the BOOK rather than in connector state because every
        // update flows through here. The first attempt kept the timestamp
        // elsewhere and simply never called the setter — the metric shipped
        // pinned at "never", which is the failure this test exists to catch.
        let book = PositionsBook::new();
        assert_eq!(book.age_secs(), None);
        book.set_account(acct(150_000.0, 0.0, 145_500.0));
        assert!(book.age_secs().is_some_and(|s| s < 5));
    }

    #[test]
    fn a_position_update_also_counts_as_liveness() {
        // Not just account summaries: an instrument-level update proves the
        // stream is alive too, and on a quiet account it may be all that
        // arrives.
        let book = PositionsBook::new();
        assert_eq!(book.age_secs(), None);
        book.upsert(PositionSnapshot {
            symbol: "GCZ6".into(),
            exchange: "COMEX".into(),
            product_code: "GC".into(),
            net_quantity: -1,
            direction: "short",
            avg_open_price: 4485.0,
            open_pnl: 0.0,
            day_pnl: 0.0,
            buy_qty: 0,
            sell_qty: 1,
            open_quantity: 1,
            account_id: "TPT".into(),
        });
        assert!(book.age_secs().is_some());
    }

    /// A book configured the way `main` configures it for a live account.
    fn seeded_book(starting: f64, dd: f64) -> PositionsBook {
        let book = PositionsBook::with_trailing_dd(dd);
        book.seed_starting_balance(starting);
        book
    }

    #[test]
    fn an_intraday_peak_does_not_move_the_floor() {
        // THE 2026-09-02 CORRECTION. TPT Rule 3 trails the highest END-OF-DAY
        // balance. An intraday spike that is given back before the close never
        // raises the floor.
        //
        // This test previously asserted the opposite — that a peak of 154,675
        // lifts the floor to 150,175 — and that wrong expectation is what made
        // the connector report a floor the firm's rule can never produce.
        let book = seeded_book(150_000.0, 4_500.0);
        book.set_account(acct(150_000.0, 0.0, 145_500.0));
        assert_eq!(book.risk().unwrap().computed_min_balance, 145_500.0);

        book.set_account(acct(154_675.0, 0.0, 145_500.0)); // intraday peak
        let r = book.risk().unwrap();
        assert_eq!(r.high_water_eod, 150_000.0, "peak is not an EOD balance");
        assert_eq!(r.computed_min_balance, 145_500.0, "floor must NOT move");

        // Given back before the close: the floor was never raised, so there is
        // nothing to be caught below.
        book.set_account(acct(150_005.0, 0.0, 145_500.0));
        let r = book.risk().unwrap();
        assert_eq!(r.computed_min_balance, 145_500.0);
        assert!(r.headroom > 0.0, "150,005 is well above a 145,500 floor");
        // And we now AGREE with the broker, which reported 145,500 all day.
        assert_eq!(r.floor_disagreement, 0.0);
    }

    #[test]
    fn the_floor_advances_only_at_end_of_day_and_only_on_realised_balance() {
        let book = seeded_book(150_000.0, 4_500.0);
        book.set_account(acct(152_000.0, 1_000.0, 145_500.0));
        // Open P&L is worth 1,000 but is not realised: it must not be banked.
        assert_eq!(
            book.roll_end_of_day(),
            RollOutcome::Verified {
                mark: 152_000.0,
                advanced: true
            }
        );
        let r = book.risk().unwrap();
        assert_eq!(r.high_water_eod, 152_000.0);
        assert_eq!(r.computed_min_balance, 147_500.0);

        // A losing day does not lower it.
        book.set_account(acct(149_000.0, 0.0, 145_500.0));
        // A down day IS an observed close — the mark simply does not move. That
        // distinction is what continuity depends on.
        assert!(
            matches!(
                book.roll_end_of_day(),
                RollOutcome::Verified {
                    advanced: false,
                    ..
                }
            ),
            "a down day is observed-but-not-advanced, not unobserved"
        );
        assert_eq!(book.risk().unwrap().computed_min_balance, 147_500.0);
    }

    #[test]
    fn the_floor_stops_trailing_at_the_starting_balance() {
        // "The trailing continues until the minimum account balance reaches the
        // original starting balance. Once it reaches that point, it stops
        // trailing and remains fixed." Without this cap the floor climbs
        // forever and eventually exceeds anything the rule can produce.
        let book = seeded_book(150_000.0, 4_500.0);

        book.set_account(acct(154_500.0, 0.0, 150_000.0));
        book.roll_end_of_day();
        let r = book.risk().unwrap();
        assert_eq!(r.computed_min_balance, 150_000.0, "exactly at the cap");
        assert!(r.floor_capped);

        // Further profit cannot raise it.
        book.set_account(acct(175_000.0, 0.0, 150_000.0));
        book.roll_end_of_day();
        let r = book.risk().unwrap();
        assert_eq!(r.computed_min_balance, 150_000.0, "capped, not 170,500");
        assert!(r.floor_capped);
    }

    #[test]
    fn tpt_published_worked_example_25k_account() {
        // The firm's own numbers, kept as a regression against any future
        // "improvement" to this model: a 25,000 account with a 1,500 drawdown
        // starts at 23,500, and a 1,000 winning day moves it to 24,500.
        let book = seeded_book(25_000.0, 1_500.0);
        book.set_account(acct(25_000.0, 0.0, 23_500.0));
        assert_eq!(book.risk().unwrap().computed_min_balance, 23_500.0);

        book.set_account(acct(26_000.0, 0.0, 24_500.0));
        book.roll_end_of_day();
        assert_eq!(book.risk().unwrap().computed_min_balance, 24_500.0);

        // ...and it stops at 25,000, never above.
        book.set_account(acct(40_000.0, 0.0, 25_000.0));
        book.roll_end_of_day();
        let r = book.risk().unwrap();
        assert_eq!(r.computed_min_balance, 25_000.0);
        assert!(r.floor_capped);
    }

    #[test]
    fn no_configured_drawdown_means_no_floor_is_claimed() {
        // A zero trailing_dd used to build a ZERO-WIDTH floor, which makes
        // headroom equal open P&L — so any losing tick reads as a breach.
        let book = PositionsBook::new();
        book.seed_starting_balance(150_000.0);
        book.set_account(acct(150_000.0, -10.0, 0.0));
        assert!(book.risk().is_none(), "no dd configured ⇒ no floor");
    }

    #[test]
    fn an_eod_mark_survives_a_restart_when_restored() {
        // The mark is account history, not process state. A restart that
        // forgets it silently reverts the floor to the starting balance.
        let book = seeded_book(150_000.0, 4_500.0);
        book.restore_high_water_eod(158_000.0);
        book.set_account(acct(151_000.0, 0.0, 150_000.0));
        let r = book.risk().unwrap();
        assert_eq!(r.high_water_eod, 158_000.0);
        assert_eq!(r.computed_min_balance, 150_000.0, "capped");
    }

    #[test]
    fn equity_includes_open_pnl_because_the_rule_bites_on_unrealised() {
        // A position running against you must move the headroom BEFORE it is
        // closed. This half of the rule is unchanged by the 2026-09-02
        // correction: the floor TRAILS on end-of-day realised balance, but it
        // is BREACHED at any time by realised OR unrealised loss.
        let book = seeded_book(150_000.0, 4_500.0);
        book.set_account(acct(150_000.0, 0.0, 145_500.0));
        book.set_account(acct(150_000.0, -3_000.0, 145_500.0));
        let r = book.risk().unwrap();
        assert_eq!(r.equity, 147_000.0);
        assert_eq!(r.headroom, 1_500.0);
    }

    #[test]
    fn an_unreported_balance_does_not_advance_the_eod_mark() {
        // A zero balance means "not reported yet", not "the account is empty".
        // Rolling a 0 into the mark would put the floor at -4,500 and report
        // six figures of imaginary headroom.
        let book = seeded_book(150_000.0, 4_500.0);
        book.set_account(acct(0.0, 0.0, 0.0));
        // A zero balance means "not reported yet" — the close was NOT observed.
        assert_eq!(
            book.roll_end_of_day(),
            RollOutcome::NotObserved,
            "0 is not an EOD balance, so the close was not observed"
        );
        assert_eq!(book.risk().unwrap().computed_min_balance, 145_500.0);
    }

    #[test]
    fn no_starting_balance_means_no_floor_is_claimed() {
        // Without the starting balance there is no cap, so any floor computed
        // here could exceed what the rule permits. Claim nothing instead.
        let book = PositionsBook::with_trailing_dd(4_500.0);
        book.set_account(acct(150_000.0, 0.0, 145_500.0));
        assert!(book.risk().is_none(), "unseeded ⇒ no floor claimed");
    }

    // ── live-connect: pure mapping from the vendor protobuf types ────────────
    #[cfg(feature = "live-connect")]
    #[test]
    fn map_instrument_update_extracts_position() {
        let u = rithmic_rs::rti::InstrumentPnLPositionUpdate {
            symbol: Some("ESU6".to_string()),
            exchange: Some("CME".to_string()),
            product_code: Some("ES".to_string()),
            net_quantity: Some(-2),
            avg_open_fill_price: Some(5012.25),
            open_position_pnl: Some("-37.50".to_string()),
            day_pnl: Some(-40.0),
            buy_qty: Some(0),
            sell_qty: Some(2),
            open_position_quantity: Some(2),
            account_id: Some("ACCT1".to_string()),
            ..Default::default()
        };

        let snap = map_instrument_update(&u).expect("has symbol → maps");
        assert_eq!(snap.symbol, "ESU6");
        assert_eq!(snap.net_quantity, -2);
        assert_eq!(snap.direction, "short");
        assert_eq!(snap.avg_open_price, 5012.25);
        assert_eq!(snap.open_pnl, -37.50);
        assert_eq!(snap.day_pnl, -40.0);
        assert_eq!(snap.open_quantity, 2);
        assert_eq!(snap.account_id, "ACCT1");
    }

    #[cfg(feature = "live-connect")]
    #[test]
    fn map_instrument_update_without_symbol_is_none() {
        let u = rithmic_rs::rti::InstrumentPnLPositionUpdate::default();
        assert!(map_instrument_update(&u).is_none());
    }

    #[cfg(feature = "live-connect")]
    #[test]
    fn map_account_update_extracts_summary() {
        let a = rithmic_rs::rti::AccountPnLPositionUpdate {
            account_id: Some("ACCT1".to_string()),
            net_quantity: Some(1),
            open_position_pnl: Some("15.0".to_string()),
            day_pnl: Some("22.5".to_string()),
            account_balance: Some("52000.00".to_string()),
            ..Default::default()
        };

        let s = map_account_update(&a);
        assert_eq!(s.account_id, "ACCT1");
        assert_eq!(s.net_quantity, 1);
        assert_eq!(s.open_pnl, 15.0);
        assert_eq!(s.day_pnl, 22.5);
        assert_eq!(s.account_balance, 52000.0);
    }
}
