// =============================================================================
// config.rs — Rithmic connector configuration + the capability gate.
//
// All values are read from environment variables. Credentials (RITHMIC_USER /
// RITHMIC_PASSWORD) are injected by the spawner secret store at spawn time, the
// same way crypto bots receive their exchange keys (the 3-slot broker secret:
// user → api_key, password → api_secret, system → api_passphrase).
//
// The gate logic (`GateDecision`) is the heart of the "capability gate in
// action": the connector only ever touches Rithmic when it is BOTH enabled
// (RITHMIC_ENABLED=true) AND credentialed. Everything here is pure and unit-
// tested without a live connection.
// =============================================================================

use std::env;

use crate::persistence::QuestDbConfig;

/// Which Rithmic environment to target. Mirrors `rithmic_rs::RithmicEnv` but is
/// kept as our own type so the config surface does not leak the vendor crate
/// (and so the skeleton compiles without the `live-connect` feature).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RithmicEnvironment {
    /// Rithmic Paper Trading (demo/development). Safe default.
    #[default]
    Demo,
    /// Rithmic Test system (conformance / dev-kit login).
    Test,
    /// Rithmic 01 (production). Requires conformance sign-off. Read-only here.
    Live,
}

impl RithmicEnvironment {
    /// Parse from the `RITHMIC_ENV` string; unknown values fall back to `Demo`.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "test" => Self::Test,
            "live" | "production" | "prod" => Self::Live,
            _ => Self::Demo,
        }
    }

    /// The conventional Rithmic system_name default for this environment. Used
    /// only when `RITHMIC_SYSTEM_NAME` is not explicitly provided.
    pub fn default_system_name(self) -> &'static str {
        match self {
            Self::Demo => "Rithmic Paper Trading",
            Self::Test => "Rithmic Test",
            Self::Live => "Rithmic 01",
        }
    }
}

/// The outcome of evaluating whether the connector should touch Rithmic.
///
/// This is the capability gate. `Ready` is the ONLY variant that permits a
/// network connection; every other variant is a clean, logged no-op.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum GateDecision {
    /// `RITHMIC_ENABLED` is false (the default). Feature-flagged off.
    Disabled,
    /// Enabled, but one or more required credentials/URL are absent. We log the
    /// missing pieces and idle — no partial/ambiguous connection attempts.
    MissingCredentials {
        /// Names of the required-but-empty fields (never their values).
        missing: Vec<String>,
    },
    /// Enabled AND fully credentialed. The connector may open the read-only
    /// Ticker plant.
    Ready,
}

impl GateDecision {
    /// True only when the connector is cleared to open a Rithmic session.
    pub fn is_ready(&self) -> bool {
        matches!(self, GateDecision::Ready)
    }

    /// A short human-readable reason, for logs and the `/status` payload.
    pub fn reason(&self) -> String {
        match self {
            GateDecision::Disabled => {
                "RITHMIC_ENABLED is false — connector idle (no Rithmic session opened)".to_string()
            }
            GateDecision::MissingCredentials { missing } => format!(
                "RITHMIC_ENABLED is true but required config is absent: {} — connector idle",
                missing.join(", ")
            ),
            GateDecision::Ready => {
                "enabled + credentialed — cleared to open the read-only Ticker plant".to_string()
            }
        }
    }
}

/// One Rithmic login. The platform may hold two: a DATA credential that streams
/// market data continuously, and the TRADING credential the operator's own
/// terminal needs. Keeping them apart is the entire point — see
/// [`RithmicConfig::data`].
#[derive(Debug, Clone, PartialEq)]
pub struct RithmicCredential {
    /// Rithmic username.
    pub user: String,
    /// Rithmic password.
    pub password: String,
    /// Exact, case-sensitive system name (e.g. "Rithmic Paper Trading").
    pub system_name: String,
    /// Primary gateway URL.
    pub gateway_url: String,
    /// Alternate gateway URL; defaults to the primary when unset.
    pub gateway_alt_url: String,
}

impl RithmicCredential {
    /// Whether this credential has enough to attempt a login. A half-filled
    /// credential must be treated as ABSENT rather than tried: a login attempt
    /// with a blank password is how an account gets locked.
    pub fn is_usable(&self) -> bool {
        !self.user.trim().is_empty()
            && !self.password.trim().is_empty()
            && !self.gateway_url.trim().is_empty()
    }
}

/// Full connector configuration, parsed from the environment.
#[derive(Debug, Clone)]
pub struct RithmicConfig {
    /// Master runtime feature flag. Default: false. Nothing connects unless true.
    pub enabled: bool,

    /// Target Rithmic environment (demo/test/live).
    pub environment: RithmicEnvironment,

    /// Primary gateway WebSocket URL (`wss://…`). Handed out by Rithmic after
    /// onboarding; empty until then.
    pub gateway_url: String,

    /// Alternative/beta gateway URL used by the vendor crate's alternating
    /// reconnect strategy. Falls back to `gateway_url` when unset.
    pub gateway_alt_url: String,

    /// Rithmic `system_name` (e.g. "Rithmic Paper Trading"). Third secret slot.
    pub system_name: String,

    /// Login username (secret slot 1 / api_key). Injected by the spawner store.
    pub user: String,

    /// Login password (secret slot 2 / api_secret). Injected by the spawner store.
    pub password: String,

    /// Application name registered with Rithmic (required by R|Protocol login).
    pub app_name: String,

    /// Application version string (required by R|Protocol login).
    pub app_version: String,

    /// Instrument(s) to subscribe, as given by `RITHMIC_INSTRUMENT`. Either a
    /// single symbol (`"GC"`) or a comma-separated list (`"GC,NQ,ES"`). Use
    /// [`Self::instruments`] rather than reading this directly.
    ///
    /// A LIST rather than one symbol because **Rithmic permits only ONE
    /// concurrent session per credential** — measured 2026-08-24: starting a
    /// second connector on the same login killed the first with a
    /// `permission denied` close frame. Running one container per instrument
    /// is therefore impossible on a single credential, so a single session has
    /// to carry every symbol.
    pub instrument: String,

    /// Default exchange, from `RITHMIC_EXCHANGE`. Applies to any instrument
    /// that has no `SYMBOL:EXCHANGE` override in [`Self::instrument`].
    ///
    /// Per-instrument overrides matter because the target set spans venues:
    /// **GC is COMEX while NQ/ES are CME**, so one flat default cannot serve
    /// them together and a mismatched exchange fails the subscribe.
    pub exchange: String,

    /// Trading account id for the read-only PnL/positions subscription.
    /// Optional: market data (ticker) runs without it; positions need the
    /// full triple (account_id/fcm_id/ib_id).
    pub account_id: String,

    /// Futures Commission Merchant id (positions subscription). Optional.
    pub fcm_id: String,

    /// Introducing Broker id (positions subscription). Optional.
    pub ib_id: String,

    /// An INDEPENDENT login used only for market data.
    ///
    /// Rithmic permits one session per credential, so a single login cannot
    /// serve both the platform and the operator's own terminal. On 2026-08-31
    /// that cost 626 minutes of Globex overnight plus 146 minutes spanning a
    /// live position, and the platform could not answer "did my stop fill".
    /// A second TPT account does NOT fix it — the firm issues one login and
    /// varies only the account number — so the second credential has to come
    /// from a different FCM entirely.
    ///
    /// `None` means "run both plants on the primary credential", which is the
    /// pre-2026-09 behaviour and still correct when only one login exists.
    pub data: Option<RithmicCredential>,

    /// Dollars of trailing drawdown the prop account allows below its
    /// high-water equity. Used to compute the floor OURSELVES — the broker's
    /// `min_account_balance` field reported a static starting value through the
    /// 2026-08-31 session and cannot be trusted as the operative number.
    /// Zero disables the computation (no floor claimed rather than a wrong one).
    pub trailing_dd: f64,

    /// Starting equity for the trailing floor, so a process booting mid-drawdown
    /// anchors its high-water to the account's real peak instead of to wherever
    /// it happened to restart. Zero means "seed from the first live update".
    pub starting_equity: f64,

    /// Host for the /health + /status server.
    pub health_host: String,

    /// Port for the /health + /status server (FKS bot contract: :9091).
    pub health_port: u16,

    /// QuestDB ILP endpoint the persistence sink writes `candles_futures` to.
    pub questdb: QuestDbConfig,
}

impl RithmicConfig {
    /// Parse configuration from the process environment.
    pub fn from_env() -> Self {
        Self::from_getter(|k| env::var(k).ok())
    }

    /// Parse configuration from an arbitrary getter. This is the deterministic
    /// core used by tests — no process-global env mutation required.
    pub fn from_getter(get: impl Fn(&str) -> Option<String>) -> Self {
        let get_or = |k: &str, default: &str| {
            get(k)
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default.to_string())
        };

        let enabled = parse_bool(get("RITHMIC_ENABLED").as_deref(), false);
        let environment = RithmicEnvironment::parse(&get_or("RITHMIC_ENV", "demo"));

        let gateway_url = get_or("RITHMIC_GATEWAY_URL", "");
        // Alt URL defaults to the primary so a single-URL config still works.
        let gateway_alt_url = get("RITHMIC_GATEWAY_ALT_URL")
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| gateway_url.clone());

        let system_name = get_or("RITHMIC_SYSTEM_NAME", environment.default_system_name());

        // The DATA credential, resolved before the struct literal consumes the
        // primary URLs. Unset fields inherit the primary's, so a second login
        // on the same gateway needs only USER/PASSWORD.
        //
        // Kept as None unless USABLE: a half-filled credential is treated as
        // absent rather than attempted, because a login with a blank password
        // is how an account gets locked — measured 2026-08-24, nine attempts in
        // twenty-five seconds against a bad system name.
        let data_credential = {
            let cred = RithmicCredential {
                user: get_or("RITHMIC_DATA_USER", ""),
                password: get_or("RITHMIC_DATA_PASSWORD", ""),
                system_name: get_or("RITHMIC_DATA_SYSTEM_NAME", &system_name),
                gateway_url: get_or("RITHMIC_DATA_GATEWAY_URL", &gateway_url),
                gateway_alt_url: get_or("RITHMIC_DATA_GATEWAY_ALT_URL", &gateway_alt_url),
            };
            cred.is_usable().then_some(cred)
        };

        Self {
            enabled,
            environment,
            gateway_url,
            gateway_alt_url,
            system_name,
            user: get_or("RITHMIC_USER", ""),
            password: get_or("RITHMIC_PASSWORD", ""),
            // The "josm:" prefix is assigned by Rithmic and is how they identify
            // this application across their systems. Requested in writing by the
            // RAPI team on 2026-09-07 as the first step of conformance:
            //
            //   "Please prepend "josm:" (including the ":" character) to your app
            //    name (the app_name field of request_login.proto)."
            //
            // It belongs in the DEFAULT rather than only in .env so that any
            // deployment, including a fresh one, identifies itself correctly
            // without someone remembering to set it.
            app_name: get_or("RITHMIC_APP_NAME", "josm:fks-rithmic-connector"),
            app_version: get_or("RITHMIC_APP_VERSION", env!("CARGO_PKG_VERSION")),
            instrument: get_or("RITHMIC_INSTRUMENT", "MES"),
            exchange: get_or("RITHMIC_EXCHANGE", "CME"),
            account_id: get_or("RITHMIC_ACCOUNT_ID", ""),
            fcm_id: get_or("RITHMIC_FCM_ID", ""),
            ib_id: get_or("RITHMIC_IB_ID", ""),
            // The DATA credential. Unset fields inherit the primary's, so a
            // second login on the same gateway needs only USER/PASSWORD.
            //
            // Kept as None unless USABLE: a half-filled credential is treated
            // as absent rather than attempted, because a login with a blank
            // password is how an account gets locked — measured 2026-08-24,
            // nine attempts in twenty-five seconds against a bad system name.
            data: data_credential,
            trailing_dd: get_or("RITHMIC_TRAILING_DD", "0").parse().unwrap_or(0.0),
            starting_equity: get_or("RITHMIC_STARTING_EQUITY", "0")
                .parse()
                .unwrap_or(0.0),
            health_host: get_or("RITHMIC_HEALTH_HOST", "0.0.0.0"),
            health_port: get_or("RITHMIC_HEALTH_PORT", "9091")
                .parse()
                .unwrap_or(9091),
            questdb: parse_questdb(&get),
        }
    }

    /// Evaluate the capability gate. See [`GateDecision`].
    pub fn gate(&self) -> GateDecision {
        if !self.enabled {
            return GateDecision::Disabled;
        }

        // Enabled — every field required to *authenticate* must be present.
        // We check the URL and credentials; app_name/version/system_name always
        // have defaults so are never "missing".
        let mut missing = Vec::new();
        if self.gateway_url.trim().is_empty() {
            missing.push("RITHMIC_GATEWAY_URL".to_string());
        }
        if self.user.trim().is_empty() {
            missing.push("RITHMIC_USER".to_string());
        }
        if self.password.trim().is_empty() {
            missing.push("RITHMIC_PASSWORD".to_string());
        }

        if missing.is_empty() {
            GateDecision::Ready
        } else {
            GateDecision::MissingCredentials { missing }
        }
    }

    /// Whether the read-only positions subscription can run. Requires the full
    /// account triple in addition to a `Ready` gate. When false, market data
    /// still flows; only the PnL/positions reader is skipped.
    pub fn positions_ready(&self) -> bool {
        self.gate().is_ready()
            && !self.account_id.trim().is_empty()
            && !self.fcm_id.trim().is_empty()
            && !self.ib_id.trim().is_empty()
    }

    /// Resolve `RITHMIC_INSTRUMENT` into `(symbol, exchange)` pairs.
    ///
    /// Accepts a comma-separated list, and an optional `SYMBOL:EXCHANGE`
    /// override per entry; anything without an override inherits
    /// `RITHMIC_EXCHANGE`. So all of these are valid:
    ///
    /// ```text
    ///   GC                        -> [(GC, <default>)]
    ///   GC,NQ,ES                  -> all three on <default>
    ///   GC:COMEX,NQ:CME,ES:CME    -> explicit per-instrument venues
    /// ```
    ///
    /// Blank entries are skipped (so a trailing comma is harmless) and
    /// duplicates are dropped — subscribing the same symbol twice on one
    /// session would double-count every trade into its candle bucket.
    /// Returns an empty vec if nothing usable is configured; the caller treats
    /// that as "no market data to subscribe" rather than panicking.
    pub fn instruments(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        for raw in self.instrument.split(',') {
            let entry = raw.trim();
            if entry.is_empty() {
                continue;
            }
            let (sym, exch) = match entry.split_once(':') {
                Some((s, e)) if !s.trim().is_empty() && !e.trim().is_empty() => {
                    (s.trim(), e.trim())
                }
                // A malformed "SYM:" or ":EXCH" falls back to the default
                // exchange rather than silently subscribing to a bad venue.
                _ => (entry.trim_end_matches(':'), self.exchange.trim()),
            };
            if sym.is_empty() || exch.is_empty() {
                continue;
            }
            if out.iter().any(|(s, _)| s == sym) {
                continue;
            }
            out.push((sym.to_string(), exch.to_string()));
        }
        out
    }
}

/// Resolve the QuestDB ILP endpoint from the environment.
///
/// Precedence: `QUESTDB_ILP_URL` (an `host:port`, optionally `tcp://`-prefixed)
/// wins; otherwise `QUESTDB_ILP_HOST` (default `fks_questdb`) + `QUESTDB_ILP_PORT`
/// (default `9009`). Falls back to the in-network defaults on anything malformed
/// — the sink is best-effort and must never fail config parsing.
fn parse_questdb(get: &impl Fn(&str) -> Option<String>) -> QuestDbConfig {
    let default = QuestDbConfig::default();

    let non_empty = |k: &str| get(k).filter(|v| !v.trim().is_empty());

    if let Some(url) = non_empty("QUESTDB_ILP_URL") {
        // Strip an optional scheme, then split host:port.
        let hostport = url
            .trim()
            .strip_prefix("tcp://")
            .or_else(|| url.trim().strip_prefix("http://"))
            .unwrap_or(url.trim());
        if let Some((host, port)) = hostport.rsplit_once(':')
            && let Ok(port) = port.parse::<u16>()
            && !host.is_empty()
        {
            return QuestDbConfig {
                host: host.to_string(),
                port,
            };
        }
        // Bare host with no port → keep the default ILP port.
        if !hostport.is_empty() {
            return QuestDbConfig {
                host: hostport.to_string(),
                port: default.port,
            };
        }
    }

    let host = non_empty("QUESTDB_ILP_HOST").unwrap_or(default.host);
    let port = non_empty("QUESTDB_ILP_PORT")
        .and_then(|p| p.trim().parse::<u16>().ok())
        .unwrap_or(default.port);
    QuestDbConfig { host, port }
}

/// Parse a boolean env var leniently: true/1/yes/on (case-insensitive) → true.
fn parse_bool(v: Option<&str>, default: bool) -> bool {
    match v {
        Some(s) => matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        None => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build a getter closure over a fixed map — deterministic, no env mutation.
    fn getter(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn defaults_are_safe_when_env_empty() {
        let cfg = RithmicConfig::from_getter(getter(&[]));
        assert!(!cfg.enabled, "RITHMIC_ENABLED must default to false");
        assert_eq!(cfg.environment, RithmicEnvironment::Demo);
        assert_eq!(cfg.instrument, "MES");
        assert_eq!(cfg.exchange, "CME");
        assert_eq!(cfg.health_port, 9091);
        assert_eq!(cfg.system_name, "Rithmic Paper Trading");
    }

    #[test]
    fn parses_all_fields() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_ENABLED", "true"),
            ("RITHMIC_ENV", "test"),
            ("RITHMIC_GATEWAY_URL", "wss://rituz00100.rithmic.com:443"),
            ("RITHMIC_SYSTEM_NAME", "Rithmic Test"),
            ("RITHMIC_USER", "devuser"),
            ("RITHMIC_PASSWORD", "devpass"),
            ("RITHMIC_INSTRUMENT", "ES"),
            ("RITHMIC_EXCHANGE", "CME"),
            ("RITHMIC_HEALTH_PORT", "9099"),
        ]));
        assert!(cfg.enabled);
        assert_eq!(cfg.environment, RithmicEnvironment::Test);
        assert_eq!(cfg.gateway_url, "wss://rituz00100.rithmic.com:443");
        assert_eq!(cfg.system_name, "Rithmic Test");
        assert_eq!(cfg.instrument, "ES");
        assert_eq!(cfg.health_port, 9099);
        // Alt URL falls back to the primary when unset.
        assert_eq!(cfg.gateway_alt_url, cfg.gateway_url);
    }

    #[test]
    fn env_default_system_name_used_when_absent() {
        let cfg = RithmicConfig::from_getter(getter(&[("RITHMIC_ENV", "live")]));
        assert_eq!(cfg.environment, RithmicEnvironment::Live);
        assert_eq!(cfg.system_name, "Rithmic 01");
    }

    #[test]
    fn bool_parsing_is_lenient() {
        for truthy in ["1", "true", "TRUE", "yes", "On"] {
            let cfg = RithmicConfig::from_getter(getter(&[("RITHMIC_ENABLED", truthy)]));
            assert!(cfg.enabled, "{truthy} should be truthy");
        }
        for falsy in ["0", "false", "no", "", "garbage"] {
            let cfg = RithmicConfig::from_getter(getter(&[("RITHMIC_ENABLED", falsy)]));
            assert!(!cfg.enabled, "{falsy:?} should be falsy");
        }
    }

    // ─── the gate contract ────────────────────────────────────────────────

    #[test]
    fn gate_disabled_by_default() {
        let cfg = RithmicConfig::from_getter(getter(&[]));
        assert_eq!(cfg.gate(), GateDecision::Disabled);
        assert!(!cfg.gate().is_ready());
    }

    #[test]
    fn gate_disabled_even_with_full_creds() {
        // The disabled flag dominates: creds present but flag off ⇒ no-op.
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_GATEWAY_URL", "wss://gw.example:443"),
            ("RITHMIC_USER", "u"),
            ("RITHMIC_PASSWORD", "p"),
        ]));
        assert_eq!(cfg.gate(), GateDecision::Disabled);
    }

    #[test]
    fn gate_missing_credentials_lists_absent_fields() {
        let cfg = RithmicConfig::from_getter(getter(&[("RITHMIC_ENABLED", "true")]));
        match cfg.gate() {
            GateDecision::MissingCredentials { missing } => {
                assert!(missing.contains(&"RITHMIC_GATEWAY_URL".to_string()));
                assert!(missing.contains(&"RITHMIC_USER".to_string()));
                assert!(missing.contains(&"RITHMIC_PASSWORD".to_string()));
            }
            other => panic!("expected MissingCredentials, got {other:?}"),
        }
        assert!(!cfg.gate().is_ready());
    }

    #[test]
    fn gate_missing_only_password() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_ENABLED", "true"),
            ("RITHMIC_GATEWAY_URL", "wss://gw.example:443"),
            ("RITHMIC_USER", "u"),
        ]));
        assert_eq!(
            cfg.gate(),
            GateDecision::MissingCredentials {
                missing: vec!["RITHMIC_PASSWORD".to_string()]
            }
        );
    }

    #[test]
    fn gate_ready_when_enabled_and_credentialed() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_ENABLED", "true"),
            ("RITHMIC_GATEWAY_URL", "wss://gw.example:443"),
            ("RITHMIC_USER", "u"),
            ("RITHMIC_PASSWORD", "p"),
        ]));
        assert_eq!(cfg.gate(), GateDecision::Ready);
        assert!(cfg.gate().is_ready());
    }

    // ─── QuestDB ILP endpoint resolution ──────────────────────────────────

    #[test]
    fn questdb_defaults_to_in_network_fks_questdb() {
        let cfg = RithmicConfig::from_getter(getter(&[]));
        assert_eq!(cfg.questdb.host, "fks_questdb");
        assert_eq!(cfg.questdb.port, 9009);
        assert_eq!(cfg.questdb.ilp_addr(), "fks_questdb:9009");
    }

    #[test]
    fn questdb_host_and_port_from_split_env() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("QUESTDB_ILP_HOST", "questdb.internal"),
            ("QUESTDB_ILP_PORT", "9010"),
        ]));
        assert_eq!(cfg.questdb.host, "questdb.internal");
        assert_eq!(cfg.questdb.port, 9010);
    }

    #[test]
    fn questdb_url_overrides_and_strips_scheme() {
        for url in ["tcp://qdb:9999", "qdb:9999", "http://qdb:9999"] {
            let cfg = RithmicConfig::from_getter(getter(&[("QUESTDB_ILP_URL", url)]));
            assert_eq!(cfg.questdb.host, "qdb", "url={url}");
            assert_eq!(cfg.questdb.port, 9999, "url={url}");
        }
    }

    #[test]
    fn questdb_url_bare_host_keeps_default_port() {
        let cfg = RithmicConfig::from_getter(getter(&[("QUESTDB_ILP_URL", "just-a-host")]));
        assert_eq!(cfg.questdb.host, "just-a-host");
        assert_eq!(cfg.questdb.port, 9009);
    }

    #[test]
    fn positions_ready_needs_ready_gate_plus_account_triple() {
        let base = &[
            ("RITHMIC_ENABLED", "true"),
            ("RITHMIC_GATEWAY_URL", "wss://gw.example:443"),
            ("RITHMIC_USER", "u"),
            ("RITHMIC_PASSWORD", "p"),
        ];
        // Ready gate but no account triple ⇒ positions not ready.
        let cfg = RithmicConfig::from_getter(getter(base));
        assert!(cfg.gate().is_ready());
        assert!(!cfg.positions_ready());

        // Full account triple ⇒ positions ready.
        let mut with_acct = base.to_vec();
        with_acct.extend_from_slice(&[
            ("RITHMIC_ACCOUNT_ID", "ACCT1"),
            ("RITHMIC_FCM_ID", "FCM1"),
            ("RITHMIC_IB_ID", "IB1"),
        ]);
        let cfg = RithmicConfig::from_getter(getter(&with_acct));
        assert!(cfg.positions_ready());

        // Partial triple ⇒ not ready.
        let mut partial = base.to_vec();
        partial.push(("RITHMIC_ACCOUNT_ID", "ACCT1"));
        let cfg = RithmicConfig::from_getter(getter(&partial));
        assert!(!cfg.positions_ready());
    }

    #[test]
    fn positions_not_ready_when_gate_disabled() {
        // Account triple present but the connector is disabled ⇒ never reads.
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_ACCOUNT_ID", "ACCT1"),
            ("RITHMIC_FCM_ID", "FCM1"),
            ("RITHMIC_IB_ID", "IB1"),
        ]));
        assert_eq!(cfg.gate(), GateDecision::Disabled);
        assert!(!cfg.positions_ready());
    }

    #[test]
    fn gate_treats_whitespace_creds_as_missing() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_ENABLED", "true"),
            ("RITHMIC_GATEWAY_URL", "wss://gw.example:443"),
            ("RITHMIC_USER", "   "),
            ("RITHMIC_PASSWORD", "p"),
        ]));
        // Whitespace-only user is blanked by from_getter's empty filter, so the
        // gate must report it missing rather than attempting a bad login.
        assert!(!cfg.gate().is_ready());
    }

    // ── instruments() ────────────────────────────────────────────────────────
    // RITHMIC_INSTRUMENT became a LIST because Rithmic permits only ONE session
    // per credential (measured 2026-08-24: a second connector logging in killed
    // the first with `permission denied`), so one session must carry every
    // symbol. These pin the parsing, because a wrong exchange fails the
    // subscribe and a duplicate symbol double-counts every trade in its bucket.

    #[test]
    fn instruments_single_symbol_uses_the_default_exchange() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_INSTRUMENT", "GC"),
            ("RITHMIC_EXCHANGE", "COMEX"),
        ]));
        assert_eq!(cfg.instruments(), vec![("GC".into(), "COMEX".into())]);
    }

    #[test]
    fn instruments_splits_a_csv_list() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_INSTRUMENT", "GC,NQ,ES"),
            ("RITHMIC_EXCHANGE", "CME"),
        ]));
        assert_eq!(
            cfg.instruments(),
            vec![
                ("GC".into(), "CME".into()),
                ("NQ".into(), "CME".into()),
                ("ES".into(), "CME".into()),
            ]
        );
    }

    /// THE CASE THAT MATTERS for the real target set: GC is COMEX while NQ/ES
    /// are CME, so a single default exchange cannot serve all three and each
    /// needs its own venue.
    #[test]
    fn instruments_honours_per_symbol_exchange_overrides() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_INSTRUMENT", "GC:COMEX,NQ:CME,ES:CME"),
            ("RITHMIC_EXCHANGE", "CME"),
        ]));
        assert_eq!(
            cfg.instruments(),
            vec![
                ("GC".into(), "COMEX".into()),
                ("NQ".into(), "CME".into()),
                ("ES".into(), "CME".into()),
            ]
        );
    }

    #[test]
    fn instruments_mixes_overridden_and_default_venues() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_INSTRUMENT", "GC:COMEX,NQ"),
            ("RITHMIC_EXCHANGE", "CME"),
        ]));
        assert_eq!(
            cfg.instruments(),
            vec![("GC".into(), "COMEX".into()), ("NQ".into(), "CME".into())]
        );
    }

    #[test]
    fn instruments_tolerates_whitespace_and_trailing_commas() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_INSTRUMENT", " GC : COMEX , NQ , "),
            ("RITHMIC_EXCHANGE", "CME"),
        ]));
        assert_eq!(
            cfg.instruments(),
            vec![("GC".into(), "COMEX".into()), ("NQ".into(), "CME".into())]
        );
    }

    /// Subscribing the same symbol twice on one session would double-count
    /// every trade into its candle bucket — silently inflating volume and
    /// corrupting OHLC. Dedupe on first occurrence.
    #[test]
    fn instruments_drops_duplicates() {
        let cfg = RithmicConfig::from_getter(getter(&[
            ("RITHMIC_INSTRUMENT", "GC,NQ,GC"),
            ("RITHMIC_EXCHANGE", "CME"),
        ]));
        assert_eq!(
            cfg.instruments(),
            vec![("GC".into(), "CME".into()), ("NQ".into(), "CME".into())]
        );
    }

    #[test]
    fn instruments_is_empty_when_nothing_configured() {
        let cfg = RithmicConfig::from_getter(getter(&[("RITHMIC_INSTRUMENT", " , , ")]));
        assert!(
            cfg.instruments().is_empty(),
            "caller treats empty as 'nothing to subscribe', never a panic"
        );
    }
}

#[cfg(test)]
mod data_credential_tests {
    use super::*;
    use std::collections::HashMap;

    fn getter(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    const PRIMARY: &[(&str, &str)] = &[
        ("RITHMIC_ENABLED", "true"),
        ("RITHMIC_USER", "tpt_login"),
        ("RITHMIC_PASSWORD", "tpt_secret"),
        ("RITHMIC_GATEWAY_URL", "wss://rprotocol.rithmic.com"),
        ("RITHMIC_SYSTEM_NAME", "Rithmic Paper Trading"),
    ];

    fn with(extra: &[(&str, &str)]) -> RithmicConfig {
        let mut all = PRIMARY.to_vec();
        all.extend_from_slice(extra);
        RithmicConfig::from_getter(getter(&all))
    }

    #[test]
    fn one_login_means_no_data_credential_and_unchanged_behaviour() {
        // The pre-2026-09 shape. Both plants share the primary, and the kill
        // switch still governs market data because that session IS the one the
        // operator's terminal wants.
        assert!(with(&[]).data.is_none());
    }

    #[test]
    fn a_second_login_is_picked_up_and_inherits_what_it_does_not_override() {
        // The EdgeClear case: a different FCM, same gateway. Only user and
        // password need stating.
        let cfg = with(&[
            ("RITHMIC_DATA_USER", "edgeclear_login"),
            ("RITHMIC_DATA_PASSWORD", "edgeclear_secret"),
        ]);
        let d = cfg.data.expect("a usable data credential");
        assert_eq!(d.user, "edgeclear_login");
        assert_eq!(d.gateway_url, "wss://rprotocol.rithmic.com");
        assert_eq!(d.system_name, "Rithmic Paper Trading");
        // And it must be a DIFFERENT login from the trading one, or the whole
        // exercise is pointless.
        assert_ne!(d.user, cfg.user);
    }

    #[test]
    fn a_half_filled_data_credential_is_absent_not_attempted() {
        // A username with no password must NOT be tried. Repeated failed logins
        // are how a brokerage account gets locked — measured 2026-08-24, nine
        // attempts in twenty-five seconds against a bad system name. Falling
        // back to the single-credential path is the safe failure.
        assert!(
            with(&[("RITHMIC_DATA_USER", "edgeclear_login")])
                .data
                .is_none()
        );
        assert!(
            with(&[("RITHMIC_DATA_PASSWORD", "edgeclear_secret")])
                .data
                .is_none()
        );
        assert!(
            with(&[
                ("RITHMIC_DATA_USER", "   "),
                ("RITHMIC_DATA_PASSWORD", "edgeclear_secret"),
            ])
            .data
            .is_none(),
            "whitespace is not a username"
        );
    }

    #[test]
    fn a_data_credential_may_live_on_a_different_gateway() {
        // A different FCM can mean a different gateway host entirely.
        let cfg = with(&[
            ("RITHMIC_DATA_USER", "edgeclear_login"),
            ("RITHMIC_DATA_PASSWORD", "edgeclear_secret"),
            ("RITHMIC_DATA_GATEWAY_URL", "wss://edge.rithmic.com"),
            ("RITHMIC_DATA_SYSTEM_NAME", "Rithmic 01"),
        ]);
        let d = cfg.data.expect("usable");
        assert_eq!(d.gateway_url, "wss://edge.rithmic.com");
        assert_eq!(d.system_name, "Rithmic 01");
        // The trading credential is untouched by any of it.
        assert_eq!(cfg.gateway_url, "wss://rprotocol.rithmic.com");
        assert_eq!(cfg.system_name, "Rithmic Paper Trading");
    }
}
