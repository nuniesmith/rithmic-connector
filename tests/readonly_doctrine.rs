// =============================================================================
// readonly_doctrine.rs — MECHANICAL enforcement of the read-only doctrine.
//
// The connector must NEVER submit, modify, or cancel an order (see connector.rs
// header + crate CLAUDE.md "no autonomous execution"). BUT the full order-entry
// API (`RithmicOrderPlant`, `place_order`, `cancel_order`, `exit_position`, …)
// is re-exported by the `rithmic-rs` vendor crate and is COMPILED INTO this
// binary under the default `live-connect` feature — nothing in the language
// stops a future one-line edit from calling it against the same live-money
// credentials. Until this test, the only barrier was human code review.
//
// This test fails `cargo test` (and therefore CI) if any order-entry CALL or the
// order plant type ever appears in the crate's OWN source. The banned patterns
// are chosen to match real order code only — never the doctrine prose (which
// writes the bare word "RithmicOrderPlant" but never `.place_order(` etc.).
// =============================================================================

use std::fs;
use std::path::Path;

/// Order-entry patterns that appear ONLY in real order-submission code, never in
/// the read-only doctrine's comments. Extend this list if the vendor API grows.
const BANNED: &[&str] = &[
    ".place_order(",
    ".place_bracket_order(",
    ".place_oco_order(",
    ".modify_order(",
    ".cancel_order(",
    ".cancel_all_orders(",
    ".exit_position(",
    "RithmicOrderPlant::",
    "RithmicOrderPlantHandle",
    "use rithmic_rs::RithmicOrderPlant",
];

/// Recursively collect `(file, pattern)` hits for any banned pattern under `dir`.
fn scan(dir: &Path, hits: &mut Vec<String>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            scan(&path, hits);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let content = fs::read_to_string(&path).expect("read .rs file");
            for pat in BANNED {
                if content.contains(pat) {
                    hits.push(format!("{}  →  {pat}", path.display()));
                }
            }
        }
    }
}

/// The crate's `src/` must contain no order-entry usage. This is the build-time
/// barrier the read-only doctrine relies on (defense in depth beyond review).
#[test]
fn no_order_entry_in_source() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    scan(&src, &mut hits);
    assert!(
        hits.is_empty(),
        "READ-ONLY DOCTRINE VIOLATED — order-entry usage found in the connector source:\n  {}\n\
         \nThis crate must NEVER open the order plant or submit/modify/cancel an order. \
         If this is intentional, the doctrine itself is changing — that requires a deliberate, \
         reviewed decision, not a silent edit.",
        hits.join("\n  ")
    );
}
