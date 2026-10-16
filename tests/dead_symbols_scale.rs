//! Issue #530: `dead_symbols` cost grew with candidates x unresolved
//! references because a per-candidate subquery scanned every unresolved
//! reference of the graph version. A repository with thousands of dead
//! candidates and tens of thousands of unresolved references must still be
//! judged in a few seconds.

mod common;

use lidx::rpc;
use serde_json::json;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

const FILES: usize = 30;
const DEAD_PER_FILE: usize = 40;
const TYPE_FILES: usize = 20;
const TYPES_PER_FILE: usize = 150;
const EXTERNAL_CALLS_PER_FILE: usize = 300;
/// Generous: the quadratic shape took a multiple of this on the same size.
const BOUND: Duration = Duration::from_secs(10);

fn generate() -> Vec<(String, String)> {
    let mut files = Vec::new();
    for f in 0..FILES {
        let mut py = String::from("import ext\n\n");
        let mut ts = String::new();
        let mut rs = String::new();
        let mut cs = format!("public class Svc{f}\n{{\n");
        for d in 0..DEAD_PER_FILE {
            let _ = writeln!(py, "def dead_{f}_{d}():\n    pass\n");
            let _ = writeln!(ts, "function dead{f}x{d}() {{ return {d}; }}");
            let _ = writeln!(rs, "fn dead_{f}_{d}() {{}}");
            let _ = writeln!(cs, "    private void Dead{f}x{d}() {{ }}");
        }
        py.push_str("def caller():\n");
        ts.push_str("export function caller() {\n");
        rs.push_str("pub fn caller() {\n");
        cs.push_str("    public void Caller()\n    {\n");
        for c in 0..EXTERNAL_CALLS_PER_FILE {
            let _ = writeln!(py, "    ext.call_{c}()");
            let _ = writeln!(ts, "  ext.call{c}();");
            let _ = writeln!(rs, "    ext::call_{c}();");
            let _ = writeln!(cs, "        Ext.Call{c}();");
        }
        cs.push_str("    }\n}\n");
        ts.push_str("}\n");
        rs.push_str("}\n");
        files.push((format!("py/m{f}.py"), py));
        files.push((format!("ts/m{f}.ts"), ts));
        files.push((format!("rs/m{f}.rs"), rs));
        files.push((format!("cs/Svc{f}.cs"), cs));
    }
    // Many small types: judging a type walks its CONTAINS subtree, which must
    // not scan every CONTAINS edge of the index per step.
    for f in 0..TYPE_FILES {
        let mut cs = String::new();
        for t in 0..TYPES_PER_FILE {
            let _ = writeln!(
                cs,
                "public class Box{f}x{t}\n{{\n    private int a;\n    private void M() {{ }}\n}}"
            );
        }
        files.push((format!("cs/Boxes{f}.cs"), cs));
    }
    files
}

#[test]
fn dead_symbols_cost_does_not_scale_with_unresolved_references() {
    let generated = generate();
    let files: Vec<(&str, &str)> = generated
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    let (_tmp, _root, mut indexer) = common::live_indexer("lidx-dead-scale-", &files);

    let start = Instant::now();
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        json!({"include_unused_imports": false, "include_orphan_tests": false, "limit": 100000}),
    )
    .unwrap();
    let elapsed = start.elapsed();

    let total = result["dead_symbols_total_available"].as_u64().unwrap() as usize;
    assert!(
        total >= FILES * DEAD_PER_FILE,
        "the fixture's dead functions are reported: {total}"
    );
    assert!(
        elapsed < BOUND,
        "dead_symbols took {elapsed:?} for {total} claims"
    );
}
