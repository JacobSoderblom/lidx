use lidx::context;
use lidx::db::Db;
use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-ctx-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

struct TempRepo {
    pub repo_root: PathBuf,
    pub db_path: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.repo_root);
    }
}

impl TempRepo {
    fn new(fixture: &str) -> Self {
        let src = fixture_path(fixture);
        let repo_root = temp_repo_dir(fixture);
        copy_dir(&src, &repo_root);
        let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
        Self { repo_root, db_path }
    }

    fn with_files(label: &str, files: &[(String, String)]) -> Self {
        let repo_root = temp_repo_dir(label);
        for (rel, body) in files {
            let path = repo_root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        }
        let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
        Self { repo_root, db_path }
    }

    fn index(&self) -> Indexer {
        let mut indexer = Indexer::new(self.repo_root.clone(), self.db_path.clone()).unwrap();
        indexer.reindex().unwrap();
        indexer
    }
}

#[test]
fn context_symbols_for_file() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/core.py", gv).unwrap();
    assert_eq!(ctx.path, "pkg/core.py");
    assert!(
        ctx.symbol_summary.contains("symbols"),
        "Expected symbol summary, got: {}",
        ctx.symbol_summary
    );
    // core.py has: module, Base class, Greeter class, greet method, make_greeter function, imports
    assert!(
        !ctx.symbol_summary.starts_with("0 symbols"),
        "Expected non-zero symbols"
    );
}

#[test]
fn context_cross_file_callers() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // pkg/b.py defines helper() which is called by pkg/a.py
    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/b.py", gv).unwrap();

    // Should show a.py as a caller of helper()
    let caller_files: Vec<&str> = ctx
        .cross_file_callers
        .iter()
        .map(|c| c.file_path.as_str())
        .collect();
    assert!(
        caller_files.iter().any(|f| f.contains("a.py")),
        "Expected pkg/a.py as caller. Callers: {:?}",
        ctx.cross_file_callers
    );
}

#[test]
fn context_cross_file_callees() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // app.py calls make_greeter() and Greeter.greet() from pkg/core.py
    let ctx = context::build_file_context(db, &temp.repo_root, "app.py", gv).unwrap();

    // Should show callees from core.py
    // app.py imports from pkg.core and calls make_greeter/greet
    assert!(
        !ctx.cross_file_callees.is_empty(),
        "Expected callees from app.py. Got none."
    );
}

#[test]
fn context_missing_db() {
    let dir = temp_repo_dir("nodb");
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    // DB doesn't exist — Db::new will create it but no symbols
    let db = Db::new(&db_path).unwrap();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(&db, &dir, "nonexistent.py", gv).unwrap();
    assert_eq!(ctx.symbol_summary, "0 symbols");
    assert!(ctx.cross_file_callers.is_empty());
    assert!(ctx.cross_file_callees.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn context_text_format() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/core.py", gv).unwrap();
    let text = context::format_text(&ctx);

    // Should start with file header
    assert!(
        text.starts_with("# pkg/core.py"),
        "Expected header, got: {}",
        text
    );
    assert!(text.contains("symbols"));
}

#[test]
fn context_text_format_empty() {
    let dir = temp_repo_dir("empty-text");
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let db = Db::new(&db_path).unwrap();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(&db, &dir, "nonexistent.py", gv).unwrap();
    let text = context::format_text(&ctx);
    assert!(text.is_empty(), "Expected empty text for no-symbol file");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn context_rpc_method() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "context",
        serde_json::json!({"path": "pkg/core.py"}),
    )
    .unwrap();

    // Default format is text
    assert!(
        result.get("context").is_some(),
        "Expected 'context' key in response: {:?}",
        result
    );
    let text = result["context"].as_str().unwrap();
    assert!(text.contains("pkg/core.py"));
}

#[test]
fn context_rpc_method_json_format() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "context",
        serde_json::json!({"path": "pkg/core.py", "format": "json"}),
    )
    .unwrap();

    // JSON format returns the full struct
    assert!(
        result.get("path").is_some(),
        "Expected 'path' key in JSON response: {:?}",
        result
    );
    assert_eq!(result["path"].as_str().unwrap(), "pkg/core.py");
    assert!(result.get("symbol_summary").is_some());
}

#[test]
fn context_rpc_method_graph_version_aliases() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    let baseline = rpc::handle_method(
        &mut indexer,
        "context",
        serde_json::json!({"path": "pkg/core.py"}),
    )
    .unwrap();

    // "as_of" and "version" are accepted aliases for "graph_version". Serde
    // silently drops unknown keys, so equality with the baseline alone would
    // not prove an alias is wired — also query a version with no symbols and
    // require the output to change, which shows the value was consumed.
    for alias in ["graph_version", "as_of", "version"] {
        let result = rpc::handle_method(
            &mut indexer,
            "context",
            serde_json::json!({"path": "pkg/core.py", alias: gv}),
        )
        .unwrap();
        assert_eq!(
            result, baseline,
            "context with {alias}={gv} should match the default-version output"
        );

        let empty = rpc::handle_method(
            &mut indexer,
            "context",
            serde_json::json!({"path": "pkg/core.py", alias: gv + 1}),
        )
        .unwrap();
        assert_ne!(
            empty,
            baseline,
            "context with {alias}={} (no symbols) should differ from the current-version output",
            gv + 1
        );
    }
}

#[test]
fn context_rpc_method_missing_path_errors() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let err = rpc::handle_method(&mut indexer, "context", serde_json::json!({})).unwrap_err();
    assert!(
        err.to_string().contains("path"),
        "missing-path error should mention 'path', got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Cross-file reference naming (#252)
// ---------------------------------------------------------------------------

const TS_CALLER: &str = "src/tables/get.ts";
const TS_CALLEE: &str = "src/get/query.ts";

/// Python pair (`use.py` calls `lib.py`, with a duplicate call) plus a
/// TypeScript pair where `getTable` calls the imported `findCatalogItem` and
/// also an unresolvable `missingThing`.
fn name_fixture() -> TempRepo {
    let files = vec![
        (
            TS_CALLER.to_string(),
            "import { findCatalogItem } from '../get/query.js';\n\
             export function getTable(id: string) {\n  \
             return findCatalogItem(id) + missingThing(id);\n}\n"
                .to_string(),
        ),
        (
            TS_CALLEE.to_string(),
            "export function findCatalogItem(id: string) {\n  return id;\n}\n".to_string(),
        ),
        (
            "lib.py".to_string(),
            "def f0():\n    return 0\ndef f1():\n    return 1\n".to_string(),
        ),
        (
            "use.py".to_string(),
            "from lib import f0, f1\ndef g0():\n    return f0()\n\
             def g1():\n    return f0() + f0() + f1()\n"
                .to_string(),
        ),
    ];
    TempRepo::with_files("names", &files)
}

const FIXTURE_FILES: [&str; 4] = [TS_CALLER, TS_CALLEE, "lib.py", "use.py"];

fn names(refs: &[context::CrossRef]) -> Vec<(String, String)> {
    let mut v: Vec<_> = refs
        .iter()
        .map(|c| (c.symbol_name.clone(), c.file_path.clone()))
        .collect();
    v.sort();
    v
}

fn pair(name: &str, file: &str) -> (String, String) {
    (name.to_string(), file.to_string())
}

fn rpc_context(indexer: &mut Indexer, path: &str, format: &str) -> serde_json::Value {
    rpc::handle_method(
        indexer,
        "context",
        serde_json::json!({"path": path, "format": format}),
    )
    .unwrap()
}

#[test]
fn context_prints_exact_resolved_qualnames_for_callers_and_callees() {
    let temp = name_fixture();
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    let lib = context::build_file_context(db, &temp.repo_root, "lib.py", gv).unwrap();
    assert_eq!(
        names(&lib.cross_file_callers),
        vec![pair("use.g0", "use.py"), pair("use.g1", "use.py")]
    );

    let user = context::build_file_context(db, &temp.repo_root, "use.py", gv).unwrap();
    assert_eq!(
        names(&user.cross_file_callees),
        vec![pair("lib.f0", "lib.py"), pair("lib.f1", "lib.py")]
    );

    // Precondition that makes this test pin the bug: the extractor's raw guess
    // on these edges differs from the resolved qualname (`use.f0` vs `lib.f0`).
    let use_syms = db.get_symbols_for_file("use.py", gv).unwrap();
    let ids: Vec<i64> = use_syms.iter().map(|s| s.id).collect();
    let mut checked = 0;
    for edge in db
        .edges_for_symbols(&ids, None, gv)
        .unwrap()
        .values()
        .flatten()
        .filter(|e| e.kind == "CALLS")
    {
        let target = db
            .get_symbol_by_id(edge.target_symbol_id.unwrap())
            .unwrap()
            .unwrap();
        assert_ne!(
            edge.target_qualname.as_deref(),
            Some(target.qualname.as_str()),
            "fixture must keep raw guess != resolved qualname"
        );
        assert!(
            user.cross_file_callees
                .iter()
                .any(|c| c.symbol_name == target.qualname),
            "callee {} not printed",
            target.qualname
        );
        checked += 1;
    }
    assert!(checked >= 2, "expected the CALLS edges to be inspected");
}

#[test]
fn context_ts_callee_name_matches_file_beside_it() {
    let temp = name_fixture();
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // Raw guess is prefixed with the *caller's* module and names nothing real.
    let syms = db.get_symbols_for_file(TS_CALLER, gv).unwrap();
    let ids: Vec<i64> = syms.iter().map(|s| s.id).collect();
    let raw: Vec<String> = db
        .edges_for_symbols(&ids, None, gv)
        .unwrap()
        .values()
        .flatten()
        .filter(|e| e.kind == "CALLS" && e.target_symbol_id.is_some())
        .filter_map(|e| e.target_qualname.clone())
        .collect();
    assert_eq!(raw, vec!["src/tables/get.findCatalogItem".to_string()]);

    let ctx = context::build_file_context(db, &temp.repo_root, TS_CALLER, gv).unwrap();
    assert_eq!(
        names(&ctx.cross_file_callees),
        vec![pair("src/get/query.findCatalogItem", TS_CALLEE)]
    );
    let callee = &ctx.cross_file_callees[0];
    assert!(
        callee
            .symbol_name
            .starts_with(callee.file_path.trim_end_matches(".ts")),
        "name {} must belong to the file {} printed beside it",
        callee.symbol_name,
        callee.file_path
    );

    let callee_ctx = context::build_file_context(db, &temp.repo_root, TS_CALLEE, gv).unwrap();
    assert_eq!(
        names(&callee_ctx.cross_file_callers),
        vec![pair("src/tables/get.getTable", TS_CALLER)]
    );
}

#[test]
fn context_unresolved_targets_are_omitted_not_guessed() {
    let temp = name_fixture();
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(db, &temp.repo_root, TS_CALLER, gv).unwrap();
    // getTable also calls `missingThing`, which resolves to nothing.
    assert_eq!(
        ctx.cross_file_callees.len(),
        1,
        "{:?}",
        ctx.cross_file_callees
    );
    let text = context::format_text(&ctx);
    assert!(!text.contains("missingThing"), "{text}");
    assert!(!text.contains('?'), "{text}");
}

#[test]
fn context_no_id_prefix_in_text_or_json() {
    let temp = name_fixture();
    let mut indexer = temp.index();

    let mut saw_caller = false;
    for path in FIXTURE_FILES {
        let text = rpc_context(&mut indexer, path, "text")["context"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(!text.contains("id:"), "{path}: {text}");
        let json = rpc_context(&mut indexer, path, "json");
        assert!(!json.to_string().contains("id:"), "{path}: {json}");
        saw_caller |= !json["cross_file_callers"].as_array().unwrap().is_empty();
    }
    assert!(saw_caller, "fixture produced no callers to check");
}

#[test]
fn context_printed_names_round_trip_through_read_symbol() {
    let temp = name_fixture();
    let mut indexer = temp.index();

    let mut printed = Vec::new();
    for path in FIXTURE_FILES {
        let json = rpc_context(&mut indexer, path, "json");
        for key in ["cross_file_callers", "cross_file_callees"] {
            for r in json[key].as_array().unwrap() {
                printed.push(r["symbol_name"].as_str().unwrap().to_string());
            }
        }
    }
    printed.sort();
    printed.dedup();
    assert_eq!(
        printed,
        vec![
            "lib.f0",
            "lib.f1",
            "src/get/query.findCatalogItem",
            "src/tables/get.getTable",
            "use.g0",
            "use.g1",
        ]
    );

    for name in &printed {
        let read = rpc::handle_method(
            &mut indexer,
            "read_symbol",
            serde_json::json!({"qualname": name}),
        )
        .unwrap();
        assert_eq!(
            read["qualname"].as_str(),
            Some(name.as_str()),
            "read_symbol did not resolve {name} exactly: {read}"
        );
        assert!(read.get("resolved").is_none(), "{name}: {read}");
    }
}

#[test]
fn context_and_explain_symbol_report_same_qualnames() {
    let temp = name_fixture();
    let mut indexer = temp.index();

    let ctx = rpc_context(&mut indexer, TS_CALLER, "json");
    let ctx_callee = ctx["cross_file_callees"][0]["symbol_name"]
        .as_str()
        .unwrap()
        .to_string();
    let explain = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"qualname": "src/tables/get.getTable"}),
    )
    .unwrap();
    let explained: Vec<&str> = explain["callees"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["symbol"]["qualname"].as_str().unwrap())
        .collect();
    assert_eq!(explained, vec![ctx_callee.as_str()]);

    let ctx = rpc_context(&mut indexer, TS_CALLEE, "json");
    let ctx_caller = ctx["cross_file_callers"][0]["symbol_name"]
        .as_str()
        .unwrap()
        .to_string();
    let explain = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"qualname": "src/get/query.findCatalogItem"}),
    )
    .unwrap();
    let explained: Vec<&str> = explain["callers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["symbol"]["qualname"].as_str().unwrap())
        .collect();
    assert_eq!(explained, vec![ctx_caller.as_str()]);
}

#[test]
fn context_dedups_repeated_calls_and_caps_lists() {
    // `use.py` calls f0 three times from g1 and once from g0: callees dedup to
    // one entry per target, callers to one per calling symbol (checked in
    // context_prints_exact_resolved_qualnames_for_callers_and_callees too).
    let temp = name_fixture();
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();
    let user = context::build_file_context(db, &temp.repo_root, "use.py", gv).unwrap();
    assert_eq!(
        user.cross_file_callees.len(),
        2,
        "{:?}",
        user.cross_file_callees
    );

    // 20 distinct functions in lib2.py, each called from its own function in
    // use2.py: both lists must stop at the 15-entry cap with distinct names.
    let n = 20;
    let lib: String = (0..n)
        .map(|i| format!("def h{i}():\n    return {i}\n"))
        .collect();
    let imports = (0..n)
        .map(|i| format!("h{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let user_src: String = (0..n)
        .map(|i| format!("def c{i}():\n    return h{i}()\n"))
        .collect();
    let temp = TempRepo::with_files(
        "caps",
        &[
            ("lib2.py".to_string(), lib),
            (
                "use2.py".to_string(),
                format!("from lib2 import {imports}\n{user_src}"),
            ),
        ],
    );
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    for (path, pick) in [("lib2.py", true), ("use2.py", false)] {
        let ctx = context::build_file_context(db, &temp.repo_root, path, gv).unwrap();
        let refs = if pick {
            &ctx.cross_file_callers
        } else {
            &ctx.cross_file_callees
        };
        assert_eq!(refs.len(), 15, "{path}: cap not applied: {refs:?}");
        let mut uniq = names(refs);
        uniq.dedup();
        assert_eq!(uniq.len(), 15, "{path}: duplicates in {refs:?}");
    }
}
