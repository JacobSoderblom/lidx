/// Integration tests for the `outline` and `read_symbol` reading methods (issue #93).
///
/// `outline` is implemented (issue #95): a compact, no-bodies skeleton of an indexed
/// file's symbols in source order -- kind, qualname, signature, line range, nesting
/// parent (via the extractor's own `CONTAINS` edges), first doc line. Markdown files
/// (not a DB-indexed language -- see `is_markdown_path` in `src/rpc/handlers.rs`) list
/// ATX headings instead, read straight off disk.
///
/// `read_symbol` with a single `qualname`/`query` selector is implemented (issue #96):
/// exact source cut from disk by stored byte span, line-numbered, with staleness
/// detection against the indexed file hash. `qualnames` (multi-symbol reads),
/// `skeleton`, and `context_lines` stay a follow-up (#98) and are still asserted
/// against a "not implemented" stub below.
///
/// Setup mirrors `tests/repo_map.rs` / `tests/next_hops_validity.rs`: copy a fixture repo
/// into a temp dir and index it. Follow-up tickets extend this same file (and can reuse
/// `setup_repo`/`indexed`) once read_symbol's remaining params grow real behaviour.
mod common;

use lidx::indexer::Indexer;
use lidx::rpc::{self, METHOD_LIST};
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
    dir.push(format!("lidx-reading-methods-{label}-{nanos}-{counter}"));
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

/// Reusable setup helper -- copies `fixture` into a fresh temp dir and indexes it.
/// Follow-up tickets extending this file should keep using this.
fn setup_repo(fixture: &str) -> (PathBuf, PathBuf) {
    let src = fixture_path(fixture);
    let repo_root = temp_repo_dir(fixture);
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
}

fn indexed(fixture: &str) -> (Indexer, PathBuf) {
    let (repo_root, db_path) = setup_repo(fixture);
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (indexer, repo_root)
}

/// Like `indexed`, but builds the repo from inline source (`(rel_path, contents)`
/// pairs) rather than a fixture directory -- used for the `ext:` external-stub case,
/// which needs a minimal file with an unresolved external call rather than a full
/// fixture from `tests/fixtures/`.
fn indexed_from_source(label: &str, files: &[(&str, &str)]) -> (Indexer, PathBuf) {
    let repo_root = temp_repo_dir(label);
    common::write_files(&repo_root, files);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (indexer, repo_root)
}

fn assert_not_unknown_method(err_msg: &str) {
    assert!(
        !err_msg.to_lowercase().contains("unknown method"),
        "expected a not-implemented error, got 'unknown method': {}",
        err_msg
    );
}

fn assert_not_implemented(err_msg: &str) {
    assert!(
        err_msg.to_lowercase().contains("not implemented"),
        "expected a 'not implemented' error, got: {}",
        err_msg
    );
}

// --- registration ---

#[test]
fn outline_and_read_symbol_are_registered_methods() {
    assert!(
        METHOD_LIST.contains(&"outline"),
        "outline should be in METHOD_LIST"
    );
    assert!(
        METHOD_LIST.contains(&"read_symbol"),
        "read_symbol should be in METHOD_LIST"
    );
}

// --- outline ---

/// Rust fixture: a struct, an `impl` block with a method, and a nested `fn`
/// inside that method's body. Written inline (no checked-in fixture fits) so
/// exact qualnames/line ranges/parents are known -- values below were
/// confirmed against the real extractor output, not hand-derived.
const OUTLINE_RUST_SRC: &str = r#"struct Foo {
    pub x: i32,
}

impl Foo {
    pub fn bar(&self) -> i32 {
        fn helper() -> i32 {
            42
        }
        helper()
    }
}

fn top_level() -> i32 {
    1
}
"#;

fn entry<'a>(entries: &'a [serde_json::Value], qualname: &str) -> &'a serde_json::Value {
    entries
        .iter()
        .find(|e| e["qualname"] == qualname)
        .unwrap_or_else(|| panic!("no entry with qualname '{qualname}' in {entries:#?}"))
}

#[test]
fn outline_rust_struct_impl_nested_fn_lists_qualnames_kinds_lines_and_parents_in_order() {
    let (mut indexer, repo_root) =
        indexed_from_source("rust-struct-impl", &[("src/lib.rs", OUTLINE_RUST_SRC)]);

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "src/lib.rs"}),
    )
    .unwrap();

    assert_eq!(result["path"], "src/lib.rs");
    assert_eq!(result["language"], "rust");
    assert_eq!(
        result["total_lines"].as_i64().unwrap(),
        OUTLINE_RUST_SRC.lines().count() as i64
    );

    let entries = result["entries"].as_array().expect("entries array");
    // The file-root `module` symbol every extractor emits is hidden (redundant with
    // `path`/`language`), so only these four should appear, in source order.
    let qualnames: Vec<&str> = entries
        .iter()
        .filter_map(|e| e["qualname"].as_str())
        .collect();
    assert_eq!(
        qualnames,
        vec![
            "crate::Foo",
            "crate::Foo::bar",
            "crate::Foo::helper",
            "crate::top_level"
        ],
        "{entries:#?}"
    );
    assert!(
        !qualnames.contains(&"crate"),
        "the whole-file root module symbol shouldn't appear as an entry: {entries:#?}"
    );

    let foo = entry(entries, "crate::Foo");
    assert_eq!(foo["kind"], "struct");
    assert_eq!(foo["name"], "Foo");
    assert_eq!(foo["start_line"], 1);
    assert_eq!(foo["end_line"], 3);
    assert!(foo.get("parent").is_none(), "{foo:#?}");

    let bar = entry(entries, "crate::Foo::bar");
    assert_eq!(bar["kind"], "method");
    assert_eq!(bar["start_line"], 6);
    assert_eq!(bar["end_line"], 11);
    assert_eq!(bar["parent"], "crate::Foo");

    // `helper` is lexically nested inside `bar`'s body, but the extractor parents
    // it on the enclosing type (not the enclosing function -- there's no
    // function-scoped container), so it's a sibling of `bar` under `Foo`, not a
    // child of `bar`. This is real, current extractor behaviour, not an outline
    // artifact -- asserted here so a future extraction change shows up as a test
    // failure instead of a silent outline regression.
    let helper = entry(entries, "crate::Foo::helper");
    assert_eq!(helper["kind"], "method");
    assert_eq!(helper["start_line"], 7);
    assert_eq!(helper["end_line"], 9);
    assert_eq!(helper["parent"], "crate::Foo");

    let top_level = entry(entries, "crate::top_level");
    assert_eq!(top_level["kind"], "function");
    assert_eq!(top_level["start_line"], 14);
    assert_eq!(top_level["end_line"], 16);
    assert!(top_level.get("parent").is_none(), "{top_level:#?}");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_python_class_methods_nest_under_class() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "pkg/core.py"}),
    )
    .unwrap();

    assert_eq!(result["language"], "python");
    let entries = result["entries"].as_array().expect("entries array");
    let qualnames: Vec<&str> = entries
        .iter()
        .filter_map(|e| e["qualname"].as_str())
        .collect();
    assert_eq!(
        qualnames,
        vec![
            "pkg.core.Base",
            "pkg.core.Greeter",
            "pkg.core.Greeter.greet",
            "pkg.core.make_greeter",
        ],
        "{entries:#?}"
    );

    let base = entry(entries, "pkg.core.Base");
    assert_eq!(base["kind"], "class");
    assert!(base.get("parent").is_none(), "{base:#?}");

    let greeter = entry(entries, "pkg.core.Greeter");
    assert_eq!(greeter["kind"], "class");
    assert!(greeter.get("parent").is_none(), "{greeter:#?}");
    assert_eq!(greeter["doc"], "Greeter doc.");

    let greet = entry(entries, "pkg.core.Greeter.greet");
    assert_eq!(greet["kind"], "method");
    assert_eq!(greet["parent"], "pkg.core.Greeter");
    assert_eq!(greet["doc"], "Greets someone.");
    assert!(
        greet["signature"].as_str().is_some_and(|s| !s.is_empty()),
        "{greet:#?}"
    );

    let make_greeter = entry(entries, "pkg.core.make_greeter");
    assert_eq!(make_greeter["kind"], "function");
    assert!(make_greeter.get("parent").is_none(), "{make_greeter:#?}");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_markdown_headings_list_with_line_ranges_and_nesting() {
    let md = "# Title\n\
        \n\
        Some intro text.\n\
        \n\
        ## Section One\n\
        \n\
        Content one.\n\
        \n\
        ### Subsection\n\
        \n\
        Sub content.\n\
        \n\
        ## Section Two\n\
        \n\
        Content two.\n";
    let (mut indexer, repo_root) = indexed_from_source("markdown-headings", &[("NOTES.md", md)]);

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "NOTES.md"}),
    )
    .unwrap();

    assert_eq!(result["language"], "markdown");
    assert_eq!(
        result["total_lines"].as_i64().unwrap(),
        md.lines().count() as i64
    );

    let entries = result["entries"].as_array().expect("entries array");
    let kinds: Vec<&str> = entries.iter().filter_map(|e| e["kind"].as_str()).collect();
    assert_eq!(kinds, vec!["h1", "h2", "h3", "h2"], "{entries:#?}");

    let title = &entries[0];
    assert_eq!(title["name"], "Title");
    assert_eq!(title["qualname"], "Title");
    assert_eq!(title["start_line"], 1);
    assert_eq!(
        title["end_line"], 15,
        "nothing closes out the h1, so it runs to EOF"
    );
    assert!(title.get("parent").is_none(), "{title:#?}");

    let section_one = &entries[1];
    assert_eq!(section_one["name"], "Section One");
    assert_eq!(section_one["qualname"], "Title > Section One");
    assert_eq!(section_one["start_line"], 5);
    assert_eq!(
        section_one["end_line"], 12,
        "closed by Section Two at line 13"
    );
    assert_eq!(section_one["parent"], "Title");

    let subsection = &entries[2];
    assert_eq!(subsection["name"], "Subsection");
    assert_eq!(subsection["qualname"], "Title > Section One > Subsection");
    assert_eq!(subsection["start_line"], 9);
    assert_eq!(
        subsection["end_line"], 12,
        "closed by Section Two at line 13"
    );
    assert_eq!(subsection["parent"], "Title > Section One");

    let section_two = &entries[3];
    assert_eq!(section_two["name"], "Section Two");
    assert_eq!(section_two["qualname"], "Title > Section Two");
    assert_eq!(section_two["start_line"], 13);
    assert_eq!(section_two["end_line"], 15);
    // Section Two closes out both Subsection (h3) and Section One (h2), so its
    // parent is Title, not Section One.
    assert_eq!(section_two["parent"], "Title");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_kinds_filter_restricts_to_requested_kinds() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "pkg/core.py", "kinds": ["class"]}),
    )
    .unwrap();

    let entries = result["entries"].as_array().expect("entries array");
    let qualnames: Vec<&str> = entries
        .iter()
        .filter_map(|e| e["qualname"].as_str())
        .collect();
    assert_eq!(
        qualnames,
        vec!["pkg.core.Base", "pkg.core.Greeter"],
        "kinds=[class] should drop the method and the top-level function: {entries:#?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_max_depth_filter_limits_nesting() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // depth 0 = top-level (no parent in this file): Base, Greeter, make_greeter.
    // Greeter.greet is depth 1 (parent Greeter) and should be excluded.
    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "pkg/core.py", "max_depth": 0}),
    )
    .unwrap();

    let entries = result["entries"].as_array().expect("entries array");
    let qualnames: Vec<&str> = entries
        .iter()
        .filter_map(|e| e["qualname"].as_str())
        .collect();
    assert_eq!(
        qualnames,
        vec!["pkg.core.Base", "pkg.core.Greeter", "pkg.core.make_greeter"],
        "max_depth=0 should keep only top-level entries: {entries:#?}"
    );
    assert!(
        !qualnames.contains(&"pkg.core.Greeter.greet"),
        "max_depth=0 should drop the depth-1 method: {entries:#?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_excludes_ext_symbols() {
    let (mut indexer, repo_root) = indexed_from_source(
        "outline-ext-stub",
        &[(
            "caller.py",
            "import requests\n\n\ndef fetch():\n    return requests.get(\"https://example.com\")\n",
        )],
    );

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "caller.py"}),
    )
    .unwrap();

    let entries = result["entries"].as_array().expect("entries array");
    assert!(
        entries.iter().all(|e| {
            e["kind"] != "external"
                && !e["qualname"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with("ext:")
        }),
        "no entry should be an external stub: {entries:#?}"
    );
    // Sanity: the real, local `fetch` function is still there.
    assert!(
        entries.iter().any(|e| e["qualname"] == "caller.fetch"),
        "{entries:#?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_unindexed_path_errors_with_hint() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "pkg/does_not_exist.py"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    let lower = err_msg.to_lowercase();
    assert!(
        lower.contains("read") || lower.contains("reindex"),
        "expected a fallback hint (Read or reindex) in the error, got: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_is_materially_smaller_than_the_file_it_describes() {
    // Realistic-sized bodies (comments + several statements each), compact
    // signatures -- the outline (no bodies) should come out clearly smaller than
    // the source file, not just by a hair.
    let src = r#"/// Sums the squares of every integer in `0..x`, wrapping the running total
/// back down whenever it crosses the 1000 mark. Used by the billing job to
/// keep per-tenant usage counters bounded without a modulo on every add.
fn compute_one(x: i32) -> i32 {
    let mut total = 0;
    for i in 0..x {
        // Accumulate the square of this step.
        total += i * i;
        // Keep the running total bounded so downstream consumers never see
        // a value outside the expected 0..1000 range.
        if total > 1000 {
            total -= 1000;
        }
    }
    total
}

/// Applies a cheap pseudo-random mixing step `y` times to `x`. Not
/// cryptographically secure -- only used for deterministic test fixtures
/// that need "random-looking" but reproducible numbers.
fn compute_two(x: i32, y: i32) -> i32 {
    let mut result = x;
    for _ in 0..y {
        // Multiply-then-add is enough entropy for fixture data; a real PRNG
        // would use a proper algorithm here instead.
        result = result.wrapping_mul(2).wrapping_add(1);
    }
    result
}

/// Sums `values` modulo 97, folding as it goes rather than summing first,
/// so this stays correct even for slices long enough to overflow a running
/// i32 total before the final modulo.
fn compute_three(values: &[i32]) -> i32 {
    let mut acc = 0;
    for value in values {
        acc += value;
        // Fold back into range immediately -- see the doc comment above for
        // why this can't wait until after the loop.
        acc %= 97;
    }
    acc
}

/// A saturating-at-zero accumulator: negative additions clamp the running
/// total at zero instead of going negative, matching the semantics of the
/// on-disk usage counters this type mirrors in tests.
struct Accumulator {
    total: i64,
}

impl Accumulator {
    /// Starts a fresh accumulator at zero.
    fn new() -> Self {
        Accumulator { total: 0 }
    }

    /// Adds `value`, clamping at zero rather than allowing the total to go
    /// negative -- mirrors the on-disk counter's saturating-subtract behaviour.
    fn add(&mut self, value: i64) {
        self.total += value;
        if self.total < 0 {
            self.total = 0;
        }
    }
}
"#;
    let (mut indexer, repo_root) =
        indexed_from_source("outline-size-check", &[("src/lib.rs", src)]);

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "src/lib.rs"}),
    )
    .unwrap();

    let outline_bytes = serde_json::to_string(&result).unwrap().len();
    let file_bytes = src.len();
    assert!(
        outline_bytes < file_bytes,
        "outline ({outline_bytes} bytes) should be smaller than the file it describes \
         ({file_bytes} bytes): {result:#}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_missing_path_is_rejected() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(&mut indexer, "outline", serde_json::json!({}));

    assert!(
        result.is_err(),
        "outline without 'path' should be rejected by validation"
    );
    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert!(
        !err_msg.to_lowercase().contains("not implemented"),
        "missing-path rejection should be a validation error, not the not-implemented stub: {}",
        err_msg
    );

    let _ = std::fs::remove_dir_all(&_repo_root);
}

// --- read_symbol ---

#[test]
fn read_symbol_qualname_returns_exact_source_with_line_numbers() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    )
    .unwrap();

    assert_eq!(result["qualname"], "pkg.core.Greeter");
    assert_eq!(result["kind"], "class");
    assert_eq!(result["path"], "pkg/core.py");
    assert_eq!(result["stale"], false, "{result:#}");
    assert!(
        result.get("next_hops").is_none(),
        "a fresh (non-stale) read shouldn't emit a reindex hop: {result:#}"
    );

    let start_line = result["start_line"].as_i64().expect("start_line");
    let end_line = result["end_line"].as_i64().expect("end_line");
    assert_eq!(start_line, 9, "{result:#}");
    assert_eq!(end_line, 13, "{result:#}");

    // Build the expected line-numbered text straight from the file on disk using
    // the response's own start/end_line, rather than hardcoding fixture content.
    let content = std::fs::read_to_string(repo_root.join("pkg/core.py")).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    let expected_source = lines[(start_line as usize - 1)..(end_line as usize)]
        .iter()
        .enumerate()
        .map(|(i, line)| format!("{}: {}", start_line + i as i64, line))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        result["source"].as_str().unwrap(),
        expected_source,
        "source should be byte-identical to the fixture slice, line-numbered"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_query_resolves_same_symbol_as_explain_symbol() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let read_result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"query": "Greeter"}),
    )
    .unwrap();
    let explain_result = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"query": "Greeter"}),
    )
    .unwrap();

    assert!(
        read_result.get("ambiguous").is_none(),
        "expected an unambiguous resolution: {read_result:#}"
    );
    assert_eq!(read_result["qualname"], "pkg.core.Greeter");
    assert_eq!(
        read_result["qualname"].as_str().unwrap(),
        explain_result["symbol"]["qualname"].as_str().unwrap(),
        "read_symbol and explain_symbol must resolve a partial query to the same symbol"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_ambiguous_query_returns_candidates_and_no_source() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // py_mvp has both `pkg.b.helper` (function) and `pkg.utils.Helper` (class) --
    // a case-insensitive tie on the exact-name-match tier `find_symbols` ranks first.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"query": "helper"}),
    )
    .unwrap();

    assert_eq!(result["ambiguous"], true, "{result:#}");
    assert!(
        result.get("source").is_none(),
        "an ambiguous match must not return source: {result:#}"
    );
    let candidates = result["candidates"].as_array().expect("candidates array");
    let qualnames: Vec<&str> = candidates
        .iter()
        .filter_map(|c| c["qualname"].as_str())
        .collect();
    assert!(
        qualnames.contains(&"pkg.b.helper"),
        "expected pkg.b.helper among candidates: {qualnames:?}"
    );
    assert!(
        qualnames.contains(&"pkg.utils.Helper"),
        "expected pkg.utils.Helper among candidates: {qualnames:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_ext_qualname_is_not_found() {
    let (mut indexer, repo_root) = indexed_from_source(
        "ext-stub",
        &[(
            "caller.py",
            "import requests\n\n\ndef fetch():\n    return requests.get(\"https://example.com\")\n",
        )],
    );

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "ext:requests.get"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("not found"),
        "expected a not-found error for an ext: qualname, got: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_stale_file_flags_and_still_returns_text() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let before = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    )
    .unwrap();
    assert_eq!(before["stale"], false, "{before:#}");

    // Append a trailing comment on disk without reindexing -- doesn't touch
    // Greeter's byte span, so the slice stays valid, but the file hash changes.
    let file_path = repo_root.join("pkg/core.py");
    let mut content = std::fs::read_to_string(&file_path).unwrap();
    content.push_str("\n# trailing comment, added after indexing\n");
    std::fs::write(&file_path, content).unwrap();

    let after = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    )
    .unwrap();

    assert_eq!(after["stale"], true, "{after:#}");
    assert_eq!(
        after["source"], before["source"],
        "stale text should still be returned, unchanged: {after:#}"
    );
    let hops = after["next_hops"]
        .as_array()
        .expect("stale read should emit next_hops");
    assert!(
        hops.iter().any(|h| h["method"] == "reindex"),
        "expected a reindex next hop on a stale read: {hops:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_missing_file_errors_with_reindex_hint() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    std::fs::remove_file(repo_root.join("pkg/core.py")).unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("reindex"),
        "expected a reindex hint in the missing-file error, got: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_with_qualnames_list_returns_not_implemented_error() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": ["pkg.core.Greeter", "pkg.utils.helper"]}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert_not_implemented(&err_msg);

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_accepts_skeleton_and_context_lines_params_but_ignores_them() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    // skeleton/context_lines are part of the final param schema (#98 scope), so
    // passing them must not fail validation -- but #96 doesn't implement them yet,
    // so a single-symbol qualname read still succeeds normally, ignoring both.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter", "skeleton": true, "context_lines": 3}),
    )
    .unwrap();

    assert_eq!(result["qualname"], "pkg.core.Greeter");
    assert!(result.get("source").is_some(), "{result:#}");

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_with_no_selector_is_rejected() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(&mut indexer, "read_symbol", serde_json::json!({}));

    assert!(
        result.is_err(),
        "read_symbol with none of qualname/query/qualnames should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert!(
        !err_msg.to_lowercase().contains("not implemented"),
        "no-selector rejection should be a validation error, not the not-implemented stub: {}",
        err_msg
    );

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_with_multiple_selectors_is_rejected() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter", "query": "Greeter"}),
    );

    assert!(
        result.is_err(),
        "read_symbol with more than one of qualname/query/qualnames should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert!(
        !err_msg.to_lowercase().contains("not implemented"),
        "multi-selector rejection should be a validation error, not the not-implemented stub: {}",
        err_msg
    );

    let _ = std::fs::remove_dir_all(&_repo_root);
}
