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
/// detection against the indexed file hash.
///
/// `qualnames` (multi-symbol reads), `skeleton`, and `context_lines` are implemented
/// (issue #98): `qualnames` fills a `symbols` array in request order under a shared
/// `max_bytes` budget, omitting trailing symbols whole (by qualname, under `omitted`)
/// rather than truncating one mid-body; `skeleton` on a container symbol returns its
/// children's signatures/line ranges (via the same `CONTAINS`-edge nesting `outline`
/// uses) instead of the full body, with a bounded `read_symbol` next_hop per child;
/// `context_lines` widens the returned source by N lines each side, clamped at file
/// bounds.
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
        serde_json::json!({"qualname": "pkg.core.Greeter.greet"}),
    )
    .unwrap();

    assert_eq!(result["qualname"], "pkg.core.Greeter.greet");
    assert_eq!(result["kind"], "method");
    assert_eq!(result["path"], "pkg/core.py");
    assert_eq!(result["stale"], false, "{result:#}");
    assert!(
        result.get("next_hops").is_none(),
        "a fresh (non-stale) read shouldn't emit a reindex hop: {result:#}"
    );

    let start_line = result["start_line"].as_i64().expect("start_line");
    let end_line = result["end_line"].as_i64().expect("end_line");
    assert_eq!(start_line, 11, "{result:#}");
    assert_eq!(end_line, 13, "{result:#}");

    // Build the expected line-numbered text from the REAL file lines
    // (start_line..=end_line), not a raw byte slice -- `greet` is indented
    // (nested inside `Greeter`), so its stored start_byte lands after the
    // leading whitespace on line 11. The returned source must still carry
    // that whitespace, matching the file exactly line for line.
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
        "source should be byte-identical to the real file lines, line-numbered"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_context_lines_does_not_panic_when_file_shrank_after_indexing() {
    // `foo` is defined at line 10 -- indexed with that start_line/end_line
    // stored in the DB.
    let src = "# 1\n# 2\n# 3\n# 4\n# 5\n# 6\n# 7\n# 8\n# 9\ndef foo():\n    return 1\n";
    let (mut indexer, repo_root) = indexed_from_source("shrink-panic", &[("m.py", src)]);

    let before = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "m.foo"}),
    )
    .unwrap();
    assert_eq!(before["start_line"], 10, "{before:#}");

    // Overwrite the file with a single line of the SAME total byte length,
    // without reindexing: `slice_bytes` on the stale start_byte/end_byte can
    // still succeed (both offsets remain within the new content's length),
    // but the file now has far fewer lines than the indexed start_line/end_line.
    let replacement = "a".repeat(src.len().saturating_sub(1)) + "\n";
    assert_eq!(
        replacement.len(),
        src.len(),
        "fixture must keep the byte length constant"
    );
    std::fs::write(repo_root.join("m.py"), &replacement).unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "m.foo", "context_lines": 2}),
    );
    assert!(
        result.is_ok(),
        "context_lines must not panic when the indexed start_line/end_line outlive \
         the file's current line count: {result:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_does_not_panic_when_stale_start_byte_lands_mid_multibyte_char() {
    // `foo`'s indexed start_byte is 6 (`"x = 1\n"` is 6 ASCII bytes).
    let src = "x = 1\ndef foo():\n    return 1\n";
    let (mut indexer, repo_root) = indexed_from_source("stale-multibyte", &[("m.py", src)]);

    let before = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "m.foo"}),
    )
    .unwrap();
    assert_eq!(before["start_line"], 2, "{before:#}");

    // Overwrite the file with multibyte characters, without reindexing: byte
    // offset 6 (the stale start_byte) now lands in the middle of a 2-byte
    // UTF-8 'é' rather than on a char boundary -- indexing the new content at
    // that raw offset must not panic.
    let replacement = format!("a{}\n", "é".repeat(22));
    std::fs::write(repo_root.join("m.py"), &replacement).unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "m.foo"}),
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("reindex"),
        "expected the pre-existing 'symbol span no longer valid ... reindex' error, got: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_qualnames_does_not_panic_when_stale_start_byte_lands_mid_multibyte_char() {
    let src = "x = 1\ndef foo():\n    return 1\n";
    let (mut indexer, repo_root) =
        indexed_from_source("stale-multibyte-multi", &[("m.py", src)]);

    let replacement = format!("a{}\n", "é".repeat(22));
    std::fs::write(repo_root.join("m.py"), &replacement).unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": ["m.foo"]}),
    )
    .unwrap();

    let symbols = result["symbols"].as_array().expect("symbols array");
    assert!(symbols.is_empty(), "{result:#}");
    let errors = result["errors"].as_array().expect("errors array");
    assert_eq!(errors.len(), 1, "{result:#}");
    assert_eq!(errors[0]["qualname"], "m.foo", "{errors:?}");
    assert!(
        errors[0]["error"]
            .as_str()
            .is_some_and(|e| e.to_lowercase().contains("reindex")),
        "{errors:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_qualnames_missing_file_records_per_qualname_error_and_continues() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // pkg/b.py is deleted from disk (without reindexing), so pkg.b.helper's
    // span can no longer be read, while pkg.core.Greeter's file is untouched.
    std::fs::remove_file(repo_root.join("pkg/b.py")).unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": ["pkg.b.helper", "pkg.core.Greeter"]}),
    )
    .unwrap();

    let symbols = result["symbols"].as_array().expect("symbols array");
    let qualnames: Vec<&str> = symbols
        .iter()
        .filter_map(|s| s["qualname"].as_str())
        .collect();
    assert_eq!(
        qualnames,
        vec!["pkg.core.Greeter"],
        "the symbol whose file is missing should be skipped, not abort the whole call: {result:#}"
    );

    let errors = result["errors"]
        .as_array()
        .expect("errors array for the failed qualname");
    assert_eq!(errors.len(), 1, "{result:#}");
    assert_eq!(errors[0]["qualname"], "pkg.b.helper", "{errors:?}");
    assert!(
        errors[0]["error"].as_str().is_some_and(|e| !e.is_empty()),
        "{errors:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_markdown_ignores_hash_lines_inside_fenced_code_blocks() {
    let md = "# Title\n\
        \n\
        ```\n\
        # not a heading, this is code\n\
        ```\n\
        \n\
        ## Real Heading\n\
        \n\
        ~~~\n\
        # also not a heading\n\
        ~~~\n\
        \n\
        ## Another Real Heading\n";
    let (mut indexer, repo_root) = indexed_from_source("markdown-fenced-code", &[("NOTES.md", md)]);

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "NOTES.md"}),
    )
    .unwrap();

    let entries = result["entries"].as_array().expect("entries array");
    let names: Vec<&str> = entries.iter().filter_map(|e| e["name"].as_str()).collect();
    assert_eq!(
        names,
        vec!["Title", "Real Heading", "Another Real Heading"],
        "hash lines inside ``` and ~~~ fences must not be parsed as headings: {entries:#?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_markdown_heading_only_strips_closing_hashes_preceded_by_space() {
    let md = "## F#\n\
        \n\
        ## Closed Heading ##\n";
    let (mut indexer, repo_root) = indexed_from_source("markdown-hash-suffix", &[("NOTES.md", md)]);

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "NOTES.md"}),
    )
    .unwrap();

    let entries = result["entries"].as_array().expect("entries array");
    let names: Vec<&str> = entries.iter().filter_map(|e| e["name"].as_str()).collect();
    assert_eq!(
        names,
        vec!["F#", "Closed Heading"],
        "a trailing '#' that isn't preceded by a space is real text, not a closing \
         sequence to strip: {entries:#?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_rejects_absolute_path() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "/etc/passwd"}),
    );

    assert!(
        result.is_err(),
        "an absolute path must be rejected: {result:?}"
    );
    let err_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        err_msg.contains("absolute") || err_msg.contains("relative"),
        "expected a clear absolute-path error, got: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn outline_rejects_path_escaping_repo_root() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // Plant a secret file directly outside the repo root (repo_root's parent
    // is the shared temp dir) and try to reach it via `..`.
    let secret_path = repo_root
        .parent()
        .expect("repo_root has a parent")
        .join("lidx-outline-escape-secret.md");
    std::fs::write(&secret_path, "SECRET_OUTSIDE_REPO_CONTENT_DO_NOT_LEAK").unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "../lidx-outline-escape-secret.md"}),
    );

    assert!(
        result.is_err(),
        "a path escaping the repo root via '..' must be rejected, not read: {result:?}"
    );
    let err_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        err_msg.contains("escape") || err_msg.contains("..") || err_msg.contains("outside"),
        "expected a clear escape-path error, got: {err_msg}"
    );

    let _ = std::fs::remove_file(&secret_path);
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
fn read_symbol_with_qualnames_list_returns_symbols_in_request_order() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // Deliberately out of file/definition order, to prove the response follows
    // request order rather than qualname or on-disk order.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": ["pkg.b.helper", "pkg.core.Greeter", "pkg.utils.add"]}),
    )
    .unwrap();

    assert!(
        result.get("ambiguous").is_none(),
        "multi-symbol reads shouldn't hit the ambiguous-query path: {result:#}"
    );
    let symbols = result["symbols"].as_array().expect("symbols array");
    let qualnames: Vec<&str> = symbols
        .iter()
        .filter_map(|s| s["qualname"].as_str())
        .collect();
    assert_eq!(
        qualnames,
        vec!["pkg.b.helper", "pkg.core.Greeter", "pkg.utils.add"],
        "{symbols:#?}"
    );
    let omitted = result["omitted"].as_array().expect("omitted array");
    assert!(omitted.is_empty(), "{result:#}");

    for sym in symbols {
        assert!(
            sym.get("source").is_some(),
            "each symbol should carry its own source text: {sym:#?}"
        );
    }

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_qualnames_small_byte_cap_omits_trailing_symbols_by_qualname() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // A cap generous enough for one symbol's JSON but not all three forces the
    // trailing symbols out, whole, into `omitted` -- never a partial/cut source.
    let one_symbol = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": ["pkg.b.helper"]}),
    )
    .unwrap();
    let one_symbol_bytes = serde_json::to_string(&one_symbol).unwrap().len();
    let cap = one_symbol_bytes + 40;

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({
            "qualnames": ["pkg.b.helper", "pkg.core.Greeter", "pkg.utils.add"],
            "max_bytes": cap,
        }),
    )
    .unwrap();

    let response_bytes = serde_json::to_string(&result).unwrap().len();
    assert!(
        response_bytes <= cap + 200,
        "response ({response_bytes} bytes) should stay close to the requested cap ({cap}): {result:#}"
    );

    let symbols = result["symbols"].as_array().expect("symbols array");
    let included: Vec<&str> = symbols
        .iter()
        .filter_map(|s| s["qualname"].as_str())
        .collect();
    assert_eq!(
        included,
        vec!["pkg.b.helper"],
        "only the first symbol should fit under the cap: {result:#}"
    );
    for sym in symbols {
        let source = sym["source"].as_str().expect("source string");
        assert!(
            source.starts_with("1: def helper():"),
            "an included symbol must be whole, not truncated mid-body: {source:?}"
        );
    }

    let omitted: Vec<&str> = result["omitted"]
        .as_array()
        .expect("omitted array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert_eq!(
        omitted,
        vec!["pkg.core.Greeter", "pkg.utils.add"],
        "the trailing symbols should be reported by qualname, not silently dropped: {result:#}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_skeleton_on_class_returns_child_signatures_and_no_bodies() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter", "skeleton": true}),
    )
    .unwrap();

    assert_eq!(result["qualname"], "pkg.core.Greeter");
    assert_eq!(result["skeleton"], true, "{result:#}");
    assert!(
        result.get("source").is_none(),
        "skeleton mode must not include the full body: {result:#}"
    );

    let children = result["children"].as_array().expect("children array");
    let qualnames: Vec<&str> = children
        .iter()
        .filter_map(|c| c["qualname"].as_str())
        .collect();
    assert_eq!(qualnames, vec!["pkg.core.Greeter.greet"], "{children:#?}");

    let greet = &children[0];
    assert_eq!(greet["kind"], "method");
    assert_eq!(greet["start_line"], 11);
    assert_eq!(greet["end_line"], 13);
    assert!(
        greet["signature"].as_str().is_some_and(|s| !s.is_empty()),
        "child entries should carry a signature: {greet:#?}"
    );
    assert!(
        children.iter().all(|c| c.get("source").is_none()),
        "child entries are signatures only, no bodies: {children:#?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_skeleton_on_rust_struct_returns_impl_method_children() {
    // Rust `impl` blocks have no symbol of their own -- their methods are
    // attributed to the struct via CONTAINS regardless of the impl block's own
    // byte range (see `container_children`'s doc comment). Skeletonizing the
    // struct should surface those methods as children.
    let (mut indexer, repo_root) = indexed_from_source(
        "skeleton-rust-struct-impl",
        &[("src/lib.rs", OUTLINE_RUST_SRC)],
    );

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "crate::Foo", "skeleton": true}),
    )
    .unwrap();

    assert_eq!(result["skeleton"], true, "{result:#}");
    assert!(result.get("source").is_none(), "{result:#}");
    let children = result["children"].as_array().expect("children array");
    let qualnames: Vec<&str> = children
        .iter()
        .filter_map(|c| c["qualname"].as_str())
        .collect();
    assert_eq!(
        qualnames,
        vec!["crate::Foo::bar", "crate::Foo::helper"],
        "{children:#?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_skeleton_container_emits_valid_read_symbol_hops_for_children() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter", "skeleton": true}),
    )
    .unwrap();

    let hops = result["next_hops"].as_array().expect("next_hops array");
    assert!(!hops.is_empty(), "{result:#}");
    for hop in hops {
        assert_eq!(hop["method"], "read_symbol", "{hop:#?}");
        assert!(
            METHOD_LIST.contains(&hop["method"].as_str().unwrap()),
            "hop method must be dispatchable: {hop:#?}"
        );
        let hop_params = &hop["params"];
        assert!(
            hop_params.get("qualname").is_some(),
            "each child hop should select by qualname: {hop:#?}"
        );
        assert_eq!(
            [
                hop_params.get("qualname").is_some(),
                hop_params.get("query").is_some(),
                hop_params.get("qualnames").is_some(),
            ]
            .into_iter()
            .filter(|x| *x)
            .count(),
            1,
            "a valid read_symbol selector has exactly one of qualname/query/qualnames: {hop:#?}"
        );

        // The hop must actually resolve.
        let followed = rpc::handle_method(&mut indexer, "read_symbol", hop_params.clone())
            .unwrap_or_else(|e| panic!("child hop failed to resolve: {e} ({hop:#?})"));
        assert!(followed.get("source").is_some(), "{followed:#?}");
    }

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_skeleton_on_leaf_symbol_falls_back_to_normal_read() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // `greet` is a method with no children of its own -- skeleton on a leaf has
    // nothing to skeletonize.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter.greet", "skeleton": true}),
    )
    .unwrap();

    assert!(result.get("skeleton").is_none(), "{result:#}");
    assert!(result.get("source").is_some(), "{result:#}");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_context_lines_adds_exactly_n_lines_each_side() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // `pkg.core.Base` spans lines 6-7 in a 16-line file, top-level (column 0,
    // so its own span isn't missing leading indentation the way a nested
    // symbol's would be) -- 2 lines of context on each side stays well within
    // file bounds: 4-9.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Base", "context_lines": 2}),
    )
    .unwrap();

    assert_eq!(result["start_line"], 6, "{result:#}");
    assert_eq!(result["end_line"], 7, "{result:#}");

    let content = std::fs::read_to_string(repo_root.join("pkg/core.py")).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    let expected = lines[3..9]
        .iter()
        .enumerate()
        .map(|(i, line)| format!("{}: {}", 4 + i as i64, line))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(result["source"].as_str().unwrap(), expected, "{result:#}");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_context_lines_clamped_at_file_bounds() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // A huge context_lines request clamps to the file's actual bounds (line 1
    // through the last line) instead of panicking or going out of range.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Base", "context_lines": 1000}),
    )
    .unwrap();

    let content = std::fs::read_to_string(repo_root.join("pkg/core.py")).unwrap();
    let total_lines = content.lines().count();
    let expected = content
        .lines()
        .enumerate()
        .map(|(i, line)| format!("{}: {}", i + 1, line))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(result["source"].as_str().unwrap(), expected, "{result:#}");
    let source_line_count = result["source"].as_str().unwrap().lines().count();
    assert_eq!(source_line_count, total_lines, "{result:#}");

    let _ = std::fs::remove_dir_all(&repo_root);
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
fn read_symbol_qualnames_large_multi_read_is_not_re_truncated_by_the_generic_response_cap() {
    // Three functions large enough that their combined response exceeds the
    // generic 30KB response cap `handle_method` applies to methods that don't
    // manage their own budget -- read_symbol DOES manage its own via
    // `max_bytes`, so the outer cap must not re-truncate it down to nothing.
    fn padded_fn(name: &str, target_body_bytes: usize) -> String {
        let mut src = format!("pub fn {name}() -> i32 {{\n    let mut x = 0;\n");
        while src.len() < target_body_bytes {
            src.push_str("    x += 1;\n");
        }
        src.push_str("    x\n}\n\n");
        src
    }

    let mut file_src = String::new();
    for i in 0..3 {
        file_src.push_str(&padded_fn(&format!("big_fn_{i}"), 15_000));
    }

    let (mut indexer, repo_root) =
        indexed_from_source("read-symbol-large-multi", &[("src/lib.rs", &file_src)]);

    let qualnames = vec![
        "crate::big_fn_0".to_string(),
        "crate::big_fn_1".to_string(),
        "crate::big_fn_2".to_string(),
    ];
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": qualnames, "max_bytes": 100000}),
    )
    .unwrap();

    assert!(
        result.get("truncated").is_none(),
        "read_symbol manages its own budget; the outer generic response cap \
         must not wrap it in a second truncated envelope: {result:#}"
    );
    let symbols = result["symbols"].as_array().expect("symbols array");
    assert_eq!(
        symbols.len(),
        3,
        "all three symbols should fit under the requested 100000-byte budget: {result:#}"
    );
    let omitted = result["omitted"].as_array().expect("omitted array");
    assert!(omitted.is_empty(), "{result:#}");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_single_large_symbol_is_capped_with_omitted_header_and_next_hops() {
    // A large generated function whose full source would blow past a modest
    // max_bytes budget -- read_symbol must never cut a symbol mid-body, so it
    // returns just the header fields plus `omitted: true` instead.
    let mut src = String::from("pub fn huge() -> i32 {\n    let mut x = 0;\n");
    for _ in 0..5000 {
        src.push_str("    x += 1;\n");
    }
    src.push_str("    x\n}\n");

    let (mut indexer, repo_root) =
        indexed_from_source("read-symbol-single-large", &[("src/lib.rs", &src)]);

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "crate::huge", "max_bytes": 20000}),
    )
    .unwrap();

    assert_eq!(result["qualname"], "crate::huge");
    assert_eq!(result["omitted"], true, "{result:#}");
    assert!(
        result.get("source").is_none(),
        "an over-budget single read must not include a partial/cut source: {result:#}"
    );
    assert!(
        result["size_bytes"].as_u64().is_some_and(|n| n > 20000),
        "expected the actual (over-budget) size to be reported: {result:#}"
    );

    let hops = result["next_hops"].as_array().expect("next_hops array");
    assert!(!hops.is_empty(), "{result:#}");
    assert!(
        hops.iter()
            .any(|h| h["method"] == "read_symbol" && h["params"]["skeleton"] == true),
        "expected a read_symbol skeleton:true suggestion for the container case: {hops:#?}"
    );
    for hop in hops {
        assert!(
            METHOD_LIST.contains(&hop["method"].as_str().unwrap()),
            "hop must be dispatchable: {hop:#?}"
        );
    }

    let _ = std::fs::remove_dir_all(&repo_root);
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
