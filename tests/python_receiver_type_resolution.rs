//! Regression coverage for receiver-type-gated CALLS edge resolution
//! (issue #45's measurement: 84% of bound CALLS edges on a real corpus were
//! a local variable's builtin method — e.g. `list.append` — colliding with
//! an unrelated domain method of the same name).
//!
//! Fixture `py_receiver_type`: `store.py` defines `EventStore.append`, a
//! domain method whose name collides with `list.append`. `caller.py` calls
//! it three different ways that must resolve differently:
//! - `self.append(None)` (inside `EventStore.flush`) — same-class call,
//!   must resolve via the pre-existing exact-qualname tier.
//! - `event_store.append(1)` through an annotated parameter — must resolve
//!   via the new receiver-type tier.
//! - `cells.append(1)` where `cells = []` — must NOT resolve at all.

use lidx::indexer::Indexer;
use rusqlite::params;
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
    dir.push(format!("lidx-{label}-{nanos}-{counter}"));
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

fn setup_repo(fixture: &str) -> (PathBuf, PathBuf) {
    let src = fixture_path(fixture);
    let repo_root = temp_repo_dir(fixture);
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
}

/// Shared assertion for the four "must not bind to EventStore.append"
/// tests below. Issue #80 added an external-stub tier, but its scope is
/// "calls into imports known to resolve outside the repo" -- a builtin or
/// otherwise-unresolved receiver type with no import involved (a local
/// variable) is not that, so it must stay unresolved exactly as before
/// #80: no edge at all (CALLS isn't a Bridge Edge kind), only an
/// `unresolved_references` store row, reason `external`, still carrying
/// the tracked-but-unresolved `receiver_type` marker (`""`). Stubbing this
/// case would name one shared stub after whichever local variable's call
/// happened to create it first, defeating "who calls X?" for every builtin
/// method name.
fn assert_stays_unresolved(conn: &rusqlite::Connection, target_qualname: &str, graph_version: i64) {
    let edge_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges
             WHERE kind = 'CALLS' AND target_qualname = ? AND graph_version = ?",
            params![target_qualname, graph_version],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        edge_count, 0,
        "a builtin/unresolved receiver type must never bind to a repo symbol, \
         and CALLS isn't a Bridge Edge kind, so no edge at all must exist"
    );
    let (receiver_type, reason): (Option<String>, String) = conn
        .query_row(
            "SELECT receiver_type, reason FROM unresolved_references
             WHERE edge_kind = 'CALLS' AND reference_name = ? AND graph_version = ?",
            params![target_qualname, graph_version],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        receiver_type.as_deref(),
        Some(""),
        "a builtin/unresolved receiver type must still be recorded as tracked-but-unresolved \
         (empty string), not left as NULL (not-tracked)"
    );
    assert_eq!(reason, "external");
}

/// A genuine `self.method()` call must still resolve to the enclosing
/// class's method — via the pre-existing exact-qualname tier, unaffected
/// by receiver-type gating.
#[test]
fn self_call_resolves_to_enclosing_class_method() {
    let (repo_root, db_path) = setup_repo("py_receiver_type");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    let append = indexer
        .db()
        .get_symbol_by_qualname("store.EventStore.append", gv)
        .unwrap()
        .expect("EventStore.append must be indexed");

    let conn = indexer.db().read_conn().unwrap();
    let (target_symbol_id, resolution_kind): (Option<i64>, Option<String>) = conn
        .query_row(
            "SELECT target_symbol_id, resolution_kind FROM edges
             WHERE kind = 'CALLS' AND target_qualname = 'store.EventStore.append' AND graph_version = ?",
            params![gv],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(
        target_symbol_id,
        Some(append.id),
        "self.append(None) must resolve to EventStore.append"
    );
    assert_eq!(
        resolution_kind.as_deref(),
        Some("exact"),
        "self.method() resolves via the exact-qualname tier"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// A method call through an annotated parameter (`event_store: EventStore`)
/// must resolve to that type's method via the new receiver-type tier. This
/// is the load-bearing case: without it, this whole change would only ever
/// delete edges, never bind a genuine one.
#[test]
fn annotated_parameter_call_resolves_via_receiver_type() {
    let (repo_root, db_path) = setup_repo("py_receiver_type");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    let append = indexer
        .db()
        .get_symbol_by_qualname("store.EventStore.append", gv)
        .unwrap()
        .expect("EventStore.append must be indexed");

    let conn = indexer.db().read_conn().unwrap();
    let (target_symbol_id, receiver_type, resolution_kind): (
        Option<i64>,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT target_symbol_id, receiver_type, resolution_kind FROM edges
             WHERE kind = 'CALLS' AND target_qualname = 'event_store.append' AND graph_version = ?",
            params![gv],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();

    assert_eq!(
        receiver_type.as_deref(),
        Some("EventStore"),
        "the annotated parameter's type must be captured on the edge"
    );
    assert_eq!(
        target_symbol_id,
        Some(append.id),
        "event_store.append(1) must resolve to EventStore.append through the annotated parameter's type"
    );
    assert_eq!(
        resolution_kind.as_deref(),
        Some("receiver_type"),
        "must be tagged as resolved via the receiver-type tier, not the bare-name tier"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// `cells = []` then `cells.append(x)` must NOT bind to EventStore.append —
/// this is the false-positive pattern issue #45 measured (84% of all bound
/// CALLS edges on a real corpus were exactly this shape).
#[test]
fn builtin_local_variable_call_does_not_bind() {
    let (repo_root, db_path) = setup_repo("py_receiver_type");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    // cells.append(1) must NOT bind to EventStore.append (or anything
    // else), and must not stub either -- no import is involved.
    let conn = indexer.db().read_conn().unwrap();
    assert_stays_unresolved(&conn, "cells.append", gv);

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// `acc` is a lambda parameter (`lambda n, acc=result: acc.append(n)`),
/// not a local of the enclosing function. A lambda never gets its own
/// `Context`/`local_types` in the extractor, so without folding the
/// lambda's own parameters into the enclosing scope, a call through one of
/// them looks like an untracked identifier and falls through to the
/// bare-name tier — this was one of the two surviving leaks in dpb
/// (`acc.append(n)` inside `walk_expr(expr, lambda n, acc=funcs: ...)`).
#[test]
fn lambda_parameter_call_does_not_bind() {
    let (repo_root, db_path) = setup_repo("py_receiver_type");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    // acc.append(n) inside the lambda body must NOT bind to
    // EventStore.append, and must not stub either -- this still requires
    // the lambda's own parameter to be tracked (folded into the enclosing
    // scope) rather than left NULL as if it were never a local at all.
    let conn = indexer.db().read_conn().unwrap();
    assert_stays_unresolved(&conn, "acc.append", gv);

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// `Registry.instances.append(self)` — a chained attribute off a *class*
/// reference (not `self`/`cls`) — must also not bind. Before this fix, any
/// attribute chain of two or more hops rooted in a name the extractor
/// didn't track as a local was assumed to be "presumably a class/module
/// reference" and left `NotTracked`, letting the pre-existing bare-name
/// pipeline resolve it anyway. This is dpb's
/// `_FakeCredential.instances.append(self)` false positive.
#[test]
fn chained_class_attribute_call_does_not_bind() {
    let (repo_root, db_path) = setup_repo("py_receiver_type");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    // Registry.instances.append(self) must NOT bind to EventStore.append,
    // and must not stub either -- no import is involved.
    let conn = indexer.db().read_conn().unwrap();
    assert_stays_unresolved(&conn, "Registry.instances.append", gv);

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// `self._events.append(event)` — a chained attribute off `self` with no
/// class-level annotation on `_events` — must also not bind. This is the
/// exact shape of issue #45's `self._buffer.append` example.
#[test]
fn chained_self_attribute_call_does_not_bind() {
    let (repo_root, db_path) = setup_repo("py_receiver_type");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    // self._events.append(event) must NOT bind (_events has no class-level
    // type annotation), and must not stub either -- no import is involved.
    let conn = indexer.db().read_conn().unwrap();
    assert_stays_unresolved(&conn, "self._events.append", gv);

    let _ = std::fs::remove_dir_all(&repo_root);
}

fn factory_callers_total(repo_root: &Path, db_path: &Path, qualname: &str) -> i64 {
    let raw = lidx::rpc::call(
        repo_root.to_path_buf(),
        db_path.to_path_buf(),
        "explain_symbol".to_string(),
        &format!(r#"{{"qualname":"{qualname}","sections":["callers"]}}"#),
        "1",
    )
    .unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
    envelope["result"]["callers_total"].as_i64().unwrap_or(-1)
}

/// Issue #207: a local assigned from a same-file factory annotated
/// `-> Type` takes that type; unannotated factories, `list[Foo]` returns and
/// reassignment from two factories must stay unresolved.
#[test]
fn factory_return_annotation_types_the_local() {
    let (repo_root, db_path) = setup_repo("py_factory_locals");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let total = |q: &str| factory_callers_total(&repo_root, &db_path, q);
    // factory + direct constructor
    assert_eq!(total("models.Coordinator.handle_trigger"), 2);
    assert_eq!(total("models.OptFoo.opt_run"), 1, "Optional[Foo]");
    assert_eq!(total("models.AsyncFoo.async_run"), 1, "await async factory");
    assert_eq!(total("models.DottedFoo.dotted_run"), 1, "dotted annotation");
    assert_eq!(total("models.StrFoo.str_run"), 1, "string forward ref");
    assert_eq!(total("models.UnannFoo.unann_run"), 0, "unannotated");
    assert_eq!(total("models.ListFoo.list_run"), 0, "list[Foo]");
    assert_eq!(total("models.MixedA.mixed_run"), 0, "reassigned");
    assert_eq!(total("models.MixedB.mixed_run"), 0, "reassigned");
    assert_eq!(total("models.PipeFoo.pipe_run"), 1, "Foo | None");
    assert_eq!(total("models.CoroFoo.coro_run"), 1, "await Awaitable[Foo]");
    assert_eq!(
        total("models.UnawaitedFoo.unawaited_run"),
        0,
        "un-awaited async"
    );
    assert_eq!(total("models.DupFoo.dup_run"), 0, "duplicate top-level def");
    assert_eq!(
        total("models.ShadowFoo.shadow_run"),
        0,
        "param shadows factory"
    );
    // `-> pkg_a.twin.Twin` must never bind the same-named class in pkg_b.
    assert_eq!(total("pkg_b.twin.Twin.twin_run"), 0, "same-named class");
    assert!(total("pkg_a.twin.Twin.twin_run") <= 1);

    // The direct-constructor call keeps its receiver_type resolution kind.
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let kind: Option<String> = conn
        .query_row(
            "SELECT resolution_kind FROM edges WHERE kind = 'CALLS'
             AND target_qualname = 'coord2.handle_trigger' AND graph_version = ?",
            params![gv],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(kind.as_deref(), Some("receiver_type"));

    let _ = std::fs::remove_dir_all(&repo_root);
}
