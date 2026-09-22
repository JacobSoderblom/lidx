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

    let conn = indexer.db().read_conn().unwrap();
    let (target_symbol_id, receiver_type, resolution_kind): (
        Option<i64>,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT target_symbol_id, receiver_type, resolution_kind FROM edges
             WHERE kind = 'CALLS' AND target_qualname = 'cells.append' AND graph_version = ?",
            params![gv],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();

    assert_eq!(
        target_symbol_id, None,
        "cells.append(1) must NOT bind to EventStore.append (or anything else)"
    );
    assert_eq!(
        receiver_type.as_deref(),
        Some(""),
        "a builtin-typed local must be recorded as tracked-but-unresolved (empty string), \
         not left as NULL (not-tracked)"
    );
    assert_eq!(
        resolution_kind, None,
        "an edge that never binds must have no resolution provenance"
    );

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

    let conn = indexer.db().read_conn().unwrap();
    let (target_symbol_id, receiver_type): (Option<i64>, Option<String>) = conn
        .query_row(
            "SELECT target_symbol_id, receiver_type FROM edges
             WHERE kind = 'CALLS' AND target_qualname = 'self._events.append' AND graph_version = ?",
            params![gv],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(
        target_symbol_id, None,
        "self._events.append(event) must NOT bind — _events has no class-level type annotation"
    );
    assert_eq!(receiver_type.as_deref(), Some(""));

    let _ = std::fs::remove_dir_all(&repo_root);
}
