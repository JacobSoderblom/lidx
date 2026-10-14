//! Value references through incremental sync. A reference whose target does
//! not exist is stored like an unresolved call and retried when the target
//! appears, so every scenario ends equal to a fresh reindex of the same tree.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const USER_PY: &str = "from lib import helper\n\n\ndef register(cb):\n    return cb\n\n\ndef run():\n    register(helper)\n";
const LIB_PY: &str = "def helper():\n    return 1\n";
const LIB_RENAMED_PY: &str = "def helper2():\n    return 1\n";

fn references(indexer: &Indexer) -> Vec<(String, String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, COALESCE(t.qualname, ''), e.kind FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind IN ('REFERENCES', 'USES') AND e.call_shape IN ('ref', 'ref:this')
               AND e.graph_version = ?",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// Stored unresolved value references, and what the counters make of them.
fn stored_rows(indexer: &Indexer) -> i64 {
    let conn = indexer.db().read_conn().unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM unresolved_references WHERE edge_kind = 'REFERENCES'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

fn counted(indexer: &Indexer) -> (i64, i64) {
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();
    let conn = db.read_conn().unwrap();
    let ids: Vec<i64> = conn
        .prepare("SELECT id FROM symbols WHERE graph_version = ?")
        .unwrap()
        .query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let summary = db
        .unresolved_reference_summary(gv)
        .unwrap()
        .iter()
        .map(|r| r.count)
        .sum();
    (
        summary,
        db.unresolved_reference_count_for_symbols(&ids, gv).unwrap(),
    )
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-valref-sync-", files);
    let indexer = Indexer::new(root.clone(), db_path).unwrap();
    (tmp, root, indexer)
}

fn assert_incremental_equals_fresh(indexer: &Indexer, final_files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn deleting_the_target_drops_the_edge_like_a_fresh_index() {
    let (_tmp, root, mut indexer) = indexed(&[("lib.py", LIB_PY), ("user.py", USER_PY)]);
    assert_eq!(
        references(&indexer),
        [("user.run".into(), "lib.helper".into(), "REFERENCES".into())]
    );

    std::fs::remove_file(root.join("lib.py")).unwrap();
    indexer.sync_rel_paths(&["lib.py".to_string()]).unwrap();

    assert!(
        references(&indexer).is_empty(),
        "{:?}",
        references(&indexer)
    );
    assert_eq!(stored_rows(&indexer), 1);
    assert_incremental_equals_fresh(&indexer, &[("user.py", USER_PY)]);
}

#[test]
fn a_target_added_later_is_picked_up_like_a_fresh_index() {
    let (_tmp, root, mut indexer) = indexed(&[("user.py", USER_PY)]);
    assert!(references(&indexer).is_empty());
    assert_eq!(stored_rows(&indexer), 1);

    common::write_files(&root, &[("lib.py", LIB_PY)]);
    indexer.sync_rel_paths(&["lib.py".to_string()]).unwrap();

    assert_eq!(
        references(&indexer),
        [("user.run".into(), "lib.helper".into(), "REFERENCES".into())]
    );
    assert_eq!(stored_rows(&indexer), 0, "the row is consumed on success");
    assert_incremental_equals_fresh(&indexer, &[("lib.py", LIB_PY), ("user.py", USER_PY)]);
}

#[test]
fn renaming_the_target_away_and_back_matches_a_fresh_index() {
    let (_tmp, root, mut indexer) = indexed(&[("lib.py", LIB_PY), ("user.py", USER_PY)]);

    common::write_files(&root, &[("lib.py", LIB_RENAMED_PY)]);
    indexer.sync_rel_paths(&["lib.py".to_string()]).unwrap();
    assert!(
        references(&indexer).is_empty(),
        "{:?}",
        references(&indexer)
    );
    assert_eq!(stored_rows(&indexer), 1);
    assert_incremental_equals_fresh(
        &indexer,
        &[("lib.py", LIB_RENAMED_PY), ("user.py", USER_PY)],
    );

    common::write_files(&root, &[("lib.py", LIB_PY)]);
    indexer.sync_rel_paths(&["lib.py".to_string()]).unwrap();
    assert_eq!(
        references(&indexer),
        [("user.run".into(), "lib.helper".into(), "REFERENCES".into())]
    );
    assert_incremental_equals_fresh(&indexer, &[("lib.py", LIB_PY), ("user.py", USER_PY)]);
}

#[test]
fn editing_the_referencing_file_moves_the_edge() {
    let (_tmp, root, mut indexer) = indexed(&[("lib.py", LIB_PY), ("user.py", USER_PY)]);
    let called = USER_PY.replace("register(helper)", "register(helper)\n    helper()");

    common::write_files(&root, &[("user.py", &called)]);
    indexer.sync_rel_paths(&["user.py".to_string()]).unwrap();

    // The pair now has a CALLS edge, so the reference is dropped.
    assert!(
        references(&indexer).is_empty(),
        "{:?}",
        references(&indexer)
    );
    assert_incremental_equals_fresh(&indexer, &[("lib.py", LIB_PY), ("user.py", &called)]);
}

#[test]
fn unresolved_value_references_stay_out_of_the_counts() {
    let (_tmp, _root, indexer) = indexed(&[("user.py", USER_PY)]);
    assert_eq!(stored_rows(&indexer), 1);
    let (summary, per_symbol) = counted(&indexer);
    let others: i64 = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM unresolved_references WHERE edge_kind != 'REFERENCES'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(summary, others, "summary counts the other rows only");
    assert_eq!(per_symbol, others, "per-symbol count");
}

const APP_TS: &str = "import { helper } from './lib';\nfunction register(cb: unknown) {}\nexport function run() { register(helper); }\n";
const LIB_FN_TS: &str = "export function helper() { return 1; }\n";
const LIB_CONST_TS: &str = "export const helper = 5;\n";

#[test]
fn a_constant_turning_into_a_function_changes_the_stored_kind_like_a_fresh_index() {
    let (_tmp, root, mut indexer) =
        indexed(&[("src/app.ts", APP_TS), ("src/lib.ts", LIB_CONST_TS)]);
    assert_eq!(
        references(&indexer),
        [("src/app.run".into(), "src/lib.helper".into(), "USES".into())]
    );

    common::write_files(&root, &[("src/lib.ts", LIB_FN_TS)]);
    indexer.sync_rel_paths(&["src/lib.ts".to_string()]).unwrap();
    assert_eq!(
        references(&indexer),
        [(
            "src/app.run".into(),
            "src/lib.helper".into(),
            "REFERENCES".into()
        )]
    );
    assert_incremental_equals_fresh(
        &indexer,
        &[("src/app.ts", APP_TS), ("src/lib.ts", LIB_FN_TS)],
    );
}

#[test]
fn a_function_turning_into_a_constant_changes_the_stored_kind_like_a_fresh_index() {
    let (_tmp, root, mut indexer) = indexed(&[("src/app.ts", APP_TS), ("src/lib.ts", LIB_FN_TS)]);
    assert_eq!(
        references(&indexer),
        [(
            "src/app.run".into(),
            "src/lib.helper".into(),
            "REFERENCES".into()
        )]
    );

    common::write_files(&root, &[("src/lib.ts", LIB_CONST_TS)]);
    indexer.sync_rel_paths(&["src/lib.ts".to_string()]).unwrap();
    assert_eq!(
        references(&indexer),
        [("src/app.run".into(), "src/lib.helper".into(), "USES".into())]
    );
    assert_incremental_equals_fresh(
        &indexer,
        &[("src/app.ts", APP_TS), ("src/lib.ts", LIB_CONST_TS)],
    );
}

const BOX_GETTER_TS: &str =
    "export class Box {\n  static create = () => 1;\n  get size() { return 1; }\n  run() {}\n}\n";
const BOX_METHOD_TS: &str =
    "export class Box {\n  static create = () => 1;\n  size() { return 1; }\n  run() {}\n}\n";
const BOX_USER_TS: &str = "import { Box } from './box';\nconst make = Box.create;\nconst box = new Box();\nexport const table = { make };\nexport function use() {\n  const cb = () => 1;\n  return [make, { cb }];\n}\nexport const size = box.size;\n";

fn ts_references(indexer: &Indexer) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = references(indexer)
        .into_iter()
        .map(|(s, t, _)| (s, t))
        .collect();
    rows.sort();
    rows
}

#[test]
fn a_getter_turning_into_a_method_adds_the_reference_like_a_fresh_index() {
    let (_tmp, root, mut indexer) =
        indexed(&[("src/box.ts", BOX_GETTER_TS), ("src/user.ts", BOX_USER_TS)]);
    let want: Vec<(String, String)> = vec![
        ("src/user".into(), "src/box.Box.create".into()),
        ("src/user".into(), "src/user.make".into()),
        ("src/user.use".into(), "src/user.make".into()),
        ("src/user.use".into(), "src/user.use.cb".into()),
    ];
    assert_eq!(ts_references(&indexer), want);

    common::write_files(&root, &[("src/box.ts", BOX_METHOD_TS)]);
    indexer.sync_rel_paths(&["src/box.ts".to_string()]).unwrap();
    let mut want = want;
    want.push(("src/user".into(), "src/box.Box.size".into()));
    want.sort();
    assert_eq!(ts_references(&indexer), want);
    assert_incremental_equals_fresh(
        &indexer,
        &[("src/box.ts", BOX_METHOD_TS), ("src/user.ts", BOX_USER_TS)],
    );

    common::write_files(&root, &[("src/box.ts", BOX_GETTER_TS)]);
    indexer.sync_rel_paths(&["src/box.ts".to_string()]).unwrap();
    assert_incremental_equals_fresh(
        &indexer,
        &[("src/box.ts", BOX_GETTER_TS), ("src/user.ts", BOX_USER_TS)],
    );
}

#[test]
fn editing_a_local_callback_and_its_user_matches_a_fresh_index() {
    let (_tmp, root, mut indexer) =
        indexed(&[("src/box.ts", BOX_GETTER_TS), ("src/user.ts", BOX_USER_TS)]);
    let renamed = BOX_USER_TS
        .replace("const cb = ", "const cb2 = ")
        .replace("{ cb }", "{ cb2 }");
    common::write_files(&root, &[("src/user.ts", &renamed)]);
    indexer
        .sync_rel_paths(&["src/user.ts".to_string()])
        .unwrap();
    assert!(
        ts_references(&indexer).contains(&("src/user.use".into(), "src/user.use.cb2".into())),
        "{:?}",
        ts_references(&indexer)
    );
    assert_incremental_equals_fresh(
        &indexer,
        &[("src/box.ts", BOX_GETTER_TS), ("src/user.ts", &renamed)],
    );
}
