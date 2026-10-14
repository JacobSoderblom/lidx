//! A `get` / `set` accessor is run by a property read or write. The access is
//! a `USES` edge (call shape `member`) on the accessor, so `dead_symbols`
//! reports an accessor nothing reads or writes, keeps one that is accessed,
//! and demotes (never hides) one only a receiver the index cannot resolve
//! may access.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use lidx::rpc;

const MODEL: &str = "export class Model {
  get used() { return 1; }
  get unread() { return 2; }
  get level() { return 3; }
  set level(v: number) {}
  get unwritten() { return 4; }
  set unwritten(v: number) {}
  get pulled() { return 5; }
  get opaque() { return 6; }
  dead() { return 3; }
  copy() {
    const { pulled } = this;
    return this.used + pulled;
  }
}
";
const USE: &str = "import { Model } from './model';
export function make(m: Model, anything: any) {
  m.level = 7;
  return anything.opaque;
}
";

/// Not exported: the dead-symbol tiers apply (an export is public API).
const INNER: &str = "class Inner {
  get hidden() { return 1; }
  get opaque() { return 2; }
}
export function run(x: any) {
  new Inner();
  return x.opaque;
}
";

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("src/model.ts", MODEL),
        ("src/use.ts", USE),
        ("src/inner.ts", INNER),
    ]
}

/// `(qualname, confidence)` of every reported symbol.
fn reported(indexer: &mut Indexer) -> Vec<(String, String)> {
    let result = rpc::handle_method(
        indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false, "include_public_api": true}),
    )
    .unwrap();
    let rows = |key: &str| result[key].as_array().cloned().unwrap_or_default();
    rows("dead_symbols")
        .into_iter()
        .chain(rows("public_api"))
        .filter_map(|s| {
            Some((
                s["qualname"].as_str()?.to_string(),
                s["confidence"].as_str().unwrap_or("").to_string(),
            ))
        })
        .collect()
}

fn names(reported: &[(String, String)]) -> Vec<&str> {
    reported.iter().map(|(q, _)| q.as_str()).collect()
}

#[test]
fn an_accessor_is_dead_only_when_nothing_reads_or_writes_it() {
    let (_tmp, _root, mut indexer) = common::live_indexer("lidx-ts-dead-accessor-", &files());
    let dead = reported(&mut indexer);
    let dead_names = names(&dead);
    assert!(dead_names.contains(&"src/model.Model.dead"), "{dead:?}");
    assert!(dead_names.contains(&"src/model.Model.unread"), "{dead:?}");
    // The get / set pair: unread and unwritten, both are dead.
    assert!(
        dead_names.contains(&"src/model.Model.unwritten"),
        "{dead:?}"
    );
    for live in [
        "src/model.Model.used",   // `this.used`
        "src/model.Model.level",  // `m.level = 7` on a typed receiver
        "src/model.Model.pulled", // `const { pulled } = this`
    ] {
        assert!(!dead_names.contains(&live), "{live}: {dead:?}");
    }
}

#[test]
fn an_accessor_only_an_unresolved_receiver_may_read_is_demoted_not_hidden() {
    let (_tmp, _root, mut indexer) = common::live_indexer("lidx-ts-dead-accessor-", &files());
    let dead = reported(&mut indexer);
    let tier = |q: &str| {
        dead.iter()
            .find(|(name, _)| name == q)
            .unwrap_or_else(|| panic!("{q} hidden: {dead:?}"))
            .1
            .clone()
    };
    assert_eq!(tier("src/inner.Inner.hidden"), "high", "{dead:?}");
    assert_eq!(tier("src/inner.Inner.opaque"), "medium", "{dead:?}");
}

/// The fallback reads the indexed files as they are on disk now. A file that
/// is no longer valid UTF-8 is still scanned (bytes); a file that vanished
/// after indexing may read anything, so every accessor is `medium`. Neither
/// panics.
#[test]
fn the_unresolved_receiver_fallback_survives_files_changed_after_indexing() {
    let (_tmp, root, mut indexer) = common::live_indexer("lidx-ts-dead-accessor-", &files());
    let tier = |dead: &[(String, String)], q: &str| {
        dead.iter()
            .find(|(name, _)| name == q)
            .unwrap_or_else(|| panic!("{q} hidden: {dead:?}"))
            .1
            .clone()
    };

    std::fs::write(
        root.join("src/use.ts"),
        b"export const x = \xff\xfe; // .nothing\n",
    )
    .unwrap();
    let dead = reported(&mut indexer);
    assert_eq!(tier(&dead, "src/inner.Inner.hidden"), "high", "{dead:?}");
    assert_eq!(tier(&dead, "src/inner.Inner.opaque"), "medium", "{dead:?}");

    std::fs::remove_file(root.join("src/use.ts")).unwrap();
    let dead = reported(&mut indexer);
    assert_eq!(tier(&dead, "src/inner.Inner.hidden"), "medium", "{dead:?}");
}

#[test]
fn accessor_liveness_survives_incremental_sync() {
    let (_tmp, root, mut indexer) = common::live_indexer("lidx-ts-dead-accessor-", &files());
    // The only reader goes away: `level` is dead, like in a fresh index.
    let without_reader = "import { Model } from './model';\nexport function make(m: Model, anything: any) {\n  return anything.opaque;\n}\n";
    common::write_files(&root, &[("src/use.ts", without_reader)]);
    indexer.sync_rel_paths(&["src/use.ts".to_string()]).unwrap();
    assert!(
        names(&reported(&mut indexer)).contains(&"src/model.Model.level"),
        "level is unwritten now"
    );
    let final_files = [
        ("src/model.ts", MODEL),
        ("src/use.ts", without_reader),
        ("src/inner.ts", INNER),
    ];
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&final_files);
    common::assert_matches_fresh(&snapshot, &fresh);

    // It comes back, and so does the accessor's liveness.
    common::write_files(&root, &[("src/use.ts", USE)]);
    indexer.sync_rel_paths(&["src/use.ts".to_string()]).unwrap();
    assert!(!names(&reported(&mut indexer)).contains(&"src/model.Model.level"));
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&files());
    common::assert_matches_fresh(&snapshot, &fresh);

    // A field turned into an accessor in another file is picked up too.
    let field_model = MODEL.replace("get used() { return 1; }", "used = 1;");
    common::write_files(&root, &[("src/model.ts", &field_model)]);
    indexer
        .sync_rel_paths(&["src/model.ts".to_string()])
        .unwrap();
    common::write_files(&root, &[("src/model.ts", MODEL)]);
    indexer
        .sync_rel_paths(&["src/model.ts".to_string()])
        .unwrap();
    assert!(!names(&reported(&mut indexer)).contains(&"src/model.Model.used"));
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&files());
    common::assert_matches_fresh(&snapshot, &fresh);

    // A field an importer writes through a typed receiver becomes an accessor
    // in the exporter alone: the importer is re-extracted, and the write
    // (dropped while `level` was a field) reaches the accessor.
    let field_level = MODEL
        .replace("  get level() { return 3; }\n", "  level = 3;\n")
        .replace("  set level(v: number) {}\n", "");
    common::write_files(&root, &[("src/model.ts", &field_level)]);
    indexer
        .sync_rel_paths(&["src/model.ts".to_string()])
        .unwrap();
    common::write_files(&root, &[("src/model.ts", MODEL)]);
    indexer
        .sync_rel_paths(&["src/model.ts".to_string()])
        .unwrap();
    assert!(!names(&reported(&mut indexer)).contains(&"src/model.Model.level"));
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&files());
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// A property access the index could not bind (`this.foo` where the base class
/// is not in the repository) is stored for retry but is no unresolved call:
/// the unresolved summary leaves it out, like an unresolved value reference.
#[test]
fn unresolved_property_accesses_are_not_counted_as_unresolved_references() {
    let files = [(
        "src/child.ts",
        "import { Missing } from 'elsewhere';\nexport class Child extends Missing {\n  f() { return this.foo; }\n}\n",
    )];
    let (_tmp, _root, indexer) = common::live_indexer("lidx-ts-member-rows-", &files);
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();
    let conn = db.read_conn().unwrap();
    let stored = |filter: &str| -> i64 {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = {gv} {filter}"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(
        stored("AND call_shape = 'member'"),
        1,
        "the access is stored"
    );
    let counted: i64 = db
        .unresolved_reference_summary(gv)
        .unwrap()
        .iter()
        .map(|r| r.count)
        .sum();
    assert_eq!(
        counted,
        stored(
            "AND COALESCE(call_shape, '') != 'member' AND NOT (edge_kind IN ('REFERENCES', 'USES') AND call_shape LIKE 'ref%')"
        )
    );
}
