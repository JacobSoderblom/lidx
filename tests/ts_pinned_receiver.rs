//! A receiver type pinned through an import that never resolves to a repo
//! file must be stored as the bare type name, never as the raw
//! `specifier\0member` placeholder (`ReceiverType::Pinned`).

mod common;

use lidx::indexer::Indexer;

const USER: &str = r#"import type { Missing } from 'no-such-package';
import { Gone as Renamed } from './no-such-file';
import { Real } from './real';
export function run(a: Missing, b: Renamed, c: Real) {
  a.go();
  b.go();
  c.go();
}
"#;

const REAL: &str = "export class Real { go() {} }\n";

#[test]
fn unlocated_import_pin_is_never_persisted_raw() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(tmp.path(), &[("user.ts", USER), ("real.ts", REAL)]);
    let root = tmp.path().to_path_buf();
    let mut indexer = Indexer::new(root.clone(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let stored: Vec<String> = ["edges", "unresolved_references"]
        .iter()
        .flat_map(|table| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT receiver_type FROM {table}
                     WHERE graph_version = ? AND receiver_type IS NOT NULL"
                ))
                .unwrap();
            stmt.query_map([gv], |r| r.get::<_, String>(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        stored.iter().any(|t| t == "Missing") && stored.iter().any(|t| t == "Renamed"),
        "unlocated pins degrade to the bare type name: {stored:?}"
    );
    assert!(
        stored.iter().any(|t| t == "\u{2}real.Real"),
        "a located pin stays pinned: {stored:?}"
    );
    for t in &stored {
        assert!(!t.contains('\0'), "raw placeholder stored: {t:?}");
        assert!(!t.contains("no-such"), "raw specifier stored: {t:?}");
    }
}
