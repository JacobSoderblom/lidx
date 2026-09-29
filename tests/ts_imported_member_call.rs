//! Issue #113: `x.m()` where `x` is imported from a repo module must bind to
//! `x` (object-literal members aren't indexed), never an `ext:` stub.

mod common;

use lidx::indexer::Indexer;

const API_CLIENT: &str = "export const apiClient = {\n  get(url: string) { return url; },\n  post(url: string) { return url; },\n};\n";

fn call_targets(files: &[(&str, &str)], caller: &str) -> Vec<String> {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-ts-imported-member-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = ?",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, caller], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn relative_import_member_call_binds_to_imported_object() {
    let targets = call_targets(
        &[
            ("lib/api-client.ts", API_CLIENT),
            (
                "queries/client.ts",
                "import { apiClient } from '../lib/api-client';\n\nexport function load() {\n  return apiClient.get('/x');\n}\n",
            ),
        ],
        "queries/client.load",
    );
    assert_eq!(targets, vec!["lib/api-client.apiClient".to_string()]);
}

#[test]
fn alias_import_member_call_binds_to_imported_object() {
    let targets = call_targets(
        &[
            (
                "tsconfig.json",
                "{\"compilerOptions\":{\"paths\":{\"@/*\":[\"./*\"]}}}",
            ),
            ("lib/api-client.ts", API_CLIENT),
            (
                "queries/client.ts",
                "import { apiClient } from '@/lib/api-client';\n\nexport function load() {\n  return apiClient.get('/x');\n}\n",
            ),
        ],
        "queries/client.load",
    );
    assert_eq!(targets, vec!["lib/api-client.apiClient".to_string()]);
}
