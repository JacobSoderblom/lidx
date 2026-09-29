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

fn single_call_target(files: &[(&str, &str)]) -> Vec<String> {
    call_targets(files, "use.go")
}

const NS_LIB: &str = "export function fn() {}\n";

#[test]
fn namespace_import_missing_member_stays_unbound() {
    let t = single_call_target(&[
        ("x.ts", NS_LIB),
        (
            "use.ts",
            "import * as ns from './x';\nexport function go() {\n  return ns.missing();\n}\n",
        ),
    ]);
    assert!(!t.iter().any(|q| q == "x"), "{t:?}");
}

#[test]
fn namespace_import_existing_member_binds() {
    let t = single_call_target(&[
        ("x.ts", NS_LIB),
        (
            "use.ts",
            "import * as ns from './x';\nexport function go() {\n  return ns.fn();\n}\n",
        ),
    ]);
    assert_eq!(t, vec!["x.fn".to_string()]);
}

#[test]
fn third_party_default_import_stays_external() {
    let t = single_call_target(&[(
        "use.ts",
        "import axios from 'axios';\nexport function go() {\n  return axios.get('/x');\n}\n",
    )]);
    assert_eq!(t, vec!["ext:axios.get".to_string()]);
}

#[test]
fn imported_class_missing_static_does_not_bind_to_class() {
    let t = single_call_target(&[
        ("c.ts", "export class Foo {}\n"),
        (
            "use.ts",
            "import { Foo } from './c';\nexport function go() {\n  return Foo.missingStatic();\n}\n",
        ),
    ]);
    assert!(!t.iter().any(|q| q == "c.Foo"), "{t:?}");
}

#[test]
fn local_instance_call_is_not_import_bound() {
    let t = single_call_target(&[
        ("c.ts", "export class Client { get() {} }\n"),
        (
            "use.ts",
            "import { Client } from './c';\nexport function go() {\n  const c = new Client();\n  return c.get();\n}\n",
        ),
    ]);
    // `new Client()` -> the class; `c.get()` -> the real method, unchanged.
    assert!(t.contains(&"c.Client.get".to_string()), "{t:?}");
    assert!(!t.iter().any(|q| q.starts_with("ext:")), "{t:?}");
}
