//! Issue #211: HTTP_CALL edges for calls through in-repo `fetch`/axios
//! wrappers defined in another file.

mod common;

use lidx::indexer::Indexer;

type Call = (String, String, String);

fn http_calls_of(indexer: &Indexer) -> Vec<Call> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, e.target_qualname, e.detail FROM edges e \
             JOIN symbols s ON s.id = e.source_symbol_id \
             WHERE e.graph_version = ? AND e.kind = 'HTTP_CALL' ORDER BY e.id",
        )
        .unwrap();
    let mut calls: Vec<Call> = stmt
        .query_map(rusqlite::params![gv], |r| {
            let detail: serde_json::Value = serde_json::from_str(&r.get::<_, String>(2)?).unwrap();
            Ok((
                r.get(0)?,
                r.get(1)?,
                detail["method"].as_str().unwrap().to_string(),
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    calls.sort();
    calls
}

fn http_calls(files: &[(&str, &str)]) -> Vec<Call> {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(tmp.path(), files);
    let db = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db).unwrap();
    indexer.reindex().unwrap();
    http_calls_of(&indexer)
}

fn call(source: &str, target: &str, method: &str) -> Call {
    (source.into(), target.into(), method.into())
}

#[test]
fn default_exported_wrappers_are_recognised() {
    let calls = http_calls(&[
        (
            "lib/fn.ts",
            "export default function api(url: string, init?: RequestInit) { return fetch(url, init); }\n",
        ),
        (
            "lib/obj.ts",
            "export default {\n  get(url: string) { return fetch(url); },\n  \
             post: (url: string) => fetch(url, { method: \"POST\" }),\n};\n",
        ),
        (
            "lib/anon.ts",
            "export default (url: string) => fetch(url, { method: \"PUT\" });\n",
        ),
        (
            "c.ts",
            "import api from \"./lib/fn\";\nimport http from \"./lib/obj\";\nimport anon from \"./lib/anon\";\n\
             export function a() { return api(\"/api/a\"); }\n\
             export function b() { return http.get(\"/api/b\"); }\n\
             export function c() { return http.post(\"/api/c\"); }\n\
             export function d() { return anon(\"/api/d\"); }\n",
        ),
    ]);
    assert_eq!(
        calls,
        [
            call("c.a", "/api/a", "GET"),
            call("c.b", "/api/b", "GET"),
            call("c.c", "/api/c", "POST"),
            call("c.d", "/api/d", "PUT"),
        ]
    );
}

#[test]
fn url_may_be_any_parameter() {
    let calls = http_calls(&[
        (
            "lib/http.ts",
            "export function request(method: string, url: string) { return fetch(url, { method }); }\n\
             export const api = { del: (id: number, url: string) => request(\"DELETE\", url) };\n",
        ),
        (
            "c.ts",
            "import { request, api } from \"./lib/http\";\n\
             export function a() { return request(\"POST\", \"/api/a\"); }\n\
             export function b() { return api.del(1, \"/api/b\"); }\n",
        ),
    ]);
    assert_eq!(
        calls,
        [
            call("c.a", "/api/a", "POST"),
            call("c.b", "/api/b", "DELETE")
        ]
    );
}

#[test]
fn leading_interpolation_is_not_invented_into_a_path() {
    let calls = http_calls(&[
        (
            "lib/http.ts",
            "export function request(url: string) { return fetch(url); }\n",
        ),
        (
            "c.ts",
            "import { request } from \"./lib/http\";\n\
             export function dyn(id: string) { return request(`${id}/api/x`); }\n\
             export function base() { return request(`${process.env.API_BASE}/api/y`); }\n",
        ),
    ]);
    assert_eq!(calls, [call("c.base", "/api/y", "GET")]);
}

#[test]
fn wrapper_imported_through_tsconfig_alias() {
    let calls = http_calls(&[
        (
            "tsconfig.json",
            r#"{"compilerOptions": {"paths": {"@/*": ["./*"]}}}"#,
        ),
        (
            "lib/w.ts",
            "export const api = { get: (url: string) => fetch(url) };\n",
        ),
        (
            "q/c.ts",
            "import { api } from \"@/lib/w\";\nexport function list() { return api.get(\"/api/t\"); }\n",
        ),
    ]);
    assert_eq!(calls, [call("q/c.list", "/api/t", "GET")]);
}

#[test]
fn axios_wrapper_end_to_end() {
    let calls = http_calls(&[
        (
            "lib/w.ts",
            "import axios from \"axios\";\n\
             export function post(url: string, body: unknown) { return axios.post(url, body); }\n\
             export const client = { fetchAll: (path: string) => axios.get(path) };\n",
        ),
        (
            "c.ts",
            "import { post, client } from \"./lib/w\";\n\
             export function a() { return post(\"/api/a\", {}); }\n\
             export function b() { return client.fetchAll(`/api/b?x=${1}`); }\n",
        ),
    ]);
    assert_eq!(
        calls,
        [call("c.a", "/api/a", "POST"), call("c.b", "/api/b", "GET")]
    );
}

/// No wrapper placeholder edge, resolved or not, may reach the database.
#[test]
fn pending_wrapper_edges_never_reach_the_db() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(
        tmp.path(),
        &[
            (
                "lib/w.ts",
                "export function req(url: string) { return fetch(url); }\n\
                 export function other(x: string) { return x; }\n",
            ),
            (
                "c.ts",
                "import { req, other } from \"./lib/w\";\nimport { gone } from \"./missing\";\n\
                 import def from \"./lib/w\";\n\
                 export function a() { req(\"/api/a\"); other(\"/api/b\"); gone(\"/api/c\"); def(\"/api/d\"); }\n",
            ),
        ],
    );
    let db = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db).unwrap();
    indexer.reindex().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges WHERE kind LIKE '%pending%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);
    assert_eq!(http_calls_of(&indexer), [call("c.a", "/api/a", "GET")]);
}
