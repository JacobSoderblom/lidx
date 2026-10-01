//! Issue #233: an HTTP_CALL must bridge only to a route in the same service,
//! never to an unrelated service that happens to declare the same path.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;

const SERVER: &str = r#"
from aiohttp import web

class HealthServer:
    def __init__(self):
        self.app = web.Application()
        self.app.router.add_get("/health/live", self._liveness)

    async def _liveness(self, request):
        return web.Response(text="ok")
"#;

const TEST: &str = r#"
async def test_liveness_returns_200(client):
    resp = await client.get("/health/live")
    assert resp.status == 200
"#;

const TS: &str = r#"
export async function healthRoutes(fastify) {
  fastify.get("/health/live", async () => ({ ok: true }));
}
"#;

/// Index `files`, then return the files of every hop `trace_flow` reaches from
/// `start` over HTTP edges.
fn hops(files: &[(&str, &str)], start: &str) -> Vec<(String, String)> {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-http-bridge-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    let params = serde_json::json!({
        "start_qualname": start,
        "max_hops": 2,
        "kinds": ["CALLS", "HTTP_CALL", "HTTP_ROUTE"],
    })
    .to_string();
    let resp = rpc::call(
        tmp.path().to_path_buf(),
        db,
        "trace_flow".to_string(),
        &params,
        "1",
    )
    .unwrap();
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    let result = &v["result"];
    let mut out = Vec::new();
    collect(result, &mut out);
    out
}

/// (file_path, qualname) of every symbol-looking object that is a HTTP_ROUTE hop.
fn collect(v: &serde_json::Value, out: &mut Vec<(String, String)>) {
    match v {
        serde_json::Value::Object(m) => {
            if m.get("edge_kind").and_then(|k| k.as_str()) == Some("HTTP_ROUTE") {
                let sym = &m["symbol"];
                out.push((
                    sym["file_path"].as_str().unwrap_or_default().to_string(),
                    sym["qualname"].as_str().unwrap_or_default().to_string(),
                ));
            }
            m.values().for_each(|x| collect(x, out));
        }
        serde_json::Value::Array(a) => a.iter().for_each(|x| collect(x, out)),
        _ => {}
    }
}

const START: &str = "py.orchestrator.tests.test_health_server.test_liveness_returns_200";

#[test]
fn python_test_does_not_bridge_to_unrelated_node_service() {
    let got = hops(
        &[
            ("py/orchestrator/src/orch/health_server.py", SERVER),
            ("py/orchestrator/tests/test_health_server.py", TEST),
            ("node/datacatalog-api/src/routes/health.ts", TS),
        ],
        START,
    );
    assert!(
        got.iter().all(|(f, _)| !f.starts_with("node/")),
        "false cross-service bridge: {got:?}"
    );
    assert!(
        got.iter()
            .any(|(f, q)| f.starts_with("py/orchestrator/src") && q.ends_with("_liveness")),
        "same-service bridge missing: {got:?}"
    );
}

#[test]
fn python_test_bridges_to_python_route_without_the_node_service() {
    let got = hops(
        &[
            ("py/orchestrator/src/orch/health_server.py", SERVER),
            ("py/orchestrator/tests/test_health_server.py", TEST),
        ],
        START,
    );
    assert!(
        got.iter()
            .any(|(f, q)| f.starts_with("py/orchestrator/src") && q.ends_with("_liveness")),
        "{got:?}"
    );
}

#[test]
fn call_does_not_bridge_to_a_sibling_python_service() {
    let got = hops(
        &[
            ("py/orchestrator/src/orch/health_server.py", SERVER),
            ("py/orchestrator/tests/test_health_server.py", TEST),
            ("py/other/src/other/health_server.py", SERVER),
        ],
        START,
    );
    assert!(
        got.iter().all(|(f, _)| !f.starts_with("py/other/")),
        "bridged into sibling service: {got:?}"
    );
    assert!(
        got.iter().any(|(f, _)| f.starts_with("py/orchestrator/")),
        "{got:?}"
    );
}

#[test]
fn ambiguous_services_produce_no_bridge() {
    let got = hops(
        &[
            ("py/orchestrator/tests/test_health_server.py", TEST),
            ("py/a/src/a/health_server.py", SERVER),
            ("node/b/src/routes/health.ts", TS),
        ],
        START,
    );
    assert!(
        got.is_empty(),
        "definite bridge from ambiguous evidence: {got:?}"
    );
}

#[test]
fn unique_route_in_another_service_still_bridges() {
    let got = hops(
        &[
            ("py/orchestrator/tests/test_health_server.py", TEST),
            ("py/a/src/a/health_server.py", SERVER),
        ],
        START,
    );
    assert!(
        got.iter().any(|(f, _)| f.starts_with("py/a/")),
        "a lone route must stay reachable: {got:?}"
    );
}

/// Index `files`, run `method`, and return the raw response text.
fn call_raw(files: &[(&str, &str)], method: &str, params: serde_json::Value) -> String {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-http-bridge-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    rpc::call(
        tmp.path().to_path_buf(),
        db,
        method.to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap()
}

const MARKER: &str = "cross_service_http";

fn trace(files: &[(&str, &str)], start: &str) -> String {
    call_raw(
        files,
        "trace_flow",
        serde_json::json!({
            "start_qualname": start,
            "max_hops": 2,
            "kinds": ["CALLS", "HTTP_CALL", "HTTP_ROUTE"],
        }),
    )
}

fn impact(files: &[(&str, &str)], qualname: &str) -> String {
    call_raw(
        files,
        "analyze_impact",
        serde_json::json!({"qualname": qualname, "direction": "downstream", "max_depth": 3, "include_tests": true}),
    )
}

#[test]
fn speculative_fallback_is_marked_and_same_service_is_not() {
    let fallback = [
        ("py/orchestrator/tests/test_health_server.py", TEST),
        ("py/a/src/a/health_server.py", SERVER),
    ];
    let same = [
        ("py/orchestrator/src/orch/health_server.py", SERVER),
        ("py/orchestrator/tests/test_health_server.py", TEST),
    ];
    let t = trace(&fallback, START);
    assert!(t.contains("py/a/") && t.contains(MARKER), "{t}");
    assert!(t.contains("speculative"), "{t}");
    let t = trace(&same, START);
    assert!(t.contains("_liveness") && !t.contains(MARKER), "{t}");
    let i = impact(&fallback, START);
    assert!(i.contains("py/a/") && i.contains(MARKER), "{i}");
    let i = impact(&same, START);
    assert!(i.contains("_liveness") && !i.contains(MARKER), "{i}");
}

const TS_CLIENT: &str = r#"
export async function loadX() {
  return fetch('/api/x');
}
"#;

const PY_ROUTE: &str = r#"
from aiohttp import web

async def get_x(request):
    return web.json_response({})

def setup(app):
    app.router.add_get("/api/x", get_x)
"#;

const CS_ROUTE: &str = r#"
using Microsoft.AspNetCore.Mvc;
namespace Api {
  [ApiController]
  public class XController : ControllerBase {
    [HttpGet("/api/x")]
    public IActionResult GetX() => Ok();
  }
}
"#;

#[test]
fn frontend_fetch_bridges_to_single_backend_route() {
    for (path, src, needle) in [
        ("api/src/routes.py", PY_ROUTE, "get_x"),
        ("api/src/XController.cs", CS_ROUTE, "GetX"),
    ] {
        let files = [("web/src/api.ts", TS_CLIENT), (path, src)];
        let t = trace(&files, "web/src/api.loadX");
        assert!(
            t.contains(needle),
            "trace_flow lost the bridge to {path}: {t}"
        );
        let i = impact(&files, "web/src/api.loadX");
        assert!(
            i.contains(needle),
            "analyze_impact lost the bridge to {path}: {i}"
        );
    }
}

#[test]
fn caller_in_neither_of_two_backends_gets_no_definite_bridge() {
    let files = [
        ("web/src/api.ts", TS_CLIENT),
        ("api1/src/routes.py", PY_ROUTE),
        ("api2/src/XController.cs", CS_ROUTE),
    ];
    let t = trace(&files, "web/src/api.loadX");
    assert!(
        !t.contains("get_x") && !t.contains("GetX"),
        "definite bridge from ambiguous backends: {t}"
    );
    let i = impact(&files, "web/src/api.loadX");
    assert!(!i.contains("get_x") && !i.contains("GetX"), "{i}");
}
