//! Issue #233: aiohttp's imperative route registration must yield HTTP_ROUTE
//! edges, not just decorator/Django/FastAPI routes.

use lidx::indexer::extract::{ExtractedFile, LanguageExtractor};
use lidx::indexer::python::PythonExtractor;

fn extract(source: &str) -> ExtractedFile {
    let mut extractor = PythonExtractor::new().unwrap();
    extractor.extract(source, "svc.server").unwrap()
}

/// (source_qualname, method, path) of every HTTP_ROUTE edge.
fn routes(file: &ExtractedFile) -> Vec<(String, String, String)> {
    let mut out: Vec<_> = file
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .map(|e| {
            let detail: serde_json::Value =
                serde_json::from_str(e.detail.as_deref().unwrap()).unwrap();
            (
                e.source_qualname.clone().unwrap(),
                detail["method"].as_str().unwrap().to_string(),
                e.target_qualname.clone().unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

fn r(h: &str, m: &str, p: &str) -> (String, String, String) {
    (h.to_string(), m.to_string(), p.to_string())
}

#[test]
fn router_add_get_in_class_method_targets_handler() {
    let file = extract(
        r#"
from aiohttp import web

class HealthServer:
    def __init__(self):
        self.app = web.Application()

    def build(self):
        self.app.router.add_get("/health/live", self._liveness)
        self.app.router.add_get("/health/ready", self._readiness)

    async def _liveness(self, request):
        return web.Response(text="ok")

    async def _readiness(self, request):
        return web.Response(text="ok")
"#,
    );
    assert_eq!(
        routes(&file),
        vec![
            r("svc.server.HealthServer._liveness", "GET", "/health/live"),
            r("svc.server.HealthServer._readiness", "GET", "/health/ready"),
        ]
    );
}

#[test]
fn router_add_post_and_other_verbs() {
    let file = extract(
        r#"
def setup(app):
    app.router.add_post("/jobs", create_job)
    app.router.add_put("/jobs/1", put_job)
    app.router.add_patch("/jobs/1", patch_job)
    app.router.add_delete("/jobs/1", delete_job)
"#,
    );
    let got = routes(&file);
    assert_eq!(got.len(), 4, "{got:?}");
    assert!(got.contains(&r("svc.server.create_job", "POST", "/jobs")));
    assert!(
        got.contains(&r("svc.server.delete_job", "DELETE", "/jobs/{}")),
        "{got:?}"
    );
}

#[test]
fn router_add_route_with_explicit_method() {
    let file = extract(
        r#"
def setup(app):
    app.router.add_route("PUT", "/items", handler)
    app.router.add_route("*", "/any", other)
"#,
    );
    assert_eq!(
        routes(&file),
        vec![
            r("svc.server.handler", "PUT", "/items"),
            r("svc.server.other", "ANY", "/any"),
        ]
    );
}

#[test]
fn web_route_table_form() {
    let file = extract(
        r#"
from aiohttp import web

def setup(app):
    app.add_routes([
        web.get("/status", status),
        web.post("/submit", submit),
        web.route("DELETE", "/gone", gone),
    ])
"#,
    );
    assert_eq!(
        routes(&file),
        vec![
            r("svc.server.gone", "DELETE", "/gone"),
            r("svc.server.status", "GET", "/status"),
            r("svc.server.submit", "POST", "/submit"),
        ]
    );
}

#[test]
fn unrelated_add_get_is_not_a_route() {
    let file = extract(
        r#"
def f(registry, cache):
    registry.add_get("/x", handler)
    cache.get("/y", default)
"#,
    );
    assert!(routes(&file).is_empty());
}
