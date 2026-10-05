//! Issue #335: route paths that are same-file consts, template literals, or
//! template literals filled from a `for...of` over a literal array.

mod common;

use lidx::indexer::Indexer;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::javascript::TypescriptExtractor;

/// (method, target, evidence line) of every HTTP_ROUTE edge, sorted.
fn routes(source: &str) -> Vec<(String, String, i64)> {
    let mut x = TypescriptExtractor::new().unwrap();
    let mut out: Vec<_> = x
        .extract(source, "routes")
        .unwrap()
        .edges
        .into_iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .map(|e| {
            let detail: serde_json::Value =
                serde_json::from_str(e.detail.as_deref().unwrap()).unwrap();
            (
                detail["method"].as_str().unwrap().to_string(),
                e.target_qualname.unwrap(),
                e.evidence_start_line.unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

fn targets(source: &str) -> Vec<String> {
    let mut t: Vec<String> = routes(source).into_iter().map(|r| r.1).collect();
    t.sort();
    t
}

const FIXTURE: &str = r#"export async function routes(app: any) {
  for (const [path, kind] of [['dataproducts', 'a'], ['datasources', 'b']] as const) {
    app.get(`/llm/${path}`, async () => kind);
  }
  for (const [path, h] of [['trace', 1], ['impact', 2]] as const) {
    app.get(`/llm/lineage/${path}`, async () => h);
  }
}
"#;

#[test]
fn loop_template_routes_emit_one_edge_per_element() {
    let r = routes(FIXTURE);
    assert_eq!(
        r,
        vec![
            ("GET".into(), "/llm/dataproducts".into(), 3),
            ("GET".into(), "/llm/datasources".into(), 3),
            ("GET".into(), "/llm/lineage/impact".into(), 6),
            ("GET".into(), "/llm/lineage/trace".into(), 6),
        ]
    );
}

#[test]
fn loop_routes_coexist_with_param_route_and_literal_route() {
    let src = r#"export async function routes(app: any) {
  app.get('/llm/search', async () => 1);
  app.get('/llm/dataproducts/:id', async () => 1);
  for (const [path] of [['dataproducts']] as const) {
    app.get(`/llm/${path}`, async () => 1);
  }
}
"#;
    assert_eq!(
        targets(src),
        vec!["/llm/dataproducts", "/llm/dataproducts/{}", "/llm/search"]
    );
}

#[test]
fn single_identifier_loop_variable_is_bound() {
    let src = "function r(app: any) { for (const p of ['a', 'b']) { app.post(`/x/${p}`, h); } }";
    assert_eq!(targets(src), vec!["/x/a", "/x/b"]);
}

#[test]
fn nested_loops_produce_the_product() {
    let src = r#"function r(app: any) {
  for (const a of ['one', 'two']) {
    for (const b of ['x', 'y']) {
      app.get(`/${a}/${b}`, h);
    }
  }
}"#;
    assert_eq!(targets(src), vec!["/one/x", "/one/y", "/two/x", "/two/y"]);
}

#[test]
fn const_and_static_template_paths_emit() {
    let src = r#"const P = '/api/things';
const BASE = '/api';
function r(app: any) {
  app.get(P, h);
  app.get(`/api/static`, h);
  app.get(`${BASE}/x`, h);
}"#;
    assert_eq!(targets(src), vec!["/api/static", "/api/things", "/api/x"]);
}

#[test]
fn chained_route_accepts_template_and_const() {
    let src = r#"const P = '/api/items';
function r(router: any) {
  router.route(P).get(h);
  router.route(`/api/other/${LATER}`).post(h);
  for (const p of ['a']) { router.route(`/api/loop/${p}`).post(h); }
}
const LATER = 'x';"#;
    assert_eq!(
        targets(src),
        vec!["/api/items", "/api/loop/a", "/api/other/x"]
    );
}

#[test]
fn unresolvable_non_loop_hole_emits_nothing() {
    let src = r#"import { API_PREFIX } from './cfg';
function r(app: any, id: string) {
  app.get(`/api/items/${id}`, h);
  app.get(`${API_PREFIX}/x`, h);
  app.get(`/api/${UNKNOWN}/y`, h);
  app.get(imported, h);
}"#;
    assert!(targets(src).is_empty(), "{:?}", targets(src));
}

#[test]
fn parameter_shadowing_the_loop_variable_is_not_substituted() {
    let src = r#"function r(app: any) {
  for (const p of ['a', 'b']) {
    register((p: string) => { app.get(`/s/${p}`, h); });
    register(function (p) { app.get(`/t/${p}`, h); });
    app.get(`/u/${p}`, h);
  }
}"#;
    assert_eq!(targets(src), vec!["/u/a", "/u/b"]);
}

#[test]
fn combination_cap_falls_back_to_param_segments() {
    let nine = "['a','b','c','d','e','f','g','h','i']";
    let eight = "['a','b','c','d','e','f','g','h']";
    let src = format!(
        "function r(app: any) {{ for (const a of {nine}) {{ for (const b of {eight}) {{ \
         app.get(`/p/${{a}}/${{b}}`, h); }} }} }}"
    );
    assert_eq!(targets(&src), vec!["/p/{}/{}"]);
    let ok = format!(
        "function r(app: any) {{ for (const a of {eight}) {{ for (const b of {eight}) {{ \
         app.get(`/p/${{a}}/${{b}}`, h); }} }} }}"
    );
    assert_eq!(targets(&ok).len(), 64);
}

#[test]
fn fastify_object_form_in_loop_is_expanded() {
    let src = r#"export async function routes(app: any) {
  for (const p of ['alpha', 'beta']) {
    app.route({ method: 'GET', url: `/x/${p}`, handler: async () => 1 });
  }
  app.route({ method: ['GET', 'POST'], url: '/plain', handler: async () => 1 });
}"#;
    assert_eq!(
        routes(src),
        vec![
            ("GET".to_string(), "/plain".to_string(), 5),
            ("GET".to_string(), "/x/alpha".to_string(), 3),
            ("GET".to_string(), "/x/beta".to_string(), 3),
            ("POST".to_string(), "/plain".to_string(), 5),
        ]
    );
}

#[test]
fn unresolvable_loop_source_never_fabricates_a_name() {
    let src = r#"function r(app: any, names: string[]) {
  for (const n of names) { app.get(`/a/${n}`, h); }
  for (const m of getNames()) { app.get(`/b/${m}`, h); }
  for (const [k] of load()) { app.get(`/c/${k}`, h); }
  for (const q of ['ok', dyn]) { app.get(`/d/${q}`, h); }
  for (const w of names) { app.get(w, h); }
}"#;
    assert_eq!(
        targets(src),
        vec!["/a/{}", "/b/{}", "/c/{}", "/d/ok", "/d/{}"]
    );
}

#[test]
fn literal_route_unchanged() {
    let src = "function r(app: any) { app.get('/api/users/:id', h); }";
    assert_eq!(
        routes(src),
        vec![("GET".to_string(), "/api/users/{}".to_string(), 1)]
    );
}

/// Targets of the current graph version's HTTP_ROUTE edges (the generic
/// edge snapshot drops unresolved targets, which would hide a bad route).
fn current_route_targets(indexer: &Indexer) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT target_qualname FROM edges WHERE kind='HTTP_ROUTE' AND graph_version=?1 \
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map([gv], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn incremental_reindex_matches_fresh_index() {
    let edited = FIXTURE.replace("'trace'", "'tracing'");
    let (_t1, root, db_path) = common::index_repo("lidx-loop-routes-", &[("routes.ts", FIXTURE)]);
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    assert_eq!(current_route_targets(&indexer).len(), 4);
    common::write_files(&root, &[("routes.ts", &edited)]);
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let incremental = common::golden::snapshot_edges(indexer.db(), gv).unwrap();
    let incremental_targets = current_route_targets(&indexer);

    let (_t2, root2, db2) = common::index_repo("lidx-loop-routes-", &[("routes.ts", &edited)]);
    let fresh_indexer = Indexer::new(root2, db2).unwrap();
    let fgv = fresh_indexer.db().current_graph_version().unwrap();
    let fresh = common::golden::snapshot_edges(fresh_indexer.db(), fgv).unwrap();
    common::assert_matches_fresh(&incremental, &fresh);
    assert_eq!(incremental_targets, current_route_targets(&fresh_indexer));
    assert_eq!(
        incremental_targets,
        vec![
            "/llm/dataproducts",
            "/llm/datasources",
            "/llm/lineage/impact",
            "/llm/lineage/tracing"
        ]
    );
}

/// Targets of the HTTP_ROUTE edges a client HTTP_CALL is bridged to, by trace.
#[test]
fn client_call_bridges_to_the_generated_route_not_the_param_route() {
    let routes_ts = r#"export async function routes(app: any) {
  app.get('/llm/dataproducts/:id', async function byId() { return 1; });
  for (const p of ['dataproducts']) {
    app.get(`/llm/${p}`, async function list() { return 2; });
  }
}
"#;
    let client_ts = "export async function load() { await fetch('/llm/dataproducts'); }\n";
    let tmp = tempfile::Builder::new()
        .prefix("lidx-loop-bridge-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("svc/routes.ts", routes_ts), ("svc/client.ts", client_ts)],
    );
    let db = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    let params = serde_json::json!({
        "start_qualname": "client.load",
        "max_hops": 2,
        "kinds": ["CALLS", "HTTP_CALL", "HTTP_ROUTE"],
    })
    .to_string();
    let resp = lidx::rpc::call(
        tmp.path().to_path_buf(),
        db,
        "trace_flow".to_string(),
        &params,
        "1",
    )
    .unwrap();
    assert!(resp.contains("\"HTTP_ROUTE\""), "no route hop: {resp}");
    assert!(
        resp.contains(r#""path":"/llm/dataproducts""#),
        "generated route not reached: {resp}"
    );
    assert!(
        !resp.contains("/llm/dataproducts/{}"),
        "bridged to the param route: {resp}"
    );
}
