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
  router.route(`/api/other/${id}`).post(h);
}"#;
    assert_eq!(targets(src), vec!["/api/items", "/api/other/{}"]);
}

#[test]
fn unresolvable_hole_becomes_param_segment() {
    let src = "function r(app: any, id: string) { app.get(`/api/items/${id}`, h); }";
    assert_eq!(targets(src), vec!["/api/items/{}"]);
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
