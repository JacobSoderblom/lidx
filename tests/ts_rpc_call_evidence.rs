//! Issue #204: a TS/JS `new X()` receiver is a gRPC client only on positive
//! evidence (generated stub import or proto-loader root), never merely
//! because it is constructed.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

const PROTO: &str = "syntax = \"proto3\";\npackage datacatalog;\n\
service DataCatalogService {\n  rpc GetItem (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";

const REAL: &str = "import { DataCatalogServiceClient } from './gen/datacatalog_grpc_pb';\n\
export function callReal() {\n  const c = new DataCatalogServiceClient('h');\n  c.getItem({});\n}\n";

const NOISE: &str = "import sql from 'mssql';\n\
class Repository { find() { return 1; } }\n\
export function useMap() {\n\
  const m = new Map<string, string>();\n  m.set('a', 'b');\n  m.has('a');\n\
  const s = new Set<string>();\n  s.add('x');\n\
  const d = new Date();\n  d.getTime();\n\
  const r = new sql.Request();\n  r.input('a', 1);\n\
  const repo = new Repository();\n  repo.find();\n}\n";

struct Repo {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    db: std::path::PathBuf,
    indexer: Indexer,
}

fn repo(files: &[(&str, &str)]) -> Repo {
    let (tmp, root, db) = common::index_repo("lidx-ts-rpc-evidence-", files);
    let indexer = Indexer::new(root.clone(), db.clone()).unwrap();
    Repo {
        _tmp: tmp,
        root,
        db,
        indexer,
    }
}

/// Every stored RPC_CALL row, including name-only ones no query surfaces.
fn rpc_call_edges(repo: &Repo) -> Vec<(String, Value)> {
    let conn = repo.indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT target_qualname, detail FROM edges WHERE kind = 'RPC_CALL'")
        .unwrap();
    stmt.query_map([], |r| {
        Ok((
            r.get::<_, Option<String>>(0)?.unwrap_or_default(),
            r.get::<_, Option<String>>(1)?.unwrap_or_default(),
        ))
    })
    .unwrap()
    .map(|r| {
        let (t, d) = r.unwrap();
        (t, serde_json::from_str(&d).unwrap_or(Value::Null))
    })
    .collect()
}

/// RPC_CALL targets as returned by the edge query layer (sorted).
fn surfaced_rpc_calls(repo: &Repo) -> Vec<String> {
    let db = repo.indexer.db();
    let gv = db.current_graph_version().unwrap();
    let kinds = vec!["RPC_CALL".to_string()];
    let mut targets: Vec<String> = db
        .list_edges(
            1000,
            0,
            None,
            None,
            Some(&kinds),
            None,
            None,
            None,
            false,
            None,
            gv,
            None,
            None,
            None,
        )
        .unwrap()
        .into_iter()
        .filter_map(|e| e.target_qualname)
        .collect();
    targets.sort();
    targets
}

fn proto_services(repo: &Repo) -> Vec<String> {
    let conn = repo.indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM symbols WHERE kind = 'service'")
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

const ORDER_PROTO: &str = "syntax = \"proto3\";\npackage shop;\n\
service OrderService {\n  rpc PlaceOrder (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";

/// A `*Client` imported from a neutral path: accepted on its name alone.
const NAME_ONLY: &str = "import { OrderServiceClient } from './order';\n\
export function place() {\n  const c = new OrderServiceClient('h');\n  c.placeOrder({});\n}\n";

#[test]
fn rpc_call_edges_only_name_services_in_the_index() {
    let repo = repo(&[
        ("protos/datacatalog.proto", PROTO),
        ("src/real.ts", REAL),
        ("src/noise.ts", NOISE),
    ]);
    let services = proto_services(&repo);
    assert!(
        services.iter().any(|s| s == "DataCatalogService"),
        "{services:?}"
    );
    let edges = rpc_call_edges(&repo);
    assert!(
        edges
            .iter()
            .any(|(t, _)| t == "/datacatalogservice/getitem"),
        "real client edge missing: {edges:?}"
    );
    for (target, detail) in &edges {
        let service = detail["service"].as_str().unwrap_or_default();
        assert!(
            services.iter().any(|s| s.as_str() == service),
            "RPC_CALL {target} names service {service:?} absent from the index {services:?}"
        );
    }
    assert_eq!(
        surfaced_rpc_calls(&repo),
        vec!["/datacatalogservice/getitem"]
    );
}

#[test]
fn name_only_client_surfaces_only_when_proto_service_is_indexed() {
    let with_proto = repo(&[("protos/order.proto", ORDER_PROTO), ("src/o.ts", NAME_ONLY)]);
    assert_eq!(
        surfaced_rpc_calls(&with_proto),
        vec!["/orderservice/placeorder"]
    );
    let without = repo(&[("src/o.ts", NAME_ONLY)]);
    assert_eq!(rpc_call_edges(&without).len(), 1, "row is still stored");
    assert!(
        surfaced_rpc_calls(&without).is_empty(),
        "name-only edge without a proto service must not surface"
    );
}

#[test]
fn name_only_client_visibility_flips_with_the_proto_like_a_fresh_index() {
    let mut inc = repo(&[("src/o.ts", NAME_ONLY)]);
    assert!(surfaced_rpc_calls(&inc).is_empty());

    common::write_files(&inc.root, &[("protos/order.proto", ORDER_PROTO)]);
    inc.indexer
        .sync_rel_paths(&["protos/order.proto".to_string()])
        .unwrap();
    let fresh = repo(&[("protos/order.proto", ORDER_PROTO), ("src/o.ts", NAME_ONLY)]);
    assert_eq!(surfaced_rpc_calls(&inc), surfaced_rpc_calls(&fresh));
    assert_eq!(surfaced_rpc_calls(&inc), vec!["/orderservice/placeorder"]);

    std::fs::remove_file(inc.root.join("protos/order.proto")).unwrap();
    inc.indexer
        .sync_rel_paths(&["protos/order.proto".to_string()])
        .unwrap();
    let fresh = repo(&[("src/o.ts", NAME_ONLY)]);
    assert_eq!(surfaced_rpc_calls(&inc), surfaced_rpc_calls(&fresh));
    assert!(surfaced_rpc_calls(&inc).is_empty());
}

#[test]
fn trace_flow_over_rpc_kinds_on_map_and_set_only_has_no_hops() {
    let repo = repo(&[("src/noise.ts", NOISE)]);
    assert!(rpc_call_edges(&repo).is_empty());
    let resp = rpc::call(
        repo.root.clone(),
        repo.db.clone(),
        "trace_flow".to_string(),
        r#"{"start_qualname":"src/noise.useMap","direction":"downstream","max_hops":5,"kinds":["RPC_CALL","RPC_IMPL"]}"#,
        "1",
    )
    .unwrap();
    let v: Value = serde_json::from_str(&resp).unwrap();
    let text = v["result"].to_string();
    assert!(!text.contains("RPC_CALL"), "unexpected RPC hop: {text}");
}
