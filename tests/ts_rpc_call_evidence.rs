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
}

fn repo(files: &[(&str, &str)]) -> Repo {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-ts-rpc-evidence-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let root = tmp.path().to_path_buf();
    let db = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    Repo {
        _tmp: tmp,
        root,
        db,
    }
}

fn rpc_call_edges(repo: &Repo) -> Vec<(String, Value)> {
    let indexer = Indexer::new(repo.root.clone(), repo.db.clone()).unwrap();
    let conn = indexer.db().read_conn().unwrap();
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

fn proto_services(repo: &Repo) -> Vec<String> {
    let indexer = Indexer::new(repo.root.clone(), repo.db.clone()).unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM symbols WHERE kind = 'service'")
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

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
        for builtin in ["map", "set", "date", "request", "repository"] {
            assert!(
                !target.to_lowercase().contains(&format!("/{builtin}/")),
                "built-in/library type leaked into RPC target {target}"
            );
        }
    }
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
