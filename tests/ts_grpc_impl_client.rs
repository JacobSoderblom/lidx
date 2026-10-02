//! Issue #208: TS gRPC server/client wiring. The RPC_IMPL edge belongs to the
//! real handler (also when wrapped or imported), client calls survive
//! re-binding and `.bind`, and the proto rpc reaches both ends.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

const PROTO: &str = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc GetTables (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";

const HANDLER: &str = "export function getTables(call, cb) { cb(null, {}); }\n";

const PLUGIN: &str = r#"
import { getTables } from "../features/datacatalog/tables/get";
import { DataCatalogServiceService } from "../gen/datacatalog_grpc_pb";
export function grpcPlugin(fastify) {
  fastify.grpcServer.addService(DataCatalogServiceService, {
    getTables: grpcHandler(getTables, fastify),
  });
}
"#;

const CLIENT: &str = r#"
import { DataCatalogServiceClient } from "../gen/datacatalog_grpc_pb";
const client = new DataCatalogServiceClient("h");
const dataCatalogClient = client;
export function loadTables(req) {
  return dataCatalogClient.getTables.bind(dataCatalogClient)(req);
}
"#;

const FILES: &[(&str, &str)] = &[
    ("protos/datacatalog.proto", PROTO),
    ("src/features/datacatalog/tables/get.ts", HANDLER),
    ("src/plugins/grpc.ts", PLUGIN),
    ("src/client/tables.ts", CLIENT),
];

struct Repo {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    db: std::path::PathBuf,
    indexer: Indexer,
}

fn repo() -> Repo {
    let (tmp, root, db) = common::index_repo("lidx-ts-grpc-impl-", FILES);
    let indexer = Indexer::new(root.clone(), db.clone()).unwrap();
    Repo {
        _tmp: tmp,
        root,
        db,
        indexer,
    }
}

fn call(repo: &Repo, method: &str, params: Value) -> Value {
    let resp = rpc::call(
        repo.root.clone(),
        repo.db.clone(),
        method.to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap();
    serde_json::from_str::<Value>(&resp).unwrap()["result"].clone()
}

fn rows(repo: &Repo, kind: &str) -> Vec<(String, String, Value)> {
    let conn = repo.indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, e.target_qualname, e.detail FROM edges e
             LEFT JOIN symbols s ON s.id = e.source_symbol_id WHERE e.kind = ?",
        )
        .unwrap();
    stmt.query_map([kind], |r| {
        Ok((
            r.get::<_, Option<String>>(0)?.unwrap_or_default(),
            r.get::<_, Option<String>>(1)?.unwrap_or_default(),
            r.get::<_, Option<String>>(2)?.unwrap_or_default(),
        ))
    })
    .unwrap()
    .map(|r| {
        let (s, t, d) = r.unwrap();
        (s, t, serde_json::from_str(&d).unwrap_or(Value::Null))
    })
    .collect()
}

const RPC_SYMBOL: &str = "datacatalog.v1.DataCatalogService.GetTables";

fn reaches(trace: &Value, qualname_suffix: &str) -> bool {
    trace.to_string().contains(qualname_suffix)
}

#[test]
fn rpc_impl_edge_is_sourced_at_the_wrapped_imported_handler() {
    let repo = repo();
    let impls = rows(&repo, "RPC_IMPL");
    assert_eq!(impls.len(), 1, "{impls:?}");
    assert!(
        impls[0].0.ends_with("get.getTables"),
        "RPC_IMPL source must be the handler, got {impls:?}"
    );
}

#[test]
fn client_call_through_alias_and_bind_emits_rpc_call_never_bind() {
    let repo = repo();
    let calls = rows(&repo, "RPC_CALL");
    assert!(
        calls.iter().any(|(s, _, d)| s.ends_with("loadTables")
            && d["rpc"] == "getTables"
            && d["service"].as_str().is_some()),
        "{calls:?}"
    );
    for (_, target, d) in &calls {
        assert!(!["bind", "call", "apply"].contains(&d["rpc"].as_str().unwrap_or("")));
        assert!(!target.ends_with("/bind"), "{target}");
    }
}

#[test]
fn proto_rpc_reaches_handler_and_handler_reaches_proto() {
    let repo = repo();
    let explain = call(
        &repo,
        "explain_symbol",
        serde_json::json!({"query": RPC_SYMBOL}),
    );
    assert!(reaches(&explain, "getTables"), "explain_symbol: {explain}");

    let down = call(
        &repo,
        "trace_flow",
        serde_json::json!({"start_qualname": RPC_SYMBOL, "direction": "downstream",
            "max_hops": 3, "kinds": ["RPC_CALL", "RPC_IMPL", "RPC_ROUTE", "CALLS"]}),
    );
    assert!(reaches(&down, "get.getTables"), "downstream: {down}");

    let up = call(
        &repo,
        "trace_flow",
        serde_json::json!({"start_qualname": "src/features/datacatalog/tables/get.getTables",
            "direction": "upstream", "max_hops": 3,
            "kinds": ["RPC_CALL", "RPC_IMPL", "RPC_ROUTE", "CALLS"]}),
    );
    assert!(reaches(&up, "GetTables"), "upstream: {up}");
}
