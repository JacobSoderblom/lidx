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
    repo_with(FILES)
}

fn repo_with(files: &[(&str, &str)]) -> Repo {
    let (tmp, root, db) = common::index_repo("lidx-ts-grpc-impl-", files);
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
    // The bare imported service identifier carries no package itself: it
    // comes from the proto route the path binds to.
    assert_eq!(impls[0].2["package"], "datacatalog.v1");
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
    assert!(
        calls
            .iter()
            .all(|(_, _, d)| d["package"] == "datacatalog.v1")
    );
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

fn packages(repo: &Repo) -> Vec<(String, Value)> {
    ["RPC_IMPL", "RPC_CALL"]
        .iter()
        .flat_map(|k| rows(repo, k))
        .map(|(s, _, d)| (s, d["package"].clone()))
        .collect()
}

#[test]
fn package_and_handler_do_not_depend_on_indexing_order() {
    let fresh = repo();
    let ts_first = repo_with(&[
        ("src/plugins/grpc.ts", PLUGIN),
        ("src/client/tables.ts", CLIENT),
    ]);
    // No proto, no handler file yet: package unknown, edge at the plugin.
    assert!(packages(&ts_first).iter().all(|(_, p)| p.is_null()));
    common::write_files(
        &ts_first.root,
        &[
            ("protos/datacatalog.proto", PROTO),
            ("src/features/datacatalog/tables/get.ts", HANDLER),
        ],
    );
    let mut ts_first = ts_first;
    ts_first
        .indexer
        .sync_rel_paths(&[
            "protos/datacatalog.proto".to_string(),
            "src/features/datacatalog/tables/get.ts".to_string(),
        ])
        .unwrap();
    let mut a = packages(&fresh);
    let mut b = packages(&ts_first);
    a.sort_by_key(|(s, _)| s.clone());
    b.sort_by_key(|(s, _)| s.clone());
    assert_eq!(a, b);
    assert!(b.iter().all(|(_, p)| p == "datacatalog.v1"));
    let impls = rows(&ts_first, "RPC_IMPL");
    assert!(impls[0].0.ends_with("get.getTables"), "{impls:?}");

    // Removing the proto flips the package back, like a fresh index.
    std::fs::remove_file(ts_first.root.join("protos/datacatalog.proto")).unwrap();
    ts_first
        .indexer
        .sync_rel_paths(&["protos/datacatalog.proto".to_string()])
        .unwrap();
    assert!(packages(&ts_first).iter().all(|(_, p)| p.is_null()));
}

const MULTI_PROTO: &str = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc GetTables (Req) returns (Res);\n\
  rpc ListTables (Req) returns (Res);\n  rpc DeleteTable (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";

const MULTI_PLUGIN: &str = r#"
import fastifyPkg from "fastify";
import { serverCfg } from "../lib/server";
import { getTables } from "../features/get";
import { listTables } from "../features/list";
import { DataCatalogServiceService } from "../gen/datacatalog_grpc_pb";
export function grpcPlugin(fastify) {
  fastify.grpcServer.addService(DataCatalogServiceService, {
    getTables: grpcHandler(fastifyPkg, getTables),
    listTables: grpcHandler(serverCfg, listTables),
    deleteTable: grpcHandler(fastifyPkg),
  });
}
"#;

#[test]
fn wrapper_argument_choice_prefers_a_function_and_falls_back_to_scope() {
    let repo = repo_with(&[
        ("protos/datacatalog.proto", MULTI_PROTO),
        (
            "src/lib/server.ts",
            "export const serverCfg = { port: 1 };\n",
        ),
        ("src/features/get.ts", HANDLER),
        (
            "src/features/list.ts",
            "export function listTables(call, cb) { cb(null, {}); }\n",
        ),
        ("src/plugins/grpc.ts", MULTI_PLUGIN),
    ]);
    let impls = rows(&repo, "RPC_IMPL");
    let source_of = |rpc: &str| -> String {
        impls
            .iter()
            .find(|(_, _, d)| d["rpc"] == rpc)
            .unwrap_or_else(|| panic!("{rpc}: {impls:?}"))
            .0
            .clone()
    };
    assert!(source_of("getTables").ends_with("get.getTables"));
    assert!(source_of("listTables").ends_with("list.listTables"));
    assert!(source_of("deleteTable").ends_with("grpc.grpcPlugin"));
}

#[test]
fn rpc_named_call_is_a_real_rpc_but_bind_on_a_method_is_not() {
    let proto = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc Call (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";
    let client = r#"
import { DataCatalogServiceClient } from "../gen/datacatalog_grpc_pb";
const client = new DataCatalogServiceClient("h");
export function go(req) {
  client.call(req);
  client.call.bind(client)(req);
}
"#;
    let repo = repo_with(&[("protos/c.proto", proto), ("src/c.ts", client)]);
    let calls = rows(&repo, "RPC_CALL");
    assert!(!calls.is_empty());
    assert!(
        calls.iter().all(|(_, _, d)| d["rpc"] == "call"),
        "{calls:?}"
    );
}
