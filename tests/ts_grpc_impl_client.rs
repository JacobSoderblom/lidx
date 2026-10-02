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

/// RPC_CALL targets as the query layer surfaces them.
fn surfaced_rpc_calls(repo: &Repo) -> Vec<String> {
    let db = repo.indexer.db();
    let gv = db.current_graph_version().unwrap();
    let kinds = vec!["RPC_CALL".to_string()];
    db.list_edges(
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
    .collect()
}

#[test]
fn indirection_named_rpc_surfaces_only_with_a_matching_proto_rpc() {
    let proto = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc Call (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";
    let other = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc GetTables (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";
    let bind_src = r#"
import { DataCatalogServiceClient } from "../gen/datacatalog_grpc_pb";
const client = new DataCatalogServiceClient("h");
export function go(req) { client.bind(req); }
"#;
    let call_src = bind_src.replace("client.bind(req)", "client.call(req)");

    let no_rpc = repo_with(&[("protos/c.proto", other), ("src/c.ts", bind_src)]);
    assert!(surfaced_rpc_calls(&no_rpc).is_empty());
    let with_rpc = repo_with(&[("protos/c.proto", proto), ("src/c.ts", &call_src)]);
    assert_eq!(surfaced_rpc_calls(&with_rpc).len(), 1);

    // Incremental equals fresh when the proto gains / loses `rpc Call`.
    let mut inc = repo_with(&[("protos/c.proto", other), ("src/c.ts", &call_src)]);
    assert!(surfaced_rpc_calls(&inc).is_empty());
    common::write_files(&inc.root, &[("protos/c.proto", proto)]);
    inc.indexer
        .sync_rel_paths(&["protos/c.proto".to_string()])
        .unwrap();
    assert_eq!(surfaced_rpc_calls(&inc), surfaced_rpc_calls(&with_rpc));
    common::write_files(&inc.root, &[("protos/c.proto", other)]);
    inc.indexer
        .sync_rel_paths(&["protos/c.proto".to_string()])
        .unwrap();
    assert!(surfaced_rpc_calls(&inc).is_empty());
}

const REAL_CLIENT: &str = include_str!("fixtures/ts_grpc/datacatalog-service-client.ts");

#[test]
fn real_ts_proto_client_file_yields_rpc_calls_and_proto_callers() {
    let proto = "syntax = \"proto3\";\npackage datacatalog.v1;\n\
service DataCatalogService {\n  rpc List (Req) returns (Res);\n  rpc Get (Req) returns (Res);\n\
  rpc GetTables (Req) returns (Res);\n  rpc GetDependencyGraph (Req) returns (Res);\n}\n\
message Req {}\nmessage Res {}\n";
    let repo = repo_with(&[
        ("protos/datacatalog.proto", proto),
        (
            "node/datacatalog-ui/lib/datacatalog-service-client.ts",
            REAL_CLIENT,
        ),
    ]);
    let calls = rows(&repo, "RPC_CALL");
    let mut rpcs: Vec<String> = calls
        .iter()
        .map(|(_, _, d)| d["rpc"].as_str().unwrap().to_string())
        .collect();
    rpcs.sort();
    assert_eq!(rpcs, ["get", "getDependencyGraph", "getTables", "list"]);
    assert!(
        calls
            .iter()
            .all(|(_, _, d)| d["package"] == "datacatalog.v1")
    );
    let explain = call(
        &repo,
        "explain_symbol",
        serde_json::json!({"query": "datacatalog.v1.DataCatalogService.GetTables"}),
    );
    assert!(explain["callers_total"].as_u64().unwrap() >= 1, "{explain}");
}

const HANDLER_Q: &str = "src/features/datacatalog/tables/get.getTables";
const CLIENT_Q: &str = "src/client/tables.loadTables";

fn trace(repo: &Repo, start: &str, direction: &str) -> String {
    call(
        repo,
        "trace_flow",
        serde_json::json!({"start_qualname": start, "direction": direction, "max_hops": 4,
            "kinds": ["RPC_CALL", "RPC_IMPL", "RPC_ROUTE", "CALLS"]}),
    )
    .to_string()
}

fn impact(repo: &Repo, start: &str, direction: &str) -> String {
    call(
        repo,
        "analyze_impact",
        serde_json::json!({"qualname": start, "direction": direction, "max_depth": 4}),
    )["affected"]
        .to_string()
}

#[test]
fn rpc_route_in_the_middle_connects_client_proto_and_handler_both_ways() {
    let repo = repo();
    // Proto rpc: callers upstream, implementer downstream.
    assert!(trace(&repo, RPC_SYMBOL, "upstream").contains("loadTables"));
    assert!(impact(&repo, RPC_SYMBOL, "upstream").contains("loadTables"));
    assert!(impact(&repo, RPC_SYMBOL, "downstream").contains("getTables"));
    // Handler upstream: the proto rpc and the client (guessed-path caller).
    let up = trace(&repo, HANDLER_Q, "upstream");
    assert!(
        up.contains("GetTables") && up.contains("loadTables"),
        "{up}"
    );
    let up = impact(&repo, HANDLER_Q, "upstream");
    assert!(up.contains("loadTables"), "{up}");
    // Client downstream: the handler (and its proto route).
    let down = trace(&repo, CLIENT_Q, "downstream");
    assert!(down.contains("getTables"), "{down}");
    assert!(impact(&repo, CLIENT_Q, "downstream").contains("getTables"));
}
