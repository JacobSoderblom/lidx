//! Issue #327: a C# `*ServiceBase` impl fans out one RPC_IMPL candidate per
//! bare `using`; once an indexed `.proto` route backs one of them the rest
//! are dropped, and they come back if the route goes away.

mod common;

use lidx::indexer::Indexer;

const PROTO: &str = "syntax = \"proto3\";\npackage sync.v1;\n\
option csharp_namespace = \"Sync.V1\";\n\
service SyncService { rpc Sync(SyncRequest) returns (SyncResponse); }\n\
message SyncRequest {} message SyncResponse {}\n";

const IMPL: &str = "using Grpc.Core;\nusing Microsoft.Extensions.Options;\n\
using Dpb.DataMgr.Options;\nusing Sync.V1;\n\
namespace Dpb.DataMgr.DataProduct.Grpc;\n\
internal sealed class SyncServiceImpl : SyncService.SyncServiceBase {\n\
  public override Task<SyncResponse> Sync(SyncRequest request, ServerCallContext context) => null;\n}\n";

fn impl_targets(indexer: &Indexer) -> Vec<String> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT target_qualname FROM edges WHERE kind = 'RPC_IMPL' ORDER BY target_qualname",
        )
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db) = common::index_repo("lidx-cs-rpc-prune-", files);
    let indexer = Indexer::new(root.clone(), db).unwrap();
    (tmp, root, indexer)
}

#[test]
fn proto_route_prunes_non_matching_candidates() {
    let (_t, _r, indexer) = index(&[("sync.proto", PROTO), ("SyncServiceImpl.cs", IMPL)]);
    assert_eq!(impl_targets(&indexer), vec!["/sync.v1.syncservice/sync"]);
}

#[test]
fn candidates_kept_without_proto() {
    let (_t, _r, indexer) = index(&[("SyncServiceImpl.cs", IMPL)]);
    let targets = impl_targets(&indexer);
    assert!(targets.len() > 1, "{targets:?}");
    assert!(targets.contains(&"/sync.v1.syncservice/sync".to_string()));
}

#[test]
fn incremental_sync_matches_fresh_index_when_proto_added_or_removed() {
    let (_t, root, mut indexer) = index(&[("SyncServiceImpl.cs", IMPL)]);
    let before = impl_targets(&indexer);
    common::write_files(&root, &[("sync.proto", PROTO)]);
    indexer.sync_rel_paths(&["sync.proto".to_string()]).unwrap();
    assert_eq!(impl_targets(&indexer), vec!["/sync.v1.syncservice/sync"]);
    std::fs::remove_file(root.join("sync.proto")).unwrap();
    indexer.sync_rel_paths(&["sync.proto".to_string()]).unwrap();
    assert_eq!(impl_targets(&indexer), before);
}
