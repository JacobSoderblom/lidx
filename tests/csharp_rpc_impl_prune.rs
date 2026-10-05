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

/// Incremental sync of `edits` over `base` must equal a fresh index of the
/// resulting tree.
fn assert_incremental_matches_fresh(base: &[(&str, &str)], edits: &[(&str, Option<&str>)]) {
    let (_t, root, mut indexer) = index(base);
    let mut tree: Vec<(&str, &str)> = base.to_vec();
    let mut changed = Vec::new();
    for (path, content) in edits {
        tree.retain(|(p, _)| p != path);
        match content {
            Some(c) => {
                common::write_files(&root, &[(path, c)]);
                tree.push((path, c));
            }
            None => std::fs::remove_file(root.join(path)).unwrap(),
        }
        changed.push(path.to_string());
    }
    indexer.sync_rel_paths(&changed).unwrap();
    let (_t2, _r2, fresh) = index(&tree);
    assert_eq!(impl_targets(&indexer), impl_targets(&fresh));
}

const IMPL_TWO: &str = "using Grpc.Core;\nusing Sync.V1;\nusing Other.V1;\n\
namespace Dpb.DataMgr.DataProduct.Grpc;\n\
internal sealed class SyncServiceImpl : SyncService.SyncServiceBase {\n\
  public override Task<SyncResponse> Sync(SyncRequest request, ServerCallContext context) => null;\n}\n";

fn proto_pkg(pkg: &str) -> String {
    PROTO.replace("package sync.v1", &format!("package {pkg}"))
}

#[test]
fn proto_added_then_removed_matches_fresh() {
    assert_incremental_matches_fresh(
        &[("SyncServiceImpl.cs", IMPL)],
        &[("sync.proto", Some(PROTO))],
    );
    assert_incremental_matches_fresh(
        &[("sync.proto", PROTO), ("SyncServiceImpl.cs", IMPL)],
        &[("sync.proto", None)],
    );
}

#[test]
fn proto_package_switching_to_another_candidate_matches_fresh() {
    let other = proto_pkg("other.v1");
    assert_incremental_matches_fresh(
        &[("sync.proto", PROTO), ("SyncServiceImpl.cs", IMPL_TWO)],
        &[("sync.proto", Some(&other))],
    );
}

#[test]
fn second_proto_backing_a_second_candidate_matches_fresh() {
    let other = proto_pkg("other.v1");
    assert_incremental_matches_fresh(
        &[("sync.proto", PROTO), ("SyncServiceImpl.cs", IMPL_TWO)],
        &[("other.proto", Some(&other))],
    );
}
