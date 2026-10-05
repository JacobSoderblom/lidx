//! Issue #326: Bicep topic resources and code literals share one channel key.

mod common;

use lidx::indexer::Indexer;
use lidx::traversal::{TraceConfig, TraceDirection, trace_flow};

const BICEP: &str = "resource topicDataProxyCommands 'Microsoft.ServiceBus/namespaces/topics@2025-05-01-preview' = {\n  name: 'sbt-dataproxy-commands'\n  parent: serviceBus\n  properties: {}\n}\n";
const BICEP_OTHER: &str = "resource topicOther 'Microsoft.ServiceBus/namespaces/topics@2025-05-01-preview' = {\n  name: 'sbt-other-topic'\n  parent: serviceBus\n  properties: {}\n}\n";
const LIT: &str = "public class Lit { private readonly IMessageBus _bus;\n  public async Task Go() { await _bus.PublishAsync(\"sbt-dataproxy-commands\", msg); } }\n";

fn channel_edges(indexer: &Indexer) -> Vec<(String, String, String)> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.kind, COALESCE(s.qualname, ''), COALESCE(e.target_qualname, '')
             FROM edges e LEFT JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.kind LIKE 'CHANNEL%' ORDER BY 1, 2, 3",
        )
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn bicep_and_code_literal_share_key_and_trace_bridges() {
    let (_tmp, root, db_path) =
        common::index_repo("lidx-chan-key-", &[("sb.bicep", BICEP), ("Lit.cs", LIT)]);
    let indexer = Indexer::new(root, db_path).unwrap();
    let edges = channel_edges(&indexer);
    let targets: std::collections::BTreeSet<_> = edges.iter().map(|e| e.2.as_str()).collect();
    assert_eq!(
        targets,
        ["channel://dataproxycommands"].into_iter().collect(),
        "{edges:?}"
    );

    let gv = indexer.db().current_graph_version().unwrap();
    let go = indexer
        .db()
        .get_symbol_by_qualname("Lit.Lit.Go", gv)
        .unwrap()
        .expect("Lit.Lit.Go");
    let result = trace_flow(
        indexer.db(),
        vec![go.id],
        None,
        None,
        gv,
        &TraceConfig {
            direction: TraceDirection::Downstream,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        result
            .hops
            .iter()
            .any(|h| h.symbol.qualname.contains("topicDataProxyCommands")),
        "trace_flow from Lit.Go must reach the Bicep topic symbol: {:?}",
        result
            .hops
            .iter()
            .map(|h| &h.symbol.qualname)
            .collect::<Vec<_>>()
    );
}

#[test]
fn incremental_edit_matches_fresh_index() {
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-chan-key-inc-",
        &[("sb.bicep", BICEP_OTHER), ("Lit.cs", LIT)],
    );
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    common::write_files(&root, &[("sb.bicep", BICEP)]);
    indexer.sync_rel_paths(&["sb.bicep".to_string()]).unwrap();
    let incremental = channel_edges(&indexer);

    let (_tmp2, root2, db2) = common::index_repo(
        "lidx-chan-key-fresh-",
        &[("sb.bicep", BICEP), ("Lit.cs", LIT)],
    );
    let fresh = channel_edges(&Indexer::new(root2, db2).unwrap());
    assert_eq!(incremental, fresh);
    let targets: std::collections::BTreeSet<_> = incremental.iter().map(|e| e.2.as_str()).collect();
    assert_eq!(
        targets,
        ["channel://dataproxycommands"].into_iter().collect(),
        "{incremental:?}"
    );
    assert!(!fresh.is_empty());
}
