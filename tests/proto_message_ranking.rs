/// Test that proto messages rank at priority 0 (high priority) in find_symbols results.
///
/// Issue: Proto messages were added in PR #139 but were not added to the priority-0
/// bucket in the symbol-kind ranking, causing them to fall to priority 2 and appear
/// below lower-priority-numbered symbols in ambiguous-name lookups.
///
/// This test verifies that a message symbol outranks an enum_value symbol when both
/// match the same query.
use lidx::indexer::Indexer;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-proto-ranking-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_file(repo: &Path, rel_path: &str, contents: &str) {
    let path = repo.join(rel_path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

#[test]
fn proto_message_ranks_higher_than_enum_value() {
    // Create a temp repo with a proto file
    let repo_root = temp_repo_dir("message-ranking");

    // A proto file with:
    // - A message named "Config" (priority 0)
    // - An enum "ConfigStatus" with value "Config" (enum_value is priority 2)
    // When searching for "Config", both match but the message should come first.
    let proto_content = r#"
syntax = "proto3";

package example;

message Config {
  string name = 1;
  ConfigStatus status = 2;
}

enum ConfigStatus {
  CONFIG_STATUS_UNSPECIFIED = 0;
  Config = 1;  // Note: this is an enum value named "Config"
}
"#;

    write_file(&repo_root, "example.proto", proto_content);

    // Index the repo
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // Search for "Config" - should match both the message and the enum value
    let results = db.find_symbols("Config", 10, None, gv).unwrap();

    // Should have at least the message and the enum value
    assert!(
        !results.is_empty(),
        "Expected at least 1 result for 'Config'"
    );

    // Find the message and enum_value in results
    let message = results
        .iter()
        .find(|s| s.kind == "message" && s.name == "Config");
    let enum_value = results
        .iter()
        .find(|s| s.kind == "enum_value" && s.name == "Config");

    // Message should exist
    assert!(
        message.is_some(),
        "Expected 'Config' message in results, got kinds: {:?}",
        results.iter().map(|r| r.kind.as_str()).collect::<Vec<_>>()
    );

    // If enum_value exists, message should come before it
    if let (Some(msg), Some(ev)) = (message, enum_value) {
        let msg_idx = results.iter().position(|s| s.id == msg.id).unwrap();
        let ev_idx = results.iter().position(|s| s.id == ev.id).unwrap();
        assert!(
            msg_idx < ev_idx,
            "Message should rank higher than enum_value, but enum_value at index {} comes before message at index {}",
            ev_idx,
            msg_idx
        );
    }
}
