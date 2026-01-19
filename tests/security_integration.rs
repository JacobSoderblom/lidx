// Security Integration Tests
// Tests for path traversal, symlink escapes, and other security validations

use anyhow::Result;
use lidx::indexer::Indexer;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

// Helper to create a test repository with indexer
fn setup_test_repo() -> Result<(TempDir, Indexer)> {
    let temp_dir = TempDir::new()?;
    let repo_root = temp_dir.path().to_path_buf();

    // Create a simple test file
    fs::write(repo_root.join("test.py"), "def hello(): pass")?;

    // Create indexer with temp database
    let db_path = temp_dir.path().join(".lidx.db");
    let mut indexer = Indexer::new(repo_root.clone(), db_path)?;
    indexer.reindex()?;

    Ok((temp_dir, indexer))
}

// Helper to create a test SARIF file
fn create_test_sarif(repo_root: &PathBuf, name: &str) -> Result<PathBuf> {
    let sarif_content = r#"{
  "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
  "version": "2.1.0",
  "runs": []
}"#;
    let sarif_path = repo_root.join(name);
    fs::write(&sarif_path, sarif_content)?;
    Ok(sarif_path)
}

#[test]
fn test_diagnostics_import_path_traversal_blocked() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");

    // Attempt path traversal with relative path
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": "../../../etc/passwd"
        }),
    );

    // Should fail with path error (either "not found" or "escapes repo root")
    assert!(result.is_err(), "Path traversal should be blocked");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not found") || err_msg.contains("escapes repo root"),
        "Error should mention path issue, got: {}",
        err_msg
    );
}

#[test]
fn test_diagnostics_import_absolute_path_blocked() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");

    // Attempt to read file outside repo with absolute path
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": "/etc/passwd"
        }),
    );

    // Should fail with "escapes repo root"
    assert!(
        result.is_err(),
        "Absolute path outside repo should be blocked"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("escapes repo root"),
        "Error should mention repo escape, got: {}",
        err_msg
    );
}

#[test]
fn test_diagnostics_import_symlink_escape_blocked() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");
    let repo_root = indexer.repo_root();

    // Create a symlink pointing outside the repo
    // Note: This test may fail on systems without symlink support
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let link_path = repo_root.join("bad_link");

        // Try to create symlink to /etc (should work)
        if symlink("/etc", &link_path).is_ok() {
            // Attempt to read through the symlink
            let result = lidx::rpc::handle_method(
                &mut indexer,
                "diagnostics_import",
                serde_json::json!({
                    "path": "bad_link/passwd"
                }),
            );

            // Should fail - canonicalize resolves symlinks and checks containment
            assert!(result.is_err(), "Symlink escape should be blocked");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("escapes repo root") || err_msg.contains("not found"),
                "Error should indicate path issue, got: {}",
                err_msg
            );
        }
    }
}

#[test]
fn test_diagnostics_import_valid_path_works() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");
    let repo_root = indexer.repo_root();

    // Create a valid SARIF file in the repo
    create_test_sarif(repo_root, "valid.sarif").expect("failed to create SARIF");

    // Import should succeed
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": "valid.sarif"
        }),
    );

    assert!(result.is_ok(), "Valid path should work: {:?}", result);

    // Check result indicates success
    let value = result.unwrap();
    assert!(
        value.get("imported").is_some(),
        "Result should contain 'imported' field"
    );
}

#[test]
fn test_diagnostics_import_valid_absolute_path_in_repo() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");
    let repo_root = indexer.repo_root();

    // Create a valid SARIF file
    let sarif_path =
        create_test_sarif(repo_root, "absolute.sarif").expect("failed to create SARIF");

    // Use absolute path within repo
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": sarif_path.to_string_lossy()
        }),
    );

    assert!(
        result.is_ok(),
        "Absolute path within repo should work: {:?}",
        result
    );
}

#[test]
fn test_diagnostics_import_subdirectory_path() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");
    let repo_root = indexer.repo_root();

    // Create subdirectory with SARIF file
    let subdir = repo_root.join("diagnostics");
    fs::create_dir(&subdir).expect("failed to create subdir");
    create_test_sarif(&subdir, "report.sarif").expect("failed to create SARIF");

    // Should work with relative path including subdirectory
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": "diagnostics/report.sarif"
        }),
    );

    assert!(
        result.is_ok(),
        "Subdirectory path should work: {:?}",
        result
    );
}

#[test]
fn test_diagnostics_import_parent_then_back() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");
    let repo_root = indexer.repo_root();

    // Create a file we can reference
    create_test_sarif(repo_root, "target.sarif").expect("failed to create SARIF");

    // Try path like "subdir/../target.sarif" which stays in repo but uses ..
    let subdir = repo_root.join("subdir");
    fs::create_dir(&subdir).expect("failed to create subdir");

    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": "subdir/../target.sarif"
        }),
    );

    // This should work - canonicalize will resolve it to target.sarif in repo
    assert!(
        result.is_ok(),
        "Path with .. that stays in repo should work: {:?}",
        result
    );
}

// Test that open_file still has path validation (verify existing security)
#[test]
fn test_open_file_path_traversal_blocked() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");

    // Attempt path traversal via open_file
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "open_file",
        serde_json::json!({
            "path": "../../../etc/passwd"
        }),
    );

    // Should fail
    assert!(
        result.is_err(),
        "open_file path traversal should be blocked"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not found") || err_msg.contains("escapes repo root"),
        "Error should indicate path issue, got: {}",
        err_msg
    );
}

#[test]
fn test_open_file_absolute_path_blocked() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");

    // Attempt absolute path outside repo via open_file
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "open_file",
        serde_json::json!({
            "path": "/etc/passwd"
        }),
    );

    // Should fail with "escapes repo root"
    assert!(result.is_err(), "open_file absolute path should be blocked");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("escapes repo root"),
        "Error should mention repo escape, got: {}",
        err_msg
    );
}

#[test]
fn test_open_file_valid_path_works() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");

    // Open valid file
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "open_file",
        serde_json::json!({
            "path": "test.py"
        }),
    );

    assert!(result.is_ok(), "Valid path should work: {:?}", result);

    // Check result contains file content
    let value = result.unwrap();
    assert!(
        value.get("text").is_some(),
        "Result should contain 'text' field"
    );
    let text = value.get("text").and_then(|v| v.as_str()).unwrap_or("");
    assert!(text.contains("def hello"), "Should return file content");
}

#[test]
fn test_empty_path_rejected() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");

    // Test empty path
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": ""
        }),
    );

    assert!(result.is_err(), "Empty path should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("requires path") || err_msg.contains("empty"),
        "Error should indicate path is required/empty, got: {}",
        err_msg
    );
}

#[test]
fn test_whitespace_only_path_rejected() {
    let (_temp, mut indexer) = setup_test_repo().expect("setup failed");

    // Test whitespace-only path
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "diagnostics_import",
        serde_json::json!({
            "path": "   "
        }),
    );

    assert!(result.is_err(), "Whitespace-only path should be rejected");
}
