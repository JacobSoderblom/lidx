pub mod golden;

use std::path::{Path, PathBuf};

/// Recursively copy `src`'s contents into `dst` (which is created if it
/// doesn't exist). Used only to materialize a fixture into a fresh temp
/// dir -- other integration tests keep their own, separate copies of this
/// (not refactored here; see `setup_repo`'s doc).
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

/// Copy `fixture` (a path under `tests/fixtures/`, may contain `/`) into a
/// fresh `tempfile` temp dir and return `(guard, repo_root, db_path)`.
///
/// Keep `guard` bound (e.g. `let (_tmp, repo_root, db_path) = ...`) for as
/// long as the test needs the directory -- its `Drop` removes the temp dir
/// automatically, so callers don't need a manual `remove_dir_all` at the
/// end of the test.
///
/// This is the golden-harness-only home for what used to be
/// `tests/common/golden.rs`'s private `setup_repo`/`copy_dir`/
/// `temp_repo_dir` helpers (plus a `TEMP_COUNTER`, now unnecessary --
/// `tempfile` already guarantees a unique directory per call). About 16
/// other integration test files under `tests/` still carry their own,
/// separate copies of a similar handmade helper; those are intentionally
/// left alone here rather than folded into a repo-wide refactor.
pub fn setup_repo(fixture: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let src = golden::fixture_path(fixture);
    let prefix = format!("lidx-{}-", fixture.replace('/', "-"));
    let tmp = tempfile::Builder::new().prefix(&prefix).tempdir().unwrap();
    copy_dir(&src, tmp.path());
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (tmp, repo_root, db_path)
}
