use lidx::search;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-search-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn search_text_fuzzy_matches_compound_tokens() {
    let repo_root = temp_repo_dir("fuzzy");
    let file_path = repo_root.join("ds_rendering.py");
    let content = "\
# scaffolding data_source rendering
def render_ds():
    pass
";
    std::fs::write(&file_path, content).unwrap();

    let options = search::SearchOptions::new(None);
    let results = search::search_text(&repo_root, "scaffold datasource", 10, options).unwrap();
    assert!(results.iter().any(|hit| hit.path == "ds_rendering.py"));

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn search_text_ranks_code_ahead_of_docs() {
    let repo_root = temp_repo_dir("rank");
    let docs_path = repo_root.join("docs").join("guide.md");
    let src_path = repo_root.join("src").join("ds_rendering.py");
    std::fs::create_dir_all(docs_path.parent().unwrap()).unwrap();
    std::fs::create_dir_all(src_path.parent().unwrap()).unwrap();
    std::fs::write(&docs_path, "scaffold datasource\n").unwrap();
    std::fs::write(&src_path, "# scaffold datasource\n").unwrap();

    let options = search::SearchOptions::new(None);
    let results = search::search_text(&repo_root, "scaffold datasource", 1, options).unwrap();
    assert_eq!(results[0].path, "src/ds_rendering.py");

    let mut docs_only = search::SearchOptions::new(None);
    docs_only.scope = Some(search::SearchScope::Docs);
    let docs_results =
        search::search_text(&repo_root, "scaffold datasource", 10, docs_only).unwrap();
    assert!(docs_results.iter().all(|hit| hit.path.starts_with("docs/")));

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn search_text_respects_gitignore_by_default() {
    let repo_root = temp_repo_dir("ignore");
    std::fs::write(repo_root.join(".gitignore"), "ignored.py\n").unwrap();
    std::fs::write(repo_root.join("ignored.py"), "needle\n").unwrap();

    let options = search::SearchOptions::new(None);
    let results = search::search_text(&repo_root, "needle", 10, options).unwrap();
    assert!(results.iter().all(|hit| hit.path != "ignored.py"));

    let mut include_ignored = search::SearchOptions::new(None);
    include_ignored.no_ignore = true;
    let results = search::search_text(&repo_root, "needle", 10, include_ignored).unwrap();
    assert!(results.iter().any(|hit| hit.path == "ignored.py"));

    let _ = std::fs::remove_dir_all(&repo_root);
}
