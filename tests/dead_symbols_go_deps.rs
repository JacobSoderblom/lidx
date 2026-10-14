//! Issue #495: a method that, with its type's other exported methods,
//! satisfies an interface a Go dependency declares is called by the
//! dependency through it. The dependency's interfaces are read from the
//! module cache or `vendor/` at index time (never run, never downloaded);
//! without those sources nothing changes.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const DEP_SOURCE: &str = "package dep\n\n\
    type Options struct{}\n\
    type Cmd func()\n\
    type Msg interface{}\n\n\
    // Runner is started and stopped by Run.\n\
    type Runner interface {\n\
    \tStart(opts Options) error\n\
    \tStop()\n\
    }\n\n\
    type Namer interface {\n\
    \tName() string\n\
    }\n\n\
    type Model interface {\n\
    \tInit() Cmd\n\
    \tUpdate(msg Msg) (Model, Cmd)\n\
    \tView() string\n\
    }\n\n\
    type Tracker interface {\n\
    \tTrack(opts Options) error\n\
    }\n\n\
    func Run(r Runner) { _ = r.Start(Options{}); r.Stop() }\n";

const GO_MOD: &str = "module example.com/app\n\ngo 1.22\n\nrequire example.com/dep v1.0.0\n";

const REPO: &[(&str, &str)] = &[
    ("go.mod", GO_MOD),
    (
        "main.go",
        "package main\n\nimport (\n\t\"example.com/app/internal/app\"\n\t\"example.com/dep\"\n)\n\n\
         func main() {\n\tdep.Run(app.NewService())\n\t_ = app.Partial{}\n\t_ = app.Multi{}\n\t_ = app.NewAdapter()\n}\n",
    ),
    (
        "internal/app/app.go",
        "package app\n\nimport \"example.com/dep\"\n\n\
         type Service struct{}\n\n\
         func NewService() *Service { return &Service{} }\n\n\
         func (s *Service) Start(opts dep.Options) error { return nil }\n\
         func (s *Service) Stop() {}\n\
         func (s *Service) Name() string { return \"svc\" }\n\
         func (s *Service) Helper() int { return 1 }\n\n\
         type Partial struct{}\n\n\
         func (p Partial) Start(opts dep.Options) error { return nil }\n\n\
         type Multi struct{}\n\n\
         func (m Multi) Init() dep.Cmd { return nil }\n\
         func (m Multi) Update(msg dep.Msg) (dep.Model, dep.Cmd) { return m, nil }\n\
         func (m Multi) View() string { return \"\" }\n\n\
         type Unused struct{}\n\n\
         func (u *Unused) Start(opts dep.Options) error { return nil }\n\
         func (u *Unused) Stop() {}\n\n\
         type Adapter struct{}\n\n\
         func NewAdapter() *Adapter { return &Adapter{} }\n\n\
         func (a *Adapter) Track(opts dep.Options) error { _ = &readCloser{}; return nil }\n\n\
         type readCloser struct{ n int }\n\n\
         func (c *readCloser) Close() error { return nil }\n",
    ),
];

/// A module cache holding `example.com/dep@v1.0.0`.
fn module_cache(root: &Path) -> PathBuf {
    let cache = root.join("modcache");
    common::write_files(&cache, &[("example.com/dep@v1.0.0/dep.go", DEP_SOURCE)]);
    cache
}

fn indexer_with_cache(root: &Path, cache: &Path) -> Indexer {
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.to_path_buf(), db_path)
        .unwrap()
        .with_go_module_cache(Some(cache.to_path_buf()));
    indexer.reindex().unwrap();
    indexer
}

fn call(indexer: &mut Indexer) -> Value {
    rpc::handle_method(
        indexer,
        "dead_symbols",
        json!({"include_unused_imports": false, "include_orphan_tests": false,
               "include_public_api": true, "include_external_interface": true,
               "limit": 1000}),
    )
    .unwrap()
}

fn group(result: &Value, name: &str) -> Vec<String> {
    result[name]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|s| s["qualname"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn claimed(result: &Value) -> Vec<String> {
    let mut all = group(result, "dead_symbols");
    all.extend(group(result, "public_api"));
    all
}

fn has(names: &[String], suffix: &str) -> bool {
    names.iter().any(|q| q.ends_with(suffix))
}

fn listed(result: &Value, suffix: &str) -> Value {
    result["implements_external_interface"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["qualname"].as_str().unwrap().ends_with(suffix))
        .unwrap_or_else(|| panic!("{suffix} not listed: {result}"))
        .clone()
}

fn dep_packages(indexer: &Indexer) -> BTreeSet<(String, String, String)> {
    indexer
        .db()
        .go_dep_packages()
        .unwrap()
        .into_iter()
        .collect()
}

fn dep_origin(indexer: &Indexer, pkg: &str) -> Option<String> {
    let conn = indexer.db().read_conn().unwrap();
    conn.query_row(
        "SELECT origin FROM go_dep_packages WHERE pkg_path = ?1",
        [pkg],
        |r| r.get(0),
    )
    .ok()
}

#[test]
fn methods_satisfying_a_dependency_interface_are_live_with_their_type() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-go-deps-")
        .tempdir()
        .unwrap();
    let root = tmp.path().join("repo");
    common::write_files(&root, REPO);
    let cache = module_cache(tmp.path());
    let mut ix = indexer_with_cache(&root, &cache);
    assert_eq!(
        dep_packages(&ix),
        [(
            "example.com/dep".to_string(),
            "v1.0.0".to_string(),
            "example.com/dep".to_string()
        )]
        .into_iter()
        .collect(),
    );
    assert_eq!(
        dep_origin(&ix, "example.com/dep").as_deref(),
        Some("modcache")
    );
    let r = call(&mut ix);
    let dead = claimed(&r);
    for live in [
        "Service.Start",
        "Service.Stop",
        "Service.Name",
        "Multi.Init",
        "Multi.Update",
        "Multi.View",
    ] {
        assert!(!has(&dead, live), "{live} claimed: {dead:?}");
    }
    let row = listed(&r, "Service.Start");
    assert_eq!(row["confidence"], "medium", "{row}");
    assert_eq!(
        row["reason"].as_str().unwrap(),
        "implements_external_interface: implements example.com/dep.Runner (dependency), on a live type",
        "{row}"
    );
    let row = listed(&r, "Multi.Update");
    assert!(
        row["reason"]
            .as_str()
            .unwrap()
            .contains("example.com/dep.Model (dependency)"),
        "{row}"
    );
    // not part of any interface
    assert!(has(&dead, "Service.Helper"), "{dead:?}");
    // a partial match (Partial has Start but not Stop) proves nothing
    assert!(has(&dead, "Partial.Start"), "{dead:?}");
    // the same methods on a type nothing references stay dead
    assert!(has(&dead, "Unused.Start"), "{dead:?}");
    assert!(has(&dead, "Unused.Stop"), "{dead:?}");
}

#[test]
fn a_rescued_method_makes_the_type_it_builds_live_for_the_next_pass() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-go-deps-cascade-")
        .tempdir()
        .unwrap();
    let root = tmp.path().join("repo");
    common::write_files(&root, REPO);
    let cache = module_cache(tmp.path());
    let mut ix = indexer_with_cache(&root, &cache);
    let r = call(&mut ix);
    let dead = claimed(&r);
    // `Adapter.Track` is `dep.Tracker`'s; it is the only thing that builds a
    // `readCloser`, whose `Close` is `io.Closer`'s: live on the second pass
    assert!(!has(&dead, "Adapter.Track"), "{dead:?}");
    assert!(!has(&dead, "readCloser.Close"), "{dead:?}");
    assert!(
        listed(&r, "readCloser.Close")["reason"]
            .as_str()
            .unwrap()
            .contains("io.Closer"),
        "{r}"
    );
    assert!(has(&dead, "Unused.Stop"), "{dead:?}");
}

#[test]
fn without_dependency_sources_nothing_changes() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-go-deps-none-")
        .tempdir()
        .unwrap();
    let root = tmp.path().join("repo");
    common::write_files(&root, REPO);
    let empty = tmp.path().join("empty-cache");
    std::fs::create_dir_all(&empty).unwrap();
    let mut ix = indexer_with_cache(&root, &empty);
    assert!(dep_packages(&ix).is_empty());
    let r = call(&mut ix);
    let dead = claimed(&r);
    for method in [
        "Service.Start",
        "Service.Stop",
        "Service.Name",
        "Multi.Init",
        "Multi.Update",
        "Multi.View",
        "Service.Helper",
        "Partial.Start",
    ] {
        assert!(has(&dead, method), "{method} not claimed: {dead:?}");
    }
    assert!(
        r["implements_external_interface"]
            .as_array()
            .is_none_or(|rows| rows.is_empty()),
        "{r}"
    );
}

#[test]
fn a_vendored_dependency_is_read_from_vendor() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-go-deps-vendor-")
        .tempdir()
        .unwrap();
    let root = tmp.path().join("repo");
    common::write_files(&root, REPO);
    common::write_files(
        &root,
        &[
            (
                "vendor/modules.txt",
                "# example.com/dep v1.0.0\n## explicit; go 1.22\nexample.com/dep\n",
            ),
            ("vendor/example.com/dep/dep.go", DEP_SOURCE),
        ],
    );
    let empty = tmp.path().join("empty-cache");
    std::fs::create_dir_all(&empty).unwrap();
    let mut ix = indexer_with_cache(&root, &empty);
    assert_eq!(
        dep_origin(&ix, "example.com/dep").as_deref(),
        Some("vendor")
    );
    let r = call(&mut ix);
    let dead = claimed(&r);
    for live in [
        "Service.Start",
        "Service.Stop",
        "Service.Name",
        "Multi.View",
    ] {
        assert!(!has(&dead, live), "{live} claimed: {dead:?}");
    }
    assert!(has(&dead, "Partial.Start"), "{dead:?}");
    // `Run`'s body is indexed with the vendored copy, so `Start`/`Stop` are
    // reached by ordinary dispatch; `Name` is called by nothing in the
    // repository and is live only through the vendored `Namer` interface.
    assert!(
        listed(&r, "Service.Name")["reason"]
            .as_str()
            .unwrap()
            .contains("example.com/dep.Namer (dependency)")
    );
}

/// The stored dependency packages and the claims after a sync, to compare
/// with a fresh reindex of the same tree.
fn state(ix: &mut Indexer) -> (BTreeSet<(String, String, String)>, BTreeSet<String>) {
    let packages = dep_packages(ix);
    let claims = claimed(&call(ix)).into_iter().collect();
    (packages, claims)
}

#[test]
fn incremental_sync_after_a_go_mod_edit_matches_a_fresh_reindex() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-go-deps-sync-")
        .tempdir()
        .unwrap();
    let cache = module_cache(tmp.path());

    // 1. the requirement is dropped: the package is no longer wanted
    let root = tmp.path().join("drop");
    common::write_files(&root, REPO);
    let mut ix = indexer_with_cache(&root, &cache);
    assert_eq!(dep_packages(&ix).len(), 1);
    std::fs::write(root.join("go.mod"), "module example.com/app\n\ngo 1.22\n").unwrap();
    ix.sync_rel_paths(&["go.mod".to_string()]).unwrap();
    let synced = state(&mut ix);
    assert!(synced.0.is_empty(), "{synced:?}");
    let fresh_root = tmp.path().join("drop-fresh");
    common::write_files(&fresh_root, REPO);
    std::fs::write(
        fresh_root.join("go.mod"),
        "module example.com/app\n\ngo 1.22\n",
    )
    .unwrap();
    let mut fresh = indexer_with_cache(&fresh_root, &cache);
    assert_eq!(synced, state(&mut fresh));
    assert!(has(
        &synced.1.iter().cloned().collect::<Vec<_>>(),
        "Service.Start"
    ));

    // 2. the requirement is added: the package is read
    let root = tmp.path().join("add");
    common::write_files(&root, REPO);
    std::fs::write(root.join("go.mod"), "module example.com/app\n\ngo 1.22\n").unwrap();
    let mut ix = indexer_with_cache(&root, &cache);
    assert!(dep_packages(&ix).is_empty());
    std::fs::write(root.join("go.mod"), GO_MOD).unwrap();
    ix.sync_rel_paths(&["go.mod".to_string()]).unwrap();
    let synced = state(&mut ix);
    assert_eq!(synced.0.len(), 1, "{synced:?}");
    let fresh_root = tmp.path().join("add-fresh");
    common::write_files(&fresh_root, REPO);
    let mut fresh = indexer_with_cache(&fresh_root, &cache);
    assert_eq!(synced, state(&mut fresh));
    assert!(!has(
        &synced.1.iter().cloned().collect::<Vec<_>>(),
        "Service.Start"
    ));

    // 3. a version bump the cache does not hold: read again for the new
    //    version, nothing to find, so the old rows go
    std::fs::write(
        root.join("go.mod"),
        "module example.com/app\n\ngo 1.22\n\nrequire example.com/dep v1.1.0\n",
    )
    .unwrap();
    ix.sync_rel_paths(&["go.mod".to_string()]).unwrap();
    assert!(dep_packages(&ix).is_empty());
}
