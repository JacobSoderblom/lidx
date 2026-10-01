//! Issue #228: Bicep `module` / `using` references resolve to the module
//! symbol of the referenced file. The target is normalised exactly like the
//! module qualname (`..` collapsed, `.bicep`/`.bicepparam` stripped).

use lidx::indexer::Indexer;
use lidx::indexer::bicep::{module_name_from_rel_path, module_ref_target};
use rusqlite::params;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    dir: PathBuf,
    indexer: Indexer,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn index(files: &[(&str, &str)]) -> Fixture {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!(
        "lidx-bicep-refs-{nanos}-{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    for (name, src) in files {
        let path = dir.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, src).unwrap();
    }
    let mut indexer = Indexer::new(dir.clone(), dir.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    Fixture { dir, indexer }
}

impl Fixture {
    /// Resolved IMPORTS_FILE edges as (source qualname, target qualname).
    fn resolved(&self) -> Vec<(String, String)> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT s.qualname, t.qualname FROM edges e
                 JOIN symbols s ON s.id = e.source_symbol_id
                 JOIN symbols t ON t.id = e.target_symbol_id
                 WHERE e.kind = 'IMPORTS_FILE'",
            )
            .unwrap();
        stmt.query_map(params![], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// Unresolved IMPORTS_FILE references as (reference_name, reason).
    fn unresolved(&self) -> Vec<(String, String)> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT reference_name, reason FROM unresolved_references
                 WHERE edge_kind = 'IMPORTS_FILE'",
            )
            .unwrap();
        stmt.query_map(params![], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }
}

const MODULE: &str = "param location string\n";

#[test]
fn issue_fixture_resolves_module_and_using() {
    let fx = index(&[
        (
            "infra/bicep/modules/keyVault.bicep",
            "module privateEndpoint 'privateEndpoint.bicep' = {\n  name: 'pe'\n}\n",
        ),
        ("infra/bicep/modules/privateEndpoint.bicep", MODULE),
        ("infra/bicep/workload.bicep", MODULE),
        (
            "infra/bicep/params/acc.workload.bicepparam",
            "using '../workload.bicep'\n\nparam location = 'x'\n",
        ),
    ]);
    let resolved = fx.resolved();
    assert!(
        resolved
            .iter()
            .any(|(_, t)| t == "infra/bicep/modules/privateEndpoint"),
        "{resolved:?}"
    );
    assert!(
        resolved.iter().any(|(_, t)| t == "infra/bicep/workload"),
        "{resolved:?}"
    );
    assert_eq!(resolved.len(), 2, "{resolved:?}");
    assert!(fx.unresolved().is_empty(), "{:?}", fx.unresolved());
}

#[test]
fn multi_level_parent_resolves() {
    let fx = index(&[
        (
            "infra/app/web/main.bicep",
            "module net '../../shared/net.bicep' = {\n  name: 'n'\n}\n",
        ),
        ("infra/shared/net.bicep", MODULE),
    ]);
    let resolved = fx.resolved();
    assert_eq!(resolved.len(), 1, "{resolved:?}");
    assert_eq!(resolved[0].1, "infra/shared/net");
}

#[test]
fn missing_target_is_unresolved_and_names_path() {
    let fx = index(&[(
        "infra/main.bicep",
        "module ghost 'modules/ghost.bicep' = {\n  name: 'g'\n}\n",
    )]);
    assert!(fx.resolved().is_empty());
    let unresolved = fx.unresolved();
    assert_eq!(unresolved.len(), 1, "{unresolved:?}");
    assert!(
        unresolved[0].0.contains("infra/modules/ghost"),
        "{unresolved:?}"
    );
    assert_eq!(unresolved[0].1, "no_candidates");
}

#[test]
fn reference_escaping_repo_root_is_unresolved() {
    // `outside/x` exists one level up from the repo root by name only: an
    // escaping chain must never collapse onto a real in-repo symbol.
    let fx = index(&[
        (
            "main.bicep",
            "module esc '../x.bicep' = {\n  name: 'e'\n}\n",
        ),
        ("x.bicep", MODULE),
    ]);
    assert!(fx.resolved().is_empty(), "{:?}", fx.resolved());
    let unresolved = fx.unresolved();
    assert_eq!(unresolved.len(), 1, "{unresolved:?}");
    assert!(unresolved[0].0.contains("../x"), "{unresolved:?}");
    assert_eq!(unresolved[0].1, "no_candidates");
}

#[test]
fn target_and_module_qualname_share_one_normalisation() {
    let cases = [
        (
            "infra/bicep/modules/keyVault.bicep",
            "privateEndpoint.bicep",
        ),
        ("infra/bicep/params/acc.bicepparam", "../workload.bicep"),
        ("infra/app/web/main.bicep", "../../shared/net.bicep"),
        ("infra/main.bicep", "./modules/./a.bicep"),
    ];
    for (file, raw) in cases {
        let target = module_ref_target(file, raw);
        let joined = Path::new(file).parent().unwrap().join(raw);
        let collapsed = lidx::util::normalize_path(&joined);
        assert_eq!(
            target,
            module_name_from_rel_path(&collapsed),
            "{file} {raw}"
        );
    }
    // An escaping reference keeps its leading `..`, so it names the path and
    // cannot equal any in-repo module qualname.
    assert_eq!(module_ref_target("main.bicep", "../x.bicep"), "../x");
    // `a/..` and absolute references keep the raw text and match nothing.
    assert_eq!(module_ref_target("main.bicep", "a/.."), "a/..");
    assert_eq!(
        module_ref_target("a/main.bicep", "/infra/y.bicep"),
        "/infra/y.bicep"
    );
}

#[test]
fn absolute_reference_is_unresolved() {
    let fx = index(&[
        (
            "main.bicep",
            "module abs '/infra/y.bicep' = {\n  name: 'a'\n}\n",
        ),
        ("infra/y.bicep", MODULE),
    ]);
    assert!(fx.resolved().is_empty(), "{:?}", fx.resolved());
    let unresolved = fx.unresolved();
    assert_eq!(unresolved.len(), 1, "{unresolved:?}");
    assert!(unresolved[0].0.contains("/infra/y"), "{unresolved:?}");
    assert_eq!(unresolved[0].1, "no_candidates");
}

#[test]
fn registry_reference_and_using_none_make_no_file_edge() {
    let fx = index(&[
        (
            "main.bicep",
            "module reg 'br:example.azurecr.io/bicep/mod:v1' = {\n  name: 'r'\n}\n",
        ),
        ("p.bicepparam", "using none\n\nparam a = 'x'\n"),
    ]);
    assert!(fx.resolved().is_empty(), "{:?}", fx.resolved());
    assert!(
        fx.unresolved().iter().all(|(n, _)| !n.ends_with(".bicep")),
        "{:?}",
        fx.unresolved()
    );
}
