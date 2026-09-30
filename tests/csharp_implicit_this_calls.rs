//! Issue #216: an unqualified call inside a C# type body has an implicit
//! receiver (`this`, or the type itself in a static context). It binds to
//! the enclosing type's own member, then its base chain, before any
//! same-named method of an imported namespace competes.
//!
//! Scan order confounds every C# resolution repro, so each fixture runs
//! with the declaring (base) file sorted both first and last.

use lidx::indexer::Indexer;
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    dir: PathBuf,
    indexer: Indexer,
    gv: i64,
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
        "lidx-cs-implicit-this-{nanos}-{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    for (name, src) in files {
        std::fs::write(dir.join(name), src).unwrap();
    }
    let mut indexer = Indexer::new(dir.clone(), dir.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    Fixture { dir, indexer, gv }
}

impl Fixture {
    fn targets(&self, caller: &str) -> Vec<String> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT t.qualname FROM edges e
                 JOIN symbols s ON s.id = e.source_symbol_id
                 JOIN symbols t ON t.id = e.target_symbol_id
                 WHERE e.kind = 'CALLS' AND s.qualname = ? AND e.graph_version = ?
                   AND t.kind != 'external'",
            )
            .unwrap();
        stmt.query_map(params![caller, self.gv], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn unresolved_reasons(&self, caller: &str) -> Vec<String> {
        let conn = self.indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT ur.reason FROM unresolved_references ur
                 JOIN symbols s ON s.id = ur.source_symbol_id
                 WHERE s.qualname = ? AND ur.edge_kind = 'CALLS'",
            )
            .unwrap();
        stmt.query_map(params![caller], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }
}

/// Runs `check` with the base-declaring file scanned first, then last.
fn both_orders(base: &str, others: &[(&str, &str)], check: impl Fn(&Fixture)) {
    let mut first = vec![("A_base.cs", base)];
    first.extend(others.iter().copied());
    check(&index(&first));
    let mut last: Vec<(&str, &str)> = others.to_vec();
    last.push(("Z_base.cs", base));
    check(&index(&last));
}

const MSSQL_BASE: &str = "namespace Dpb.DataMgr.Database {
    public abstract class MssqlRepositoryBase {
        protected int QueryAsync(string sql) { return 1; }
    }
}";
const COMMON_BASE: &str = "namespace Dpb.Common.Database {
    public abstract class RepositoryBase {
        protected int QueryAsync(string sql) { return 2; }
    }
}";
const TEAM_REPO: &str = "using Dpb.Common.Database;
namespace Dpb.DataMgr.Teams {
    public class MssqlTeamRepository : Dpb.DataMgr.Database.MssqlRepositoryBase {
        public int Get() { return QueryAsync(\"select\"); }
    }
}";

#[test]
fn inherited_bare_call_beats_same_named_imported_method() {
    both_orders(
        MSSQL_BASE,
        &[("B_common.cs", COMMON_BASE), ("C_team.cs", TEAM_REPO)],
        |f| {
            assert_eq!(
                f.targets("Dpb.DataMgr.Teams.MssqlTeamRepository.Get"),
                vec!["Dpb.DataMgr.Database.MssqlRepositoryBase.QueryAsync"]
            );
        },
    );
}

#[test]
fn own_method_beats_same_named_imported_method() {
    let own = "using Dpb.Common.Database;
namespace App { public class Repo {
    int QueryAsync(string s) { return 1; }
    public int Get() { return QueryAsync(\"x\"); } } }";
    both_orders(COMMON_BASE, &[("B_repo.cs", own)], |f| {
        assert_eq!(f.targets("App.Repo.Get"), vec!["App.Repo.QueryAsync"]);
    });
    // The same-named imported method exists twice: still the own member.
    let other =
        "namespace Other { public class X { public int QueryAsync(string s) { return 3; } } }";
    both_orders(
        COMMON_BASE,
        &[("B_repo.cs", own), ("C_other.cs", other)],
        |f| {
            assert_eq!(f.targets("App.Repo.Get"), vec!["App.Repo.QueryAsync"]);
        },
    );
}

#[test]
fn two_level_base_chain_resolves_to_grandparent() {
    let grand = "using Dpb.Common.Database;
namespace G { public class Grand { protected int QueryAsync(string s) { return 1; } } }";
    let parent = "namespace P { public class Parent : G.Grand { } }";
    let child = "using Dpb.Common.Database;
namespace C { public class Child : P.Parent { public int Get() { return QueryAsync(\"x\"); } } }";
    both_orders(
        grand,
        &[
            ("B_common.cs", COMMON_BASE),
            ("C_parent.cs", parent),
            ("D_child.cs", child),
        ],
        |f| assert_eq!(f.targets("C.Child.Get"), vec!["G.Grand.QueryAsync"]),
    );
    // Middle and leaf declared before the grandparent as well.
    let files = [
        ("A_child.cs", child),
        ("B_parent.cs", parent),
        ("C_common.cs", COMMON_BASE),
        ("Z_grand.cs", grand),
    ];
    assert_eq!(
        index(&files).targets("C.Child.Get"),
        vec!["G.Grand.QueryAsync"]
    );
}

#[test]
fn name_only_in_two_imported_namespaces_stays_ambiguous() {
    let a = "namespace A { public class X { public int Frobnicate() { return 1; } } }";
    let b = "namespace B { public class Y { public int Frobnicate() { return 2; } } }";
    let caller = "using A;
using B;
namespace App { public class Base { }
    public class Repo : Base { public int Get() { return Frobnicate(); } } }";
    both_orders(a, &[("B_b.cs", b), ("C_caller.cs", caller)], |f| {
        assert!(f.targets("App.Repo.Get").is_empty());
        assert_eq!(f.unresolved_reasons("App.Repo.Get"), vec!["ambiguous"]);
    });
}

#[test]
fn external_base_does_not_error_and_records_as_before() {
    let caller = "using System;
namespace App { public class Repo : Some.External.Base {
    public int Get() { return Missing(); } } }";
    let f = index(&[("Repo.cs", caller)]);
    assert!(f.targets("App.Repo.Get").is_empty());
    assert_eq!(f.unresolved_reasons("App.Repo.Get"), vec!["no_candidates"]);
    // A repo-unique name is still found through the bare-name fallback.
    let helper =
        "namespace Util { public static class H { public static int Only() { return 1; } } }";
    let caller = "namespace App { public class Repo : Some.External.Base {
    public int Get() { return Only(); } } }";
    let f = index(&[("H.cs", helper), ("Repo.cs", caller)]);
    assert_eq!(f.targets("App.Repo.Get"), vec!["Util.H.Only"]);
}

#[test]
fn static_method_calling_static_member_resolves() {
    let src = "using Dpb.Common.Database;
namespace App { public class Base { protected static int Shared() { return 1; } }
    public class Repo : Base {
        static int Own() { return 1; }
        public static int Get() { return Own() + Shared(); } } }";
    let other = "namespace Other { public class X { public static int Own() { return 3; } public static int Shared() { return 4; } } }";
    both_orders(other, &[("B_repo.cs", src)], |f| {
        let mut got = f.targets("App.Repo.Get");
        got.sort();
        assert_eq!(got, vec!["App.Base.Shared", "App.Repo.Own"]);
    });
}

#[test]
fn local_function_shadows_inherited_method() {
    let src =
        "namespace App { public class Base { protected int QueryAsync(string s) { return 1; } }
    public class Repo : Base {
        public int Get() { int QueryAsync(string s) { return 2; } return QueryAsync(\"x\"); } } }";
    let f = index(&[("Repo.cs", src)]);
    assert!(
        !f.targets("App.Repo.Get")
            .contains(&"App.Base.QueryAsync".to_string()),
        "a local function shadows the base method"
    );
}

#[test]
fn explain_symbol_on_base_method_lists_subclass_caller() {
    let mut f = index(&[
        ("A_base.cs", MSSQL_BASE),
        ("B_common.cs", COMMON_BASE),
        ("C_team.cs", TEAM_REPO),
    ]);
    let out = lidx::rpc::handle_method(
        &mut f.indexer,
        "explain_symbol",
        serde_json::json!({"qualname": "Dpb.DataMgr.Database.MssqlRepositoryBase.QueryAsync"}),
    )
    .unwrap()
    .to_string();
    assert!(
        out.contains("MssqlTeamRepository.Get"),
        "subclass caller missing: {out}"
    );
}

#[test]
fn nested_type_binds_own_base_then_outer_static_members() {
    let outer = "namespace App { public class Outer {
    static int OuterStatic() { return 1; }
    public class NBase { protected int Inh() { return 1; } }
    public class Inner : NBase { public int Get() { return Inh() + OuterStatic(); } } } }";
    let distractor = "namespace Other { public class X {
    public int Inh() { return 2; } public static int OuterStatic() { return 3; } } }";
    both_orders(distractor, &[("B_outer.cs", outer)], |f| {
        let mut got = f.targets("App.Outer.Inner.Get");
        got.sort();
        assert_eq!(got, vec!["App.Outer.NBase.Inh", "App.Outer.OuterStatic"]);
    });
}

#[test]
fn generic_base_class_resolves_inherited_call() {
    let base = "namespace App { public class Base<T> { protected int Shared() { return 1; } } public class Foo { } }";
    let repo = "using Dpb.Common.Database;
namespace App { public class Repo : Base<Foo> { public int Get() { return Shared(); } } }";
    let other = "namespace Other { public class X { public int Shared() { return 2; } } }";
    both_orders(base, &[("B_repo.cs", repo), ("C_other.cs", other)], |f| {
        assert_eq!(f.targets("App.Repo.Get"), vec!["App.Base.Shared"]);
    });
}

#[test]
fn partial_class_split_across_files_resolves_inherited_call() {
    let base = "namespace App { public class Base { protected int Shared() { return 1; } } }";
    let part1 =
        "namespace App { public partial class Repo : Base { public int A() { return 1; } } }";
    let part2 =
        "namespace App { public partial class Repo { public int Get() { return Shared(); } } }";
    let other = "namespace Other { public class X { public int Shared() { return 2; } } }";
    both_orders(
        base,
        &[
            ("B_p1.cs", part1),
            ("C_p2.cs", part2),
            ("D_other.cs", other),
        ],
        |f| assert_eq!(f.targets("App.Repo.Get"), vec!["App.Base.Shared"]),
    );
    // The part declaring the base list sorted after the part that calls.
    let files = [
        ("A_p2.cs", part2),
        ("B_p1.cs", part1),
        ("C_other.cs", other),
        ("D_base.cs", base),
    ];
    assert_eq!(
        index(&files).targets("App.Repo.Get"),
        vec!["App.Base.Shared"]
    );
}

#[test]
fn bare_delegate_call_does_not_bind_unrelated_method() {
    let other = "namespace Other { public class X { public void handler() { } public void cb() { } public void onDone() { } } }";
    let repo = "using System;
namespace App { public class Repo {
    private Action handler;
    public void Go(Action cb) { onDone(); handler(); cb(); Action local = null; local(); }
    private Action onDone; } }";
    both_orders(other, &[("B_repo.cs", repo)], |f| {
        // A delegate field may be the target (its own symbol); a method of
        // an unrelated type, or a parameter/local, never is.
        let got = f.targets("App.Repo.Go");
        assert!(
            got.iter().all(|t| t.starts_with("App.Repo.")),
            "delegate invocations bound to an unrelated method: {got:?}"
        );
    });
}

#[test]
fn explicit_this_and_base_calls_resolve_inherited_method() {
    let base = "namespace App { public class Base { protected int Shared() { return 1; } } }";
    let repo = "using Dpb.Common.Database;
namespace App { public class Repo : Base {
    public int ViaThis() { return this.Shared(); }
    public int ViaBase() { return base.Shared(); } } }";
    let other = "namespace Other { public class X { public int Shared() { return 2; } } }";
    both_orders(base, &[("B_repo.cs", repo), ("C_other.cs", other)], |f| {
        assert_eq!(f.targets("App.Repo.ViaThis"), vec!["App.Base.Shared"]);
        assert_eq!(f.targets("App.Repo.ViaBase"), vec!["App.Base.Shared"]);
    });
}

#[test]
fn local_function_inside_lambda_and_declaration_initializer_shadows_base() {
    let src = "using System;
namespace App { public class Base { protected int Q() { return 1; } }
    public class Repo : Base {
        public void InLambda() { Action a = () => { int Q() { return 2; } var x = Q(); }; }
        public int InInitializer() { int Q() { return 2; } int r = Q(); return r; } } }";
    let f = index(&[("Repo.cs", src)]);
    for caller in ["App.Repo.InLambda", "App.Repo.InInitializer"] {
        assert!(
            !f.targets(caller).contains(&"App.Base.Q".to_string()),
            "{caller}: local function must shadow the base method"
        );
    }
}
