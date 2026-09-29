//! Issue #184: C# receiver inference through partial-class / base-class
//! callees, chained calls, `var`s bound from deferred `var`s, and
//! factory-returned gRPC clients. Two same-named `Write` methods make a
//! wrong or missing receiver type visible: the bare-name tier refuses
//! (ambiguous), so a `Write` edge resolves only through the inferred type.
//! Each scenario also checks incremental sync == fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use rusqlite::params;
use std::path::PathBuf;

const TYPES: &str = r#"
namespace App
{
    public class StoreBase { public void Write() { } }
    public class Store : StoreBase { }
    public class Other { public void Write() { } }
}
"#;

const REPO: &str = r#"
using System.Threading.Tasks;
namespace App
{
    public class Repo
    {
        public Store Open() { return null; }
        public Repo Nested() { return null; }
        public async Task<Store> OpenAsync() { return null; }
        public Repo Factory() { return null; }
        public Other Wrong() { return null; }
    }
}
"#;

const BASE: &str = r#"
namespace App
{
    public class Base
    {
        protected Store Inherited() { return null; }
        protected Repo GetRepo() { return null; }
    }
}
"#;

const DERIVED: &str = r#"
namespace App
{
    public class Derived : Base
    {
        public void Bare() { var s = Inherited(); s.Write(); }
        public void This() { var s = this.Inherited(); s.Write(); }
        public void BaseCall() { var s = base.Inherited(); s.Write(); }
        public void Chained() { GetRepo().Open().Write(); }
        public void Unknown() { var s = Missing(); s.Write(); }
    }
}
"#;

const PART_A: &str = r#"
namespace App
{
    public partial class Service
    {
        public void Run() { var s = Open(); s.Write(); var t = this.Open(); t.Write(); }
    }
}
"#;

const PART_B: &str = r#"
namespace App
{
    public partial class Service
    {
        private Store Open() { return null; }
    }
}
"#;

const CALLER: &str = r#"
using System.Threading.Tasks;
namespace App
{
    public class Caller
    {
        public void Chain(Repo repo) { repo.Open().Write(); }
        public void Deep(Repo repo) { repo.Nested().Nested().Open().Write(); }
        public async Task Awaited(Repo repo) { (await repo.OpenAsync()).Write(); }
        public void VarFromVar(Repo repo) { var a = repo.Factory(); var b = a.Open(); b.Write(); }
        public void VarChain(Repo repo) { var a = repo.Factory(); var b = a.Factory(); var c = b.Open(); c.Write(); }
        public void VarThenChain(Repo repo) { var a = repo.Factory(); a.Nested().Open().Write(); }
        public void TooDeep(Repo repo) { repo.Nested().Nested().Nested().Nested().Nested().Nested().Nested().Nested().Nested().Open().Write(); }
        public void Unknown(Repo repo) { var a = repo.Missing(); var b = a.Open(); b.Write(); }
        public void Builtin(Repo repo) { repo.Wrong().Nested().Write(); }
    }
}
"#;

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-csharp-residue-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

fn all_files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Types.cs", TYPES),
        ("Repo.cs", REPO),
        ("Base.cs", BASE),
        ("Derived.cs", DERIVED),
        ("PartA.cs", PART_A),
        ("PartB.cs", PART_B),
        ("Caller.cs", CALLER),
    ]
}

/// Qualnames of every edge of `kind` from `caller` whose target is named `name`.
fn targets(indexer: &Indexer, caller: &str, kind: &str, name: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = ? AND s.qualname = ? AND e.graph_version = ? AND t.name = ?",
        )
        .unwrap();
    stmt.query_map(params![kind, caller, gv, name], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn write_targets(indexer: &Indexer, caller: &str) -> Vec<String> {
    targets(indexer, caller, "CALLS", "Write")
}

fn assert_matches_fresh(indexer: &Indexer, files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(files);
    common::assert_matches_fresh(&snapshot, &fresh);
    // The snapshot omits bridge-edge targets: compare `RPC_CALL` ones too.
    let (_t, _r, fresh_indexer) = indexed(files);
    assert_eq!(rpc_edges(indexer), rpc_edges(&fresh_indexer));
}

/// Every `RPC_CALL` edge as `(caller, target_qualname, bound target)`, sorted.
fn rpc_edges(indexer: &Indexer) -> Vec<(String, Option<String>, Option<String>)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, e.target_qualname, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'RPC_CALL' AND e.graph_version = ? ORDER BY 1, 2",
        )
        .unwrap();
    stmt.query_map(params![gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn partial_and_base_class_callees_infer_the_receiver() {
    let (_tmp, _root, indexer) = indexed(&all_files());
    for caller in [
        "App.Service.Run",
        "App.Derived.Bare",
        "App.Derived.This",
        "App.Derived.BaseCall",
    ] {
        let found = write_targets(&indexer, caller);
        assert!(
            !found.is_empty() && found.iter().all(|t| t == "App.StoreBase.Write"),
            "{caller}: {found:?}"
        );
    }
    // Two calls in `Service.Run`, both through `Open()` in another partial file.
    assert_eq!(write_targets(&indexer, "App.Service.Run").len(), 2);
    assert!(write_targets(&indexer, "App.Derived.Unknown").is_empty());
}

#[test]
fn chained_calls_infer_from_each_return_type() {
    let (_tmp, _root, indexer) = indexed(&all_files());
    for caller in [
        "App.Caller.Chain",
        "App.Caller.Deep",
        "App.Caller.Awaited",
        "App.Derived.Chained",
    ] {
        assert_eq!(
            write_targets(&indexer, caller),
            vec!["App.StoreBase.Write".to_string()],
            "{caller}"
        );
    }
    // `Wrong()` returns `Other`, which has no `Nested`; and 10 nested calls
    // exceed the depth cap.
    for caller in ["App.Caller.Builtin", "App.Caller.TooDeep"] {
        assert!(write_targets(&indexer, caller).is_empty(), "{caller}");
    }
}

#[test]
fn var_bound_from_a_deferred_var_chains_inference() {
    let (_tmp, _root, indexer) = indexed(&all_files());
    for caller in [
        "App.Caller.VarFromVar",
        "App.Caller.VarChain",
        "App.Caller.VarThenChain",
    ] {
        assert_eq!(
            write_targets(&indexer, caller),
            vec!["App.StoreBase.Write".to_string()],
            "{caller}"
        );
    }
    assert!(write_targets(&indexer, "App.Caller.Unknown").is_empty());
}

#[test]
fn inference_follows_callee_edits_incrementally() {
    let files = all_files();
    let (_tmp, root, mut indexer) = indexed(&files);

    // Retarget every return type: `Base.Inherited`, `Repo.Open` and the
    // partial `Service.Open` now return `Other`.
    let base = BASE.replace("Store Inherited", "Other Inherited");
    let repo = REPO.replace("Store Open()", "Other Open()");
    let part_b = PART_B.replace("Store Open", "Other Open");
    common::write_files(
        &root,
        &[
            ("Base.cs", &base),
            ("Repo.cs", &repo),
            ("PartB.cs", &part_b),
        ],
    );
    indexer
        .sync_rel_paths(&["Base.cs".into(), "Repo.cs".into(), "PartB.cs".into()])
        .unwrap();
    assert_eq!(
        write_targets(&indexer, "App.Derived.Bare"),
        vec!["App.Other.Write".to_string()]
    );
    assert_eq!(
        write_targets(&indexer, "App.Caller.VarChain"),
        vec!["App.Other.Write".to_string()]
    );
    let edited: Vec<(&str, &str)> = vec![
        ("Types.cs", TYPES),
        ("Repo.cs", &repo),
        ("Base.cs", &base),
        ("Derived.cs", DERIVED),
        ("PartA.cs", PART_A),
        ("PartB.cs", &part_b),
        ("Caller.cs", CALLER),
    ];
    assert_matches_fresh(&indexer, &edited);

    // Re-parent `Derived` off `Base`: the inherited callee disappears.
    let derived = DERIVED.replace(" : Base", "");
    common::write_files(&root, &[("Derived.cs", &derived)]);
    indexer.sync_rel_paths(&["Derived.cs".into()]).unwrap();
    assert!(write_targets(&indexer, "App.Derived.Bare").is_empty());
    let edited: Vec<(&str, &str)> = vec![
        ("Types.cs", TYPES),
        ("Repo.cs", &repo),
        ("Base.cs", &base),
        ("Derived.cs", &derived),
        ("PartA.cs", PART_A),
        ("PartB.cs", &part_b),
        ("Caller.cs", CALLER),
    ];
    assert_matches_fresh(&indexer, &edited);

    // Delete the partial file that declares `Open`: `Service.Run` unbinds.
    std::fs::remove_file(root.join("PartB.cs")).unwrap();
    indexer.sync_rel_paths(&["PartB.cs".into()]).unwrap();
    assert!(write_targets(&indexer, "App.Service.Run").is_empty());
    let edited: Vec<(&str, &str)> = vec![
        ("Types.cs", TYPES),
        ("Repo.cs", &repo),
        ("Base.cs", &base),
        ("Derived.cs", &derived),
        ("PartA.cs", PART_A),
        ("Caller.cs", CALLER),
    ];
    assert_matches_fresh(&indexer, &edited);
}

#[test]
fn chained_callee_added_later_resolves() {
    let files: Vec<_> = all_files()
        .into_iter()
        .filter(|(p, _)| *p != "Repo.cs")
        .collect();
    let (_tmp, root, mut indexer) = indexed(&files);
    assert!(write_targets(&indexer, "App.Caller.Deep").is_empty());
    common::write_files(&root, &[("Repo.cs", REPO)]);
    indexer.sync_rel_paths(&["Repo.cs".into()]).unwrap();
    assert_eq!(
        write_targets(&indexer, "App.Caller.Deep"),
        vec!["App.StoreBase.Write".to_string()]
    );
    assert_matches_fresh(&indexer, &all_files());
}

const ASSIGNER: &str = r#"
namespace App
{
    public class Assigner : Base
    {
        private Other _f;
        private Other _g;
        public void Plain(Repo repo) { var x = repo.Wrong(); x = repo.Open(); x.Write(); }
        public void BeforeAfter(Repo repo) { var x = repo.Wrong(); x.Write(); x = repo.Open(); x.Write(); }
        public void BaseAssign(Repo repo) { var x = repo.Wrong(); x = base.Inherited(); x.Write(); }
        public void BareAssign(Repo repo) { var x = repo.Wrong(); x = Inherited(); x.Write(); }
        public void ViaCall(Repo repo) { var a = repo.Wrong(); a = repo.Nested(); var b = a.Open(); b.Write(); }
        public void Field(Repo repo) { _f = repo.Open(); _f.Write(); }
        public void ThisField(Repo repo) { this._g = repo.Open(); this._g.Write(); }
        public void InBranch(Repo repo, bool c) { var x = repo.Wrong(); if (c) { x = repo.Open(); x.Write(); } }
        public void AfterBranch(Repo repo, bool c) { var x = repo.Wrong(); if (c) { x = repo.Open(); } x.Write(); }
        public void Embedded(Repo repo, bool c) { var x = repo.Wrong(); if (c) x = repo.Open(); x.Write(); }
        public void InLoop(Repo repo, bool c) { var x = repo.Wrong(); while (c) { x.Write(); x = repo.Open(); } }
        public void Dominated(Repo repo, bool c) { var x = repo.Wrong(); if (c) { x = repo.Open(); } x = repo.Nested().Open(); x.Write(); }
        public void Unknown(Repo repo) { var x = repo.Open(); x = repo.Missing(); x.Write(); }
    }
}
"#;

fn assign_files(repo: &str) -> Vec<(&'static str, &str)> {
    let mut files = all_files();
    files.retain(|(p, _)| *p != "Repo.cs");
    files.push(("Repo.cs", repo));
    files.push(("Assigner.cs", ASSIGNER));
    files
}

#[test]
fn plain_assignments_update_the_tracked_type_flow_sensitively() {
    let (_tmp, _root, indexer) = indexed(&assign_files(REPO));
    let store = vec!["App.StoreBase.Write".to_string()];
    for name in [
        "Plain",
        "BaseAssign",
        "BareAssign",
        "ViaCall",
        "Field",
        "ThisField",
        "InBranch",
        "Dominated",
    ] {
        assert_eq!(
            write_targets(&indexer, &format!("App.Assigner.{name}")),
            store,
            "{name}"
        );
    }
    let mut both = write_targets(&indexer, "App.Assigner.BeforeAfter");
    both.sort();
    assert_eq!(both, ["App.Other.Write", "App.StoreBase.Write"]);
    // Ambiguous after a branch, an embedded statement or a loop: untracked
    // (`InLoop`'s first call is in the loop the assignment sits in).
    // A callee the resolver can't type leaves it untracked too (`Unknown`).
    for name in ["AfterBranch", "Embedded", "InLoop", "Unknown"] {
        assert!(
            write_targets(&indexer, &format!("App.Assigner.{name}")).is_empty(),
            "{name}"
        );
    }
}

#[test]
fn assignment_types_follow_callee_edits_incrementally() {
    let (_tmp, root, mut indexer) = indexed(&assign_files(REPO));
    let repo = REPO.replace("Store Open()", "Other Open()");
    common::write_files(&root, &[("Repo.cs", &repo)]);
    indexer.sync_rel_paths(&["Repo.cs".into()]).unwrap();
    assert_eq!(
        write_targets(&indexer, "App.Assigner.Plain"),
        vec!["App.Other.Write".to_string()]
    );
    assert_matches_fresh(&indexer, &assign_files(&repo));
}

// ---- gRPC clients returned by a factory in another file -----------------

const FACTORY: &str = r#"
using Acme.Protos;
using Alias = Acme.Aliased;
using System.Threading.Tasks;
namespace App
{
    public static class Clients
    {
        public static Alias.Greeter.GreeterClient CreateAliased() { return null; }
        public static Greeter.GreeterClient CreateClient(string address) { return null; }
        public static async Task<Greeter.GreeterClient> CreateAsync() { return null; }
        public static Store Plain() { return null; }
    }
}
"#;

const FACTORY_BASE: &str = r#"
using Acme.Protos;
using System.Threading.Tasks;
namespace App
{
    public class ClientBase
    {
        protected Greeter.GreeterClient Connect() { return null; }
    }
}
"#;

const RPC_CALLER: &str = r#"
using Caller.Only;
using System.Threading.Tasks;
namespace App
{
    public class Greeting : ClientBase
    {
        public void Static() { var c = Clients.CreateClient("x"); c.SayHello(null); }
        public async Task Async() { var c = await Clients.CreateAsync(); await c.SayHelloAsync(null); }
        public void Chained() { Clients.CreateClient("x").SayBye(null); }
        public void Inherited() { var c = Connect(); c.SayHello(null); }
        public void Aliased() { var c = Clients.CreateAliased(); c.SayHello(null); }
        public void NotAClient() { var c = Clients.Plain(); c.SayHello(null); }
    }
}
"#;

fn rpc_files(factory: &str) -> Vec<(&str, &str)> {
    vec![
        ("Types.cs", TYPES),
        ("Factory.cs", factory),
        ("ClientBase.cs", FACTORY_BASE),
        ("Greeting.cs", RPC_CALLER),
    ]
}

/// `RPC_CALL` targets of every call from `caller`, sorted.
fn rpc_calls(indexer: &Indexer, caller: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.target_qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.kind = 'RPC_CALL' AND s.qualname = ? AND e.graph_version = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(params![caller, gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn factory_returned_grpc_clients_produce_rpc_call_edges() {
    let (_tmp, _root, indexer) = indexed(&rpc_files(FACTORY));
    // One edge per candidate package (each `using` in the calling file).
    let say_hello = "/acme.protos.greeter/sayhello";
    let say_bye = "/acme.protos.greeter/saybye";
    assert!(rpc_calls(&indexer, "App.Greeting.Static").contains(&say_hello.to_string()));
    assert!(rpc_calls(&indexer, "App.Greeting.Async").contains(&say_hello.to_string()));
    assert!(rpc_calls(&indexer, "App.Greeting.Chained").contains(&say_bye.to_string()));
    assert!(rpc_calls(&indexer, "App.Greeting.Inherited").contains(&say_hello.to_string()));
    assert!(rpc_calls(&indexer, "App.Greeting.NotAClient").is_empty());
    // Packages come from the factory's file (its usings and aliases), not
    // the caller's, which only imports `Caller.Only`.
    assert!(
        rpc_calls(&indexer, "App.Greeting.Aliased")
            .contains(&"/acme.aliased.greeter/sayhello".to_string())
    );
    for caller in ["Static", "Async", "Chained", "Inherited", "Aliased"] {
        for target in rpc_calls(&indexer, &format!("App.Greeting.{caller}")) {
            assert!(!target.contains("caller.only"), "{caller}: {target}");
        }
    }
}

#[test]
fn factory_edits_move_rpc_call_edges_incrementally() {
    let (_tmp, root, mut indexer) = indexed(&rpc_files(FACTORY));
    // The factory stops returning a client: every derived edge must go.
    let plain = FACTORY.replace("Greeter.GreeterClient CreateClient", "Store CreateClient");
    common::write_files(&root, &[("Factory.cs", &plain)]);
    indexer.sync_rel_paths(&["Factory.cs".into()]).unwrap();
    assert!(rpc_calls(&indexer, "App.Greeting.Static").is_empty());
    assert!(rpc_calls(&indexer, "App.Greeting.Chained").is_empty());
    assert!(
        rpc_calls(&indexer, "App.Greeting.Async")
            .contains(&"/acme.protos.greeter/sayhello".to_string())
    );
    assert_matches_fresh(&indexer, &rpc_files(&plain));

    // ... and comes back.
    common::write_files(&root, &[("Factory.cs", FACTORY)]);
    indexer.sync_rel_paths(&["Factory.cs".into()]).unwrap();
    assert!(
        rpc_calls(&indexer, "App.Greeting.Static")
            .contains(&"/acme.protos.greeter/sayhello".to_string())
    );
    assert_matches_fresh(&indexer, &rpc_files(FACTORY));

    // Editing an unrelated caller re-derives nothing extra.
    let caller = RPC_CALLER.replace("SayBye", "SayGoodbye");
    common::write_files(&root, &[("Greeting.cs", &caller)]);
    indexer.sync_rel_paths(&["Greeting.cs".into()]).unwrap();
    assert!(
        rpc_calls(&indexer, "App.Greeting.Chained")
            .contains(&"/acme.protos.greeter/saygoodbye".to_string())
    );
    let files = vec![
        ("Types.cs", TYPES),
        ("Factory.cs", FACTORY),
        ("ClientBase.cs", FACTORY_BASE),
        ("Greeting.cs", caller.as_str()),
    ];
    assert_matches_fresh(&indexer, &files);

    // Renaming the inherited factory unbinds the call through it.
    let base = FACTORY_BASE.replace("Connect", "Reconnect");
    common::write_files(&root, &[("ClientBase.cs", &base)]);
    indexer.sync_rel_paths(&["ClientBase.cs".into()]).unwrap();
    assert!(rpc_calls(&indexer, "App.Greeting.Inherited").is_empty());
    let files = vec![
        ("Types.cs", TYPES),
        ("Factory.cs", FACTORY),
        ("ClientBase.cs", base.as_str()),
        ("Greeting.cs", caller.as_str()),
    ];
    assert_matches_fresh(&indexer, &files);

    // Deleting the base class file (one of several declaring `namespace App`)
    // unbinds the inherited factory.
    std::fs::remove_file(root.join("ClientBase.cs")).unwrap();
    indexer.sync_rel_paths(&["ClientBase.cs".into()]).unwrap();
    assert!(rpc_calls(&indexer, "App.Greeting.Inherited").is_empty());
    let files = vec![
        ("Types.cs", TYPES),
        ("Factory.cs", FACTORY),
        ("Greeting.cs", caller.as_str()),
    ];
    assert_matches_fresh(&indexer, &files);
}
