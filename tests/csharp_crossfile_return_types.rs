//! Issue #180: `var x = recv.Method(..)` / `var x = Type.Method(..)` binds
//! `x`'s type from the callee's declared return type even when the callee
//! lives in another file. Two same-named `Write` methods make a wrong or
//! missing receiver type visible: the bare-name tier refuses (ambiguous),
//! so a `Write` edge resolves only through the inferred receiver type.

use lidx::indexer::Indexer;
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

const TYPES: &str = r#"
namespace App
{
    public class Store { public void Write() { } }
    public class Other { public void Write() { } }
    public class Logger { public Store Info() { return null; } }
    public class Base { public Other Logger { get { return null; } } }
}
"#;

const REPO: &str = r#"
using System.Threading.Tasks;
namespace App
{
    public class Repo
    {
        public Store Open() { return null; }
        public static Store Create() { return null; }
        public async Task<Store> OpenAsync() { return null; }
        public T Get<T>() { return default; }
        public Other Dup(int a) { return null; }
        public Store Dup(string a) { return null; }
        public Task<Store> Lazy() { return null; }
        public Store Twice(int a) { return null; }
        public Store Twice(string a) { return null; }
        public int Count() { return 0; }
    }
}
"#;

const INHERITED: &str = r#"
namespace App
{
    public class Derived : Base
    {
        public void M() { var r = Logger.Info(); r.Write(); }
    }
}
"#;

const PROGRAM: &str = r#"
using App;
var s = Repo.Create();
s.Write();
"#;

const CALLER: &str = r#"
using System.Collections.Generic;
using System.Threading.Tasks;
namespace App
{
    public class Caller
    {
        private readonly Repo _repo = new Repo();

        public void FromParam(Repo repo) { var s = repo.Open(); s.Write(); }
        public void FromStatic() { var s = Repo.Create(); s.Write(); }
        public async Task FromAwait(Repo repo) { var s = await repo.OpenAsync().ConfigureAwait(false); s.Write(); }
        public void FromField() { var s = _repo.Open(); s.Write(); }
        public void FromThisField() { var s = this._repo.Open(); s.Write(); }
        public void FromLocal() { var r = new Repo(); var s = r.Open(); s.Write(); }
        public void Generic(Repo repo) { var g = repo.Get<Store>(); g.Write(); }
        public void Overloaded(Repo repo) { var d = repo.Dup(1); d.Write(); }
        public void NotAwaited(Repo repo) { var t = repo.Lazy(); t.Write(); }
        public void OverloadsAgree(Repo repo) { var t = repo.Twice(1); t.Write(); }
        public void Builtin(Repo repo) { var n = repo.Count(); n.Write(); }
        public void Unknown(Repo repo) { var u = repo.Missing(); u.Write(); }
        public void Deconstruct(List<(string, Store)> pairs) { foreach (var (k, v) in pairs) { v.Write(); } }
        public void DeconstructDict(Dictionary<string, Store> map) { foreach (var (k, v) in map) { v.Write(); } }
        public void DeconstructUnknown(Repo repo) { foreach (var (k, v) in repo.Missing()) { v.Write(); } }
    }
}
"#;

fn setup() -> (PathBuf, Indexer, i64) {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-csharp-crossfile-{nanos}-{n}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("Types.cs"), TYPES).unwrap();
    std::fs::write(dir.join("Repo.cs"), REPO).unwrap();
    std::fs::write(dir.join("Caller.cs"), CALLER).unwrap();
    std::fs::write(dir.join("Derived.cs"), INHERITED).unwrap();
    std::fs::write(dir.join("Program.cs"), PROGRAM).unwrap();
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    (dir, indexer, gv)
}

/// Qualnames of every `*.Write` a call in `caller` resolved to.
fn write_targets(indexer: &Indexer, gv: i64, caller: &str) -> Vec<String> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = ? AND e.graph_version = ?
               AND t.name = 'Write'",
        )
        .unwrap();
    stmt.query_map(params![caller, gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn cross_file_return_types_bind_the_receiver() {
    let (dir, indexer, gv) = setup();
    for caller in [
        "FromParam",
        "FromStatic",
        "FromAwait",
        "FromField",
        "FromThisField",
        "FromLocal",
        "OverloadsAgree",
        "Deconstruct",
        "DeconstructDict",
    ] {
        assert_eq!(
            write_targets(&indexer, gv, &format!("App.Caller.{caller}")),
            vec!["App.Store.Write".to_string()],
            "{caller}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ambiguous_generic_or_unknown_return_types_stay_untracked() {
    let (dir, indexer, gv) = setup();
    for caller in [
        "Generic",
        "Overloaded",
        "NotAwaited",
        "Builtin",
        "Unknown",
        "DeconstructUnknown",
    ] {
        assert!(
            write_targets(&indexer, gv, &format!("App.Caller.{caller}")).is_empty(),
            "{caller} bound a Write target"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn capitalised_receiver_bound_to_an_instance_method_stays_untracked() {
    // `Logger` is an inherited property here, not the unrelated `Logger`
    // type, whose `Info` is an instance method.
    let (dir, indexer, gv) = setup();
    assert!(write_targets(&indexer, gv, "App.Derived.M").is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn top_level_statements_bind_cross_file_static_return_types() {
    let (dir, indexer, gv) = setup();
    let conn = indexer.db().read_conn().unwrap();
    let targets: Vec<String> = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN files f ON f.id = e.file_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND f.path = 'Program.cs' AND e.graph_version = ?
               AND t.name = 'Write'",
        )
        .unwrap()
        .query_map(params![gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(targets, vec!["App.Store.Write".to_string()]);
    let _ = std::fs::remove_dir_all(&dir);
}
