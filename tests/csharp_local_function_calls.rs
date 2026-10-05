//! Issue #345: calls written inside a C# local function body attribute to
//! the enclosing named symbol (a local function is not a symbol itself).

use lidx::indexer::Indexer;
use rusqlite::params;
use std::path::Path;

const SOURCE: &str = r#"namespace Demo {
    public class Foo { public int M() { return 1; } }
    public class Bar { public int M() { return 2; } }
    public class Svc {
        public Task<string> OpenAsync(string c) { return null; }
        public int Plain(string r) { return 0; }
        public int Deep() { return 3; }
        public int InLambda() { return 4; }
        public int InCtor() { return 5; }
        public int InAccessor() { return 6; }

        public async Task<int> RunAsync(string conn) {
            async Task<int> LocalAsync(string c) {
                var r = await OpenAsync(c);
                return Plain(r);
            }
            return await LocalAsync(conn);
        }

        public int Shadow() {
            int Plain() { return Deep(); }
            int Outer() { return Plain() + InLambda(); }
            return Plain() + Outer();
        }

        public int Param() {
            Foo x = new Foo();
            int L(Bar x) { return x.M(); }
            return L(null);
        }

        public int Nested() {
            int A() {
                static int B() { return Deep(); }
                return B();
            }
            Func<int> f = () => { int C() { return InLambda(); } return C(); };
            return A() + f();
        }

        public Svc() {
            int Init() { return InCtor(); }
            Init();
        }

        public int Prop {
            get {
                int G() { return InAccessor(); }
                return G();
            }
        }
    }
}
"#;

fn setup(dir: &Path) -> Indexer {
    std::fs::write(dir.join("Svc.cs"), SOURCE).unwrap();
    let mut indexer =
        Indexer::new(dir.to_path_buf(), dir.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    indexer
}

/// Every CALLS edge among non-external targets as `(source, target)`.
fn calls(indexer: &Indexer) -> Vec<(String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, COALESCE(t.qualname, e.target_qualname, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ?
             ORDER BY 1, 2",
        )
        .unwrap();
    stmt.query_map(params![gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn has(edges: &[(String, String)], src: &str, dst: &str) -> bool {
    edges.iter().any(|(s, t)| s == src && t == dst)
}

#[test]
fn local_function_calls_attribute_to_enclosing_symbol() {
    let tmp = tempfile::tempdir().unwrap();
    let edges = calls(&setup(tmp.path()));
    let d = |s: &str| format!("Demo.Svc.{s}");

    // Fixture: calls inside the local function, none for LocalAsync itself.
    assert!(has(&edges, &d("RunAsync"), &d("OpenAsync")), "{edges:?}");
    assert!(has(&edges, &d("RunAsync"), &d("Plain")), "{edges:?}");
    assert!(
        !edges
            .iter()
            .any(|(s, t)| s == &d("RunAsync") && t.ends_with("LocalAsync")),
        "{edges:?}"
    );

    // Local function named like a member: no edge to the member, at depth 1
    // and inside a nested local function.
    assert!(
        !edges
            .iter()
            .any(|(s, t)| s == &d("Shadow") && t == &d("Plain")),
        "{edges:?}"
    );
    // ...while real calls inside those local functions still attribute to the
    // enclosing member (depth 1 via Plain's body, nested via Outer's body).
    assert!(has(&edges, &d("Shadow"), &d("Deep")), "{edges:?}");
    assert!(has(&edges, &d("Shadow"), &d("InLambda")), "{edges:?}");

    // Parameter shadowing an outer variable resolves against the parameter type.
    assert!(
        !has(&edges, &d("Param"), "Demo.Foo.M"),
        "param mis-typed as outer Foo: {edges:?}"
    );

    // Nested, static, lambda-hosted, constructor and accessor local functions.
    assert!(has(&edges, &d("Nested"), &d("Deep")), "{edges:?}");
    assert!(has(&edges, &d("Nested"), &d("InLambda")), "{edges:?}");
    assert!(has(&edges, "Demo.Svc..ctor", &d("InCtor")), "{edges:?}");
    assert!(
        edges
            .iter()
            .any(|(s, t)| s.starts_with("Demo.Svc.Prop") && t == &d("InAccessor")),
        "{edges:?}"
    );
}

#[test]
fn local_function_param_binds_to_its_own_type() {
    let tmp = tempfile::tempdir().unwrap();
    let edges = calls(&setup(tmp.path()));
    assert!(has(&edges, "Demo.Svc.Param", "Demo.Bar.M"), "{edges:?}");
}

#[test]
fn incremental_reindex_matches_fresh_index() {
    let tmp = tempfile::tempdir().unwrap();
    let mut indexer = setup(tmp.path());
    let fresh = calls(&indexer);

    let path = tmp.path().join("Svc.cs");
    std::fs::write(&path, SOURCE.replace("InLambda()", "InLambdaX()")).unwrap();
    indexer.sync_rel_paths(&["Svc.cs".to_string()]).unwrap();
    std::fs::write(&path, SOURCE).unwrap();
    indexer.sync_rel_paths(&["Svc.cs".to_string()]).unwrap();

    let after = calls(&indexer);
    assert_eq!(after, fresh);
    // Concrete edges, so the test fails if local-function calls are dropped
    // on both paths.
    assert!(
        has(&after, "Demo.Svc.RunAsync", "Demo.Svc.OpenAsync"),
        "{after:?}"
    );
    assert!(has(&after, "Demo.Svc.Nested", "Demo.Svc.Deep"), "{after:?}");
}
