//! Go method calls bind through the receiver's type, read from declarations
//! that may live in other files (a constructor's result, a struct field, a
//! package variable, an embedded type), and bind nothing when the type is
//! unknown. Every incremental scenario ends equal to a fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";

/// `s := New(); s.Handle()` with `New` and the type in other files.
const CALLER: &str = "package app\n\nfunc Run() {\n\ts := New()\n\ts.Handle()\n}\n";
const SERVER: &str = "package app\n\ntype Server struct{}\n\nfunc (s *Server) Handle() {}\n";
const NEW: &str = "package app\n\nfunc New() *Server { return &Server{} }\n";
/// A same-named decoy that an untyped guess would bind.
const DECOY: &str = "package other\n\ntype Pool struct{}\n\nfunc (p *Pool) Handle() {}\n";

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-go-recv-", files);
    let indexer = Indexer::new(root.clone(), db_path).unwrap();
    (tmp, root, indexer)
}

fn assert_incremental_equals_fresh(indexer: &Indexer, final_files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Target qualnames of the resolved CALLS edges of `source`, sorted.
fn calls_from(indexer: &Indexer, source: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.qualname = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn sync(indexer: &mut Indexer, root: &std::path::Path, files: &[(&str, &str)]) {
    common::write_files(root, files);
    let paths: Vec<String> = files.iter().map(|(p, _)| p.to_string()).collect();
    indexer.sync_rel_paths(&paths).unwrap();
}

#[test]
fn a_constructors_declared_result_types_the_local() {
    let files = [
        ("go.mod", GO_MOD),
        ("caller.go", CALLER),
        ("server.go", SERVER),
        ("new.go", NEW),
        ("other/o.go", DECOY),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        calls_from(&indexer, "caller.Run"),
        ["new.New", "server.Server.Handle"]
    );
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn a_constructor_added_later_types_the_local_like_a_fresh_index() {
    let base = [
        ("go.mod", GO_MOD),
        ("caller.go", CALLER),
        ("server.go", SERVER),
        ("other/o.go", DECOY),
    ];
    let (_tmp, root, mut indexer) = indexed(&base);
    assert!(calls_from(&indexer, "caller.Run").is_empty());
    assert_incremental_equals_fresh(&indexer, &base);

    sync(&mut indexer, &root, &[("new.go", NEW)]);
    assert_eq!(
        calls_from(&indexer, "caller.Run"),
        ["new.New", "server.Server.Handle"]
    );
    let mut files = base.to_vec();
    files.push(("new.go", NEW));
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn editing_a_constructors_result_type_retargets_the_call() {
    let pool = "package app\n\ntype Pool struct{}\n\nfunc (p *Pool) Handle() {}\n";
    let new_pool = "package app\n\nfunc New() *Pool { return &Pool{} }\n";
    let base = [
        ("go.mod", GO_MOD),
        ("caller.go", CALLER),
        ("server.go", SERVER),
        ("pool.go", pool),
        ("new.go", NEW),
    ];
    let (_tmp, root, mut indexer) = indexed(&base);
    assert!(calls_from(&indexer, "caller.Run").contains(&"server.Server.Handle".to_string()));

    sync(&mut indexer, &root, &[("new.go", new_pool)]);
    assert_eq!(
        calls_from(&indexer, "caller.Run"),
        ["new.New", "pool.Pool.Handle"]
    );
    let files = [
        ("go.mod", GO_MOD),
        ("caller.go", CALLER),
        ("server.go", SERVER),
        ("pool.go", pool),
        ("new.go", new_pool),
    ];
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn a_type_declared_in_another_file_and_removed_unbinds_the_call() {
    let base = [
        ("go.mod", GO_MOD),
        ("caller.go", CALLER),
        ("server.go", SERVER),
        ("new.go", NEW),
        ("other/o.go", DECOY),
    ];
    let (_tmp, root, mut indexer) = indexed(&base);
    std::fs::remove_file(root.join("server.go")).unwrap();
    indexer.sync_rel_paths(&["server.go".to_string()]).unwrap();
    assert_eq!(calls_from(&indexer, "caller.Run"), ["new.New"]);
    assert_incremental_equals_fresh(
        &indexer,
        &[
            ("go.mod", GO_MOD),
            ("caller.go", CALLER),
            ("new.go", NEW),
            ("other/o.go", DECOY),
        ],
    );
}

#[test]
fn a_struct_field_type_edited_later_retargets_the_chain() {
    let holder = "package app\n\ntype Holder struct{ srv *Server }\n\nfunc (h *Holder) Go() {\n\th.srv.Handle()\n}\n";
    let holder2 = "package app\n\ntype Holder struct{ srv *Pool }\n\nfunc (h *Holder) Go() {\n\th.srv.Handle()\n}\n";
    let pool = "package app\n\ntype Pool struct{}\n\nfunc (p *Pool) Handle() {}\n";
    let base = [
        ("go.mod", GO_MOD),
        ("holder.go", holder),
        ("server.go", SERVER),
        ("pool.go", pool),
    ];
    let (_tmp, root, mut indexer) = indexed(&base);
    assert_eq!(
        calls_from(&indexer, "holder.Holder.Go"),
        ["server.Server.Handle"]
    );
    sync(&mut indexer, &root, &[("holder.go", holder2)]);
    assert_eq!(
        calls_from(&indexer, "holder.Holder.Go"),
        ["pool.Pool.Handle"]
    );
    assert_incremental_equals_fresh(
        &indexer,
        &[
            ("go.mod", GO_MOD),
            ("holder.go", holder2),
            ("server.go", SERVER),
            ("pool.go", pool),
        ],
    );
}

#[test]
fn a_method_promoted_from_an_embedded_type_follows_the_struct_declaration() {
    let caller = "package app\n\nfunc Run(w *Wrapper) {\n\tw.Ping()\n}\n";
    let base_ty = "package app\n\ntype Base struct{}\n\nfunc (b *Base) Ping() {}\n";
    let plain = "package app\n\ntype Wrapper struct{ n int }\n";
    let embedding = "package app\n\ntype Wrapper struct{ Base }\n";
    let files = |wrapper: &'static str| {
        vec![
            ("go.mod", GO_MOD),
            ("caller.go", caller),
            ("base.go", base_ty),
            ("wrapper.go", wrapper),
        ]
    };
    let (_tmp, root, mut indexer) = indexed(&files(plain));
    assert!(calls_from(&indexer, "caller.Run").is_empty());

    sync(&mut indexer, &root, &[("wrapper.go", embedding)]);
    assert_eq!(calls_from(&indexer, "caller.Run"), ["base.Base.Ping"]);
    assert_incremental_equals_fresh(&indexer, &files(embedding));

    // The type's own method shadows the promoted one.
    let own = "package app\n\ntype Wrapper struct{ Base }\n\nfunc (w *Wrapper) Ping() {}\n";
    sync(&mut indexer, &root, &[("wrapper.go", own)]);
    assert_eq!(calls_from(&indexer, "caller.Run"), ["wrapper.Wrapper.Ping"]);
    assert_incremental_equals_fresh(&indexer, &files(own));

    sync(&mut indexer, &root, &[("wrapper.go", plain)]);
    assert!(calls_from(&indexer, "caller.Run").is_empty());
    assert_incremental_equals_fresh(&indexer, &files(plain));
}

#[test]
fn a_package_variables_type_is_read_from_its_declaration() {
    let caller = "package app\n\nfunc Run() {\n\tdefaultServer.Handle()\n}\n";
    let var_server = "package app\n\nvar defaultServer *Server\n";
    let var_pool = "package app\n\nvar defaultServer *Pool\n";
    let pool = "package app\n\ntype Pool struct{}\n\nfunc (p *Pool) Handle() {}\n";
    let files = |var: &'static str| {
        vec![
            ("go.mod", GO_MOD),
            ("caller.go", caller),
            ("server.go", SERVER),
            ("pool.go", pool),
            ("vars.go", var),
        ]
    };
    let (_tmp, root, mut indexer) = indexed(&files(var_server));
    assert_eq!(calls_from(&indexer, "caller.Run"), ["server.Server.Handle"]);
    sync(&mut indexer, &root, &[("vars.go", var_pool)]);
    assert_eq!(calls_from(&indexer, "caller.Run"), ["pool.Pool.Handle"]);
    assert_incremental_equals_fresh(&indexer, &files(var_pool));
}

#[test]
fn a_call_on_an_untyped_receiver_binds_no_same_named_method() {
    let caller = "package app\n\nfunc Run(x interface{}) {\n\tfor _, v := range items() {\n\t\tv.Handle()\n\t}\n\tx.Handle()\n}\n";
    let files = [
        ("go.mod", GO_MOD),
        ("caller.go", caller),
        ("server.go", SERVER),
        ("other/o.go", DECOY),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert!(calls_from(&indexer, "caller.Run").is_empty());
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn an_imported_standard_library_type_is_an_external_stub() {
    let caller = "package app\n\nimport \"bytes\"\n\nfunc Run() string {\n\tvar b bytes.Buffer\n\treturn b.String()\n}\n";
    let decoy = "package app\n\ntype R struct{}\n\nfunc (r R) String() string { return \"\" }\n";
    let files = [("go.mod", GO_MOD), ("caller.go", caller), ("r.go", decoy)];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        calls_from(&indexer, "caller.Run"),
        ["ext:bytes.Buffer.String"]
    );
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn a_generic_receivers_methods_are_methods_of_the_type_without_its_arguments() {
    let set = "package app\n\ntype Set[T comparable] struct{ m map[T]bool }\n\nfunc (s *Set[T]) Add(v T) { s.m[v] = true }\n\nfunc (s *Set[T]) AddAll(vs []T) {\n\tfor _, v := range vs {\n\t\ts.Add(v)\n\t}\n}\n";
    let caller = "package app\n\nfunc Run() {\n\tvar s Set[string]\n\ts.Add(\"a\")\n}\n";
    let files = [("go.mod", GO_MOD), ("set.go", set), ("caller.go", caller)];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(calls_from(&indexer, "caller.Run"), ["set.Set.Add"]);
    assert_eq!(calls_from(&indexer, "set.Set.AddAll"), ["set.Set.Add"]);
}

#[test]
fn an_aliased_go_import_is_judged_by_its_alias() {
    use lidx::rpc;
    let used = "package app\n\nimport str \"strings\"\n\nfunc Run() string { return str.ToUpper(\"a\") }\n";
    let unused = "package app\n\nimport s2 \"strings\"\n\nfunc Idle() {}\n";
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-go-alias-",
        &[("go.mod", GO_MOD), ("used.go", used), ("unused.go", unused)],
    );
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();
    let files: Vec<String> = result["unused_imports"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["file_path"].as_str().map(String::from))
        .collect();
    assert!(files.contains(&"unused.go".to_string()), "{result}");
    assert!(!files.contains(&"used.go".to_string()), "{result}");
}

#[test]
fn struct_and_variable_typing_data_is_not_shown_in_signatures() {
    use lidx::rpc;
    let src = "package app\n\nimport \"net\"\n\ntype Client struct {\n\tconn net.Conn\n\tBase\n}\n\ntype Base struct{}\n\nvar shared = New()\n\nvar conn net.Conn\n\nfunc New() *Client { return nil }\n";
    let (_tmp, root, db_path) =
        common::index_repo("lidx-go-sig-", &[("go.mod", GO_MOD), ("a.go", src)]);
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let outline =
        rpc::handle_method(&mut indexer, "outline", serde_json::json!({"path": "a.go"})).unwrap();
    let text = outline.to_string();
    assert!(!text.contains("go_type"), "{text}");
    assert!(!text.contains("~Base"), "{text}");
    let sym = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "a.Client"}),
    )
    .unwrap();
    assert!(!sym.to_string().contains("go_type"), "{sym}");
}
