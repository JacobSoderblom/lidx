//! Issue #239: a call to one of two same-arity C# overloads cannot be told
//! apart by arity. It must be an in-repo ambiguity in the unresolved store,
//! never an `ext:` stub, and `explain_symbol` must report the overload set
//! like `read_symbol` does. Argument-type matching is NOT implemented, so
//! the ambiguity outcome is what is asserted for the `Stream` call.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::Connection;

const PARSER: &str = "namespace Pkg.Manifest;\n\
public static class ManifestParser\n{\n\
    public static object Parse(string json) { return null; }\n\
    public static object Parse(System.IO.Stream stream) { return null; }\n\
    public static object Pick(string a) { return null; }\n\
    public static object Pick(string a, string b) { return null; }\n}\n";
const CALLER: &str = "using System.IO;\nusing Pkg.Manifest;\nnamespace Pkg.Publish;\n\
public class ZipFolderPublisher\n{\n    public void Publish(Stream manifestStream)\n    {\n\
        var result = ManifestParser.Parse(manifestStream);\n\
        var one = ManifestParser.Pick(\"a\");\n\
        var two = ManifestParser.Pick(\"a\", \"b\");\n\
        var ext = File.ReadAllText(\"x\");\n    }\n}\n";

/// What `probe` read back: `Publish`'s CALLS edges as `(target_qualname,
/// bound target qualname)` and its unresolved CALLS rows as `(name, reason)`.
struct Probe {
    edges: Vec<(String, Option<String>)>,
    unresolved: Vec<(String, String)>,
    /// Qualnames of the repo symbols sharing the `Parse` reference name.
    parse_candidates: Vec<String>,
    root: std::path::PathBuf,
    db_path: std::path::PathBuf,
    _tmp: tempfile::TempDir,
}

fn rpc_call(p: &Probe, method: &str, params: &str) -> serde_json::Value {
    let out = rpc::call(
        p.root.clone(),
        p.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    serde_json::from_str::<serde_json::Value>(&out).unwrap()["result"].clone()
}

fn probe(files: &[(&str, &str)]) -> Probe {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-239-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.graph_version();
    let conn = Connection::open(&db_path).unwrap();

    // No edge anywhere may target an external stub named after a repo type.
    let stubs: Vec<String> = conn
        .prepare(
            "SELECT t.qualname FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND t.kind = 'external'",
        )
        .unwrap()
        .query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(
        !stubs
            .iter()
            .any(|q| q.contains("ManifestParser") || q.contains("Pkg.")),
        "stub named after an in-repo entity: {stubs:?}"
    );

    let mut edges: Vec<(String, Option<String>)> = conn
        .prepare(
            "SELECT e.target_qualname, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS'
               AND s.qualname = 'Pkg.Publish.ZipFolderPublisher.Publish'",
        )
        .unwrap()
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    edges.sort();
    let mut unresolved: Vec<(String, String)> = conn
        .prepare(
            "SELECT ur.reference_name, ur.reason FROM unresolved_references ur
             JOIN symbols s ON s.id = ur.source_symbol_id
             WHERE ur.graph_version = ? AND ur.edge_kind = 'CALLS'
               AND s.qualname = 'Pkg.Publish.ZipFolderPublisher.Publish'",
        )
        .unwrap()
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    unresolved.sort();

    let parse_candidates: Vec<String> = conn
        .prepare(
            "SELECT qualname FROM symbols WHERE graph_version = ? AND kind = 'method'
             AND qualname = 'Pkg.Manifest.ManifestParser.Parse'",
        )
        .unwrap()
        .query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    Probe {
        edges,
        unresolved,
        parse_candidates,
        root,
        db_path,
        _tmp: tmp,
    }
}

fn check(files: &[(&str, &str)]) {
    let Probe {
        edges,
        unresolved,
        parse_candidates,
        ..
    } = probe(files);
    // The unresolved row's reference name is the qualname both overloads
    // share, so it names the whole candidate set (there is no separate
    // candidate column in the store).
    assert_eq!(parse_candidates.len(), 2, "both overloads are candidates");
    // Same-arity overloads: ambiguity, not a stub and not a guess.
    assert!(
        unresolved
            .iter()
            .any(|(n, reason)| n.ends_with("ManifestParser.Parse") && reason == "ambiguous"),
        "Parse must be an in-repo ambiguity: {unresolved:?}"
    );
    assert!(
        edges
            .iter()
            .all(|(t, resolved)| !(t.ends_with("ManifestParser.Parse") && resolved.is_some())),
        "Parse must not bind: {edges:?}"
    );
    // Overloads differing in arity still resolve by arity.
    assert!(
        edges.iter().any(|(t, r)| t.ends_with("ManifestParser.Pick")
            && r.as_deref() == Some("Pkg.Manifest.ManifestParser.Pick")),
        "Pick must resolve: {edges:?}"
    );
    assert!(
        !unresolved.iter().any(|(n, _)| n.ends_with("Pick")),
        "Pick must not be unresolved: {unresolved:?}"
    );
    // A genuine framework call stays external.
    assert!(
        edges.iter().any(|(t, r)| t.contains("ReadAllText")
            && r.as_deref().is_some_and(|q| q.starts_with("ext:"))),
        "File.ReadAllText must stay an external stub: {edges:?}"
    );
}

#[test]
fn same_arity_overload_is_ambiguous_declaring_file_first() {
    check(&[("a/ManifestParser.cs", PARSER), ("z/Publisher.cs", CALLER)]);
}

#[test]
fn same_arity_overload_is_ambiguous_declaring_file_last() {
    check(&[("a/Publisher.cs", CALLER), ("z/ManifestParser.cs", PARSER)]);
}

#[test]
fn explain_symbol_reports_overload_set_like_read_symbol() {
    let p = probe(&[("a/ManifestParser.cs", PARSER), ("z/Publisher.cs", CALLER)]);
    let q = r#"{"qualname":"Pkg.Manifest.ManifestParser.Parse"}"#;
    let read = rpc_call(&p, "read_symbol", q);
    let explain = rpc_call(&p, "explain_symbol", q);
    assert_eq!(read["overloaded"], true, "{read}");
    assert_eq!(explain["overloaded"], true, "{explain}");
    assert_eq!(explain["count"], read["count"], "{explain}");
    assert_eq!(explain["count"], 2, "{explain}");
    let hops = explain["next_hops"].as_array().unwrap();
    assert_eq!(hops.len(), 2, "{explain}");
    // Each hop explains exactly one overload, as a normal explanation.
    for hop in hops {
        assert_eq!(hop["method"], "explain_symbol");
        let one = rpc_call(&p, "explain_symbol", &hop["params"].to_string());
        assert!(one.get("overloaded").is_none(), "{one}");
        assert_eq!(
            one["symbol"]["qualname"],
            "Pkg.Manifest.ManifestParser.Parse"
        );
        assert!(one["symbol"]["id"].is_i64(), "{one}");
    }
}

/// Same-qualname Rust `cfg` twins are overload sets too (`read_symbol`
/// already reports them); `explain_symbol` deliberately agrees with it.
#[test]
fn explain_symbol_reports_rust_cfg_twins_like_read_symbol() {
    let src = "#[cfg(unix)]\npub fn plat() {}\n#[cfg(windows)]\npub fn plat() {}\n";
    let tmp = tempfile::Builder::new()
        .prefix("lidx-239-rs-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), &[("src/lib.rs", src)]);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.graph_version();
    let qn: String = Connection::open(&db_path)
        .unwrap()
        .query_row(
            "SELECT qualname FROM symbols WHERE graph_version = ? AND name = 'plat' LIMIT 1",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    let q = serde_json::json!({ "qualname": qn }).to_string();
    let call = |m: &str| -> serde_json::Value {
        let out = rpc::call(tmp.path().to_path_buf(), db_path.clone(), m.into(), &q, "1").unwrap();
        serde_json::from_str::<serde_json::Value>(&out).unwrap()["result"].clone()
    };
    assert_eq!(call("read_symbol")["overloaded"], true);
    assert_eq!(call("explain_symbol")["overloaded"], true);
}

/// The ambiguity arm must not capture a foreign receiver that merely
/// suffix-matches two in-repo symbols: `requests.get()` still binds to its
/// external stub.
#[test]
fn python_external_receiver_with_loose_in_repo_matches_still_binds_stub() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-239-py-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[
            (
                "vendor_a/shim.py",
                "class requests:\n    def get(self, url):\n        return url\n",
            ),
            (
                "vendor_b/shim.py",
                "class requests:\n    def get(self, url):\n        return url\n",
            ),
            (
                "app/caller.py",
                "import requests\n\n\ndef fetch():\n    return requests.get(\"u\")\n",
            ),
        ],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.graph_version();
    let conn = Connection::open(&db_path).unwrap();
    let bound: Vec<Option<String>> = conn
        .prepare(
            "SELECT t.qualname FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.name = 'fetch'",
        )
        .unwrap()
        .query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(bound, vec![Some("ext:requests.get".to_string())]);
}
