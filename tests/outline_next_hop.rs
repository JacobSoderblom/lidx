//! Issue #236: an `outline` next_hop must point at something in the outlined
//! file and be followable without fuzzy substitution.

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    dir: PathBuf,
    _indexer: Indexer,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn setup(files: &[(&str, &str)]) -> Fixture {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-outline-hop-{nanos}-{n}"));
    for (path, content) in files {
        let full = dir.join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, content).unwrap();
    }
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    Fixture {
        dir,
        _indexer: indexer,
    }
}

fn call(fx: &Fixture, method: &str, params: Value) -> Value {
    let raw = rpc::call(
        fx.dir.clone(),
        fx.dir.join(".lidx").join(".lidx.sqlite"),
        method.to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method} {params}: {envelope}"
    );
    envelope["result"].clone()
}

const A_CS: &str =
    "namespace Acme.Foo\n{\n    public class A\n    {\n        public void Run() { }\n    }\n}\n";
const B_CS: &str =
    "namespace Acme.Foo\n{\n    public class B\n    {\n        public void Go() { }\n    }\n}\n";
const ONLY_NS_CS: &str = "namespace Acme.Empty\n{\n}\n";
const DOC_MD: &str =
    "# Service Bus Messaging\n\nSome intro text.\n\n## Topics\n\nTopic details here.\n";
const PLAIN_CS: &str = "namespace Acme.Plain\n{\n    public class Widget\n    {\n        public void Spin() { }\n    }\n}\n";

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Foo/A.cs", A_CS),
        ("Foo/B.cs", B_CS),
        ("Foo/Empty.cs", ONLY_NS_CS),
        ("Plain.cs", PLAIN_CS),
        ("docs/messaging.md", DOC_MD),
        // A symbol whose name matches the Markdown heading, elsewhere.
        (
            "Other.cs",
            "namespace Acme.Other\n{\n    public class Svc\n    {\n        public void ServiceBusMessaging() { }\n    }\n}\n",
        ),
    ]
}

/// Follows every hop and returns every file path the response mentions
/// (`path` fields at any depth).
fn follow(fx: &Fixture, hop: &Value) -> Vec<String> {
    let method = hop["method"].as_str().unwrap();
    let result = call(fx, method, hop["params"].clone());
    let mut paths = Vec::new();
    collect_paths(&result, &mut paths);
    paths
}

fn collect_paths(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, val) in m {
                if (k == "path" || k == "file_path")
                    && let Some(s) = val.as_str()
                {
                    out.push(s.to_string());
                }
                collect_paths(val, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_paths(x, out)),
        _ => {}
    }
}

fn outline_hops(fx: &Fixture, path: &str) -> Vec<Value> {
    let result = call(fx, "outline", json!({"path": path}));
    result["next_hops"].as_array().cloned().unwrap_or_default()
}

#[test]
fn markdown_outline_emits_no_read_symbol_hop() {
    let fx = setup(&files());
    let hops = outline_hops(&fx, "docs/messaging.md");
    assert!(
        hops.iter().all(|h| h["method"] != "read_symbol"),
        "{hops:?}"
    );
    for hop in &hops {
        let paths = follow(&fx, hop);
        assert!(!paths.is_empty(), "{hop}");
        assert!(paths.iter().all(|p| p == "docs/messaging.md"), "{paths:?}");
    }
}

#[test]
fn csharp_namespace_first_entry_hops_into_same_file() {
    let fx = setup(&files());
    let hops = outline_hops(&fx, "Foo/B.cs");
    assert!(!hops.is_empty());
    assert_eq!(hops[0]["method"], "read_symbol", "{hops:?}");
    assert_eq!(hops[0]["params"]["qualname"], "Acme.Foo.B", "{hops:?}");
}

#[test]
fn every_outline_hop_resolves_in_the_outlined_file() {
    let fx = setup(&files());
    for path in [
        "Foo/A.cs",
        "Foo/B.cs",
        "Foo/Empty.cs",
        "Plain.cs",
        "docs/messaging.md",
    ] {
        for hop in outline_hops(&fx, path) {
            let paths = follow(&fx, &hop);
            assert!(!paths.is_empty(), "{path}: {hop}");
            assert!(paths.iter().all(|p| p == path), "{path}: {hop}: {paths:?}");
        }
    }
}

#[test]
fn namespace_only_file_emits_no_read_symbol_hop() {
    let fx = setup(&files());
    let hops = outline_hops(&fx, "Foo/Empty.cs");
    assert!(
        hops.iter().all(|h| h["method"] != "read_symbol"),
        "{hops:?}"
    );
}

#[test]
fn class_first_entry_still_emits_read_symbol_hop() {
    let fx = setup(&files());
    let hops = outline_hops(&fx, "Plain.cs");
    assert_eq!(hops.len(), 1, "{hops:?}");
    assert_eq!(hops[0]["method"], "read_symbol");
    assert_eq!(hops[0]["params"]["qualname"], "Acme.Plain.Widget");
}
