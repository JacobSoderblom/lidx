//! Issue #225: Python env-var reads must derive `env://` URIs from string
//! values (literals, same-file constants), never from expression text.

mod common;

use lidx::indexer::Indexer;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::python::PythonExtractor;

fn env_targets(source: &str) -> Vec<String> {
    let mut extractor = PythonExtractor::new().unwrap();
    let out = extractor.extract(source, "m").unwrap();
    out.edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .filter_map(|e| e.target_qualname.clone())
        .collect()
}

#[test]
fn literal_still_resolves() {
    let src =
        "import os\ndef f():\n    os.environ.get(\"X\")\n    os.getenv('Y')\n    os.environ['Z']\n";
    assert_eq!(env_targets(src), ["env://X", "env://Y", "env://Z"]);
}

#[test]
fn module_constant_resolves_to_value() {
    let src = r#"
import os
SQL_CONNECTION_STRING_ENV = "DPB_ORCHESTRATOR_SQL_CONNECTION_STRING"
def f():
    return os.environ.get(SQL_CONNECTION_STRING_ENV)
def g():
    return os.getenv(SQL_CONNECTION_STRING_ENV)
def h():
    return os.environ[SQL_CONNECTION_STRING_ENV]
"#;
    assert_eq!(
        env_targets(src),
        ["env://DPB_ORCHESTRATOR_SQL_CONNECTION_STRING"; 3]
    );
}

#[test]
fn class_constant_resolves_to_value() {
    let src = r#"
import os
class Cfg:
    KEY = "CLASS_LEVEL_VAR"
    in_body = os.environ.get(KEY)
    def m(self):
        a = os.environ.get(self.KEY)
        b = os.getenv(Cfg.KEY)
        return os.environ[self.KEY]
"#;
    assert_eq!(env_targets(src), ["env://CLASS_LEVEL_VAR"; 4]);
}

#[test]
fn parameter_emits_nothing() {
    let src = "import os\ndef f(name):\n    return os.environ.get(name)\n";
    assert!(env_targets(src).is_empty());
    let src = "import os\ndef f(name):\n    return os.environ[name]\n";
    assert!(env_targets(src).is_empty());
}

#[test]
fn call_emits_nothing() {
    let src = "import os\ndef f():\n    return os.getenv(_key(\"topic\"))\n";
    assert!(env_targets(src).is_empty());
}

#[test]
fn fstring_holes_emit_nothing_but_plain_fstring_resolves() {
    let src =
        "import os\ndef f(t):\n    os.getenv(f\"PREFIX_{t}\")\n    os.getenv(f\"PLAIN_NAME\")\n";
    assert_eq!(env_targets(src), ["env://PLAIN_NAME"]);
}

#[test]
fn reassigned_or_non_string_constant_emits_nothing() {
    let src = "import os\nK = 'A'\nK = 'B'\nN = 5\ndef f():\n    os.getenv(K)\n    os.getenv(N)\n    os.getenv(UNKNOWN)\n";
    assert!(env_targets(src).is_empty());
}

#[test]
fn no_env_uri_contains_expression_artefacts() {
    let src = r#"
import os
def f(p, q):
    os.getenv("REAL_ONE")
    os.getenv(p)
    os.getenv(g("a"))
    os.getenv("a" + p)
    os.getenv("a" "b")
    os.environ.get(q.attr)
    os.environ[p["k"]]
    os.getenv(f"{p}")
    os.getenv(p, "default")
"#;
    let uris = env_targets(src);
    assert_eq!(uris, ["env://REAL_ONE"], "only the literal read may emit");
    for uri in uris {
        let name = uri.strip_prefix("env://").unwrap();
        assert!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-'),
            "artefact in {uri}"
        );
    }
}

#[test]
fn constant_env_read_bridges_to_manifest() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-env-bridge-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[
            (
                "app.py",
                "import os\nSQL_ENV = \"DPB_ORCHESTRATOR_SQL_CONNECTION_STRING\"\ndef connect():\n    return os.environ.get(SQL_ENV)\n",
            ),
            ("k8s/deploy.yaml", YAML),
        ],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();
    let uri = "env://DPB_ORCHESTRATOR_SQL_CONNECTION_STRING";
    let readers = db
        .source_symbols_for_config_uri(uri, &["CONFIG_READ"], gv)
        .unwrap();
    let sources = db
        .source_symbols_for_config_uri(uri, &["CONFIG_SOURCE"], gv)
        .unwrap();
    assert_eq!(readers.len(), 1, "python reader must target the real name");
    assert_eq!(sources.len(), 1, "manifest must set the same name");
    assert!(
        db.source_symbols_for_config_uri("env://SQL_ENV", &[], gv)
            .unwrap()
            .is_empty(),
        "no phantom identifier URI"
    );
}

const YAML: &str = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: orch
spec:
  template:
    spec:
      containers:
        - name: orch
          image: orch:latest
          env:
            - name: DPB_ORCHESTRATOR_SQL_CONNECTION_STRING
              value: "x"
"#;

#[test]
fn parameter_shadowing_a_constant_emits_nothing() {
    let src = "import os\nK = 'REAL'\ndef f(K):\n    return os.getenv(K)\n";
    assert!(env_targets(src).is_empty());
}
