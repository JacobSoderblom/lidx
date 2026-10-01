//! Issue #237: XREF must not read interpolation holes or prose words as
//! cross-language references, and must not target symbols nothing outside
//! their file can reference (non-exported, or in a test file).

use lidx::indexer::Indexer;
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn xref_edges(files: &[(&str, &str)]) -> BTreeSet<(String, String)> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "lidx-xref-str-{nanos}-{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    for (path, body) in files {
        let full = root.join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, body).unwrap();
    }
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    let conn = Connection::open(&db_path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(s.qualname, ''), COALESCE(e.target_qualname, '')
             FROM edges e LEFT JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.kind = 'XREF'",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<BTreeSet<_>, _>>()
        .unwrap();
    let _ = std::fs::remove_dir_all(root);
    rows
}

const CSHARP: &str = r#"public class Svc
{
    public void Missing(string uniqueName)
    {
        throw new Exception($"DataProduct '{uniqueName}' not found.");
    }

    public void Literal(string DataProxy)
    {
        var s = $"call dbo.get_user for {DataProxy}";
    }

    public void Prose()
    {
        var a = "Datasource already exists, starting FullLoad now.";
    }

    public void Plain()
    {
        var a = "uniqueName";
        var b = "hiddenHelper";
    }

    public void Exposed()
    {
        var a = "PublicHelper";
    }

    public void Real()
    {
        var sql = "select * from dbo.get_user(@id)";
    }
}
"#;

const TS_TEST: &str = "const uniqueName = 'source/patient register';\n";
const TS_LIB: &str =
    "const hiddenHelper = 1;\nexport const PublicHelper = 2;\nexport function use() {}\n";
const TS_TPL: &str =
    "export function tpl() {\n  return `loading ${DataProxy} via dbo.get_user`;\n}\n";
const PY_TPL: &str = "def fmt():\n    return f\"{PublicHelper} via dbo.get_user\"\n";
const PY_TARGETS: &str =
    "class DataProxy:\n    pass\n\nclass Datasource:\n    pass\n\nclass FullLoad:\n    pass\n";
const SQL: &str = "CREATE TABLE dbo.get_user (id INT);\n";

fn edge(source: &str, target: &str) -> (String, String) {
    (source.to_string(), target.to_string())
}

#[test]
fn string_content_xrefs_are_only_real_cross_language_links() {
    let edges = xref_edges(&[
        ("Svc.cs", CSHARP),
        ("node/catalog.test.tsx", TS_TEST),
        ("node/lib.ts", TS_LIB),
        ("node/tpl.ts", TS_TPL),
        ("py/models.py", PY_TARGETS),
        ("py/fmt.py", PY_TPL),
        ("schema.sql", SQL),
    ]);
    let expected: BTreeSet<_> = [
        // Literal portion of an interpolated string still links.
        edge("Svc.Svc.Literal", "dbo.get_user"),
        // Dotted name inside a plain string.
        edge("Svc.Svc.Real", "dbo.get_user"),
        // Exported, non-test TS symbol named by a one-word string.
        edge("Svc.Svc.Exposed", "node/lib.PublicHelper"),
        // Literal parts of a JS template and a Python f-string.
        edge("node/tpl.tpl", "dbo.get_user"),
        edge("py.fmt.fmt", "dbo.get_user"),
    ]
    .into_iter()
    .collect();
    assert_eq!(edges, expected, "actual XREF edge set");
}
