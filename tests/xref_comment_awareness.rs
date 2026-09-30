//! XREF literal scanning must not read comments, lifetimes or char literals
//! as string content. Each test indexes a real repo and inspects the edges.

use lidx::indexer::Indexer;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct XrefRow {
    file: String,
    source: String,
    target: String,
    start: i64,
    end: i64,
}

/// Comment lines in fixtures carry the tag `CMT`.
const COMMENT_TAG: &str = "CMT";

const PY_TARGETS: &str = "class SecretHandler:\n    pass\n\nclass CancellationRegistry:\n    pass\n\nclass DataProxy:\n    pass\n";

fn index(files: &[(&str, &str)]) -> (PathBuf, Vec<XrefRow>) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "lidx-xref-cmt-{nanos}-{}",
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
            "SELECT f.path, COALESCE(s.qualname, ''), COALESCE(e.target_qualname, ''),
                    COALESCE(e.evidence_start_line, 0), COALESCE(e.evidence_end_line, 0)
             FROM edges e JOIN files f ON e.file_id = f.id
             LEFT JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.kind = 'XREF'",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |r| {
            Ok(XrefRow {
                file: r.get(0)?,
                source: r.get(1)?,
                target: r.get(2)?,
                start: r.get(3)?,
                end: r.get(4)?,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    (root, rows)
}

fn targets_hit(rows: &[XrefRow], file: &str, name: &str) -> bool {
    rows.iter()
        .any(|r| r.file == file && r.target.ends_with(name))
}

/// Runs the shared assertions for one fixture: nothing in the comment-trap
/// method reaches `trap_target`, no evidence span covers a comment line, and
/// the real literal still produces its edge.
fn check(file: &str, body: &str, trap_targets: &[&str], real_target: &str) {
    let (root, rows) = index(&[(file, body), ("targets.py", PY_TARGETS)]);
    let lines: Vec<&str> = body.lines().collect();
    for row in rows.iter().filter(|r| r.file == file) {
        for n in row.start..=row.end {
            let text = lines.get(n as usize - 1).copied().unwrap_or("");
            assert!(
                !text.contains(COMMENT_TAG),
                "{file}: XREF {} -> {} evidence spans comment line {n}: {text:?}",
                row.source,
                row.target
            );
        }
    }
    for trap in trap_targets {
        assert!(
            !targets_hit(&rows, file, trap),
            "{file}: bogus XREF to {trap} from code after a comment; rows: {:?}",
            rows.iter()
                .map(|r| (&r.file, &r.source, &r.target, r.start, r.end))
                .collect::<Vec<_>>()
        );
    }
    assert!(
        targets_hit(&rows, file, real_target),
        "{file}: genuine string literal lost its XREF to {real_target}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn csharp_comment_apostrophe_yields_no_phantom_xref() {
    check(
        "Svc.cs",
        "public class Svc\n{\n    public void Run()\n    {\n        // CMT the proc's own SELECT projection.\n        var readResult = SecretHandler;\n        int x = 1; // CMT it's\n        /* CMT don't */\n        var msg = \"it's done\";\n    }\n\n    public void Real()\n    {\n        var s = @\"DataProxy \"\"quoted\"\"\";\n    }\n}\n",
        &["SecretHandler"],
        "DataProxy",
    );
}

#[test]
fn rust_lifetime_and_comment_yield_no_phantom_xref() {
    check(
        "lib.rs",
        "pub fn run<'a>(x: &'a str) -> &'static str {\n    // CMT it's a helper marker\n    let _marker = CancellationRegistry;\n    let c = 'x'; // CMT don't\n    let _n = x;\n    \"closing quote's here\"\n}\n\npub fn real() -> &'static str {\n    r#\"DataProxy \"raw\"\"#\n}\n",
        &["CancellationRegistry"],
        "DataProxy",
    );
}

#[test]
fn typescript_comment_yields_no_phantom_xref() {
    check(
        "svc.ts",
        "export function run(): string {\n    // CMT the proc's own thing\n    const r = SecretHandler;\n    const n = 1; // CMT it's\n    /* CMT don't */\n    const re = /'/;\n    return \"it's done\";\n}\n\nexport function real(): string {\n    return `DataProxy ${ `nested` }`;\n}\n",
        &["SecretHandler"],
        "DataProxy",
    );
}

#[test]
fn javascript_comment_yields_no_phantom_xref() {
    check(
        "svc.js",
        "function run() {\n    // CMT the proc's own thing\n    const r = SecretHandler;\n    return \"it's done\";\n}\n\nfunction real() {\n    return 'DataProxy';\n}\n",
        &["SecretHandler"],
        "DataProxy",
    );
}

#[test]
fn go_comment_yields_no_phantom_xref() {
    check(
        "svc.go",
        "package svc\n\nfunc Run() string {\n\t// CMT the proc's own thing\n\tr := SecretHandler\n\tq := '\\''\n\t_ = r\n\t_ = q\n\treturn \"it's done\"\n}\n\nfunc Real() string {\n\treturn `DataProxy`\n}\n",
        &["SecretHandler"],
        "DataProxy",
    );
}

#[test]
fn sql_comment_yields_no_phantom_xref() {
    check(
        "procs.sql",
        "CREATE PROCEDURE dbo.run_it AS\nBEGIN\n    -- CMT the proc's own SELECT projection.\n    SELECT SecretHandler FROM t; -- CMT it's\n    /* CMT don't */\n    SELECT 'it''s done';\nEND;\n\nCREATE PROCEDURE dbo.real_it AS\nBEGIN\n    SELECT 'DataProxy';\nEND;\n",
        &["SecretHandler"],
        "DataProxy",
    );
}

#[test]
fn python_traps_still_yield_no_bogus_xref() {
    let py = "class Svc:\n    def run(self):\n        \"\"\"Docstring mentioning SecretHandler.\"\"\"\n        # CMT the proc's own thing\n        r = CancellationRegistry\n        return \"it's done\"\n";
    let cs = "public class SecretHandler\n{\n}\n\npublic class CancellationRegistry\n{\n}\n\npublic class DataProxy\n{\n}\n";
    let (root, rows) = index(&[("svc.py", py), ("Targets.cs", cs)]);
    assert!(!targets_hit(&rows, "svc.py", "SecretHandler"));
    assert!(!targets_hit(&rows, "svc.py", "CancellationRegistry"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn python_real_literal_still_yields_xref() {
    let py = "class Svc:\n    def real(self):\n        return \"DataProxy\"\n";
    let cs = "public class DataProxy\n{\n}\n";
    let (root, rows) = index(&[("svc.py", py), ("Targets.cs", cs)]);
    assert!(targets_hit(&rows, "svc.py", "DataProxy"));
    let _ = std::fs::remove_dir_all(root);
}
