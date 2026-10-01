use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::sql_extractor::{SqlExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("db/schema.sql"), "db/schema");
    assert_eq!(module_name_from_rel_path("init.psql"), "init");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
CREATE SCHEMA app;
CREATE TABLE app.users (id int);
CREATE VIEW app.user_view AS SELECT * FROM app.users;
CREATE FUNCTION app.add(a int, b int) RETURNS int AS $$ SELECT a + b; $$ LANGUAGE SQL;
CREATE TYPE app.status AS ENUM ('a', 'b');
"#;
    let module = module_name_from_rel_path("db/schema.sql");
    let mut extractor = SqlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "db/schema")));
    assert!(names.contains(&("schema", "app")));
    assert!(names.contains(&("table", "app.users")));
    assert!(names.contains(&("view", "app.user_view")));
    assert!(names.contains(&("function", "app.add")));
    assert!(names.contains(&("type", "app.status")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
}

#[test]
fn table_level_foreign_key_constraint_produces_references_edge() {
    let source = r#"
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
);
"#;
    let mut extractor = SqlExtractor::new().unwrap();
    let extracted = extractor.extract(source, "schema").unwrap();
    assert!(
        extracted.edges.iter().any(|e| {
            e.kind == "REFERENCES"
                && e.source_qualname.as_deref() == Some("orders")
                && e.target_qualname.as_deref() == Some("users")
        }),
        "Expected REFERENCES edge from orders to users for a table-level FK constraint"
    );
}

#[test]
fn empty_file_yields_module_symbol_only() {
    let mut extractor = SqlExtractor::new().unwrap();
    for source in ["", "   \n\n  "] {
        let extracted = extractor.extract(source, "empty").unwrap();
        assert_eq!(extracted.symbols.len(), 1, "source: {:?}", source);
        assert_eq!(extracted.symbols[0].kind, "module");
        assert!(extracted.edges.is_empty(), "source: {:?}", source);
    }
}

/// Plain DDL/DML without dollar-quoted bodies must not trigger the PL/pgSQL
/// scanning: no CALLS edges and no do_block symbols.
#[test]
fn plain_sql_without_dollar_quotes_emits_no_plpgsql_artifacts() {
    let source = r#"
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1);
SELECT count(*) FROM t WHERE id > 0;
UPDATE t SET id = 2 WHERE id = 1;
"#;
    let mut extractor = SqlExtractor::new().unwrap();
    let extracted = extractor.extract(source, "m").unwrap();
    assert!(!extracted.edges.iter().any(|e| e.kind == "CALLS"));
    assert!(!extracted.symbols.iter().any(|s| s.kind == "do_block"));
}

/// Regression test: the keyword scanners (DO blocks, REFERENCES, PERFORM)
/// search an uppercased copy of the text and slice the original with the
/// indices they find. Characters whose Unicode uppercase has a different
/// byte length (e.g. Turkish 'ı' -> 'I') used to misalign those offsets,
/// panicking on char boundaries or emitting garbage edges.
#[test]
fn non_ascii_text_does_not_misalign_keyword_scanning() {
    // 'ı' chars immediately before a "DO" substring used to panic.
    let source_do = "-- ıııDO note\nSELECT 1;\nDO $$ BEGIN PERFORM setup(); END $$;\n";
    let mut extractor = SqlExtractor::new().unwrap();
    let extracted = extractor.extract(source_do, "init").unwrap();
    assert_eq!(
        extracted
            .symbols
            .iter()
            .filter(|s| s.kind == "do_block")
            .count(),
        1
    );
    assert!(
        extracted
            .edges
            .iter()
            .any(|e| { e.kind == "CALLS" && e.target_qualname.as_deref() == Some("setup") }),
        "Expected CALLS edge to setup from the DO block"
    );

    // Non-ASCII column name and comment before REFERENCES used to produce a
    // REFERENCES edge pointing at a garbage fragment of the keyword itself.
    let source_fk = "CREATE TABLE orders (\n    açıklama TEXT, -- ııııııııııı\n    user_id INTEGER REFERENCES users(id)\n);\n";
    let extracted_fk = extractor.extract(source_fk, "schema").unwrap();
    let refs: Vec<_> = extracted_fk
        .edges
        .iter()
        .filter(|e| e.kind == "REFERENCES")
        .map(|e| e.target_qualname.as_deref())
        .collect();
    assert_eq!(
        refs,
        vec![Some("users")],
        "got REFERENCES targets: {refs:?}"
    );
}

/// End-to-end routing regression: indexing a repo containing a plain `.sql`
/// file must store REFERENCES and CALLS edges in the database. This verifies
/// the extractor registry maps language `sql` to the unified extractor (the
/// direct-extractor tests above would still pass if the wiring regressed).
#[test]
fn indexer_routes_plain_sql_files_through_unified_extractor() {
    let mut repo_root = std::env::temp_dir();
    repo_root.push(format!(
        "lidx-sql-routing-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&repo_root).unwrap();
    std::fs::write(
        repo_root.join("schema.sql"),
        r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id)
);

CREATE OR REPLACE FUNCTION process_order(order_id INTEGER)
RETURNS VOID AS $$
BEGIN
    PERFORM validate_order(order_id);
    INSERT INTO order_log SELECT * FROM get_order_details(order_id);
END;
$$ LANGUAGE plpgsql;
"#,
    )
    .unwrap();

    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = lidx::indexer::Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let edges = indexer
        .db()
        .list_edges(
            1000,
            0,
            None,
            None,
            Some(&["REFERENCES".to_string(), "CALLS".to_string()]),
            None,
            None,
            None,
            false,
            None,
            gv,
            None,
            None,
            None,
        )
        .unwrap();

    assert!(
        edges
            .iter()
            .any(|e| { e.kind == "REFERENCES" && e.target_qualname.as_deref() == Some("users") }),
        "Expected a REFERENCES edge to users from schema.sql, got: {:?}",
        edges
            .iter()
            .map(|e| (e.kind.as_str(), e.target_qualname.as_deref()))
            .collect::<Vec<_>>()
    );
    // `validate_order` is never defined in this file, so the CALLS reference
    // stays unresolved -- issue #79 means it has no edge at all (CALLS isn't
    // a Bridge Edge kind), only an unresolved_references store row. (The
    // PERFORM line matches both `scan_plpgsql_body`'s PERFORM-specific and
    // general-function-call branches, so it's tracked as more than one row
    // -- a pre-existing extractor quirk, not this test's concern.)
    let conn = indexer.db().read_conn().unwrap();
    let unresolved_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM unresolved_references
             WHERE edge_kind = 'CALLS' AND reference_name = 'validate_order'
               AND graph_version = ?",
            rusqlite::params![gv],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        unresolved_count >= 1,
        "Expected an unresolved CALLS reference to validate_order from schema.sql"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

fn tsql_symbols(source: &str) -> Vec<(String, String)> {
    let mut extractor = SqlExtractor::new().unwrap();
    let extracted = extractor.extract(source, "mssql/m").unwrap();
    extracted
        .symbols
        .iter()
        .map(|s| (s.kind.clone(), s.qualname.clone()))
        .collect()
}

#[test]
fn tsql_create_procedure_variants_are_extracted() {
    let source = "CREATE TABLE dpb.t1 (id int);\nGO\nCREATE OR ALTER PROCEDURE dpb.write_to_audit @a int, @b nvarchar(10)\nAS\nBEGIN\n    SET NOCOUNT ON;\n    INSERT INTO dpb.audit (id) VALUES (@a);\nEND\nGO\ncreate proc [dpb].[upsert_dataproduct] as select 1;\nGO\nCREATE PROCEDURE plain_proc AS SELECT 1;\n";
    let syms = tsql_symbols(source);
    for name in ["dpb.write_to_audit", "dpb.upsert_dataproduct", "plain_proc"] {
        assert!(
            syms.contains(&("procedure".to_string(), name.to_string())),
            "missing {name}: {syms:?}"
        );
    }
    assert!(syms.contains(&("table".to_string(), "dpb.t1".to_string())));
}

#[test]
fn tsql_tables_after_merge_and_inside_if_begin_are_extracted() {
    let source = "MERGE dpb.t1 AS t USING dpb.t2 AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.id = s.id;\nCREATE TABLE dpb.audit (id int);\nIF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'x')\nBEGIN\n    CREATE TABLE dpb.inside (id int);\nEND\nCREATE TABLE dpb.last (id int);\n";
    let syms = tsql_symbols(source);
    for name in ["dpb.audit", "dpb.inside", "dpb.last"] {
        assert!(
            syms.contains(&("table".to_string(), name.to_string())),
            "missing {name}: {syms:?}"
        );
        assert_eq!(syms.iter().filter(|(_, q)| q == name).count(), 1);
    }
}

#[test]
fn tsql_fallback_skips_noise_and_duplicates() {
    let source = "CREATE TABLE IF NOT EXISTS app.foo (id int);\n\
CREATE TABLE #tmp (id int);\n\
CREATE TABLE @tv (id int);\n\
-- CREATE TABLE app.commented (id int);\n\
/*\nCREATE TABLE app.blocked (id int);\n*/\n\
CREATE FUNCTION app.f() RETURNS void AS $$\nBEGIN\n  CREATE TABLE app.indollar (id int);\nEND;\n$$ LANGUAGE plpgsql;\n";
    let syms = tsql_symbols(source);
    let quals: Vec<&str> = syms.iter().map(|(_, q)| q.as_str()).collect();
    for bad in ["IF", "#tmp", "app.commented", "app.blocked", "app.indollar"] {
        assert!(!quals.contains(&bad), "unexpected {bad}: {quals:?}");
    }
    let foos = quals
        .iter()
        .filter(|q| q.eq_ignore_ascii_case("app.foo"))
        .count();
    assert_eq!(foos, 1, "{quals:?}");
}

#[test]
fn tsql_fallback_go_variants_tabs_and_temp_table_in_proc() {
    let source = "CREATE\tPROCEDURE dbo.a AS\nBEGIN\nCREATE TABLE #t (id int);\nSELECT 1;\nEND\nGO 2\nCREATE PROC dbo.b AS SELECT 1;\ngo;\nCREATE PROC dbo.c AS SELECT 1;\n";
    let mut extractor = SqlExtractor::new().unwrap();
    let out = extractor.extract(source, "m").unwrap();
    let a = out.symbols.iter().find(|s| s.qualname == "dbo.a").unwrap();
    assert_eq!((a.start_line, a.end_line), (1, 6));
    let b = out.symbols.iter().find(|s| s.qualname == "dbo.b").unwrap();
    assert_eq!((b.start_line, b.end_line), (7, 8));
    assert!(out.symbols.iter().any(|s| s.qualname == "dbo.c"));
    assert!(!out.symbols.iter().any(|s| s.qualname == "#t"));
}

#[test]
fn tsql_fallback_table_lines() {
    let source = "MERGE x AS t USING y AS s ON t.i = s.i WHEN MATCHED THEN UPDATE SET t.i = s.i;\nCREATE TABLE dpb.audit (\n  id int\n);\n";
    let mut extractor = SqlExtractor::new().unwrap();
    let out = extractor.extract(source, "m").unwrap();
    let t = out
        .symbols
        .iter()
        .find(|s| s.qualname == "dpb.audit")
        .unwrap();
    assert_eq!((t.start_line, t.end_line), (2, 4));
}

#[test]
fn tsql_fallback_multiline_create_and_spaced_names() {
    let source = "CREATE OR ALTER\n-- note\n\nPROCEDURE\n    [dbo].[my proc]\n    @a int\nAS\nSELECT 1;\nGO\nCREATE\nTABLE\n[dbo].[my table] (\n  id int\n);\nCREATE\nPROC dbo.next AS SELECT 1;\n";
    let mut extractor = SqlExtractor::new().unwrap();
    let out = extractor.extract(source, "m").unwrap();
    let p = out
        .symbols
        .iter()
        .find(|s| s.qualname == "dbo.my proc")
        .expect("proc");
    assert_eq!(p.kind, "procedure");
    assert_eq!(p.name, "my proc");
    assert_eq!((p.start_line, p.end_line), (1, 9));
    let t = out
        .symbols
        .iter()
        .find(|s| s.qualname == "dbo.my table")
        .expect("table");
    assert_eq!((t.start_line, t.end_line), (10, 14));
    assert!(out.symbols.iter().any(|s| s.qualname == "dbo.next"));
}
