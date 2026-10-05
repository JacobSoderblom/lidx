//! Issue #223: T-SQL extraction produced degenerate procedure spans, missed
//! triggers/nested indexes, truncated functions, bogus `type` symbols from
//! parameters and overlapping table spans.

use lidx::indexer::Indexer;
use lidx::indexer::extract::{ExtractedFile, LanguageExtractor};
use lidx::indexer::sql_extractor::SqlExtractor;
use lidx::rpc;

mod common;

fn extract(source: &str) -> ExtractedFile {
    SqlExtractor::new().unwrap().extract(source, "m").unwrap()
}

fn sym<'a>(f: &'a ExtractedFile, qualname: &str) -> &'a lidx::indexer::extract::SymbolInput {
    f.symbols
        .iter()
        .find(|s| s.qualname == qualname)
        .unwrap_or_else(|| {
            panic!(
                "missing {qualname}: {:?}",
                f.symbols
                    .iter()
                    .map(|s| (&s.kind, &s.qualname, s.start_line, s.end_line))
                    .collect::<Vec<_>>()
            )
        })
}

fn text<'a>(source: &'a str, f: &ExtractedFile, qualname: &str) -> &'a str {
    let s = sym(f, qualname);
    &source[s.start_byte as usize..s.end_byte as usize]
}

/// Invariants every SQL fixture must satisfy.
fn assert_invariants(source: &str) {
    let f = extract(source);
    let syms: Vec<_> = f.symbols.iter().filter(|s| s.kind != "module").collect();
    for s in &syms {
        assert!(
            s.start_byte < s.end_byte,
            "zero-length {} {}",
            s.kind,
            s.qualname
        );
        let body = &source[s.start_byte as usize..s.end_byte as usize];
        assert!(
            body.trim_start().len() == body.len(),
            "{} starts on whitespace: {body:?}",
            s.qualname
        );
        assert!(
            !body.starts_with("--") && !body.starts_with("/*"),
            "{} starts on a comment: {body:?}",
            s.qualname
        );
    }
    for (i, a) in syms.iter().enumerate() {
        for b in &syms[i + 1..] {
            assert!(
                a.end_byte <= b.start_byte || b.end_byte <= a.start_byte,
                "{} ({}-{}) overlaps {} ({}-{})",
                a.qualname,
                a.start_byte,
                a.end_byte,
                b.qualname,
                b.start_byte,
                b.end_byte
            );
        }
    }
}

const PROC_WITH_BANNER: &str = "-- ======================\n-- audit writer\n-- ======================\n\nCREATE OR ALTER PROCEDURE dpb.write_to_audit\n    @Id int,\n    @Type char(1) = NULL\nAS\nBEGIN\n    SET NOCOUNT ON;\n    INSERT INTO dpb.audit (id) VALUES (@Id);\nEND\nGO\n";

const SUB4: &str = "CREATE FUNCTION dbo.f() RETURNS int AS\nBEGIN\n    DECLARE @x int = 1;\n    RETURN @x;\nEND\nGO\n\n-- tables\nCREATE TABLE datacatalog.a (\n    label NVARCHAR(100) NOT NULL DEFAULT 'x ( y'\n)\nGO\n\nCREATE INDEX ix_a ON datacatalog.a (label);\nGO\n\nCREATE TABLE datacatalog.b (\n    id INT -- trailing stray ) )\n)\n";

#[test]
fn procedure_after_banner_and_blank_line_spans_create_through_go() {
    let f = extract(PROC_WITH_BANNER);
    let s = sym(&f, "dpb.write_to_audit");
    assert_eq!(s.kind, "procedure");
    assert_eq!((s.start_line, s.end_line), (5, 13));
    let body = text(PROC_WITH_BANNER, &f, "dpb.write_to_audit");
    assert!(body.starts_with("CREATE OR ALTER PROCEDURE"), "{body:?}");
    assert!(body.ends_with("END\nGO"), "{body:?}");
    assert_invariants(PROC_WITH_BANNER);
}

#[test]
fn trigger_is_extracted_with_its_span() {
    let src = "CREATE TABLE dbo.t (id int);\nGO\n\nCREATE TRIGGER dbo.trg_t ON dbo.t AFTER INSERT AS\nBEGIN\n    SET NOCOUNT ON;\nEND\nGO\n";
    let f = extract(src);
    let s = sym(&f, "dbo.trg_t");
    assert_eq!(s.kind, "trigger");
    assert_eq!((s.start_line, s.end_line), (4, 8));
    assert_invariants(src);
}

#[test]
fn function_span_includes_closing_end_and_go() {
    let src = "CREATE FUNCTION dbo.f(@a int)\nRETURNS int\nAS\nBEGIN\n    DECLARE @x int = @a;\n    IF @x > 1\n    BEGIN\n        SET @x = 1;\n    END\n    RETURN @x;\nEND\nGO\n";
    let f = extract(src);
    let s = sym(&f, "dbo.f");
    assert_eq!(s.kind, "function");
    let body = text(src, &f, "dbo.f");
    assert!(body.ends_with("END\nGO"), "{body:?}");
    assert_eq!(s.start_line, 1);
    assert_invariants(src);
}

#[test]
fn index_nested_in_if_not_exists_begin_is_extracted() {
    let src = "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_a')\nBEGIN\n    CREATE NONCLUSTERED INDEX ix_a ON dbo.a (label);\nEND\nGO\n";
    let f = extract(src);
    let s = sym(&f, "dbo.ix_a");
    assert_eq!(s.kind, "index");
    assert_eq!(
        text(src, &f, "dbo.ix_a"),
        "CREATE NONCLUSTERED INDEX ix_a ON dbo.a (label)"
    );
    assert_invariants(src);
}

#[test]
fn parameter_named_type_yields_no_type_symbol_but_create_type_does() {
    let f = extract(PROC_WITH_BANNER);
    assert!(
        !f.symbols.iter().any(|s| s.kind == "type"),
        "{:?}",
        f.symbols
            .iter()
            .map(|s| (&s.kind, &s.qualname))
            .collect::<Vec<_>>()
    );
    let src = format!(
        "{PROC_WITH_BANNER}\nCREATE TYPE dbo.IdList AS TABLE (\n    id int NOT NULL\n);\nGO\n"
    );
    let f = extract(&src);
    let t = sym(&f, "dbo.IdList");
    assert_eq!(t.kind, "type");
    assert!(text(&src, &f, "dbo.IdList").ends_with(")"));
    assert_invariants(&src);
}

#[test]
fn stray_parens_in_literal_and_comment_do_not_swallow_neighbours() {
    let f = extract(SUB4);
    for q in [
        "dbo.f",
        "datacatalog.a",
        "datacatalog.b",
        "datacatalog.ix_a",
    ] {
        sym(&f, q);
    }
    let a = text(SUB4, &f, "datacatalog.a");
    assert!(a.starts_with("CREATE TABLE datacatalog.a"), "{a:?}");
    assert!(a.ends_with("'x ( y'\n)"), "{a:?}");
    let b = text(SUB4, &f, "datacatalog.b");
    assert!(b.starts_with("CREATE TABLE datacatalog.b"), "{b:?}");
    assert!(b.ends_with("stray ) )\n)"), "{b:?}");
    assert_eq!(sym(&f, "datacatalog.ix_a").kind, "index");
    assert!(text(SUB4, &f, "dbo.f").ends_with("END\nGO"));
    assert_invariants(SUB4);
}

#[test]
fn unbalanced_parens_in_comments_and_brackets_do_not_affect_table_span() {
    for (label, col) in [
        ("line comment", "id INT, -- oops (\n    n INT"),
        ("block comment", "id INT, /* oops ( */ n INT"),
        ("bracketed identifier", "[we(ird] INT, n INT"),
        ("bracketed close", "[we)ird] INT, n INT"),
    ] {
        let src = format!(
            "MERGE x AS t USING y AS s ON t.i = s.i WHEN MATCHED THEN UPDATE SET t.i = s.i;\nCREATE TABLE dbo.a (\n    {col}\n);\nCREATE TABLE dbo.b (\n    id INT\n);\n"
        );
        let f = extract(&src);
        let a = text(&src, &f, "dbo.a");
        assert!(a.ends_with("\n)"), "{label}: {a:?}");
        assert!(!a.contains("dbo.b"), "{label}: {a:?}");
        let b = text(&src, &f, "dbo.b");
        assert_eq!(b, "CREATE TABLE dbo.b (\n    id INT\n)", "{label}");
        assert_invariants(&src);
    }
}

#[test]
fn unparseable_statement_between_valid_ones_leaves_neighbours_intact() {
    let src = "CREATE TABLE dbo.a (\n    id INT\n);\nGO\n\nFROB ((( %% @@ ;;; ]]]\nGO\n\nCREATE TABLE dbo.b (\n    id INT\n);\nGO\n\nCREATE PROCEDURE dbo.p AS SELECT 1;\nGO\n";
    let f = extract(src);
    assert_eq!(
        text(src, &f, "dbo.a"),
        "CREATE TABLE dbo.a (\n    id INT\n)"
    );
    assert_eq!(
        text(src, &f, "dbo.b"),
        "CREATE TABLE dbo.b (\n    id INT\n)"
    );
    assert_eq!(
        text(src, &f, "dbo.p"),
        "CREATE PROCEDURE dbo.p AS SELECT 1;\nGO"
    );
    assert_invariants(src);
}

#[test]
fn invariants_hold_across_all_fixtures() {
    let combined = format!("{PROC_WITH_BANNER}\n{SUB4}");
    for src in [
        PROC_WITH_BANNER,
        SUB4,
        combined.as_str(),
        "CREATE TABLE users (id SERIAL PRIMARY KEY);\nCREATE VIEW v AS SELECT * FROM users;\nCREATE UNIQUE INDEX ux ON public.users (id)\n    WHERE id > 0;\nCREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;\nCREATE TRIGGER tr AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f();\n",
    ] {
        assert_invariants(src);
    }
}

#[test]
fn combined_tsql_file_extracts_every_construct() {
    let src = format!(
        "{PROC_WITH_BANNER}\nCREATE TYPE dbo.IdList AS TABLE (id int);\nGO\nCREATE VIEW dbo.v AS\nSELECT 1 AS one;\nGO\nCREATE TRIGGER dbo.trg ON dbo.a AFTER INSERT AS\nBEGIN\n  SELECT 1;\nEND\nGO\n{SUB4}"
    );
    let f = extract(&src);
    for (k, q) in [
        ("procedure", "dpb.write_to_audit"),
        ("type", "dbo.IdList"),
        ("view", "dbo.v"),
        ("trigger", "dbo.trg"),
        ("function", "dbo.f"),
        ("table", "datacatalog.a"),
        ("table", "datacatalog.b"),
        ("index", "datacatalog.ix_a"),
    ] {
        assert_eq!(sym(&f, q).kind, k, "{q}");
    }
    assert_invariants(&src);
}

#[test]
fn sql_psql_and_tsql_files_behave_identically_and_read_symbol_returns_source() {
    let dir = std::env::temp_dir().join(format!(
        "lidx-tsql-223-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    // Distinct qualname per file so lookups don't collide.
    for ext in ["sql", "psql", "tsql"] {
        let src = PROC_WITH_BANNER.replace("write_to_audit", &format!("audit_{ext}"));
        std::fs::write(dir.join(format!("schema.{ext}")), src).unwrap();
    }
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    for ext in ["sql", "psql", "tsql"] {
        let qn = format!("dpb.audit_{ext}");
        let raw = rpc::call(
            dir.clone(),
            db_path.clone(),
            "read_symbol".to_string(),
            &serde_json::json!({ "qualname": qn }).to_string(),
            "1",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(v.get("error").is_none_or(|e| e.is_null()), "{ext}: {raw}");
        let s = v["result"].to_string();
        assert!(s.contains("CREATE OR ALTER PROCEDURE"), "{ext}: {s}");
        assert!(s.contains("SET NOCOUNT ON"), "{ext}: {s}");
        assert!(s.contains("GO"), "{ext}: {s}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn postgres_index_modifiers_are_skipped_and_anonymous_index_has_no_symbol() {
    let src = "CREATE TABLE public.t (id int);\nCREATE UNIQUE INDEX CONCURRENTLY ux ON public.t USING btree (id) WHERE id > 0;\nCREATE INDEX IF NOT EXISTS ix2 ON ONLY public.t (id);\nCREATE INDEX ON public.t (id);\n";
    let f = extract(src);
    let idx: Vec<_> = f.symbols.iter().filter(|s| s.kind == "index").collect();
    let names: Vec<_> = idx.iter().map(|s| s.qualname.as_str()).collect();
    assert_eq!(idx.len(), 2, "{names:?}");
    assert!(
        names.contains(&"public.ux") && names.contains(&"public.ix2"),
        "{names:?}"
    );
    assert!(
        !f.symbols
            .iter()
            .any(|s| s.name == "ON" || s.name == "CONCURRENTLY")
    );
    assert_invariants(src);
}

#[test]
fn index_end_stops_at_semicolon_before_a_cte() {
    let src = "IF 1 = 1\nBEGIN\n    CREATE INDEX ix_a ON dbo.a (label);\n    WITH c AS (SELECT 1 AS x) SELECT x FROM c;\nEND\n";
    let f = extract(src);
    assert_eq!(
        text(src, &f, "dbo.ix_a"),
        "CREATE INDEX ix_a ON dbo.a (label)"
    );
    let src = "IF 1 = 1\nBEGIN\n    CREATE INDEX ix_b ON dbo.a (label) INCLUDE (n)\n        WHERE n > 0\n    SELECT 1;\nEND\n";
    let f = extract(src);
    assert_eq!(
        text(src, &f, "dbo.ix_b"),
        "CREATE INDEX ix_b ON dbo.a (label) INCLUDE (n)\n        WHERE n > 0"
    );
}

#[test]
fn begin_in_comment_or_string_does_not_mark_a_function_as_block_style() {
    let src = "CREATE FUNCTION dbo.f() RETURNS int AS 'select 1 -- begin' LANGUAGE sql;\n";
    let f = extract(src);
    assert!(text(src, &f, "dbo.f").starts_with("CREATE FUNCTION"));
    assert_invariants(src);
}

const EXEC_FIXTURE: &str = "\
CREATE OR ALTER PROCEDURE dpb.audit_write @msg NVARCHAR(50) AS
BEGIN
    SET NOCOUNT ON;
END
GO

CREATE OR ALTER PROCEDURE dpb.do_work AS
BEGIN
    -- EXEC dpb.in_comment
    /* EXEC dpb.in_block */
    DECLARE @s NVARCHAR(100) = 'EXEC dpb.in_string';
    IF 1 = 1
    BEGIN
        EXEC dpb.audit_write @msg = N'x';
    END
    EXEC [dpb].[audit_write] @msg = N'a';
    EXECUTE dpb.audit_write @msg = N'b';
    EXEC @rc = dpb.audit_write @msg = N'c';
    EXEC dpb.audit_write 'update',
        'dpb',
        'multi';
    EXEC sp_executesql N'select 1';
    EXEC sp_rename 'a', 'b';
    EXEC (@sql);
    EXEC ('CREATE SCHEMA x');
END
GO
EXEC dpb.audit_write @msg = N'top';
";

fn exec_edges(f: &ExtractedFile) -> Vec<(String, String, String)> {
    f.edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .map(|e| {
            (
                e.source_qualname.clone().unwrap(),
                e.target_qualname.clone().unwrap(),
                e.evidence_snippet.clone().unwrap(),
            )
        })
        .collect()
}

#[test]
fn exec_statements_yield_one_calls_edge_each_and_dynamic_forms_none() {
    let edges = exec_edges(&extract(EXEC_FIXTURE));
    let ev: Vec<&str> = edges.iter().map(|e| e.2.as_str()).collect();
    assert_eq!(
        ev,
        [
            "EXEC dpb.audit_write @msg = N'x';",
            "EXEC [dpb].[audit_write] @msg = N'a';",
            "EXECUTE dpb.audit_write @msg = N'b';",
            "EXEC @rc = dpb.audit_write @msg = N'c';",
            "EXEC dpb.audit_write 'update',",
        ],
        "{edges:?}"
    );
    for (src, tgt, _) in &edges {
        assert_eq!(
            (src.as_str(), tgt.as_str()),
            ("dpb.do_work", "dpb.audit_write")
        );
    }
}

fn exec_calls_in_db(root: &std::path::Path, db_path: &std::path::Path) -> Vec<(String, bool)> {
    let indexer = Indexer::new(root.to_path_buf(), db_path.to_path_buf()).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, COALESCE(t.qualname, e.target_qualname), e.target_symbol_id IS NOT NULL
             FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id WHERE e.graph_version = ? AND e.kind = 'CALLS' ORDER BY 1, 2, e.id",
        )
        .unwrap();
    stmt.query_map([gv], |r| {
        Ok((
            format!("{} -> {}", r.get::<_, String>(0)?, r.get::<_, String>(1)?),
            r.get::<_, bool>(2)?,
        ))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn exec_calls_resolve_to_the_callee_symbol() {
    let (_tmp, root, db_path) = common::index_repo("lidx-tsql-342-", &[("m.sql", EXEC_FIXTURE)]);
    let calls = exec_calls_in_db(&root, &db_path);
    assert_eq!(calls.len(), 5, "{calls:?}");
    assert!(
        calls
            .iter()
            .all(|(e, resolved)| e == "dpb.do_work -> dpb.audit_write" && *resolved),
        "{calls:?}"
    );
}

#[test]
fn exec_calls_appear_in_explain_symbol_callers() {
    let (_tmp, root, db_path) = common::index_repo("lidx-tsql-342-", &[("m.sql", EXEC_FIXTURE)]);
    let raw = rpc::call(
        root,
        db_path,
        "explain_symbol".to_string(),
        &serde_json::json!({ "qualname": "dpb.audit_write" }).to_string(),
        "1",
    )
    .unwrap();
    assert!(raw.contains("dpb.do_work"), "{raw}");
}

const CALLEE_SQL: &str =
    "CREATE PROCEDURE dpb.audit_write AS\nBEGIN\n    SET NOCOUNT ON;\nEND\nGO\n";
const CALLER_SQL: &str =
    "CREATE PROCEDURE dpb.do_work AS\nBEGIN\n    EXEC dpb.audit_write;\nEND\nGO\n";

#[test]
fn exec_calls_resolve_across_files_with_different_case() {
    let caller = "CREATE PROCEDURE dpb.do_work AS\nBEGIN\n    EXEC DPB.AUDIT_WRITE;\nEND\nGO\n";
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-tsql-342-",
        &[("callee.sql", CALLEE_SQL), ("caller.sql", caller)],
    );
    let calls = exec_calls_in_db(&root, &db_path);
    assert_eq!(
        calls,
        [("dpb.do_work -> dpb.audit_write".to_string(), true)],
        "{calls:?}"
    );
}

#[test]
fn exec_calls_across_files_survive_incremental_sync_of_the_caller() {
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-tsql-342-",
        &[("callee.sql", CALLEE_SQL), ("caller.sql", CALLER_SQL)],
    );
    let fresh = exec_calls_in_db(&root, &db_path);
    assert_eq!(
        fresh,
        [("dpb.do_work -> dpb.audit_write".to_string(), true)]
    );

    let edited = format!("{CALLER_SQL}-- edited\n");
    common::write_files(&root, &[("caller.sql", &edited)]);
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.sync_rel_paths(&["caller.sql".to_string()]).unwrap();
    assert_eq!(exec_calls_in_db(&root, &db_path), fresh);

    let (_tmp2, root2, db2) = common::index_repo(
        "lidx-tsql-342-",
        &[("callee.sql", CALLEE_SQL), ("caller.sql", &edited)],
    );
    assert_eq!(exec_calls_in_db(&root2, &db2), fresh);
}

#[test]
fn exec_mixed_case_keyword_and_names() {
    let src = "CREATE PROCEDURE Dpb.Do_Work AS\nBEGIN\n    ExEc [DPB].[Audit_Write];\n    execute dpb.AUDIT_write;\nEND\nGO\nCREATE PROCEDURE dpb.audit_write AS\nBEGIN\n    SET NOCOUNT ON;\nEND\nGO\n";
    let edges = exec_edges(&extract(src));
    assert_eq!(edges.len(), 2, "{edges:?}");
    for (s, t, _) in &edges {
        assert_eq!((s.as_str(), t.as_str()), ("Dpb.Do_Work", "dpb.audit_write"));
    }
}

#[test]
fn exec_skips_xp_sp_and_variable_forms() {
    let src = "CREATE PROCEDURE dbo.p AS\nBEGIN\n    EXEC xp_cmdshell 'dir';\n    EXEC master.dbo.xp_foo;\n    EXEC SP_who;\n    EXEC @proc;\n    EXEC @proc @a = 1;\n    EXEC dbo.real_one;\nEND\nGO\n";
    let edges = exec_edges(&extract(src));
    assert_eq!(edges.len(), 1, "{edges:?}");
    assert_eq!(edges[0].1, "dbo.real_one");
}

#[test]
fn exec_inside_function_and_trigger_bodies() {
    let src = "CREATE TRIGGER dbo.trg ON dbo.t AFTER INSERT AS\nBEGIN\n    EXEC dbo.on_insert;\nEND\nGO\nCREATE FUNCTION dbo.f() RETURNS INT AS\nBEGIN\n    EXEC dbo.helper;\n    RETURN 1;\nEND\nGO\n";
    let edges = exec_edges(&extract(src));
    let pairs: Vec<(&str, &str)> = edges.iter().map(|e| (e.0.as_str(), e.1.as_str())).collect();
    assert_eq!(
        pairs,
        [("dbo.trg", "dbo.on_insert"), ("dbo.f", "dbo.helper")],
        "{edges:?}"
    );
}

/// Issue #340: `#temp` / `##temp` tables are procedure-local scratch objects,
/// never schema `table` symbols.
#[test]
fn temp_tables_are_not_table_symbols() {
    let source = "CREATE OR ALTER PROCEDURE dpb.do_work\nAS\nBEGIN\n    SET NOCOUNT ON;\n    CREATE TABLE #scratch (id INT NOT NULL);\n    CREATE TABLE ##global_scratch (id INT NOT NULL);\n    INSERT INTO #scratch VALUES (1);\nEND;\nGO\nCREATE TABLE dpb.x (id INT NOT NULL);\nGO\n";
    let f = extract(source);
    let got: Vec<_> = f
        .symbols
        .iter()
        .filter(|s| s.kind != "module")
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();
    assert!(got.contains(&("procedure", "dpb.do_work")), "{got:?}");
    assert!(got.contains(&("table", "dpb.x")), "{got:?}");
    assert!(
        !got.iter().any(|(k, q)| *k == "table" && *q != "dpb.x"),
        "{got:?}"
    );
    assert!(
        !f.edges.iter().any(|e| e
            .target_qualname
            .as_deref()
            .is_some_and(|t| t.contains("scratch") && e.kind == "CONTAINS")),
        "{:?}",
        f.edges
    );
}
