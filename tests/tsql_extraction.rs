//! Issue #223: T-SQL extraction produced degenerate procedure spans, missed
//! triggers/nested indexes, truncated functions, bogus `type` symbols from
//! parameters and overlapping table spans.

use lidx::indexer::Indexer;
use lidx::indexer::extract::{ExtractedFile, LanguageExtractor};
use lidx::indexer::sql_extractor::SqlExtractor;
use lidx::rpc;

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
        "CREATE NONCLUSTERED INDEX ix_a ON dbo.a (label);"
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
