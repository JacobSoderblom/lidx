use lidx::indexer::sql::{SqlExtractor, module_name_from_rel_path};

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
