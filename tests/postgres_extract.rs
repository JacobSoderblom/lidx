use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::postgres::PostgresExtractor;

#[test]
fn test_basic_postgres_extraction() {
    let source = r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL
);

CREATE FUNCTION get_user_count() RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM users);
END;
$$ LANGUAGE plpgsql;
"#;

    let mut extractor = PostgresExtractor::new().unwrap();
    let result = extractor.extract(source, "test_module").unwrap();

    // Check for module symbol
    let module = result.symbols.iter().find(|s| s.kind == "module");
    assert!(module.is_some());

    // Check for table
    let table = result.symbols.iter().find(|s| s.kind == "table");
    assert!(table.is_some());
    assert_eq!(table.unwrap().name, "users");

    // Check for function
    let function = result.symbols.iter().find(|s| s.kind == "function");
    assert!(function.is_some());
    assert_eq!(function.unwrap().name, "get_user_count");

    // Check for CONTAINS edges
    let contains_edges: Vec<_> = result.edges.iter().filter(|e| e.kind == "CONTAINS").collect();
    assert!(contains_edges.len() >= 2); // module contains table and function
}

#[test]
fn test_plpgsql_function_calls() {
    let source = r#"
CREATE FUNCTION validate_email(email TEXT) RETURNS BOOLEAN AS $$
BEGIN
    RETURN email LIKE '%@%';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION register_user(email TEXT) RETURNS VOID AS $$
BEGIN
    IF NOT validate_email(email) THEN
        RAISE EXCEPTION 'Invalid email';
    END IF;
    INSERT INTO users (email) VALUES (email);
END;
$$ LANGUAGE plpgsql;
"#;

    let mut extractor = PostgresExtractor::new().unwrap();
    let result = extractor.extract(source, "test_module").unwrap();

    // Check for CALLS edge from register_user to validate_email
    let calls: Vec<_> = result.edges.iter().filter(|e| e.kind == "CALLS").collect();
    let found = calls.iter().any(|e| {
        e.source_qualname.as_deref() == Some("register_user")
            && e.target_qualname.as_deref() == Some("validate_email")
    });
    assert!(found, "Expected CALLS edge from register_user to validate_email");
}

#[test]
fn test_foreign_key_references() {
    let source = r#"
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER REFERENCES departments(id)
);
"#;

    let mut extractor = PostgresExtractor::new().unwrap();
    let result = extractor.extract(source, "test_module").unwrap();

    // Check for REFERENCES edge from employees to departments
    let refs: Vec<_> = result.edges.iter().filter(|e| e.kind == "REFERENCES").collect();
    let found = refs.iter().any(|e| {
        e.source_qualname.as_deref() == Some("employees")
            && e.target_qualname.as_deref() == Some("departments")
    });
    assert!(found, "Expected REFERENCES edge from employees to departments");
}

#[test]
fn test_trigger_function_reference() {
    let source = r#"
CREATE FUNCTION log_changes() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log (operation) VALUES (TG_OP);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION log_changes();
"#;

    let mut extractor = PostgresExtractor::new().unwrap();
    let result = extractor.extract(source, "test_module").unwrap();

    // Check for trigger symbol
    let trigger = result.symbols.iter().find(|s| s.kind == "trigger");
    assert!(trigger.is_some());

    // Check for CALLS edge from trigger to function
    let calls: Vec<_> = result.edges.iter().filter(|e| e.kind == "CALLS").collect();
    let found = calls.iter().any(|e| {
        e.source_qualname.as_deref() == Some("audit_trigger")
            && e.target_qualname.as_deref() == Some("log_changes")
    });
    assert!(found, "Expected CALLS edge from audit_trigger to log_changes");
}

#[test]
fn test_do_block_extraction() {
    let source = r#"
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM users;
    PERFORM cleanup_old_records();
END
$$;
"#;

    let mut extractor = PostgresExtractor::new().unwrap();
    let result = extractor.extract(source, "test_module").unwrap();

    // Check for DO block symbol
    let do_block = result.symbols.iter().find(|s| s.kind == "do_block");
    assert!(do_block.is_some());

    // Check for function call from DO block
    let calls: Vec<_> = result.edges.iter().filter(|e| e.kind == "CALLS").collect();
    let found = calls.iter().any(|e| {
        e.source_qualname
            .as_ref()
            .map(|q| q.contains("do_block"))
            .unwrap_or(false)
            && e.target_qualname.as_deref() == Some("cleanup_old_records")
    });
    assert!(found, "Expected CALLS edge from DO block to cleanup_old_records");
}

#[test]
fn test_module_naming() {
    let extractor = PostgresExtractor::new().unwrap();

    assert_eq!(
        extractor.module_name_from_rel_path("migrations/001_init.sql"),
        "migrations/001_init"
    );
    assert_eq!(
        extractor.module_name_from_rel_path("db/schema.sql"),
        "db/schema"
    );
    assert_eq!(
        extractor.module_name_from_rel_path("simple.sql"),
        "simple"
    );
}

#[test]
fn test_complex_plpgsql() {
    let source = r#"
CREATE FUNCTION process_order(order_id INTEGER) RETURNS VOID AS $$
DECLARE
    v_user_id INTEGER;
    v_total DECIMAL;
BEGIN
    -- Get user
    SELECT user_id INTO v_user_id FROM orders WHERE id = order_id;

    -- Validate
    PERFORM validate_user(v_user_id);
    PERFORM check_inventory(order_id);

    -- Calculate total
    v_total := calculate_order_total(order_id);

    -- Update
    PERFORM update_user_balance(v_user_id, v_total);
    PERFORM send_confirmation_email(v_user_id);
END;
$$ LANGUAGE plpgsql;
"#;

    let mut extractor = PostgresExtractor::new().unwrap();
    let result = extractor.extract(source, "orders").unwrap();

    let calls: Vec<_> = result.edges.iter().filter(|e| e.kind == "CALLS").collect();

    // Check all function calls are detected
    let expected_calls = vec![
        "validate_user",
        "check_inventory",
        "calculate_order_total",
        "update_user_balance",
        "send_confirmation_email",
    ];

    for expected in expected_calls {
        let found = calls.iter().any(|e| {
            e.source_qualname.as_deref() == Some("process_order")
                && e.target_qualname.as_deref() == Some(expected)
        });
        assert!(found, "Expected CALLS edge to {}", expected);
    }
}
