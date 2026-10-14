use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Indexes the `dead_symbols_rust` fixture and returns the dead claims as
/// (qualname, confidence).
fn rust_dead_claims() -> Vec<(String, String)> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("dead_symbols_rust");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let repo_root = std::env::temp_dir().join(format!("lidx-deadrs-{nanos}-{counter}"));
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false, "include_public_api": true}),
    )
    .unwrap();
    // These tests judge the liveness rules, not the public-API policy: both groups.
    let claims = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .chain(result["public_api"].as_array().unwrap())
        .filter_map(|s| {
            Some((
                s["qualname"].as_str()?.to_string(),
                s["confidence"].as_str().unwrap_or("").to_string(),
            ))
        })
        .collect();
    let _ = std::fs::remove_dir_all(&repo_root);
    claims
}

/// The dead qualnames of the fixture.
fn rust_dead_qualnames() -> Vec<String> {
    rust_dead_claims().into_iter().map(|(q, _)| q).collect()
}

fn claimed(names: &[String], suffix: &str) -> bool {
    names.iter().any(|q| q.ends_with(suffix))
}

#[test]
fn still_reports_truly_dead() {
    let names = rust_dead_qualnames();
    assert!(
        names.iter().any(|q| q.ends_with("truly_dead_function")),
        "{names:?}"
    );
    assert!(
        names.iter().any(|q| q.ends_with("TrulyDeadStruct")),
        "{names:?}"
    );
}

#[test]
fn excludes_test_functions() {
    let names = rust_dead_qualnames();
    assert!(
        !names.iter().any(|q| q.contains("test_default_config")),
        "{names:?}"
    );
    assert!(
        !names.iter().any(|q| q.contains("test_async_thing")),
        "{names:?}"
    );
}

#[test]
fn excludes_trait_impl_methods() {
    let names = rust_dead_qualnames();
    assert!(
        !names
            .iter()
            .any(|q| q.ends_with("PoolCustomizer::on_acquire")),
        "{names:?}"
    );
}

#[test]
fn still_reports_inherent_methods_of_trait_implementors() {
    let names = rust_dead_qualnames();
    assert!(
        names.iter().any(|q| q.ends_with("inherent_dead_method")),
        "{names:?}"
    );
}

#[test]
fn excludes_external_trait_impl_methods() {
    let names = rust_dead_qualnames();
    assert!(
        !names.iter().any(|q| q.ends_with("PoolCustomizer::fmt")),
        "{names:?}"
    );
}

#[test]
fn counts_type_references_as_uses() {
    let names = rust_dead_qualnames();
    assert!(!names.iter().any(|q| q.ends_with("CrossRef")), "{names:?}");
    assert!(
        !names.iter().any(|q| q.ends_with("FileContext")),
        "{names:?}"
    );
}

#[test]
fn counts_fn_as_value_as_use() {
    let names = rust_dead_qualnames();
    assert!(!names.iter().any(|q| q.ends_with("from_env")), "{names:?}");
}

/// Issue #497, bucket A: the call binds one `#[cfg]` twin; the other is live
/// with it. Twins nothing calls are both claimed.
#[test]
fn cfg_twins_are_live_together() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::platform_name"), "{names:?}");
    assert_eq!(
        names.iter().filter(|q| q.ends_with("::dead_twin")).count(),
        2,
        "{names:?}"
    );
}

/// Bucket A2: items declared in `cfg_if!` branches and calls made in an
/// expression-level `cfg_if!` are read.
#[test]
fn cfg_if_branches_are_read() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::cfg_if_helper"), "{names:?}");
    assert!(!claimed(&names, "::in_body_helper"), "{names:?}");
}

/// Bucket B: a call and a function value in a `macro_rules!` transcriber.
#[test]
fn macro_rules_bodies_are_read() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::macro_called_from_body"), "{names:?}");
    assert!(!claimed(&names, "::sort_key"), "{names:?}");
}

/// Bucket C: functions named in attribute arguments (serde string path, clap
/// identifier and path values, thiserror call, test_case call).
#[test]
fn attribute_arguments_are_read() {
    let names = rust_dead_qualnames();
    for name in [
        "::default_name",
        "::parse_size",
        "::describe",
        "::mk_input",
        "Completer::complete_thing",
    ] {
        assert!(!claimed(&names, name), "{name} in {names:?}");
    }
}

/// Bucket D: a function-local unit struct passed as a value.
#[test]
fn local_unit_struct_as_value_is_a_use() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::V"), "{names:?}");
}

/// Bucket F: calls in `const` and `static` initialisers, inside a closure
/// too, with the closure's `let` typed.
#[test]
fn initializer_calls_are_read() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::alias"), "{names:?}");
    assert!(!claimed(&names, "Inner::new"), "{names:?}");
    assert!(!claimed(&names, "Inner::to_long"), "{names:?}");
}

/// Bucket G: a `use .. as T;` inside a fn body binds `T::f()`.
#[test]
fn body_use_alias_binds_calls() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "Thing::make"), "{names:?}");
}

/// Bucket H: a fn item nested in a body is reachable only through that body,
/// so it is not a claim (its parent is judged instead).
#[test]
fn nested_fns_are_not_claimed() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::inner"), "{names:?}");
    assert!(!claimed(&names, "::never_used"), "{names:?}");
}

/// A Rust claim is `high` only when nothing else in the Rust sources spells
/// its name; a name spelled in a string (an attribute, a message) is `medium`.
#[test]
fn high_means_spelled_nowhere_else() {
    let claims = rust_dead_claims();
    let tier = |suffix: &str| {
        claims
            .iter()
            .find(|(q, _)| q.ends_with(suffix))
            .map(|(_, c)| c.as_str())
            .unwrap_or_else(|| panic!("{suffix} not claimed: {claims:?}"))
            .to_string()
    };
    assert_eq!(tier("::spelled_nowhere"), "high");
    assert_eq!(tier("::spelled_in_string"), "medium");
    // `pub` in a library crate: public API, the opt-in group.
    assert_eq!(tier("::truly_dead_function"), "low");
}

/// A fn named like a primitive (`fn bool`) passed as a value is used.
#[test]
fn primitive_named_fn_as_value_is_a_use() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::bool"), "{names:?}");
}

/// A `pub` method of a `pub` type, implemented in a module other than the one
/// declaring the type, is public API of the library crate (its qualname hangs
/// under the implementing module, where the type is not declared).
#[test]
fn pub_method_of_a_pub_type_implemented_elsewhere_is_public_api() {
    let claims = rust_dead_claims();
    let (_, tier) = claims
        .iter()
        .find(|(q, _)| q.ends_with("::never_called_pub_method"))
        .unwrap_or_else(|| panic!("not claimed: {claims:?}"));
    assert_eq!(tier, "low", "{claims:?}");
}

/// A default method of a trait the crate implements for a std type is called
/// through values of that type, which the index cannot type: not a claim.
#[test]
fn extension_trait_methods_are_not_claimed() {
    let names = rust_dead_qualnames();
    assert!(!claimed(&names, "::label_never_called_here"), "{names:?}");
}
