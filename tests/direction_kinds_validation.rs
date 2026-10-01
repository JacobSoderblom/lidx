//! Issue #246: `direction` and `kinds` on `trace_flow` / `analyze_impact`
//! are validated up front. An unknown value is an error naming it and the
//! valid values; matching is case-insensitive for both params (and the
//! `up`/`callers`, `down`/`callees` aliases are accepted), so a value is
//! never accepted and then quietly interpreted as something else.

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

mod common;

const FILES: &[(&str, &str)] = &[(
    "src/app.py",
    "def helper():\n    return 1\n\n\
     def middle():\n    helper()\n\n\
     def top():\n    middle()\n",
)];

fn build() -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-dir-kinds-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), FILES);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn call(ix: &mut Indexer, method: &str, params: Value) -> anyhow::Result<Value> {
    rpc::handle_method(ix, method, params)
}

fn err_of(ix: &mut Indexer, method: &str, params: Value) -> String {
    call(ix, method, params.clone())
        .err()
        .unwrap_or_else(|| panic!("{method} {params} should be an error"))
        .to_string()
}

fn params(method: &str, extra: Value) -> Value {
    let mut p = if method == "trace_flow" {
        json!({"start_qualname": "src.app.middle", "max_hops": 1})
    } else {
        json!({"qualname": "src.app.middle", "max_depth": 1, "enable_test": false, "enable_historical": false})
    };
    for (k, v) in extra.as_object().unwrap() {
        p[k] = v.clone();
    }
    p
}

/// Names of the symbols a result reached (excluding the start symbol).
fn reached(method: &str, r: &Value) -> Vec<String> {
    let items = if method == "trace_flow" {
        r["trace"].as_array()
    } else {
        r["affected"].as_array()
    };
    items
        .map(|a| {
            a.iter()
                .filter_map(|n| n["symbol"]["name"].as_str().map(String::from))
                .filter(|n| n != "middle")
                .collect()
        })
        .unwrap_or_default()
}

const METHODS: [&str; 2] = ["trace_flow", "analyze_impact"];

#[test]
fn sideways_direction_errors_naming_value_and_valid_directions() {
    let (_t, mut ix) = build();
    for m in METHODS {
        let err = err_of(&mut ix, m, params(m, json!({"direction": "sideways"})));
        assert!(err.contains("sideways"), "{m}: {err}");
        assert!(
            err.contains("upstream") && err.contains("downstream"),
            "{m}: {err}"
        );
    }
}

#[test]
fn direction_is_case_insensitive_and_runs_upstream() {
    let (_t, mut ix) = build();
    for m in METHODS {
        for d in ["upstream", "Upstream", "UPSTREAM", "up", "callers"] {
            let r = call(&mut ix, m, params(m, json!({"direction": d}))).unwrap();
            assert_eq!(reached(m, &r), vec!["top"], "{m} direction={d}: {r}");
        }
        for d in ["downstream", "Downstream", "down", "callees"] {
            let r = call(&mut ix, m, params(m, json!({"direction": d}))).unwrap();
            assert_eq!(reached(m, &r), vec!["helper"], "{m} direction={d}: {r}");
        }
    }
}

#[test]
fn omitted_direction_defaults_unchanged() {
    let (_t, mut ix) = build();
    let r = call(&mut ix, "trace_flow", params("trace_flow", json!({}))).unwrap();
    assert_eq!(reached("trace_flow", &r), vec!["helper"], "{r}");
    let r = call(
        &mut ix,
        "analyze_impact",
        params("analyze_impact", json!({})),
    )
    .unwrap();
    let mut got = reached("analyze_impact", &r);
    got.sort();
    assert_eq!(got, vec!["helper", "top"], "{r}");
}

#[test]
fn both_is_impact_only() {
    let (_t, mut ix) = build();
    call(
        &mut ix,
        "analyze_impact",
        params("analyze_impact", json!({"direction": "both"})),
    )
    .unwrap();
    let err = err_of(
        &mut ix,
        "trace_flow",
        params("trace_flow", json!({"direction": "both"})),
    );
    assert!(err.contains("both") && err.contains("downstream"), "{err}");
}

#[test]
fn bogus_kind_errors_naming_value_and_valid_kinds() {
    let (_t, mut ix) = build();
    for m in METHODS {
        let err = err_of(&mut ix, m, params(m, json!({"kinds": ["CALLS", "bogus"]})));
        assert!(err.contains("bogus"), "{m}: {err}");
        assert!(
            err.contains("CALLS") && err.contains("CONFIG_BIND"),
            "{m}: {err}"
        );
    }
}

#[test]
fn lowercase_kinds_match_like_uppercase() {
    let (_t, mut ix) = build();
    for m in METHODS {
        let upper = call(&mut ix, m, params(m, json!({"kinds": ["CALLS"]}))).unwrap();
        let lower = call(&mut ix, m, params(m, json!({"kinds": ["calls"]}))).unwrap();
        assert_eq!(
            reached(m, &upper),
            vec!["helper"]
                .into_iter()
                .chain(if m == "analyze_impact" {
                    Some("top")
                } else {
                    None
                })
                .collect::<Vec<_>>(),
            "{m}: {upper}"
        );
        assert_eq!(reached(m, &lower), reached(m, &upper), "{m}: {lower}");
    }
}

fn is_unknown_kind_error<T>(r: &anyhow::Result<T>) -> bool {
    r.as_ref()
        .err()
        .is_some_and(|e| e.to_string().contains("unknown edge kind"))
}

#[test]
fn every_emitted_edge_kind_is_accepted() {
    // Drift guard: EDGE_KINDS must cover every kind the extractors write.
    let mut seen = std::collections::BTreeSet::new();
    for fixture in std::fs::read_dir("tests/fixtures").unwrap() {
        let fixture = fixture.unwrap();
        if !fixture.path().is_dir() || fixture.file_name() == "golden" {
            continue;
        }
        let (_tmp, root, db_path) = common::setup_repo(fixture.file_name().to_str().unwrap());
        let mut ix = Indexer::new(root, db_path).unwrap();
        ix.reindex().unwrap();
        let conn = ix.db().read_conn().unwrap();
        let mut stmt = conn.prepare("SELECT DISTINCT kind FROM edges").unwrap();
        let kinds: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        seen.extend(kinds);
    }
    // Kinds that only appear for specific constructs the fixtures may not hit.
    let (_t, mut ix) = build();
    let mut extra: Vec<String> = ["REFERENCES", "page_route", "MODULE_EXPORT", "ROUTE"]
        .map(String::from)
        .to_vec();
    extra.extend(seen);
    assert!(
        extra.len() > 8,
        "fixtures should yield many kinds: {extra:?}"
    );
    for k in &extra {
        for m in METHODS {
            let r = call(&mut ix, m, params(m, json!({"kinds": [k]})));
            assert!(!is_unknown_kind_error(&r), "{m} rejected kind {k}: {r:?}");
        }
    }
}
