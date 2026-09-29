/// Regression tests for issue #120: `explain_symbol`'s `sections` filter
/// didn't reallocate budget. The fixed shares (30% source, 20% callers, 20%
/// callees, 10% tests, 10% implements) applied regardless of which sections
/// were actually requested, so e.g. `sections:["callers"], max_bytes:4000`
/// only gave callers a 20%-of-4000 = 800 byte budget -- capable of returning
/// nothing at all even though 80% of the requested budget went unused.
use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!(
        "lidx-explain-section-budget-{label}-{nanos}-{counter}"
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct TempRepo {
    pub repo_root: PathBuf,
    pub db_path: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.repo_root);
    }
}

fn call(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let raw = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if let Some(err) = envelope.get("error") {
        panic!("RPC error for {method}: {err:?}");
    }
    let result = envelope["result"].clone();
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

/// Writes `count` python files, each with a distinct top-level function that
/// calls `target.target()` directly, so each contributes exactly one
/// resolved CALLS edge/caller.
fn many_callers_repo(count: usize) -> TempRepo {
    let dir = temp_repo_dir("many-callers");
    std::fs::write(dir.join("target.py"), "def target():\n    return 1\n").unwrap();
    for i in 0..count {
        std::fs::write(
            dir.join(format!("caller_{i}.py")),
            format!("from target import target\n\n\ndef wrapper_{i}():\n    return target()\n"),
        )
        .unwrap();
    }
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let repo = TempRepo {
        repo_root: dir,
        db_path,
    };
    let mut indexer = Indexer::new(repo.repo_root.clone(), repo.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    repo
}

#[test]
fn single_requested_section_gets_full_budget_not_fixed_share() {
    // 1 caller, so the only thing that can make `callers` come back empty is
    // the byte budget. First find how many bytes one caller ref costs (a
    // generous max_bytes so nothing is dropped), then pick a max_bytes equal
    // to that cost: the fixed-20%-share bug would need max_bytes*5 to fit
    // it, but reallocating the whole budget to the one requested section
    // should fit it exactly.
    let temp = many_callers_repo(1);

    let baseline = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"],"max_bytes":40000}"#,
    );
    let ref_bytes = baseline["budget"]["used_bytes"]
        .as_u64()
        .expect("baseline used_bytes") as usize;
    assert!(ref_bytes > 0, "expected the single caller to cost bytes");

    let result = call(
        &temp,
        "explain_symbol",
        &format!(
            r#"{{"qualname":"target.target","sections":["callers"],"max_bytes":{ref_bytes}}}"#
        ),
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert_eq!(
        callers.len(),
        1,
        "callers should not be empty: the whole max_bytes ({ref_bytes}) was \
         given to the only requested section, so the single caller must fit. \
         Under the old fixed-20%-share bug this would return []: {:?}",
        result
    );
    assert_eq!(result["callers_total"], serde_json::json!(1));
}

#[test]
fn unused_share_from_an_earlier_section_rolls_over_to_a_later_one() {
    // No callers at all, so 100% of whatever share `callers` would have
    // gotten goes unused. `callees` has nothing to call either, but
    // `implements` is requested alongside an empty-but-present `callers`
    // section so we can check the rollover reaches all the way down the
    // chain without depending on exact byte-size knowledge: request
    // `sections:["callers","implements"]` with a max_bytes tight enough
    // that implements' own 10%-equivalent share alone would starve it, but
    // the full budget (all of it, since callers is empty) does not.
    let dir = temp_repo_dir("rollover");
    let mut source = String::new();
    for i in 0..15 {
        source.push_str(&format!("class Base{i}:\n    pass\n\n\n"));
    }
    let bases: Vec<String> = (0..15).map(|i| format!("Base{i}")).collect();
    source.push_str(&format!("class Foo({}):\n    pass\n", bases.join(", ")));
    std::fs::write(dir.join("bases.py"), source).unwrap();
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let temp = TempRepo {
        repo_root: dir,
        db_path,
    };
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    // Baseline: implements alone (its whole budget, no competing section) --
    // used to size a max_bytes that starves implements when it must share
    // with an (empty) callers section under the OLD fixed-share behavior,
    // but should not starve it once rollover from the empty callers section
    // applies.
    let baseline = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"bases.Foo","sections":["implements"],"max_refs":50,"max_bytes":40000}"#,
    );
    let all_implements = baseline["implements"].as_array().expect("implements array");
    assert_eq!(all_implements.len(), 15, "sanity: all 15 bases resolve");
    let one_ref_bytes = serde_json::to_string(&all_implements[0]).unwrap().len();

    // `Foo` has no callers, so the entire callers share is unused and must
    // roll forward. Pick max_bytes so implements' *unrenormalized* 10% share
    // alone (i.e. under the old bug, sections:["callers","implements"] still
    // gives implements only 10% of max_bytes) would be too small to fit even
    // one base, but the full max_bytes (via rollover from the empty callers
    // section) fits several.
    let max_bytes = one_ref_bytes * 5;

    let result = call(
        &temp,
        "explain_symbol",
        &format!(
            r#"{{"qualname":"bases.Foo","sections":["callers","implements"],"max_refs":50,"max_bytes":{max_bytes}}}"#
        ),
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert!(callers.is_empty(), "Foo has no callers");

    let implements = result["implements"].as_array().expect("implements array");
    assert!(
        !implements.is_empty(),
        "implements should not be empty: the unused callers share (Foo has \
         no callers) should roll over into implements' budget. Under the \
         old fixed-share bug (10% of {max_bytes} = {}), a single base ref \
         ({one_ref_bytes} bytes) would already be too large: {:?}",
        max_bytes / 10,
        result
    );
}

#[test]
fn section_never_returns_empty_when_first_ref_would_fit_overall_budget() {
    // Two sections requested (callers, callees) so each's renormalized share
    // is 50% of max_bytes -- but Foo has no callees, so once rollover from
    // an empty callees section is accounted for that's moot; use a target
    // with several callers and no callees so the callers share alone,
    // combined with the unused callees share, still must not starve the
    // very first caller even at a byte budget smaller than a naive 50%
    // split of one ref's cost would allow.
    let temp = many_callers_repo(3);

    let baseline = call(
        &temp,
        "explain_symbol",
        r#"{"qualname":"target.target","sections":["callers"],"max_bytes":40000}"#,
    );
    let callers = baseline["callers"].as_array().expect("callers array");
    let one_ref_bytes = serde_json::to_string(&callers[0]).unwrap().len();

    // max_bytes small enough that a 50/50 split with callees (requested but
    // always empty here) would give callers less than one_ref_bytes.
    let max_bytes = one_ref_bytes + one_ref_bytes / 4;

    let result = call(
        &temp,
        "explain_symbol",
        &format!(
            r#"{{"qualname":"target.target","sections":["callers","callees"],"max_bytes":{max_bytes}}}"#
        ),
    );

    let callers = result["callers"].as_array().expect("callers array");
    assert!(
        !callers.is_empty(),
        "used_bytes (0 so far) < max_bytes ({max_bytes}) and the first \
         caller ({one_ref_bytes} bytes) fits within it, so callers must not \
         come back empty: {:?}",
        result
    );
    assert!(
        result["budget"]["used_bytes"].as_u64().unwrap() as usize <= max_bytes,
        "used_bytes must still respect the overall max_bytes cap: {:?}",
        result["budget"]
    );
}
