use crate::db::Db;
use crate::model::Symbol;
use anyhow::Result;
use serde_json::{Value, json};

/// Reference to a symbol by ID, fully-qualified name, or free-text query.
#[derive(Debug, Clone)]
pub enum SymbolRef {
    Id(i64),
    Qualname(String),
    Query(String),
}

/// Resolves a `SymbolRef` to a `Symbol` using the full fallback chain:
/// ID lookup → qualname lookup → fuzzy query → config key → "did you mean".
pub fn resolve_symbol(
    db: &Db,
    reference: SymbolRef,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<Symbol> {
    match reference {
        SymbolRef::Id(id) => db
            .get_symbol_by_id(id)?
            .ok_or_else(|| anyhow::anyhow!("symbol not found: id={}", id)),

        SymbolRef::Qualname(ref qn) => match db.get_symbol_by_qualname(qn, graph_version)? {
            Some(sym) => Ok(sym),
            None => resolve_by_query(db, qn, languages, graph_version),
        },

        SymbolRef::Query(ref query) => resolve_by_query(db, query, languages, graph_version),
    }
}

fn resolve_by_query(
    db: &Db,
    query: &str,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<Symbol> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        anyhow::bail!("no symbol found for query: {}", query);
    }

    let results = db.find_symbols(trimmed, 5, languages, graph_version)?;
    if let Some(sym) = results.into_iter().next() {
        return Ok(sym);
    }

    if languages.is_some() {
        let results = db.find_symbols(trimmed, 5, None, graph_version)?;
        if let Some(sym) = results.into_iter().next() {
            return Ok(sym);
        }
    }

    let config_uris: Vec<String> = [
        crate::indexer::config::normalize_env_var_name(trimmed),
        crate::indexer::config::normalize_secret_name(trimmed),
    ]
    .into_iter()
    .flatten()
    .collect();
    for uri in &config_uris {
        let ids = db.source_symbols_for_config_uri(uri, &[], graph_version)?;
        if let Some(&first_id) = ids.first()
            && let Some(sym) = db.get_symbol_by_id(first_id)?
        {
            return Ok(sym);
        }
    }

    let suggestion_query = trimmed
        .split_whitespace()
        .max_by_key(|t| t.len())
        .unwrap_or(trimmed);
    let suggestions = db
        .find_symbols(suggestion_query, 10, None, graph_version)
        .unwrap_or_default();
    if !suggestions.is_empty() {
        let names: Vec<String> = suggestions.into_iter().map(|s| s.qualname).collect();
        anyhow::bail!(
            "Symbol '{}' not found. Did you mean: {}?",
            query,
            names.join(", ")
        );
    }
    anyhow::bail!("no symbol found for query: {}", query);
}

/// Candidate symbols and config URIs found when resolution of a start ref fails.
pub struct RecoverySuggestions {
    /// Symbols whose name or qualname partially matches the query term.
    pub symbol_candidates: Vec<Symbol>,
    /// Config URIs that partially match the query term (e.g. `env://FOO`).
    pub config_uri_candidates: Vec<String>,
}

/// Find candidate symbols and config URIs for a failed query, using the same
/// machinery as `resolve_by_query` (token-based search, config key normalisation).
/// Returns an empty struct when no candidates are found rather than an error.
pub fn find_recovery_suggestions(db: &Db, query: &str, graph_version: i64) -> RecoverySuggestions {
    let trimmed = query.trim();

    // Candidate symbols: try each whitespace token in the query, largest first,
    // stopping at the first token that produces results. This mirrors
    // resolve_by_query's "longest token" heuristic but also tries shorter tokens
    // so that near-misses like "Greeter zzz_nonexistent" still find Greeter.
    let mut tokens: Vec<&str> = trimmed.split_whitespace().collect();
    tokens.sort_by_key(|t| std::cmp::Reverse(t.len()));
    let mut symbol_candidates: Vec<Symbol> = Vec::new();
    for token in &tokens {
        let found = db
            .find_symbols(token, 5, None, graph_version)
            .unwrap_or_default();
        if !found.is_empty() {
            symbol_candidates = found;
            break;
        }
    }
    // If no token produced results, fall back to the full query.
    if symbol_candidates.is_empty() {
        symbol_candidates = db
            .find_symbols(trimmed, 5, None, graph_version)
            .unwrap_or_default();
    }

    // Config URI candidates: normalise as env var or secret name.
    let config_uri_candidates: Vec<String> = [
        crate::indexer::config::normalize_env_var_name(trimmed),
        crate::indexer::config::normalize_secret_name(trimmed),
    ]
    .into_iter()
    .flatten()
    .filter(|uri| {
        // Only include URIs that actually have symbols connected to them.
        db.source_symbols_for_config_uri(uri, &[], graph_version)
            .ok()
            .is_some_and(|ids| !ids.is_empty())
    })
    .collect();

    RecoverySuggestions {
        symbol_candidates,
        config_uri_candidates,
    }
}

/// Build the structured recovery payload returned when start-symbol resolution
/// fails in `trace_flow` or `analyze_impact`.
///
/// The payload shape is:
/// ```json
/// {
///   "resolved": false,
///   "message": "...",
///   "next_hops": [...]
/// }
/// ```
///
/// `method` is the calling method name (`"trace_flow"` or `"analyze_impact"`) so
/// the suggested retries use the correct method name.
pub fn build_resolution_recovery_payload(
    db: &Db,
    query: &str,
    graph_version: i64,
    method: &str,
) -> Value {
    let suggestions = find_recovery_suggestions(db, query, graph_version);
    let mut next_hops: Vec<Value> = Vec::new();

    // Suggest explain_symbol for each candidate symbol.
    for sym in &suggestions.symbol_candidates {
        next_hops.push(json!({
            "method": "explain_symbol",
            "params": {"id": sym.id},
            "description": format!("Explain '{}' ({})", sym.qualname, sym.kind),
        }));
    }

    // Suggest the calling method with each candidate symbol's id.
    for sym in &suggestions.symbol_candidates {
        next_hops.push(json!({
            "method": method,
            "params": {"query": sym.name.clone()},
            "description": format!("Retry {} with near-match '{}'", method, sym.name),
        }));
    }

    // Suggest the calling method via each config URI candidate.
    // trace_flow uses `start_qualname`; analyze_impact uses `qualname`.
    let qualname_key = if method == "trace_flow" {
        "start_qualname"
    } else {
        "qualname"
    };
    for uri in &suggestions.config_uri_candidates {
        next_hops.push(json!({
            "method": method,
            "params": {qualname_key: uri},
            "description": format!("Try {} with config URI '{}'", method, uri),
        }));
    }

    // Always include a text search for the query as a fallback — `search` succeeds even
    // when nothing matches, so this hop is always executable.
    let trimmed_query = query.trim();
    let longest_token = trimmed_query
        .split_whitespace()
        .max_by_key(|t| t.len())
        .unwrap_or(trimmed_query);
    next_hops.push(json!({
        "method": "search",
        "params": {"query": longest_token, "limit": 10},
        "description": format!("Text search for '{}' to find related symbols", longest_token),
    }));

    // Deduplicate: there may be overlap between method retries and explain hops.
    // Keep insertion order; skip exact duplicates.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let next_hops: Vec<Value> = next_hops
        .into_iter()
        .filter(|h| seen.insert(h.to_string()))
        .collect();

    let message = format!(
        "Symbol '{}' not found. {} suggestion(s) below.",
        query,
        next_hops.len()
    );

    json!({
        "resolved": false,
        "message": message,
        "next_hops": next_hops,
    })
}

/// Resolve a start reference, falling back to a structured recovery payload when
/// resolution fails *and* a `recovery_query` is available (qualname/query refs).
///
/// Returns:
/// - `Ok(Ok(symbol))` — resolved successfully.
/// - `Ok(Err(payload))` — resolution failed but a recovery payload was built;
///   the caller should return it as a successful response.
/// - `Err(e)` — resolution failed with nothing to suggest (e.g. an ID miss, when
///   `recovery_query` is `None`); the caller should propagate the error.
///
/// Centralising the recovery boundary here keeps both `trace_flow` and
/// `analyze_impact` from duplicating the catch logic.
pub fn resolve_or_recovery(
    db: &Db,
    reference: SymbolRef,
    languages: Option<&[String]>,
    graph_version: i64,
    recovery_query: Option<&str>,
    method: &str,
) -> Result<std::result::Result<Symbol, Value>> {
    match resolve_symbol(db, reference, languages, graph_version) {
        Ok(sym) => Ok(Ok(sym)),
        Err(e) => match recovery_query {
            Some(query) => Ok(Err(build_resolution_recovery_payload(
                db,
                query,
                graph_version,
                method,
            ))),
            None => Err(e),
        },
    }
}

/// Expands a symbol into seed IDs for BFS traversal.
/// For container symbols (class/module/resource), returns the symbol plus its members.
pub fn expand_seeds(db: &Db, symbol_id: i64, graph_version: i64) -> Result<Vec<i64>> {
    let symbol = db
        .get_symbol_by_id(symbol_id)?
        .ok_or_else(|| anyhow::anyhow!("symbol not found: id={}", symbol_id))?;

    let is_container = matches!(symbol.kind.as_str(), "class" | "module" | "resource");
    if !is_container {
        return Ok(vec![symbol_id]);
    }

    let mut ids = vec![symbol_id];
    let file_symbols = db.get_symbols_for_file(&symbol.file_path, graph_version)?;
    for s in &file_symbols {
        if s.id != symbol_id
            && s.start_line >= symbol.start_line
            && s.end_line <= symbol.end_line
            && matches!(
                s.kind.as_str(),
                "method" | "function" | "resource" | "var" | "param" | "output"
            )
        {
            ids.push(s.id);
        }
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    fn temp_repo_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("lidx-resolve-{label}-{nanos}-{counter}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let target = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&path, &target);
            } else {
                std::fs::copy(&path, &target).unwrap();
            }
        }
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

    impl TempRepo {
        fn new(fixture: &str) -> Self {
            let src = fixture_path(fixture);
            let repo_root = temp_repo_dir(fixture);
            copy_dir(&src, &repo_root);
            let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
            Self { repo_root, db_path }
        }
    }

    fn indexed_repo(fixture: &str) -> (TempRepo, Indexer) {
        let temp = TempRepo::new(fixture);
        let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
        indexer.reindex().unwrap();
        (temp, indexer)
    }

    #[test]
    fn resolve_by_id() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let sym =
            resolve_symbol(indexer.db(), SymbolRef::Query("Greeter".into()), None, gv).unwrap();

        let resolved = resolve_symbol(indexer.db(), SymbolRef::Id(sym.id), None, gv).unwrap();
        assert_eq!(resolved.id, sym.id);
        assert_eq!(resolved.name, "Greeter");
    }

    #[test]
    fn resolve_by_qualname() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let resolved = resolve_symbol(
            indexer.db(),
            SymbolRef::Qualname("pkg.core.Greeter".into()),
            None,
            gv,
        )
        .unwrap();
        assert_eq!(resolved.name, "Greeter");
        assert_eq!(resolved.qualname, "pkg.core.Greeter");
    }

    #[test]
    fn resolve_by_query_single_word() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let resolved =
            resolve_symbol(indexer.db(), SymbolRef::Query("Greeter".into()), None, gv).unwrap();
        assert_eq!(resolved.name, "Greeter");
    }

    #[test]
    fn resolve_by_query_multi_word() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let resolved = resolve_symbol(
            indexer.db(),
            SymbolRef::Query("core Greeter".into()),
            None,
            gv,
        )
        .unwrap();
        assert_eq!(resolved.name, "Greeter");
        assert!(resolved.qualname.contains("core"));
    }

    #[test]
    fn resolve_did_you_mean_on_unresolvable() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        // Multi-word query where the AND combination finds nothing, but the
        // longest token ("Greeter") individually matches symbols — triggering
        // the "Did you mean" suggestion path.
        let err = resolve_symbol(
            indexer.db(),
            SymbolRef::Query("Greeter zzzzz".into()),
            None,
            gv,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Did you mean"),
            "expected 'Did you mean' in: {msg}"
        );
    }

    #[test]
    fn resolve_query_retries_without_language_filter() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let nonexistent_lang = vec!["haskell".to_string()];
        let resolved = resolve_symbol(
            indexer.db(),
            SymbolRef::Query("Greeter".into()),
            Some(&nonexistent_lang),
            gv,
        )
        .unwrap();
        assert_eq!(resolved.name, "Greeter");
    }

    #[test]
    fn resolve_config_key_fallback() {
        let (_temp, indexer) = indexed_repo("py_config");
        let gv = indexer.db().current_graph_version().unwrap();

        // "DATABASE_URL" is not a symbol name, but normalize_env_var_name
        // turns it into "env://DATABASE_URL" which matches a CONFIG_READ edge.
        let result = resolve_symbol(
            indexer.db(),
            SymbolRef::Query("DATABASE_URL".into()),
            None,
            gv,
        );

        // Should resolve to the symbol that reads this env var
        assert!(result.is_ok(), "config key fallback should find a symbol");
        let sym = result.unwrap();
        assert_eq!(sym.file_path, "app.py");
    }

    #[test]
    fn expand_seeds_non_container() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let func = resolve_symbol(
            indexer.db(),
            SymbolRef::Query("make_greeter".into()),
            None,
            gv,
        )
        .unwrap();
        assert_eq!(func.kind, "function");

        let seeds = expand_seeds(indexer.db(), func.id, gv).unwrap();
        assert_eq!(seeds, vec![func.id]);
    }

    #[test]
    fn resolve_nonexistent_id() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let err = resolve_symbol(indexer.db(), SymbolRef::Id(999999), None, gv).unwrap_err();
        assert!(
            err.to_string().contains("symbol not found"),
            "expected 'symbol not found' in: {}",
            err
        );
    }

    #[test]
    fn resolve_nonexistent_qualname() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let err = resolve_symbol(
            indexer.db(),
            SymbolRef::Qualname("no.such.Symbol".into()),
            None,
            gv,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no symbol found") || msg.contains("Did you mean"),
            "expected 'no symbol found' or 'Did you mean' in: {msg}",
        );
    }

    #[test]
    fn resolve_near_miss_qualname_falls_through_to_query() {
        // "core.Greeter" is not an exact qualname, but the query fallback
        // substring-matches it against "pkg.core.Greeter" — should succeed.
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let resolved = resolve_symbol(
            indexer.db(),
            SymbolRef::Qualname("core.Greeter".into()),
            None,
            gv,
        )
        .unwrap();
        assert_eq!(resolved.name, "Greeter");
        assert_eq!(resolved.qualname, "pkg.core.Greeter");
    }

    #[test]
    fn resolve_exact_qualname_wins_over_fuzzy_fallthrough() {
        // "pkg.core" is the module's exact qualname AND a substring of
        // "pkg.core.Greeter". The fuzzy fallback ranks classes above modules,
        // so getting the module back proves the exact lookup short-circuits.
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let resolved = resolve_symbol(
            indexer.db(),
            SymbolRef::Qualname("pkg.core".into()),
            None,
            gv,
        )
        .unwrap();
        assert_eq!(resolved.qualname, "pkg.core");
        assert_eq!(resolved.kind, "module");
    }

    #[test]
    fn resolve_totally_unresolvable_query() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let err = resolve_symbol(
            indexer.db(),
            SymbolRef::Query("xyzzy_not_a_symbol_at_all".into()),
            None,
            gv,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no symbol found"),
            "expected 'no symbol found' in: {msg}"
        );
    }

    #[test]
    fn expand_seeds_nonexistent_symbol() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let err = expand_seeds(indexer.db(), 999999, gv).unwrap_err();
        assert!(
            err.to_string().contains("symbol not found"),
            "expected 'symbol not found' in: {}",
            err
        );
    }

    #[test]
    fn expand_seeds_container() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let class =
            resolve_symbol(indexer.db(), SymbolRef::Query("Greeter".into()), None, gv).unwrap();
        assert_eq!(class.kind, "class");

        let seeds = expand_seeds(indexer.db(), class.id, gv).unwrap();
        assert!(seeds.contains(&class.id), "should contain the class itself");
        assert!(seeds.len() > 1, "should contain members (greet method)");

        let member_ids: Vec<i64> = seeds.iter().copied().filter(|&id| id != class.id).collect();
        for mid in &member_ids {
            let sym = indexer.db().get_symbol_by_id(*mid).unwrap().unwrap();
            assert!(
                matches!(sym.kind.as_str(), "method" | "function"),
                "member should be method or function, got: {}",
                sym.kind
            );
        }
    }

    #[test]
    fn resolve_empty_or_whitespace_input_returns_error() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        for input in ["", "   "] {
            for reference in [
                SymbolRef::Query(input.into()),
                SymbolRef::Qualname(input.into()),
            ] {
                let err = resolve_symbol(indexer.db(), reference, None, gv).unwrap_err();
                let msg = err.to_string();
                assert!(
                    msg.contains("no symbol found"),
                    "'{input}' input should fail cleanly, got: {msg}"
                );
            }
        }
    }

    #[test]
    fn resolve_with_empty_languages_list() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        // Empty languages slice should behave like no language filter
        let empty: Vec<String> = vec![];
        let resolved = resolve_symbol(
            indexer.db(),
            SymbolRef::Query("Greeter".into()),
            Some(&empty),
            gv,
        )
        .unwrap();
        assert_eq!(resolved.name, "Greeter");
    }

    #[test]
    fn expand_seeds_returns_no_duplicates() {
        let (_temp, indexer) = indexed_repo("py_mvp");
        let gv = indexer.db().current_graph_version().unwrap();

        let class =
            resolve_symbol(indexer.db(), SymbolRef::Query("Greeter".into()), None, gv).unwrap();
        let seeds = expand_seeds(indexer.db(), class.id, gv).unwrap();

        let mut seen = std::collections::HashSet::new();
        for id in &seeds {
            assert!(seen.insert(id), "duplicate seed id: {}", id);
        }
    }
}
