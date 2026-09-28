use crate::db::Db;
use crate::model::Symbol;
use anyhow::Result;

/// Reference to a symbol by ID, fully-qualified name, or free-text query.
#[derive(Debug, Clone)]
pub enum SymbolRef {
    Id(i64),
    Qualname(String),
    Query(String),
}

/// Outcome of `resolve_symbol_with_candidates`.
pub enum QueryResolution {
    Found(Box<Symbol>),
    /// Multiple candidates tied for best match at the exact-name-match tier
    /// (`find_symbols`'s own top ranking criterion) -- picking one would be
    /// a guess.
    Ambiguous(Vec<Symbol>),
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

/// Like `resolve_symbol`, but surfaces a tie at the exact-name-match tier as
/// `QueryResolution::Ambiguous` instead of silently taking the first match --
/// for callers (namely `read_symbol`) where guessing between tied candidates
/// is costly. `resolve_symbol` keeps the silent-pick behaviour, which is what
/// callers like `explain_symbol`/`trace_flow` want.
///
/// Shares its `find_symbols` lookup with the rest of the fallback chain
/// (`find_symbol_candidates` / `resolve_after_candidates`), so checking for a
/// tie doesn't cost a second query on top of the one `resolve_symbol` would
/// run for the same reference.
pub fn resolve_symbol_with_candidates(
    db: &Db,
    reference: SymbolRef,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<QueryResolution> {
    let query = match reference {
        SymbolRef::Id(_) => {
            let sym = resolve_symbol(db, reference, languages, graph_version)?;
            return Ok(QueryResolution::Found(Box::new(sym)));
        }
        SymbolRef::Qualname(qn) => {
            if let Some(sym) = db.get_symbol_by_qualname(&qn, graph_version)? {
                return Ok(QueryResolution::Found(Box::new(sym)));
            }
            qn
        }
        SymbolRef::Query(query) => query,
    };

    let (trimmed, candidates) = trimmed_query_candidates(db, &query, languages, graph_version)?;
    if let Some(tied) = tied_candidates(&trimmed, &candidates) {
        return Ok(QueryResolution::Ambiguous(tied));
    }
    let sym = resolve_after_candidates(db, &query, candidates, graph_version)?;
    Ok(QueryResolution::Found(Box::new(sym)))
}

/// Trims `query`, bails with a consistent "no symbol found" error if that
/// leaves nothing, and returns `find_symbol_candidates`' ranked matches --
/// the guard + lookup pair `resolve_by_query` and
/// `resolve_symbol_with_candidates` both need before they diverge on how they
/// handle a tie.
fn trimmed_query_candidates(
    db: &Db,
    query: &str,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<(String, Vec<Symbol>)> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        anyhow::bail!("no symbol found for query: {}", query);
    }
    let candidates = find_symbol_candidates(db, trimmed, languages, graph_version)?;
    Ok((trimmed.to_string(), candidates))
}

/// `find_symbols` lookup shared by `resolve_by_query` and
/// `resolve_symbol_with_candidates` (via `trimmed_query_candidates`): tries
/// the language filter first, then retries without one -- so a caller that
/// only wants ranked candidates doesn't have to duplicate that retry, and a
/// later fallback stage doesn't have to re-run the query to get the same
/// candidates.
fn find_symbol_candidates(
    db: &Db,
    trimmed: &str,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<Vec<Symbol>> {
    let results = db.find_symbols(trimmed, 5, languages, graph_version)?;
    if !results.is_empty() || languages.is_none() {
        return Ok(results);
    }
    db.find_symbols(trimmed, 5, None, graph_version)
}

/// Candidates whose name (case-insensitively) matches `trimmed`'s longest
/// whitespace-separated token -- `find_symbols`'s own top ranking criterion.
/// More than one such candidate is a tie: returning either would be a guess.
fn tied_candidates(trimmed: &str, candidates: &[Symbol]) -> Option<Vec<Symbol>> {
    if candidates.len() < 2 {
        return None;
    }
    let longest_token = trimmed
        .split_whitespace()
        .max_by_key(|t| t.len())
        .unwrap_or(trimmed);
    let longest_lower = longest_token.to_lowercase();
    let tied: Vec<Symbol> = candidates
        .iter()
        .filter(|s| s.name.to_lowercase() == longest_lower)
        .cloned()
        .collect();
    (tied.len() > 1).then_some(tied)
}

/// Continues the fallback chain once `find_symbols` candidates are already in
/// hand: the top candidate if there is one, else the config-key lookup, else
/// a "did you mean" error built from a broader unfiltered search.
fn resolve_after_candidates(
    db: &Db,
    query: &str,
    candidates: Vec<Symbol>,
    graph_version: i64,
) -> Result<Symbol> {
    if let Some(sym) = candidates.into_iter().next() {
        return Ok(sym);
    }

    let trimmed = query.trim();
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

fn resolve_by_query(
    db: &Db,
    query: &str,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<Symbol> {
    let (_, candidates) = trimmed_query_candidates(db, query, languages, graph_version)?;
    resolve_after_candidates(db, query, candidates, graph_version)
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
