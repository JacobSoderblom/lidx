use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;

use crate::db::Db;
use crate::model::Symbol;

pub struct RepoMapConfig {
    pub max_bytes: usize,
    pub languages: Option<Vec<String>>,
    pub paths: Option<Vec<String>>,
    pub graph_version: i64,
}

#[derive(Debug, Serialize)]
pub struct RepoMapResult {
    pub text: String,
    pub modules: usize,
    pub symbols: usize,
    pub bytes: usize,
    /// True when output was cut to honour `max_bytes`.
    pub truncated: bool,
}

/// Appended when output was cut for budget so a short map is
/// distinguishable from a complete one.
const TRUNCATION_NOTE: &str = "\n_(truncated: max_bytes reached)_\n";

/// Output buffer that never grows past the byte budget. Every write is
/// all-or-nothing, so a cut always lands on a line boundary.
struct BudgetedOutput {
    buf: String,
    /// Content limit; leaves room for `TRUNCATION_NOTE` when the budget
    /// can afford one.
    limit: usize,
    truncated: bool,
}

impl BudgetedOutput {
    fn new(budget: usize) -> Self {
        let reserve = if budget >= TRUNCATION_NOTE.len() * 2 {
            TRUNCATION_NOTE.len()
        } else {
            0
        };
        Self {
            buf: String::new(),
            limit: budget - reserve,
            truncated: false,
        }
    }

    /// Append `text` only if it fits. Once something has been refused,
    /// everything after it is refused too, so output stays a clean prefix.
    fn push(&mut self, text: &str) -> bool {
        if self.truncated || self.buf.len() + text.len() > self.limit {
            self.truncated = true;
            return false;
        }
        self.buf.push_str(text);
        true
    }

    fn finish(mut self, budget: usize) -> (String, bool) {
        if self.truncated && self.buf.len() + TRUNCATION_NOTE.len() <= budget {
            self.buf.push_str(TRUNCATION_NOTE);
        }
        (self.buf, self.truncated)
    }
}

pub fn build_repo_map(db: &Db, config: &RepoMapConfig) -> Result<RepoMapResult> {
    let budget = config.max_bytes;
    let mut out = BudgetedOutput::new(budget);
    let mut total_symbols = 0;

    // Phase 1: Module summary from module_map(depth=1)
    let modules = db.module_summary(
        1,
        config.languages.as_deref(),
        config.paths.as_deref(),
        config.graph_version,
    )?;

    out.push("# Architecture Overview\n\n");
    out.push("## Modules\n");
    for m in &modules {
        let dominant_language = if m.languages.is_empty() {
            "unknown".to_string()
        } else {
            m.languages.join(",")
        };
        // `m.path` (from `module_summary`/`module_prefix`) always carries a
        // trailing separator, including "./" for root-level files -- see
        // the "Dependencies" and "Key Symbols" sections below, which rely
        // on the same contract -- so it is not added again here. Issue
        // #134: doing so produced doubled separators like "py//" for any
        // module below the repo root.
        let line = format!(
            "- **{}** ({} files, {} symbols, {})\n",
            m.path, m.file_count, m.symbol_count, dominant_language
        );
        if !out.push(&line) {
            break;
        }
    }

    // Phase 2: Inter-module edges
    if !out.truncated {
        let edges = db.module_edges(
            1,
            config.languages.as_deref(),
            config.paths.as_deref(),
            config.graph_version,
        )?;
        if !edges.is_empty() {
            out.push("\n## Dependencies\n");
            for e in edges.iter().take(20) {
                let line = format!(
                    "- {} → {} ({} calls, {} imports, {} xrefs)\n",
                    e.0, e.1, e.2, e.3, e.4
                );
                if !out.push(&line) {
                    break;
                }
            }
        }
    }

    // Phase 3: Top symbols per module by fan-in
    if !out.truncated {
        let fan_in_symbols = db.top_fan_in_by_module(
            10,
            config.languages.as_deref(),
            config.paths.as_deref(),
            config.graph_version,
        )?;
        let mut by_module: HashMap<String, Vec<(&Symbol, i64)>> = HashMap::new();
        for (module, sym, count) in &fan_in_symbols {
            by_module
                .entry(module.clone())
                .or_default()
                .push((sym, *count));
        }

        out.push("\n## Key Symbols (by fan-in)\n");
        let mut sorted_modules: Vec<_> = by_module.keys().cloned().collect();
        sorted_modules.sort();
        'modules: for module in sorted_modules {
            if let Some(syms) = by_module.get(&module) {
                // `module` (from `top_fan_in_by_module`, now backed by the
                // same `module_prefix()` as `module_summary`) already
                // carries a trailing separator -- see the "## Modules"
                // comment above -- so it is not added again here.
                if !out.push(&format!("\n### {}\n", module)) {
                    break;
                }
                for (sym, count) in syms.iter().take(5) {
                    let line = format!(
                        "- {} **{}** `{}` (fan-in: {})\n",
                        sym.kind,
                        sym.name,
                        sym.signature.as_deref().unwrap_or(""),
                        count
                    );
                    if !out.push(&line) {
                        break 'modules;
                    }
                    total_symbols += 1;
                }
            }
        }
    }

    // Phase 4: Patterns (if budget remains)
    if !out.truncated {
        let kinds = db.count_symbols_by_kind(
            config.languages.as_deref(),
            config.paths.as_deref(),
            config.graph_version,
        )?;
        out.push("\n## Patterns\n");
        for (kind, count) in &kinds {
            if !out.push(&format!("- {}: {}\n", kind, count)) {
                break;
            }
        }
    }

    let (text, truncated) = out.finish(budget);
    let bytes = text.len();
    Ok(RepoMapResult {
        text,
        modules: modules.len(),
        symbols: total_symbols,
        bytes,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::extract::{EdgeInput, SymbolInput};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_db() -> (Db, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Db::new(&db_path).unwrap();
        (db, temp_dir)
    }

    fn make_symbol(qualname: &str, kind: &str) -> SymbolInput {
        SymbolInput {
            kind: kind.to_string(),
            name: qualname
                .split('.')
                .next_back()
                .unwrap_or(qualname)
                .to_string(),
            qualname: qualname.to_string(),
            start_line: 1,
            start_col: 0,
            end_line: 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 50,
            signature: None,
            docstring: None,
            identity: None,
        }
    }

    fn make_edge(kind: &str, source: &str, target: &str) -> EdgeInput {
        EdgeInput {
            kind: kind.to_string(),
            source_qualname: Some(source.to_string()),
            target_qualname: Some(target.to_string()),
            ..Default::default()
        }
    }

    fn default_config(graph_version: i64) -> RepoMapConfig {
        RepoMapConfig {
            max_bytes: 50_000,
            languages: None,
            paths: None,
            graph_version,
        }
    }

    // Issue #134: `module_prefix` (used by `module_summary`) already
    // returns paths with a trailing separator (e.g. "pkg/"), but the
    // "## Modules" line in the repo map appended another "/" on top of
    // that, producing "pkg//" for any module one level deep.
    #[test]
    fn module_list_has_no_doubled_slash() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid = db.upsert_file("pkg/a.py", "h1", "python", 10, 0).unwrap();
        db.insert_symbols(
            fid,
            "pkg/a.py",
            &[make_symbol("pkg.a.helper", "function")],
            gv,
            None,
        )
        .unwrap();

        let result = build_repo_map(&db, &default_config(gv)).unwrap();

        assert!(
            !result.text.contains("//"),
            "module paths should not contain a doubled separator:\n{}",
            result.text
        );
        assert!(
            result.text.contains("**pkg/**"),
            "expected a single trailing slash on the module path:\n{}",
            result.text
        );
    }

    // Issue #134: two distinct symbols sharing a bare name (e.g. a common
    // helper name repeated across files in the same top-level module) both
    // appeared under that module's "Key Symbols" section, listing the same
    // name twice with nothing to tell them apart.
    #[test]
    fn key_symbols_section_dedupes_same_name_per_module() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();

        let fid_a = db.upsert_file("pkg/a.py", "h1", "python", 10, 0).unwrap();
        let fid_b = db.upsert_file("pkg/b.py", "h2", "python", 10, 0).unwrap();
        let fid_app = db.upsert_file("app.py", "h3", "python", 10, 0).unwrap();

        let ins_a = db
            .insert_symbols(
                fid_a,
                "pkg/a.py",
                &[make_symbol("pkg.a.helper", "function")],
                gv,
                None,
            )
            .unwrap();
        let ins_b = db
            .insert_symbols(
                fid_b,
                "pkg/b.py",
                &[make_symbol("pkg.b.helper", "function")],
                gv,
                None,
            )
            .unwrap();
        let ins_app = db
            .insert_symbols(
                fid_app,
                "app.py",
                &[make_symbol("app.caller", "function")],
                gv,
                None,
            )
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("pkg.a.helper".to_string(), ins_a[0].id);
        sym_map.insert("pkg.b.helper".to_string(), ins_b[0].id);
        sym_map.insert("app.caller".to_string(), ins_app[0].id);
        db.insert_edges(
            fid_app,
            &[
                make_edge("CALLS", "app.caller", "pkg.a.helper"),
                make_edge("CALLS", "app.caller", "pkg.b.helper"),
            ],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let result = build_repo_map(&db, &default_config(gv)).unwrap();

        let helper_occurrences = result.text.matches("**helper**").count();
        assert_eq!(
            helper_occurrences, 1,
            "expected `helper` to appear once under the `pkg` module:\n{}",
            result.text
        );
    }

    // Issue #134 follow-up: the "## Modules" section (via `module_summary`)
    // and "## Key Symbols" section (via `top_fan_in_by_module`) used to
    // disagree on module identity for root-level files (no `/` in their
    // path): "## Modules" grouped them under "." while "## Key Symbols"
    // grouped them under their own bare filename plus an appended "/",
    // e.g. "### main.rs/" -- a bogus pseudo-directory unrelated to the
    // "## Modules" entry. Both sections must now agree: root-level files
    // group under "./" in both places, with no doubled or missing
    // separators.
    #[test]
    fn root_level_module_matches_between_modules_and_key_symbols_sections() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();

        let fid_main = db.upsert_file("main.rs", "h1", "rust", 10, 0).unwrap();
        let fid_other = db.upsert_file("other.rs", "h2", "rust", 10, 0).unwrap();

        let ins_main = db
            .insert_symbols(
                fid_main,
                "main.rs",
                &[make_symbol("main.run", "function")],
                gv,
                None,
            )
            .unwrap();
        let ins_other = db
            .insert_symbols(
                fid_other,
                "other.rs",
                &[make_symbol("other.caller", "function")],
                gv,
                None,
            )
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("main.run".to_string(), ins_main[0].id);
        sym_map.insert("other.caller".to_string(), ins_other[0].id);
        db.insert_edges(
            fid_other,
            &[make_edge("CALLS", "other.caller", "main.run")],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let result = build_repo_map(&db, &default_config(gv)).unwrap();

        assert!(
            !result.text.contains("//"),
            "module paths should not contain a doubled separator:\n{}",
            result.text
        );
        assert!(
            !result.text.contains("main.rs/"),
            "root-level file should not be rendered as a pseudo-directory:\n{}",
            result.text
        );
        assert!(
            result.text.contains("**./**"),
            "expected the \"## Modules\" section to label the root module \"./\":\n{}",
            result.text
        );
        assert!(
            result.text.contains("### ./"),
            "expected the \"## Key Symbols\" section to label the root module \"./\", \
             matching \"## Modules\":\n{}",
            result.text
        );
    }

    /// Index one function per `(dir, name)` and a CALLS edge per
    /// `(src_name, dst_name)` pair, with each edge owned by its source file.
    fn build_graph(db: &mut Db, gv: i64, funcs: &[(&str, &str)], calls: &[(&str, &str)]) {
        let mut sym_map = HashMap::new();
        let mut file_of = HashMap::new();
        for (dir, name) in funcs {
            let path = format!("{dir}/{name}.py");
            let fid = db.upsert_file(&path, name, "python", 10, 0).unwrap();
            let qual = format!("{dir}.{name}.{name}");
            let ins = db
                .insert_symbols(fid, &path, &[make_symbol(&qual, "function")], gv, None)
                .unwrap();
            sym_map.insert(name.to_string(), ins[0].id);
            file_of.insert(name.to_string(), (fid, qual));
        }
        let qual_map: HashMap<String, i64> = file_of
            .iter()
            .map(|(name, (_, qual))| (qual.clone(), sym_map[name]))
            .collect();
        for (src, dst) in calls {
            let (fid, sq) = &file_of[*src];
            let (_, tq) = &file_of[*dst];
            db.insert_edges(*fid, &[make_edge("CALLS", sq, tq)], &qual_map, gv, None)
                .unwrap();
        }
    }

    fn section<'a>(text: &'a str, header: &str) -> &'a str {
        let start = text
            .find(header)
            .unwrap_or_else(|| panic!("no {header}:\n{text}"));
        let rest = &text[start + header.len()..];
        let end = rest.find("\n## ").unwrap_or(rest.len());
        &rest[..end]
    }

    fn path_fixture() -> (Db, TempDir, i64) {
        let (mut db, temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        build_graph(
            &mut db,
            gv,
            &[
                ("dirA", "fa"),
                ("dirB", "fb"),
                ("dirB", "fb2"),
                ("dirC", "fc"),
                ("dirC", "fc2"),
                ("dirC", "fc3"),
            ],
            &[
                ("fa", "fb"),
                ("fb", "fa"),
                ("fb2", "fa"),
                ("fc", "fb"),
                ("fc2", "fb"),
                ("fc3", "fb"),
            ],
        );
        (db, temp, gv)
    }

    // Issue #240: Dependencies ignored the path filter. An edge with no
    // endpoint inside the filter must not appear.
    #[test]
    fn dependencies_omit_edges_wholly_outside_path_filter() {
        let (db, _temp, gv) = path_fixture();
        let mut cfg = default_config(gv);
        cfg.paths = Some(vec!["dirA".to_string()]);
        let text = build_repo_map(&db, &cfg).unwrap().text;
        let deps = section(&text, "## Dependencies");
        assert!(!deps.contains("dirC/"), "outside edge leaked:\n{deps}");
        // Unfiltered, the C -> B edge is present (the fixture is meaningful).
        let all = build_repo_map(&db, &default_config(gv)).unwrap().text;
        assert!(section(&all, "## Dependencies").contains("dirC/ → dirB/"));
    }

    // Issue #240: pins the chosen rule -- an edge with at least one endpoint
    // inside the filter is kept (in -> out and out -> in).
    #[test]
    fn dependencies_keep_edges_crossing_path_filter_boundary() {
        let (db, _temp, gv) = path_fixture();
        let mut cfg = default_config(gv);
        cfg.paths = Some(vec!["dirA".to_string()]);
        let text = build_repo_map(&db, &cfg).unwrap().text;
        let deps = section(&text, "## Dependencies");
        assert!(deps.contains("dirA/ → dirB/"), "{deps}");
        assert!(deps.contains("dirB/ → dirA/"), "{deps}");
        assert_eq!(deps.matches("\n- ").count(), 2, "{deps}");
    }

    // Issue #240: every section honours the filter, asserted per section.
    #[test]
    fn every_section_honours_path_filter() {
        let (db, _temp, gv) = path_fixture();
        let mut cfg = default_config(gv);
        cfg.paths = Some(vec!["dirA".to_string()]);
        let text = build_repo_map(&db, &cfg).unwrap().text;
        let modules = section(&text, "## Modules");
        assert!(modules.contains("dirA/"), "{modules}");
        assert!(
            !modules.contains("dirB/") && !modules.contains("dirC/"),
            "{modules}"
        );
        let deps = section(&text, "## Dependencies");
        assert!(!deps.contains("dirC/"), "{deps}");
        let keys = section(&text, "## Key Symbols (by fan-in)");
        assert!(!keys.contains("dirB/") && !keys.contains("dirC/"), "{keys}");
        assert!(!keys.contains("**fc**"), "{keys}");
        let patterns = section(&text, "## Patterns");
        assert!(patterns.contains("- function: 1"), "{patterns}");
    }

    fn big_fixture() -> (Db, TempDir, i64) {
        let (mut db, temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let names: Vec<(String, String)> = (0..25)
            .map(|i| (format!("module_dir_{i:02}"), format!("fn_{i:02}")))
            .collect();
        let funcs: Vec<(&str, &str)> = names
            .iter()
            .map(|(d, n)| (d.as_str(), n.as_str()))
            .collect();
        let calls: Vec<(&str, &str)> = (0..24).map(|i| (funcs[i].1, funcs[i + 1].1)).collect();
        build_graph(&mut db, gv, &funcs, &calls);
        (db, temp, gv)
    }

    // Issue #240: max_bytes is a hard ceiling, every cut lands on a line
    // boundary, and the cut is reported.
    #[test]
    fn output_never_exceeds_max_bytes_and_reports_truncation() {
        let (db, _temp, gv) = big_fixture();
        let full = build_repo_map(&db, &default_config(gv)).unwrap();
        assert!(!full.truncated);
        assert!(full.bytes > 1000, "fixture too small: {}", full.bytes);
        for budget in [0usize, 10, 40, 100, 250, 500, 1000, 1500] {
            let mut cfg = default_config(gv);
            cfg.max_bytes = budget;
            let r = build_repo_map(&db, &cfg).unwrap();
            assert!(
                r.text.len() <= budget,
                "budget {budget}: {} bytes",
                r.text.len()
            );
            assert_eq!(r.bytes, r.text.len());
            assert!(r.truncated, "budget {budget} should truncate");
            assert!(
                r.text.is_empty() || r.text.ends_with('\n'),
                "budget {budget} cut mid-line: {:?}",
                r.text
            );
            if budget >= 1000 {
                assert!(
                    r.text.contains("truncated"),
                    "no marker at {budget}:\n{}",
                    r.text
                );
            }
        }
    }

    // Issue #240: a budget that only fits part of the Modules section yields
    // whole lines only.
    #[test]
    fn partial_modules_section_has_only_complete_lines() {
        let (db, _temp, gv) = big_fixture();
        let mut cfg = default_config(gv);
        cfg.max_bytes = 300;
        let r = build_repo_map(&db, &cfg).unwrap();
        assert!(r.truncated);
        assert!(!section(&r.text, "## Modules").contains("## Dependencies"));
        let listed = r.text.lines().filter(|l| l.starts_with("- **")).count();
        assert!(listed > 0 && listed < 25, "{listed}:\n{}", r.text);
        for l in r.text.lines().filter(|l| l.starts_with("- **")) {
            assert!(l.ends_with(')'), "partial line {l:?}");
        }
        assert!(!r.text.contains("## Dependencies"));
    }

    // Issue #240: an unfiltered, generously budgeted map is unchanged.
    #[test]
    fn unfiltered_unbudgeted_output_is_unchanged() {
        let (db, _temp, gv) = path_fixture();
        let r = build_repo_map(&db, &default_config(gv)).unwrap();
        assert!(!r.truncated);
        let expected = "# Architecture Overview\n\n## Modules\n\
- **dirC/** (3 files, 3 symbols, python)\n\
- **dirB/** (2 files, 2 symbols, python)\n\
- **dirA/** (1 files, 1 symbols, python)\n\
\n## Dependencies\n\
- dirC/ → dirB/ (3 calls, 0 imports, 0 xrefs)\n\
- dirB/ → dirA/ (2 calls, 0 imports, 0 xrefs)\n\
- dirA/ → dirB/ (1 calls, 0 imports, 0 xrefs)\n\
\n## Key Symbols (by fan-in)\n\
\n### dirA/\n\
- function **fa** `` (fan-in: 2)\n\
\n### dirB/\n\
- function **fb** `` (fan-in: 4)\n\
\n## Patterns\n\
- function: 6\n";
        assert_eq!(r.text, expected);
    }
}
