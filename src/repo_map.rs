use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Write;

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
}

pub fn build_repo_map(db: &Db, config: &RepoMapConfig) -> Result<RepoMapResult> {
    let budget = config.max_bytes;
    let mut out = String::new();
    let mut total_symbols = 0;

    // Phase 1: Module summary from module_map(depth=1)
    let modules = db.module_summary(
        1,
        config.languages.as_deref(),
        config.paths.as_deref(),
        config.graph_version,
    )?;

    writeln!(out, "# Architecture Overview\n")?;
    writeln!(out, "## Modules")?;
    for m in &modules {
        let dominant_language = if m.languages.is_empty() {
            "unknown".to_string()
        } else {
            m.languages.join(",")
        };
        // `m.path` (from `module_summary`/`module_prefix`) already carries
        // a trailing separator -- see the "Dependencies" section below,
        // which relies on the same contract -- so it is not added again
        // here. Issue #134: doing so produced doubled separators like
        // "py//" for any module below the repo root.
        writeln!(
            out,
            "- **{}** ({} files, {} symbols, {})",
            m.path, m.file_count, m.symbol_count, dominant_language
        )?;
    }

    // Phase 2: Inter-module edges
    if out.len() + 200 < budget {
        let edges = db.module_edges(1, config.languages.as_deref(), config.graph_version)?;
        if !edges.is_empty() {
            writeln!(out, "\n## Dependencies")?;
            for e in edges.iter().take(20) {
                writeln!(out, "- {} → {} ({} calls, {} imports)", e.0, e.1, e.2, e.3)?;
            }
        }
    }

    // Phase 3: Top symbols per module by fan-in
    if out.len() + 200 < budget {
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

        writeln!(out, "\n## Key Symbols (by fan-in)")?;
        let mut sorted_modules: Vec<_> = by_module.keys().cloned().collect();
        sorted_modules.sort();
        for module in sorted_modules {
            if out.len() + 100 > budget {
                break;
            }
            if let Some(syms) = by_module.get(&module) {
                writeln!(out, "\n### {}/", module)?;
                for (sym, count) in syms.iter().take(5) {
                    let line = format!(
                        "- {} **{}** `{}` (fan-in: {})\n",
                        sym.kind,
                        sym.name,
                        sym.signature.as_deref().unwrap_or(""),
                        count
                    );
                    if out.len() + line.len() > budget {
                        break;
                    }
                    out.push_str(&line);
                    total_symbols += 1;
                }
            }
        }
    }

    // Phase 4: Patterns (if budget remains)
    if out.len() + 200 < budget {
        let kinds = db.count_symbols_by_kind(
            config.languages.as_deref(),
            config.paths.as_deref(),
            config.graph_version,
        )?;
        writeln!(out, "\n## Patterns")?;
        for (kind, count) in &kinds {
            if out.len() + 50 > budget {
                break;
            }
            writeln!(out, "- {}: {}", kind, count)?;
        }
    }

    let bytes = out.len();
    Ok(RepoMapResult {
        text: out,
        modules: modules.len(),
        symbols: total_symbols,
        bytes,
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
}
