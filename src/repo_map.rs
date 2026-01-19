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
        writeln!(
            out,
            "- **{}/** ({} files, {} symbols, {})",
            m.path, m.file_count, m.symbol_count, dominant_language
        )?;
    }

    // Phase 2: Inter-module edges
    if out.len() + 200 < budget {
        let edges = db.module_edges(1, config.languages.as_deref(), config.graph_version)?;
        if !edges.is_empty() {
            writeln!(out, "\n## Dependencies")?;
            for e in edges.iter().take(20) {
                writeln!(
                    out,
                    "- {} → {} ({} calls, {} imports)",
                    e.0, e.1, e.2, e.3
                )?;
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
