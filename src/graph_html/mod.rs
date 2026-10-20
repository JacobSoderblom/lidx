//! `lidx graph`: one self-contained, offline HTML file showing a scoped or
//! aggregated view of the indexed graph.
//!
//! The page is `template.html` with `viewer.css`, `viewer.js` and the graph
//! document (see [`doc`]) inlined; it makes no network requests. The graph
//! is read from the existing index and never re-indexed.

pub mod apps;
pub mod build;
pub mod doc;
pub mod glance;
pub mod units;
pub mod zoom;

pub use build::{Direction, Level};
pub use doc::Counts;

use crate::db::Db;
use crate::indexer::{Indexer, scan};
use crate::resolve::{QueryResolution, SymbolRef, resolve_symbol, resolve_symbol_with_candidates};
use crate::search::SearchScope;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

const TEMPLATE: &str = include_str!("template.html");
const CSS: &str = include_str!("viewer.css");
const JS: &str = include_str!("viewer.js");

/// Default `--max-nodes` for a seeded symbol graph.
pub const DEFAULT_MAX_SYMBOLS: usize = 500;
/// Default `--max-nodes` for a file/module graph (leaves).
pub const DEFAULT_MAX_LEAVES: usize = 5_000;
/// Default `--max-symbols`: symbols embedded for zooming into app cards.
pub const DEFAULT_MAX_ZOOM_SYMBOLS: usize = 60_000;

#[derive(Clone, Debug)]
pub struct GraphOptions {
    pub seeds: Vec<String>,
    pub depth: usize,
    /// `None`: `both` for seeds, `up` (callers) for `--diff`.
    pub direction: Option<Direction>,
    pub kinds: Option<Vec<String>>,
    pub languages: Option<Vec<String>>,
    pub max_nodes: Option<usize>,
    /// Symbols embedded for the app view's zoom. `None`:
    /// [`DEFAULT_MAX_ZOOM_SYMBOLS`].
    pub max_symbols: Option<usize>,
    /// `None`: `symbol` with seeds or a diff, `app` without.
    pub level: Option<Level>,
    /// Git revision to diff the working tree against.
    pub diff: Option<String>,
    pub module_depth: usize,
    /// Files shown at first; the rest is embedded but hidden. `None`: code.
    pub scope: Option<SearchScope>,
    /// Name in the page header and title. `None`: the repo's git remote
    /// name, else its top-level folder name.
    pub title: Option<String>,
}

impl Default for GraphOptions {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            depth: 2,
            direction: None,
            kinds: None,
            languages: None,
            max_nodes: None,
            max_symbols: None,
            level: None,
            diff: None,
            module_depth: 0,
            scope: None,
            title: None,
        }
    }
}

/// A generated page.
pub struct Generated {
    pub html: String,
    pub counts: Counts,
    pub scope: String,
    /// For stderr: e.g. a `--seed` that matched several symbols equally well.
    pub warnings: Vec<String>,
}

/// Build the page for `opts` from the index at `db_path`.
pub fn generate(repo_root: &Path, db_path: &Path, opts: &GraphOptions) -> Result<Generated> {
    if !db_path.exists() {
        bail!(
            "no lidx index at {}; run `lidx reindex --repo {}` first (`lidx graph` reads an existing index and never indexes)",
            db_path.display(),
            repo_root.display()
        );
    }
    let kinds = match &opts.kinds {
        Some(k) if !k.is_empty() => Some(crate::rpc::normalize_edge_kinds(k)?),
        _ => None,
    };
    let languages = scan::normalize_language_filter(opts.languages.as_deref())?;

    // `--diff` needs an indexer for analyze_diff; everything else a Db.
    let mut diff_scope: Option<(Vec<i64>, Vec<i64>)> = None;
    let indexer;
    let plain_db;
    let db: &Db = if let Some(rev) = &opts.diff {
        let mut ix = Indexer::new_with_options(
            repo_root.to_path_buf(),
            db_path.to_path_buf(),
            scan::ScanOptions::default(),
        )?;
        ensure_indexed(ix.db(), db_path)?;
        diff_scope = Some(diff_symbols(&mut ix, repo_root, rev, opts, &languages)?);
        indexer = ix;
        indexer.db()
    } else {
        plain_db = Db::new(db_path)?;
        ensure_indexed(&plain_db, db_path)?;
        &plain_db
    };

    let graph_version = db.current_graph_version()?;
    let ctx = build::Ctx {
        db,
        graph_version,
        languages: languages.clone(),
        filter: build::KindFilter {
            include: kinds.as_ref().map(|k| k.iter().cloned().collect()),
        },
        module_depth: opts.module_depth,
        file_lang: build::file_languages(db, graph_version)?,
        scope: opts.scope.unwrap_or(SearchScope::Code),
    };

    let seeded = !opts.seeds.is_empty() || diff_scope.is_some();
    let level = opts
        .level
        .unwrap_or(if seeded { Level::Symbol } else { Level::App });
    if seeded && level == Level::App {
        bail!(
            "--level app is the whole-repo overview; with --seed or --diff use --level symbol, file or module (seeds are coloured by their app)"
        );
    }
    let mut scope_parts: Vec<String> = Vec::new();
    let mut walk_direction = None;
    let mut warnings: Vec<String> = Vec::new();
    let graph = if seeded {
        let (seeds, extra, direction) = if let Some((changed, callers)) = diff_scope {
            let rev = opts.diff.as_deref().unwrap_or_default();
            scope_parts.push(format!("diff {rev}: {} changed symbols", changed.len()));
            (changed, callers, opts.direction.unwrap_or(Direction::Up))
        } else {
            let mut ids = Vec::new();
            for seed in &opts.seeds {
                let resolved = resolve_symbol(
                    db,
                    SymbolRef::Qualname(seed.clone()),
                    languages.as_deref(),
                    graph_version,
                )
                .with_context(|| format!("resolve --seed {seed:?}"))?;
                // A tie at the best match tier is a guess: say which one
                // was taken and what else matched.
                if let QueryResolution::Ambiguous(tied) = resolve_symbol_with_candidates(
                    db,
                    SymbolRef::Qualname(seed.clone()),
                    languages.as_deref(),
                    graph_version,
                )? {
                    let others: Vec<String> = tied
                        .iter()
                        .filter(|s| s.id != resolved.symbol.id)
                        .take(5)
                        .map(|s| {
                            format!(
                                "{} ({}:{})",
                                printable(&s.qualname),
                                printable(&s.file_path),
                                s.start_line
                            )
                        })
                        .collect();
                    if !others.is_empty() {
                        warnings.push(format!(
                            "--seed {seed:?} is ambiguous: using {}; also matched {}{}",
                            printable(&resolved.symbol.qualname),
                            others.join(", "),
                            if tied.len() > others.len() + 1 {
                                ", ..."
                            } else {
                                ""
                            }
                        ));
                    }
                }
                ids.push(resolved.symbol.id);
            }
            let names: Vec<String> = db
                .symbols_by_ids(&ids, None, graph_version)?
                .into_iter()
                .map(|s| s.qualname)
                .collect();
            scope_parts.push(format!("seed {}", names.join(", ")));
            (ids, Vec::new(), opts.direction.unwrap_or(Direction::Both))
        };
        scope_parts.push(format!("depth {}", opts.depth));
        scope_parts.push(direction.as_str().to_string());
        walk_direction = Some(direction.as_str().to_string());
        let scope = build::SymbolScope {
            seeds,
            extra,
            depth: opts.depth,
            direction,
            max_nodes: opts.max_nodes.unwrap_or(DEFAULT_MAX_SYMBOLS).max(1),
        };
        let graph = build::symbol_graph(&ctx, &scope)?;
        let graph = if level == Level::Symbol {
            graph
        } else {
            scope_parts.push(format!("{} level", level.as_str()));
            build::aggregate_symbol_graph(&ctx, graph, level)
        };
        let apps = apps::apps_json(&ctx, repo_root, &graph, None, None, None)?;
        (graph, apps)
    } else {
        if level == Level::Symbol {
            bail!(
                "--level symbol needs --seed or --diff: a whole-repo symbol graph is unreadable; use --level file or module"
            );
        }
        scope_parts.push(if level == Level::App {
            "whole repo".to_string()
        } else {
            format!("whole repo, {} level", level.as_str())
        });
        let files = build::repo_files(&ctx)?;
        let rows = build::repo_rows(&ctx)?;
        // The app view sits on the module graph, so Folders and Files are
        // one click away in the page.
        let graph_level = if level == Level::App {
            Level::Module
        } else {
            level
        };
        let graph = build::repo_graph_from(
            &ctx,
            graph_level,
            opts.max_nodes.unwrap_or(DEFAULT_MAX_LEAVES).max(1),
            &files,
            &rows,
        )?;
        // The sheet's cards open in place down to symbols.
        let zoom =
            (level == Level::App).then(|| opts.max_symbols.unwrap_or(DEFAULT_MAX_ZOOM_SYMBOLS));
        let apps = apps::apps_json(&ctx, repo_root, &graph, Some(&files), Some(&rows), zoom)?;
        if let Some(note) = apps.pointer("/zoom/note").and_then(Value::as_str) {
            warnings.push(note.to_string());
        }
        (graph, apps)
    };
    let (graph, apps) = graph;
    if opts.max_symbols.is_some() && apps.get("zoom").is_none() {
        warnings.push(
            "--max-symbols applies to the whole-repo app view only (its cards zoom down to symbols); ignored here"
                .to_string(),
        );
    }
    if let Some(k) = &kinds {
        scope_parts.push(format!("kinds {}", k.join(",")));
    }
    if let Some(l) = &languages {
        scope_parts.push(format!("lang {}", l.join(",")));
    }
    let file_scope = ctx.scope.as_str();
    scope_parts.push(format!("{file_scope} files"));
    let scope = scope_parts.join(" · ");

    let repo_name = match &opts.title {
        Some(t) if !t.trim().is_empty() => t.trim().to_string(),
        _ => repo_display_name(repo_root),
    };
    let meta = doc::Meta {
        repo: repo_name,
        file_scope: file_scope.to_string(),
        direction: walk_direction,
        commit: db.graph_version_commit(graph_version)?,
        graph_version,
        level: level.as_str().to_string(),
        scope: scope.clone(),
        generated_by: format!("lidx {}", env!("CARGO_PKG_VERSION")),
        generated_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    };
    let mut graph = graph;
    graph.notes.extend(warnings.iter().cloned());
    let counts = doc::counts(&graph);
    let mut document = doc::to_json(&graph, &meta);
    document["apps"] = apps;
    let title = format!("lidx graph · {}", meta.repo);
    Ok(Generated {
        html: render_html(&title, &document)?,
        counts,
        scope,
        warnings,
    })
}

/// The repository's name: its `origin` remote's last path segment (so a
/// worktree or a clone in an oddly named folder still reads `lidx`), else
/// the name of its git top-level folder, else of `repo_root` itself.
pub fn repo_display_name(repo_root: &Path) -> String {
    let git = |args: &[&str]| -> Option<String> {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(args)
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
        (out.status.success() && !text.is_empty()).then_some(text)
    };
    let folder = |p: &Path| p.file_name().map(|n| n.to_string_lossy().into_owned());
    if let Some(url) = git(&["remote", "get-url", "origin"]) {
        let name = url
            .trim_end_matches('/')
            .rsplit(['/', ':'])
            .next()
            .unwrap_or("")
            .trim_end_matches(".git");
        if !name.is_empty() {
            return name.to_string();
        }
    }
    if let Some(top) = git(&["rev-parse", "--show-toplevel"])
        && let Some(name) = folder(Path::new(&top))
    {
        return name;
    }
    let root = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.to_path_buf());
    folder(&root).unwrap_or_else(|| "repo".to_string())
}

/// `s` with control characters (ESC, CR, BEL, DEL, ...) escaped, so a crafted
/// qualname or path printed to a terminal cannot carry escape sequences.
fn printable(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_control() {
                c.escape_debug().to_string()
            } else {
                c.to_string()
            }
        })
        .collect()
}

/// An index that exists but holds nothing is as unusable as a missing one.
fn ensure_indexed(db: &Db, db_path: &Path) -> Result<()> {
    let files: i64 = db
        .read_conn()?
        .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))?;
    if files == 0 {
        bail!(
            "the lidx index at {} is empty; run `lidx reindex` first",
            db_path.display()
        );
    }
    Ok(())
}

/// `(changed symbols, their callers)` for the working tree against `rev`,
/// from `analyze_diff`.
fn diff_symbols(
    indexer: &mut Indexer,
    repo_root: &Path,
    rev: &str,
    opts: &GraphOptions,
    languages: &Option<Vec<String>>,
) -> Result<(Vec<i64>, Vec<i64>)> {
    if rev.starts_with('-') {
        bail!("--diff takes a revision, not an option: {rev:?}");
    }
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["diff", "--no-color", "--no-ext-diff", rev, "--"])
        .output()
        .context("run git diff")?;
    if !out.status.success() {
        bail!(
            "git diff {rev} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let diff = String::from_utf8_lossy(&out.stdout);
    if diff.trim().is_empty() {
        bail!("no changes against {rev}");
    }
    let mut params = json!({
        "diff": diff,
        "max_depth": opts.depth.clamp(1, 5),
        "include_tests": false,
        "include_risk": false,
        "max_bytes": 200_000,
    });
    if let Some(langs) = languages {
        params["languages"] = json!(langs);
    }
    let result = crate::rpc::analyze_diff_value(indexer, params)?;
    let ids = |key: &str| -> Vec<i64> {
        let mut seen = HashSet::new();
        result
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|e| e.pointer("/symbol/id").and_then(Value::as_i64))
            .filter(|id| seen.insert(*id))
            .collect()
    };
    let changed = ids("changed_symbols");
    if changed.is_empty() {
        bail!("the diff against {rev} touches no indexed symbol");
    }
    Ok((changed, ids("upstream")))
}

/// The page: the template with styles, script and the escaped document.
pub fn render_html(title: &str, document: &Value) -> Result<String> {
    let json = serde_json::to_string(document)?;
    let html = TEMPLATE
        .replace("__TITLE__", &escape_html(title))
        .replace("/*__CSS__*/", CSS)
        .replace("/*__JS__*/", JS);
    // Data last, so nothing in it is ever scanned for a placeholder.
    Ok(html.replacen("__DATA__", &script_safe_json(&json), 1))
}

/// `json` made safe to embed in a `<script type="application/json">` block:
/// `<`, `>` and `&` become `<`/`>`/`&` (so no `</script>` or
/// `<!--` can end or confuse the block) and U+2028/U+2029 are escaped. In
/// serialized JSON those characters only occur inside strings, where the
/// escapes decode back to the same text.
pub fn script_safe_json(json: &str) -> String {
    let mut out = String::with_capacity(json.len() + 64);
    for c in json.chars() {
        match c {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c => out.push(c),
        }
    }
    out
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// The JSON document embedded in a page written by [`render_html`].
pub fn extract_document(html: &str) -> Result<Value> {
    let start_tag = r#"<script type="application/json" id="lidx-graph-data">"#;
    let start = html
        .find(start_tag)
        .context("no graph data block in page")?
        + start_tag.len();
    let end = start
        + html[start..]
            .find("</script>")
            .context("unterminated graph data block")?;
    Ok(serde_json::from_str(&html[start..end])?)
}

/// Open `path` in the default browser.
pub fn open_in_browser(path: &Path) -> Result<()> {
    let path: PathBuf = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let mut cmd = if cfg!(target_os = "macos") {
        std::process::Command::new("open")
    } else if cfg!(windows) {
        let mut c = std::process::Command::new("cmd");
        c.args(["/C", "start", ""]);
        c
    } else {
        std::process::Command::new("xdg-open")
    };
    let status = cmd.arg(&path).status().context("launch browser")?;
    if !status.success() {
        bail!("could not open {} in a browser", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn printable_escapes_control_characters() {
        assert_eq!(printable("a\u{1b}[31mb\r\u{7f}"), "a\\u{1b}[31mb\\r\\u{7f}");
        assert_eq!(printable("pkg/ü.rs"), "pkg/ü.rs");
    }

    #[test]
    fn viewer_assets_cannot_end_their_script_block() {
        assert!(!JS.to_ascii_lowercase().contains("</script"));
        assert!(!CSS.to_ascii_lowercase().contains("</style"));
        for placeholder in ["__TITLE__", "/*__CSS__*/", "/*__JS__*/", "__DATA__"] {
            assert!(TEMPLATE.contains(placeholder), "{placeholder}");
        }
    }

    #[test]
    fn script_safe_json_round_trips_hostile_strings() {
        let hostile = "</script><script>alert(1)</script> <!-- x --> a&b \u{2028}\u{2029} </SCRIPT";
        let doc = json!({ "s": [hostile] });
        let html = render_html("t</title>", &doc).unwrap();
        let body = &html[html.find("lidx-graph-data").unwrap()..];
        let block = &body[..body.find("</script>").unwrap()];
        for bad in ["</", "<!--", "\u{2028}", "\u{2029}"] {
            assert!(!block.contains(bad), "{bad:?} leaked into the data block");
        }
        assert!(!html.contains("t</title>"));
        assert_eq!(extract_document(&html).unwrap(), doc);
    }
}
