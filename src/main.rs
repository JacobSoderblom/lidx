use anyhow::Result;
use clap::Parser;
use lidx::{cli, context, db, graph_html, indexer, init, mcp, rpc, watch};
use std::path::{Path, PathBuf};

fn default_db_path(repo: &Path) -> PathBuf {
    repo.join(".lidx").join(".lidx.sqlite")
}

/// `lidx reindex` exit status when another reindex holds the lock (EX_TEMPFAIL).
const REINDEX_BUSY_EXIT_CODE: i32 = 75;

fn main() -> Result<()> {
    let args = cli::Args::parse();

    // Trust-boundary check: runs before any DB connection or directory creation.
    indexer::validate_repo_root(args.command.repo())?;

    match args.command {
        cli::Command::Serve {
            repo,
            db,
            no_ignore,
            watch: watch_mode,
            watch_debounce_ms,
            watch_fallback_secs,
            watch_batch_max,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let watch_config = watch::WatchConfig::new(
                watch_mode,
                watch_debounce_ms,
                watch_fallback_secs,
                watch_batch_max,
                no_ignore,
            );
            rpc::serve(repo, db_path, watch_config)
        }
        cli::Command::Reindex {
            repo,
            db,
            no_ignore,
            allow_empty,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let mut indexer = indexer::Indexer::new_with_options(
                repo,
                db_path,
                indexer::scan::ScanOptions::new(no_ignore),
            )?;
            let stats = match indexer.reindex_with_options(allow_empty) {
                Ok(stats) => stats,
                Err(err) if err.downcast_ref::<db::ReindexBusy>().is_some() => {
                    // Distinct from a generic failure (1): retry later.
                    eprintln!("Error: {err}");
                    std::process::exit(REINDEX_BUSY_EXIT_CODE);
                }
                Err(err) => return Err(err),
            };
            println!("{}", serde_json::to_string_pretty(&stats)?);
            Ok(())
        }
        cli::Command::ChangedFiles {
            repo,
            db,
            no_ignore,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let mut indexer = indexer::Indexer::new_with_options(
                repo,
                db_path,
                indexer::scan::ScanOptions::new(no_ignore),
            )?;
            let changed = indexer.changed_files(None)?;
            println!("{}", serde_json::to_string_pretty(&changed)?);
            Ok(())
        }
        cli::Command::Overview { repo, db } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let db = db::Db::new(&db_path)?;
            let graph_version = db.current_graph_version()?;
            let overview = db.repo_overview(repo, None, graph_version)?;
            println!("{}", serde_json::to_string_pretty(&overview)?);
            Ok(())
        }
        cli::Command::DispatchPairs { repo, db } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let db = db::Db::new(&db_path)?;
            let graph_version = db.current_graph_version()?;
            let pairs = db.all_dispatch_pairs(graph_version)?;
            println!("{}", serde_json::to_string(&pairs)?);
            Ok(())
        }
        cli::Command::Request {
            repo,
            db,
            method,
            params,
            params_file,
            id,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let params_raw = if let Some(path) = params_file {
                std::fs::read_to_string(&path)?
            } else {
                params
            };
            let response = rpc::call(repo, db_path, method, &params_raw, &id)?;
            println!("{response}");
            Ok(())
        }
        cli::Command::McpServe {
            repo,
            db,
            no_ignore,
            watch: watch_mode,
            watch_debounce_ms,
            watch_fallback_secs,
            watch_batch_max,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let watch_config = watch::WatchConfig::new(
                watch_mode,
                watch_debounce_ms,
                watch_fallback_secs,
                watch_batch_max,
                no_ignore,
            );
            mcp::serve(repo, db_path, watch_config)
        }
        cli::Command::Context {
            repo,
            db,
            format,
            path,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            if !db_path.exists() {
                // No index yet — silent exit for hooks
                return Ok(());
            }
            let db = db::Db::new(&db_path)?;
            let graph_version = db.current_graph_version()?;
            let ctx = context::build_file_context(&db, &repo, &path, graph_version)?;
            match format.as_str() {
                "json" => {
                    let json = context::format_json(&ctx);
                    println!("{}", serde_json::to_string_pretty(&json)?);
                }
                _ => {
                    let text = context::format_text(&ctx);
                    if !text.is_empty() {
                        print!("{text}");
                    }
                }
            }
            Ok(())
        }
        cli::Command::Graph {
            repo,
            db,
            out,
            open,
            seeds,
            depth,
            direction,
            kinds,
            lang,
            max_nodes,
            level,
            diff,
            module_depth,
            scope,
            title,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let opts = graph_html::GraphOptions {
                seeds,
                depth,
                direction: direction.as_deref().map(|d| match d {
                    "up" => graph_html::Direction::Up,
                    "down" => graph_html::Direction::Down,
                    _ => graph_html::Direction::Both,
                }),
                kinds: (!kinds.is_empty()).then_some(kinds),
                languages: (!lang.is_empty()).then_some(lang),
                max_nodes,
                level: level.as_deref().map(|l| match l {
                    "symbol" => graph_html::Level::Symbol,
                    "file" => graph_html::Level::File,
                    _ => graph_html::Level::Module,
                }),
                diff,
                module_depth,
                scope: lidx::search::parse_scope(scope.as_deref())?,
                title,
            };
            let started = std::time::Instant::now();
            let page = graph_html::generate(&repo, &db_path, &opts)?;
            std::fs::write(&out, &page.html)
                .map_err(|e| anyhow::anyhow!("write {}: {e}", out.display()))?;
            println!(
                "wrote {} ({} nodes, {} edges, {} KB) in {} ms -- {}",
                out.display(),
                page.counts.nodes,
                page.counts.edges,
                page.html.len().div_ceil(1024),
                started.elapsed().as_millis(),
                page.scope
            );
            if open {
                graph_html::open_in_browser(&out)?;
            }
            Ok(())
        }
        cli::Command::Init {
            repo,
            db,
            skip_index,
            skip_hooks,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            init::run_init(&repo, &db_path, skip_index, skip_hooks)
        }
    }
}
