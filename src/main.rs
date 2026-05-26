use anyhow::Result;
use clap::Parser;
use lidx::{cli, context, db, indexer, init, mcp, rpc, watch};
use serde_json;
use std::path::PathBuf;

fn default_db_path(repo: &PathBuf) -> PathBuf {
    repo.join(".lidx").join(".lidx.sqlite")
}

fn main() -> Result<()> {
    let args = cli::Args::parse();

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
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let mut indexer = indexer::Indexer::new_with_options(
                repo,
                db_path,
                indexer::scan::ScanOptions::new(no_ignore),
            )?;
            let stats = indexer.reindex()?;
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
