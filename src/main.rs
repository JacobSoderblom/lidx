use anyhow::Result;
use clap::Parser;
use lidx::{cli, db, diagnostics, indexer, mcp, rpc, watch};
use serde_json::{Value, json};
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
        cli::Command::DiagnosticsImport { repo, db, path } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let abs = if path.is_absolute() {
                path
            } else {
                repo.join(path)
            };
            let content = std::fs::read_to_string(&abs)?;
            let diagnostics = diagnostics::parse_sarif(&content, &repo)?;
            let mut db = db::Db::new(&db_path)?;
            let imported = db.insert_diagnostics(&diagnostics)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "imported": imported }))?
            );
            Ok(())
        }
        cli::Command::DiagnosticsRun {
            repo,
            db,
            languages,
            tools,
            output_dir,
        } => {
            let db_path = db.unwrap_or_else(|| default_db_path(&repo));
            let mut indexer = indexer::Indexer::new_with_options(
                repo.clone(),
                db_path,
                indexer::scan::ScanOptions::default(),
            )?;
            let mut params = serde_json::Map::new();
            if !languages.is_empty() {
                params.insert("languages".to_string(), json!(languages));
            }
            if !tools.is_empty() {
                params.insert("tools".to_string(), json!(tools));
            }
            if let Some(dir) = output_dir {
                params.insert(
                    "output_dir".to_string(),
                    json!(dir.to_string_lossy().to_string()),
                );
            }
            let result =
                rpc::handle_method(&mut indexer, "diagnostics_run", Value::Object(params))?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
    }
}
