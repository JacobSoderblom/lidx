use crate::watch::WatchMode;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "lidx",
    version,
    about = "Code indexer v1",
    after_help = r#"Examples:
  lidx reindex --repo .
  lidx request --method repo_overview --params '{"summary":true}'
  lidx request --method list_languages --params '{}'
  lidx request --method search --params '{"query":"Indexer","limit":10}'
  lidx request --method references --params '{"qualname":"crate::indexer::Indexer::reindex","direction":"out","kinds":["CALLS"]}'
  lidx request --method search_rg --params '{"query":"def\\s+greet","context_lines":8}'
  lidx serve --repo . --watch auto
  lidx mcp-serve --repo .
"#
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Run JSONL RPC server over stdin/stdout.
    Serve {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Include files ignored by .gitignore.
        #[arg(long)]
        no_ignore: bool,
        /// File watch mode: auto|on|off.
        #[arg(long, default_value = "auto")]
        watch: WatchMode,
        /// Debounce window for filesystem events in milliseconds.
        #[arg(long, default_value_t = 300)]
        watch_debounce_ms: u64,
        /// Fallback full-scan interval in seconds when watch is unavailable.
        #[arg(long, default_value_t = 300)]
        watch_fallback_secs: u64,
        /// Trigger a full reindex when a batch exceeds this many paths.
        #[arg(long, default_value_t = 1000)]
        watch_batch_max: usize,
    },
    /// Reindex repository once and exit.
    Reindex {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Include files ignored by .gitignore.
        #[arg(long)]
        no_ignore: bool,
    },
    /// Show changed files compared to DB state.
    ChangedFiles {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Include files ignored by .gitignore.
        #[arg(long)]
        no_ignore: bool,
    },
    /// Print a repository overview.
    Overview {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
    },
    /// Run a single JSONL request and exit.
    Request {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long)]
        method: String,
        #[arg(long, default_value = "{}")]
        params: String,
        #[arg(long, value_name = "PATH")]
        params_file: Option<PathBuf>,
        #[arg(long, default_value = "1")]
        id: String,
    },
    /// Run MCP server over stdio.
    McpServe {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Include files ignored by .gitignore.
        #[arg(long)]
        no_ignore: bool,
        /// File watch mode: auto|on|off.
        #[arg(long, default_value = "auto")]
        watch: WatchMode,
        /// Debounce window for filesystem events in milliseconds.
        #[arg(long, default_value_t = 300)]
        watch_debounce_ms: u64,
        /// Fallback full-scan interval in seconds when watch is unavailable.
        #[arg(long, default_value_t = 300)]
        watch_fallback_secs: u64,
        /// Trigger a full reindex when a batch exceeds this many paths.
        #[arg(long, default_value_t = 1000)]
        watch_batch_max: usize,
    },
    /// Import SARIF diagnostics into the database.
    DiagnosticsImport {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long)]
        path: PathBuf,
    },
    /// Run diagnostics tools and import SARIF into the database.
    DiagnosticsRun {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Restrict diagnostics to specific languages.
        #[arg(long = "language", value_delimiter = ',')]
        languages: Vec<String>,
        /// Restrict diagnostics to specific tools.
        #[arg(long = "tool", value_delimiter = ',')]
        tools: Vec<String>,
        /// Output directory for generated SARIF files.
        #[arg(long)]
        output_dir: Option<PathBuf>,
    },
}
