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
  lidx request --method orient --params '{"view":"overview"}'
  lidx request --method search --params '{"query":"Indexer","limit":10}'
  lidx request --method trace_flow --params '{"start_qualname":"crate::indexer::Indexer::reindex","direction":"downstream","kinds":["CALLS"],"max_hops":1}'
  lidx request --method search --params '{"query":"def\\s+greet","context_lines":8}'
  lidx dispatch-pairs --repo .
  lidx graph --repo . --open
  lidx graph --repo . --seed crate::indexer::Indexer::reindex --direction down --depth 3
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
        /// Allow a scan that finds zero files (empties the existing index).
        #[arg(long)]
        allow_empty: bool,
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
    /// Print every (base member id, implementing member id) pair of the dispatch query as a JSON array.
    /// The type-hierarchy benchmark (benchmarks/oracle) grades these against compiler output.
    DispatchPairs {
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
    /// Show compact cross-file context for a file.
    Context {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Output format: text (default) or json.
        #[arg(long, default_value = "text")]
        format: String,
        /// File path to analyze (relative to repo root).
        path: String,
    },
    /// Write an interactive, self-contained HTML view of the indexed graph.
    ///
    /// Without --seed or --diff: a module (or file) overview of the whole
    /// repo with weighted edges. With --seed: the seeds' neighbourhood.
    /// Reads the existing index; never indexes.
    Graph {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Output HTML file.
        #[arg(long, default_value = "lidx-graph.html")]
        out: PathBuf,
        /// Open the written file in the default browser.
        #[arg(long)]
        open: bool,
        /// Seed symbol: a qualname or a search query (repeatable).
        #[arg(long = "seed", value_name = "QUALNAME_OR_QUERY")]
        seeds: Vec<String>,
        /// Hops to walk from the seeds (or the diff's changed symbols).
        #[arg(long, default_value_t = 2)]
        depth: usize,
        /// Walk direction from the seeds: up (callers), down (callees) or
        /// both. Default: both for --seed, up for --diff.
        #[arg(long, value_parser = ["up", "down", "both"])]
        direction: Option<String>,
        /// Edge kinds to show, comma-separated (default: all but CONTAINS
        /// and MODULE_FILE).
        #[arg(long, value_delimiter = ',')]
        kinds: Vec<String>,
        /// Languages to include, comma-separated.
        #[arg(long, value_delimiter = ',')]
        lang: Vec<String>,
        /// Cap on every node written to the page: the walk, the expandable
        /// halo, --diff callers, externals and bridge endpoints all count
        /// (default 500 for symbols, 5000 for file/module views). The page
        /// notes what was left out.
        #[arg(long)]
        max_nodes: Option<usize>,
        /// Node granularity. Default: symbol with --seed/--diff, module
        /// otherwise.
        #[arg(long, value_parser = ["symbol", "file", "module"])]
        level: Option<String>,
        /// Show the symbols changed against this git revision and their
        /// callers (via analyze_diff).
        #[arg(long, value_name = "REV")]
        diff: Option<String>,
        /// Cut module paths after this many directories (0: whole directory).
        #[arg(long, default_value_t = 0)]
        module_depth: usize,
        /// Files shown at first: code (default), tests, docs, examples or
        /// all. The rest is embedded and can be shown from the page.
        #[arg(long, value_parser = ["code", "tests", "docs", "examples", "all"])]
        scope: Option<String>,
        /// Name shown in the page header (default: the git remote's name).
        #[arg(long)]
        title: Option<String>,
    },
    /// Initialize lidx: index repo, install hooks, update gitignore.
    Init {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        db: Option<PathBuf>,
        /// Skip indexing.
        #[arg(long)]
        skip_index: bool,
        /// Skip hook installation.
        #[arg(long)]
        skip_hooks: bool,
    },
}

impl Command {
    /// The `--repo` path of any subcommand (every subcommand takes one).
    pub fn repo(&self) -> &std::path::Path {
        match self {
            Command::Serve { repo, .. }
            | Command::Reindex { repo, .. }
            | Command::ChangedFiles { repo, .. }
            | Command::Overview { repo, .. }
            | Command::DispatchPairs { repo, .. }
            | Command::Request { repo, .. }
            | Command::McpServe { repo, .. }
            | Command::Context { repo, .. }
            | Command::Graph { repo, .. }
            | Command::Init { repo, .. } => repo,
        }
    }
}
