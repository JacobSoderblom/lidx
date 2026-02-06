# lidx

Code indexer with MCP server for LLM-assisted code navigation.

lidx indexes your codebase into a symbol graph with cross-language references, then serves queries over [MCP](https://modelcontextprotocol.io) so AI coding assistants can navigate your code intelligently — finding callers, tracing data flow across service boundaries, and understanding impact before making changes.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/JacobSoderblom/lidx/main/install.sh | bash
```

Downloads a prebuilt binary for your platform (macOS/Linux, x64/ARM64) to `~/.local/bin`.

## Setup

Add `.lidx` to your repo's `.gitignore`:

```bash
echo ".lidx" >> .gitignore
```

Add lidx to your repo's `.mcp.json`:

```json
{
  "mcpServers": {
    "lidx": {
      "command": "lidx",
      "args": ["mcp-serve", "--repo", "."]
    }
  }
}
```

That's it. Your AI assistant can now query your codebase through lidx.

## What it does

lidx builds a symbol graph from your code and exposes it through a single MCP tool (`lidx_query`) with ~30 query methods. The graph captures:

- **Symbols** — functions, classes, methods, modules, routes, SQL tables
- **Edges** — CALLS, IMPORTS, INHERITS, TYPE_REF, HTTP_CALL/HTTP_ROUTE, RPC_CALL/RPC_IMPL, CHANNEL_PUBLISH/CHANNEL_SUBSCRIBE
- **Cross-language references** — C# calling SQL stored procedures, C# referencing Python classes, gRPC service implementations across languages

### Supported languages

Python, C#, TypeScript, JavaScript, Rust, Go, SQL, PostgreSQL (PL/pgSQL), Proto, Markdown

### Key capabilities

**Navigation** — find symbols, jump to definitions, explore neighbors, trace call chains

**Impact analysis** — multi-layer analysis (direct graph + test coverage + git co-change history) with confidence scoring. Understands what changes when you modify a symbol.

**Cross-service tracing** — automatically bridges service boundaries via gRPC, HTTP routes, and message bus channels (Azure Service Bus, RabbitMQ). Trace a request from API endpoint through message queue to background worker across languages.

**Context assembly** — `gather_context` assembles LLM-ready context from symbols, files, and search queries within a byte budget. One call gives your AI assistant exactly the code it needs.

**Incremental indexing** — watches for file changes and re-indexes automatically. No manual reindex needed during development.

## Query methods

All queries go through the `lidx_query` MCP tool with a `method` and optional `params`.

| Category | Methods |
|----------|---------|
| **Discovery** | `help`, `list_methods`, `list_languages`, `repo_overview`, `repo_insights` |
| **Search** | `find_symbol`, `suggest_qualnames`, `search_text`, `search_rg`, `grep` |
| **Navigation** | `open_symbol`, `open_file`, `neighbors`, `subgraph`, `references` |
| **Analysis** | `gather_context`, `analyze_impact`, `analyze_diff`, `trace_flow` |
| **Routes** | `route_refs`, `flow_status` |
| **Index** | `changed_files`, `index_status`, `reindex` |
| **Quality** | `top_complexity`, `duplicate_groups`, `diagnostics_run`, `diagnostics_list`, `diagnostics_summary` |

Every result includes `next_hops` — pre-computed follow-up queries so your AI assistant knows what to explore next.

## CLI usage

```bash
# Index a repo
lidx reindex --repo /path/to/repo

# Run MCP server (used by .mcp.json)
lidx mcp-serve --repo .

# Run JSONL RPC server
lidx serve --repo .

# One-off query
lidx request --method find_symbol --params '{"query":"MyClass"}'
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LIDX_SEARCH_TIMEOUT_SECS` | `30` | Ripgrep search timeout |
| `LIDX_PATTERN_MAX_LENGTH` | `10000` | Max search pattern length in bytes |
| `LIDX_POOL_SIZE` | `10` | SQLite read connection pool size |
| `LIDX_POOL_MIN_IDLE` | `2` | Minimum idle connections |

## Ignore rules

By default lidx respects `.gitignore`. Use `--no-ignore` with `reindex`, `serve`, or `mcp-serve` to include ignored files. For searches, pass `no_ignore: true` to `search_text`, `grep`, or `search_rg`.

## License

MIT
