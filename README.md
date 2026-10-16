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

The install script automatically registers lidx as a global MCP server. If you need to do it manually:

```bash
claude mcp add --transport stdio -s user lidx -- lidx mcp-serve
```

That's it. lidx is now available in all Claude Code sessions, auto-detecting the project from your working directory.

## What it does

lidx builds a symbol graph from your code and exposes it through a single MCP tool (`lidx`) with ~30 query methods. The graph captures:

- **Symbols** — functions, classes, methods, modules, routes, SQL tables
- **Edges** — CALLS, IMPORTS, INHERITS, TYPE_REF, HTTP_CALL/HTTP_ROUTE, RPC_CALL/RPC_IMPL, CHANNEL_PUBLISH/CHANNEL_SUBSCRIBE
- **Cross-language references** — C# calling SQL stored procedures, C# referencing Python classes, gRPC service implementations across languages

### Supported languages

Python, C#, TypeScript, JavaScript, Rust, Go, Lua, SQL, PostgreSQL (PL/pgSQL), Proto, Markdown, YAML, Bicep

### Key capabilities

**Navigation** — find symbols, jump to definitions, explore neighbors, trace call chains

**Impact analysis** — multi-layer analysis (direct graph + test coverage + git co-change history) with confidence scoring. Understands what changes when you modify a symbol.

**Cross-service tracing** — automatically bridges service boundaries via gRPC, HTTP routes, and message bus channels (Azure Service Bus, RabbitMQ). Trace a request from API endpoint through message queue to background worker across languages.

**Context assembly** — `gather_context` assembles LLM-ready context from symbols, files, and search queries within a byte budget. One call gives your AI assistant exactly the code it needs.

**Incremental indexing** — watches for file changes and re-indexes automatically. No manual reindex needed during development.

## Query methods

All queries go through the `lidx` MCP tool with a `method` and optional `params`.

| Category | Methods |
|----------|---------|
| **Search** | `search` |
| **Symbol** | `explain_symbol` |
| **Graph** | `trace_flow` |
| **Impact** | `analyze_impact`, `analyze_diff` |
| **Context** | `gather_context`, `context` |
| **Discovery** | `orient`, `onboard`, `repo_map` |
| **Management** | `reindex` |
| **Quality** | `top_complexity`, `dead_symbols` |

Every result includes `next_hops` — pre-computed follow-up queries so your AI assistant knows what to explore next.

## Example prompts

These are things you can say to your AI assistant once lidx is connected. The assistant will automatically call the right lidx queries behind the scenes.

### Discovery

- "Give me an overview of this repo"
- "What languages are in this project?"
- "Show me the most complex functions in the codebase"
- "Are there any duplicated code blocks?"

### Finding code

- "Find the UserService class"
- "Search for anything related to authentication"
- "Find all gRPC service definitions"
- "Search for TODO comments across the repo"

### Understanding code

- "Explain what the OrderProcessor class does — its callers, callees, and dependencies"
- "What does the register_user function call?"
- "Show me all the callers of validate_email"
- "What imports does this module depend on?"

### Impact analysis

- "If I change the User model, what else is affected?"
- "What's the blast radius of modifying the payment processing function?"
- "Show me the impact of this diff" (with a git diff or file changes)
- "What tests cover the authentication module?"

### Tracing across services

- "Trace the flow starting from the /api/orders HTTP endpoint"
- "How does a message published to the order-created topic get processed?"
- "Show me the full request path from the API gateway through gRPC to the database"
- "What services are connected to the UserUpdated message bus channel?"

### Navigation

- "Show me the neighbors of the PaymentGateway class"
- "Expand the call graph around processOrder, 3 levels deep"
- "What are the HTTP routes in this project and who calls them?"
- "Show me all cross-language references between C# and SQL"

### Code quality

- "What are the most complex functions? Any above complexity 10?"
- "Find dead symbols — functions that are never called"
- "Are there any unused imports?"

### Context for tasks

- "Gather context about the authentication system — I need to add OAuth support"
- "Pull together everything related to the order processing pipeline"
- "I need to understand the database schema and all functions that touch the users table"

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

## Graph view

`lidx graph` writes one self-contained HTML file with an interactive view of
the existing index. It makes no network requests and works offline, but it
embeds source snippets (call-site lines), docstrings, signatures and file
paths from the repo, so share it only where the code itself may go. It never
re-indexes.

```bash
lidx graph --repo . --seed crate::indexer::Indexer::reindex --direction down --depth 3 --open
```

- No `--seed`: an overview of the whole repo that starts at the top-level
  folders with weighted edges between them; double-click a folder (or use
  the depth buttons) to drill down to files (`--level file` starts open).
- `--seed <qualname or query>` (repeatable), `--depth`, `--direction up|down|both`,
  `--kinds CALLS,USES`, `--lang`, `--max-nodes` (a cap on every node in the
  page, halo and externals included): a symbol neighbourhood plus
  a one-hop halo you can reveal node by node. A one-way trace opens in the
  layered layout with the seed in the first column.
- `--diff <rev>`: the symbols changed against `rev` and their callers.
- `--scope code|tests|docs|examples|all` (default `code`): files outside it
  (tests, docs, fixtures, CI tooling, vendored code) are embedded but hidden
  until shown from the legend. `--title` names the page (default: the git
  remote's name).
- In the page:
  - Force or layered layout. Layered gives one column per hop from the seed.
  - Folders you can open and close, plus a folder-by-folder dependency
    matrix in which cycles are outlined.
  - Cross-process bridges (message bus, RPC, HTTP, config keys) drawn
    between the folders that own each side and labelled by transport.
  - Search (`/`), breadcrumbs, and back/forward history.
  - A details panel with top callers and callees and every edge with its
    call-site line.
  - Highlighting by hops, upstream or downstream, plus paths between two
    nodes and isolating a neighbourhood.
  - A minimap, pinning, and PNG, SVG and JSON export.
  - Colours from the Okabe-Ito palette, with every edge kind also told
    apart by its dash pattern.

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
