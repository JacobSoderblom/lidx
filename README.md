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

- No `--seed`: the page opens on **System at a glance**, a landing page
  for someone new to the repo:
  - The repo in one sentence (the README's first paragraph, else a
    manifest description, else a factual template) and its counts.
  - A small map of at most 12 cards in lanes, laid out the same way every
    time. Repos with two or more apps that talk across processes show the
    apps; libraries become a "used by N apps" strip and infrastructure one
    chip per tool. Every line has a verb and a count ("calls over HTTP ×9",
    "calls RPC ×4", "publishes orders", "reads 3 secrets"), only for links
    the index found on both sides. A workspace of libraries shows its
    packages and their declared dependencies. A repo that is one unit shows
    that unit's modules.
  - Every card says what it is in one sentence (manifest `description`,
    else the README in its folder, else a template such as "Go service
    serving 11 HTTP routes; calls 4 apps over RPC").
  - **Start here**: the ways in (HTTP routes, RPC methods, bus
    subscriptions, `main` and CLI commands found for Cobra, click/typer,
    commander and System.CommandLine), the most used types, the docs to
    read first (README, CONTEXT.md, ADRs) and hotspots by complexity and
    fan-in.
  - **Flows**: `trace_flow` runs from each entry point, with bus, RPC, HTTP
    and config bridging. The paths that cross the most parts and reach the
    most infrastructure are kept, three to six of them, each titled from its
    entry point ("/cart/checkout → frontend → checkoutservice →
    cartservice"). Follow one step by step over the map with the other
    parts faded, or as a sequence diagram; steps matched by name only are
    marked probable. With no entry points the flows start from the most
    called functions, and the page says so.
  - A **System tour** of about six steps (lanes, where traffic enters, one
    request, how the apps talk, where config and secrets come from, shared
    code, hotspots), and pre-computed answers under "More questions" ("Where
    do secrets come from?", "What talks to X?", "Which apps share Y?").
  - Click a card for its ways in, what it talks to and what calls it, key
    types and flows; click a line for the calls behind it; double-click a
    card to open it in Explore. `←` `→` step through a flow or the tour,
    and `Esc` leaves it with the part you reached still selected.
  Everything is computed from the index when the page is written: no model
  writes any of the text.
- **Explore ▸** (`--level app`) is the full architecture diagram of the
  repo's applications, libraries and infrastructure. Units come from
  the manifests: .NET projects, `package.json` packages, Python projects,
  crates, Go modules and main packages, Dockerfiles, and Kubernetes, Helm,
  compose, Bicep and Terraform files. Files no manifest owns form a
  `(loose)` unit per top-level folder. Cards sit in rows by role (front
  ends; APIs, services, workers and tools; libraries; infrastructure and
  data). Each card says what the unit exposes (HTTP routes, RPC services,
  bus subscriptions, commands) and what it uses (config keys, secrets,
  topics, external endpoints). Links say what they are in words, such as
  "calls ×N", "calls RPC ×N", "publishes to topic X", "reads secret X",
  "depends on" and "deploys". Test projects, examples, docs and tooling
  start hidden. Click a card for every route, topic and key with the
  symbols behind it. Scroll into a card (or double-click it) to open it in
  place: its folders, then a folder's files, then a file's classes with
  their methods, functions and constants, each with its signature and
  `file:line`. The calls, uses and bridges between whatever is shown inside
  the open folder or file are drawn with weights; links that leave it go
  through one stub per target on its edge, named with the folder, file or
  app and the count, the lightest grouped as "+N elsewhere" (hover one for
  the boxes behind it). Several cards can
  be open at once: with both ends in view a client's RPC call is drawn to
  the server method. Scroll out, `Esc` or the breadcrumb
  goes back up, and the panel lists everything inside a unit in a
  filterable tree. The URL fragment keeps what is open. `--max-symbols`
  (default 60000) caps the symbols embedded for this, whole files per
  unit; the page and stderr say what was left out. The Overview / Apps /
  Folders / Files switch (and `--level module|file`) gives the folder map;
  `G` comes back to the overview.
- `--seed <qualname or query>` (repeatable), `--depth`, `--direction up|down|both`,
  `--kinds CALLS,USES`, `--lang`, `--max-nodes` (a cap on every node in the
  page, halo and externals included): a symbol neighbourhood plus
  a one-hop halo you can reveal node by node, coloured by the app each
  symbol belongs to. A one-way trace opens in the layered layout with the
  seed in the first column.
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
