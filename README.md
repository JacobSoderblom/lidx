# lidx

Local code indexer with JSON-RPC and MCP servers.

## Ignore rules

By default lidx respects `.gitignore`, `.git/info/exclude`, and global
gitignore rules for indexing and searches. To include ignored files in the
index, use `--no-ignore` with `reindex`, `changed-files`, `serve`, or
`mcp-serve`. To include ignored files in searches, pass `no_ignore: true` to
`search_text`, `grep`, or `search_rg`.

## MCP API

The MCP server runs over stdio via `lidx mcp-serve`. It exposes one tool,
`lidx_query`, which accepts a `method` and optional `params` payload.

### JSON-RPC methods

- `initialize`: Negotiates protocol/version and returns capabilities/instructions; solves client setup.
- `notifications/initialized`: One-way init acknowledgement; solves init completion signaling.
- `ping`: Empty ping/pong response; solves liveness checks.
- `tools/list`: Lists available tools (currently `lidx_query`); solves tool discovery.
- `tools/call`: Executes a tool call with arguments; solves running MCP queries.
- `resources/list`: Returns an empty list; solves MCP clients expecting resources.
- `resources/templates/list`: Returns an empty list; solves MCP clients expecting resource templates.
- `prompts/list`: Returns an empty list; solves MCP clients expecting prompts.
- `roots/list`: Returns an empty list; solves MCP clients expecting roots.

### Tool: `lidx_query` methods

Alias: `search` maps to `search_text`.

- `help`: Returns summary, method list, aliases, and examples; solves usage discovery.
- `list_methods`: Returns method details (names, summaries, key params); use `format:"names"` for names only.
- `list_languages`: Lists supported language filters and extensions; solves query scoping.
- `repo_overview`: Returns repo stats (files, symbols, edges); solves high-level index status.
- `repo_insights`: Returns complexity, duplicates, diagnostics, last indexed; solves hotspot snapshot.
- `top_complexity`: Returns the most complex symbols; solves refactor targeting.
- `duplicate_groups`: Returns groups of duplicated code; solves dedupe opportunities.
- `find_symbol`: Searches symbols by name; solves locating definitions.
- `suggest_qualnames`: Suggests symbol qualnames with fuzzy matching; solves typo correction and discovery.
- `open_symbol`: Fetches symbol metadata and optional snippet; solves jump-to-definition.
- `open_file`: Reads file text (optionally line-ranged); solves retrieving source content.
- `neighbors`: Returns adjacent symbols/edges for a symbol; solves local dependency context.
- `subgraph`: Returns a multi-hop graph around symbol IDs; solves dependency exploration.
- `references`: Returns incoming/outgoing edges and snippets; solves find call sites/usages.
- `gather_context`: Assembles LLM-ready context from multiple sources within a byte budget. Seeds can be symbols (by qualname), files (by path and optional line range), or search queries. Expands related symbols via call graph (configurable depth/max_nodes). Returns deduplicated, deterministically-ordered content with metadata.
- `search_rg`: Runs ripgrep-based regex search; solves fast regex searches.
- `grep`: Exact/regex line search with ranking; solves precise string matching.
- `search_text`: Ranked text search with fuzzy fallback; solves "find relevant matches".
- `route_refs`: Normalized route/URL string references; solves backend/frontend path matching.
- `flow_status`: Routes without calls and calls without routes; solves stale/unused flow detection.
- `changed_files`: Lists added/modified/deleted vs the DB; solves incremental reindexing.
- `index_status`: Index freshness summary with stale hint; solves reindex decisioning.
- `reindex`: Rebuilds the index and returns stats; solves keeping the DB in sync.
- `diagnostics_run`: Runs analyzers and imports SARIF results; defaults to eslint (JS/TS), ruff (Python), clippy-sarif (Rust), dotnet build (C#), and semgrep (SQL/Markdown/Proto).
- `diagnostics_import`: Imports SARIF diagnostics; solves bringing external analyzer results in.
- `diagnostics_list`: Lists diagnostics with filters; solves browsing issues.
- `diagnostics_summary`: Summarizes diagnostics counts; solves high-level quality overview.
- `semantic_search`: Searches code semantically using embeddings; solves finding code by meaning/intent (requires `--features embeddings`).

## Semantic Code Search (Beta)

lidx supports semantic code search using local embedding models. This feature enables searching code by **meaning and intent** rather than just keywords. For example, find "user authentication logic" even if the code doesn't use those exact words.

**This is an opt-in feature** requiring explicit configuration.

### Quick Start

1. **Build with embeddings feature**:
   ```bash
   cargo build --release --features embeddings
   ```

2. **Enable embeddings**:
   ```bash
   export LIDX_EMBEDDINGS_ENABLED=true
   ```

3. **Reindex your codebase**:
   ```bash
   lidx reindex
   # First run will download model (~30MB for BGESmallENV15)
   # Progress shown: "Downloading model..."
   ```

4. **Search semantically**:
   ```bash
   # Via MCP (in Claude Desktop or other MCP client)
   {
     "method": "semantic_search",
     "params": {
       "query": "function that validates user passwords",
       "limit": 10
     }
   }

   # Via RPC
   echo '{"method":"semantic_search","params":{"query":"parse JSON data"}}' | lidx rpc
   ```

### Configuration

Control embedding behavior via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LIDX_EMBEDDINGS_ENABLED` | `false` | Enable/disable embeddings (must rebuild after enabling) |
| `LIDX_EMBEDDING_PROVIDER` | `fastembed` | Provider: `fastembed` or `ollama` |
| `LIDX_EMBEDDING_MODEL` | `BGESmallENV15` | Model name (see Supported Models below) |
| `LIDX_EMBEDDING_BATCH_SIZE` | `32` | Batch size for embedding generation |
| `LIDX_OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL (when using Ollama provider) |

### Supported Models

**FastEmbed (built-in, no external dependencies)**:
- `BGESmallENV15`: Fast, 384 dimensions, ~30MB download
- `BGEBaseENV15`: Better quality, 768 dimensions, ~120MB download

**Ollama (requires Ollama server running)**:
- `nomic-embed-text`: Optimized for code, 768 dimensions
- `mxbai-embed-large`: Alternative option, 1024 dimensions

### Performance

Measured on Apple Silicon (M-series):

- **Indexing overhead**: ~30-40% (with caching on re-index)
- **Single embedding latency**: <20ms (p50)
- **Batch embedding throughput**: 32 embeddings in <500ms
- **Search latency**: <50ms
- **Storage overhead**: ~2KB per symbol

Cache optimization:
- First reindex: Generates embeddings for all symbols
- Subsequent reindex: Only re-embeds changed symbols (~70% cache hit rate)
- Cache invalidation: Automatic on content or model change

### Usage Examples

**Find authentication logic**:
```json
{
  "method": "semantic_search",
  "params": {
    "query": "user authentication and password validation",
    "limit": 10,
    "threshold": 0.7
  }
}
```

**Find error handling code in Python**:
```json
{
  "method": "semantic_search",
  "params": {
    "query": "exception handling and error recovery",
    "languages": ["python"],
    "limit": 5
  }
}
```

**Find database connection setup**:
```json
{
  "method": "semantic_search",
  "params": {
    "query": "initialize database connection pool",
    "threshold": 0.75
  }
}
```

### Troubleshooting

**Error: "Embeddings not enabled"**
- Ensure `LIDX_EMBEDDINGS_ENABLED=true` is set
- Rebuild with `--features embeddings`: `cargo build --release --features embeddings`

**Error: "sqlite-vec not loaded"**
- The sqlite-vec extension should be bundled for macOS, Linux, and Windows
- If you see this error, check platform compatibility or file an issue

**Error: "Ollama server not reachable"**
- Start Ollama: `ollama serve` (in separate terminal)
- Verify server is running: `curl http://localhost:11434/api/tags`
- Or switch to fastembed: `export LIDX_EMBEDDING_PROVIDER=fastembed`

**Warning: "Embedding init failed"**
- This is a graceful degradation - indexing continues without embeddings
- Check the error message for details
- Common causes: model download failed, insufficient disk space

**Slow first reindex**
- First reindex generates embeddings for all symbols (one-time cost)
- Model download happens on first run (~30-120MB depending on model)
- Subsequent reindexes use cache and are much faster

### Ollama Setup (Optional)

For better performance with GPU acceleration or specialized models:

1. **Install Ollama**:
   ```bash
   # macOS/Linux
   curl https://ollama.ai/install.sh | sh

   # Or visit https://ollama.ai for other installation methods
   ```

2. **Start Ollama server**:
   ```bash
   ollama serve
   # Leave this running in a terminal
   ```

3. **Pull embedding model**:
   ```bash
   ollama pull nomic-embed-text
   # Or: ollama pull mxbai-embed-large
   ```

4. **Configure lidx**:
   ```bash
   export LIDX_EMBEDDINGS_ENABLED=true
   export LIDX_EMBEDDING_PROVIDER=ollama
   export LIDX_EMBEDDING_MODEL=nomic-embed-text
   ```

5. **Reindex**:
   ```bash
   lidx reindex
   # Will use Ollama for embeddings
   # Falls back to fastembed if Ollama unavailable
   ```

**Benefits of Ollama**:
- GPU acceleration (5-10x faster with CUDA/Metal)
- Larger models with better quality
- Shared model cache across applications

**Fallback behavior**:
- If Ollama health check fails, lidx automatically falls back to fastembed
- Clear messages shown: "Ollama health check failed... Falling back to fastembed"

## Security

### Path Validation

All file paths are validated to prevent path traversal attacks:
- Paths are canonicalized (symlinks resolved)
- Paths must exist within the repository root
- Absolute paths outside the repository are rejected
- Relative path traversal (`../../../etc`) is blocked

Protected operations:
- `open_file`: File reading
- `diagnostics_import`: SARIF file import
- `diagnostics_run`: Output directory (must be in repo)
- All search path filters (11 methods)

### ReDoS Protection

Regular expression denial of service attacks are prevented via ripgrep safeguards:
- `--timeout 30s`: Maximum search execution time (configurable)
- `--regex-size-limit 10M`: Maximum compiled regex size
- `--dfa-size-limit 10M`: Maximum DFA state machine size
- `LIDX_PATTERN_MAX_LENGTH`: Maximum pattern length (default: 10,000 bytes)

### Security Logging

Security events are logged to stderr for auditing:
- Path validation failures
- Pattern length violations
- Search timeouts
- Slow queries (>100ms)

Log format: `lidx: Security: [event description]`

## Configuration

Environment variables for tuning performance and security:

### Search Configuration
- `LIDX_SEARCH_TIMEOUT_SECS` (default: 30): Ripgrep timeout in seconds
- `LIDX_PATTERN_MAX_LENGTH` (default: 10000): Maximum search pattern length in bytes

### Database Configuration
- `LIDX_POOL_SIZE` (default: 10): Read connection pool size
- `LIDX_POOL_MIN_IDLE` (default: 2): Minimum idle connections in pool

### Embeddings Configuration
See [Semantic Code Search](#semantic-code-search-beta) section for embedding-related configuration:
- `LIDX_EMBEDDINGS_ENABLED`: Enable semantic search (requires rebuild with `--features embeddings`)
- `LIDX_EMBEDDING_PROVIDER`: Choose between `fastembed` (local) or `ollama` (server-based)
- `LIDX_EMBEDDING_MODEL`: Model name (BGESmallENV15, nomic-embed-text, etc.)
- `LIDX_EMBEDDING_BATCH_SIZE`: Batch size for embedding generation
- `LIDX_OLLAMA_BASE_URL`: Ollama server URL

Invalid values are logged as warnings and defaults are used.

## Performance

### Concurrency Model

lidx uses a hybrid connection pool for SQLite:
- **Read operations**: Connection pool (default 10 connections, concurrent)
- **Write operations**: Single connection with mutex (serialized)
- **Transactions**: Single connection (serialized, SQLite requirement)

### Performance Characteristics

Measured on Apple Silicon with 52-file repository:

**Sequential queries:**
- find_symbol: ~15ms per query
- search_text: ~38ms per query

**Concurrent queries (10 threads):**
- Throughput: ~252 queries/second
- **4x speedup** vs sequential baseline
- No blocking between concurrent reads

### Scaling

Connection pool sizing recommendations:
- **Light usage** (1-5 concurrent clients): pool_size=5
- **Medium usage** (5-15 concurrent clients): pool_size=10 (default)
- **Heavy usage** (15-30 concurrent clients): pool_size=20

Monitor pool exhaustion via logs: `lidx: Database connection pool initialized`
