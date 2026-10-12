# lidx

A code indexer exposed as an MCP server that gives AI coding assistants structured understanding of codebases — symbol graphs, cross-language traversal, config tracing, and impact analysis. Ships as a single Rust binary.

## Language

**Symbol Graph**:
The SQLite-backed store of symbols (functions, classes, modules) and typed edges between them. The primary data structure that all query methods operate on.
_Avoid_: knowledge graph, code graph (too generic), index

**Edge Kind**:
A typed, directional relationship between two symbols — CALLS, IMPORTS, CONTAINS, EXTENDS, IMPLEMENTS, RPC_IMPL, CHANNEL_PUBLISH, CONFIG_BIND, USES, REFERENCES, etc. First-class concept; new edge kinds are how lidx learns new architectural patterns.
USES is a symbol read that is not a call: a type use (Rust, C#, JS/TS), a JS/TS member read, or a **value reference** that lands on a constant or variable (data read, not a function named). REFERENCES has two meanings that never share a row: a SQL foreign key (table to table), and a **value reference** — a function, method or class used as a value instead of called (a callback argument, a dispatch-table entry, a C# method group, `return fn`), extracted by every language (Python, TS/JS, Rust, Go, C#). A value reference is resolved by the same tiers as a call to the same text, then kept only for an exact, import, receiver-type or inherited match on a function, class or (qualified only) method; a pair that also has a CALLS edge keeps only the CALLS edge. Unresolved ones live in `unresolved_references` like any other, but are left out of its counts (`trace_flow`/`analyze_impact` stop reasons, the summary), since most are locals and parameters.
MODULE_EXPORT is an internal-only kind: a Python `__all__` export signal stored only as an unresolved reference for `unused_imports`, not advertised in the MCP edge-kind list.
_Avoid_: link, relation ("reference" alone is ambiguous: say REFERENCES, value reference, or unresolved reference)

**Edge View**:
A named set of **Edge Kinds** in `src/edge_views.rs` that every consumer reads instead of carrying its own list, so a new kind is placed once. The four that define "use": EXECUTION (control can flow: CALLS, the bridge kinds, XREF, CONFIG_SOURCE/READ; never REFERENCES), CONSUMER (EXECUTION plus CONFIG_BIND; what `trace_flow` walks by default and what `explain_symbol` lists as `callers`/`callees`/`tests`), SYMBOL_USE (CONSUMER plus EXTENDS/IMPLEMENTS/INHERITS, USES, REFERENCES) and REACHABILITY (what keeps a symbol alive for `dead_symbols`). Whatever SYMBOL_USE holds beyond CONSUMER is `explain_symbol`'s `other_edges`, grouped by kind and capped per kind with a total, so non-call uses never inflate `callers`.
_Avoid_: edge group, edge category

**Bridge Edge**:
An **Edge Kind** that crosses process or language boundaries — RPC_CALL↔RPC_IMPL (plus RPC_ROUTE, the .proto definition side), CHANNEL_PUBLISH↔CHANNEL_SUBSCRIBE, HTTP_CALL↔HTTP_ROUTE. Traversal methods automatically cross these.
_Avoid_: cross-language edge (too narrow — bridges also cross process boundaries within one language)

**String-Targeted Edge Kind**:
An **Edge Kind** `Db::insert_edges` keeps a live edge for even when unresolved (issue #79's exemption from "every other kind's unresolved reference lives only in `unresolved_references`") — grouped by why its `target_qualname`/`detail` text has to stay readable off the edge itself, not just a resolved `target_symbol_id`. Three groups, not one reason: **Bridge Edge** kinds, whose target is a cross-process join key that `trace_flow`'s traversal bridging looks up by that text; CONFIG_SOURCE/CONFIG_READ/CONFIG_BIND, whose target is a config key/secret URI (`secret://...`, `env://...`) that a config-URI-rooted `trace_flow`/`analyze_impact` looks up the same way, not a Bridge Edge pair itself; and XREF, whose target is usually a real symbol qualname but is read directly, confidence-gated, by evidence-based consumers regardless of whether it resolved.
_Avoid_: assuming this list is the same as **Bridge Edge** — CONFIG_BIND and XREF stay string-targeted for a different reason and aren't traversal-bridged

**XREF**:
Transitional **Edge Kind** for cross-language references that don't yet have a named pattern. Should shrink over time as specific patterns are promoted to dedicated Edge Kinds (as CONFIG_* and CHANNEL_* were). Treat new XREF edges as a signal that a new named Edge Kind may be warranted.
_Avoid_: using XREF as a permanent home for patterns that recur across codebases

**External Stub**:
A synthetic symbol (`kind = 'external'`, qualname `ext:<name>`) that a CALLS edge binds to when its target is known to resolve outside the repo — an import known not to resolve here (standard library, third-party package), or a Rust/Go fully-qualified path whose syntax never routes through import candidates at all. Not a real extracted symbol: one stub per distinct `ext:` qualname per graph version, shared by every call site, and excluded from repo-internal listings (`repo_overview`'s counts, XREF candidates, `dead_symbols`, `top_complexity`, `repo_map`).
_Avoid_: stubbing every unresolved call — a local variable of builtin/unknown type (`cells.append(1)`) has no import behind it, so it stays unresolved instead of getting a stub named after that variable

## Example dialogue

> **Dev:** "I added a C# method that calls a Python service over a message bus. What edge kind should I emit?"
>
> **Domain expert:** "If it's a known pattern — Service Bus, RabbitMQ, SQS — emit CHANNEL_PUBLISH. The extractor should detect the framework and populate the detail field with the channel name. trace_flow will auto-cross to the CHANNEL_SUBSCRIBE on the Python side."
>
> **Dev:** "What if it's a custom IPC mechanism we haven't seen before?"
>
> **Domain expert:** "Start with XREF. That's the incubator — it'll show up in cross-language queries, but it signals that someone should look at whether this pattern deserves its own Edge Kind. If we see the same IPC pattern in a second codebase, promote it."
