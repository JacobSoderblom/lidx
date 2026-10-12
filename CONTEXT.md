# lidx

A code indexer exposed as an MCP server that gives AI coding assistants structured understanding of codebases — symbol graphs, cross-language traversal, config tracing, and impact analysis. Ships as a single Rust binary.

## Language

**Symbol Graph**:
The SQLite-backed store of symbols (functions, classes, modules) and typed edges between them. The primary data structure that all query methods operate on.
_Avoid_: knowledge graph, code graph (too generic), index

**Edge Kind**:
A typed, directional relationship between two symbols — CALLS, IMPORTS, CONTAINS, EXTENDS, IMPLEMENTS, RPC_IMPL, CHANNEL_PUBLISH, CONFIG_BIND, USES (type/fn-value reference, not a call; Rust, C#, JS/TS), etc. First-class concept; new edge kinds are how lidx learns new architectural patterns.
MODULE_EXPORT is an internal-only kind: a Python `__all__` export signal stored only as an unresolved reference for `unused_imports`, not advertised in the MCP edge-kind list.
_Avoid_: link, reference, relation

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

**Stop Reason**:
Why a `trace_flow` branch ends at a leaf hop (issue #410): `depth_limit`, `already_visited` (a cycle or a diamond), `filtered`, `truncated`, `unresolved_calls`, `no_callees`, `external`. Upstream, `no_callees` and `unresolved_calls` are `no_callers` and `unresolved_callers`. `unresolved_*` means a resolution gap -- the write path recorded a reference it could not bind (`unresolved_references`), so the trace may be missing edges -- not a genuine leaf; only `no_callees` / `no_callers` say nothing was found. `unresolved_callers` is a possible-caller signal matched by name only (ambiguous references, distinct sources) and is replaced by `no_callers` with `fuzzy_candidates` when too noisy. Shown only on leaf hops; the trace carries per-reason counts as `stop_summary`.
_Avoid_: treating a leaf with `unresolved_calls` as "execution ends here"; calling `already_visited` a cycle (it also covers diamonds)

## Example dialogue

> **Dev:** "I added a C# method that calls a Python service over a message bus. What edge kind should I emit?"
>
> **Domain expert:** "If it's a known pattern — Service Bus, RabbitMQ, SQS — emit CHANNEL_PUBLISH. The extractor should detect the framework and populate the detail field with the channel name. trace_flow will auto-cross to the CHANNEL_SUBSCRIBE on the Python side."
>
> **Dev:** "What if it's a custom IPC mechanism we haven't seen before?"
>
> **Domain expert:** "Start with XREF. That's the incubator — it'll show up in cross-language queries, but it signals that someone should look at whether this pattern deserves its own Edge Kind. If we see the same IPC pattern in a second codebase, promote it."
