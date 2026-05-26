# lidx

A code indexer exposed as an MCP server that gives AI coding assistants structured understanding of codebases — symbol graphs, cross-language traversal, config tracing, and impact analysis. Ships as a single Rust binary.

## Language

**Symbol Graph**:
The SQLite-backed store of symbols (functions, classes, modules) and typed edges between them. The primary data structure that all query methods operate on.
_Avoid_: knowledge graph, code graph (too generic), index

**Edge Kind**:
A typed, directional relationship between two symbols — CALLS, IMPORTS, CONTAINS, EXTENDS, IMPLEMENTS, RPC_IMPL, CHANNEL_PUBLISH, CONFIG_BIND, etc. First-class concept; new edge kinds are how lidx learns new architectural patterns.
_Avoid_: link, reference, relation

**Bridge Edge**:
An **Edge Kind** that crosses process or language boundaries — RPC_CALL↔RPC_IMPL, CHANNEL_PUBLISH↔CHANNEL_SUBSCRIBE, HTTP_CALL↔HTTP_ROUTE. Traversal methods automatically cross these.
_Avoid_: cross-language edge (too narrow — bridges also cross process boundaries within one language)

**XREF**:
Transitional **Edge Kind** for cross-language references that don't yet have a named pattern. Should shrink over time as specific patterns are promoted to dedicated Edge Kinds (as CONFIG_* and CHANNEL_* were). Treat new XREF edges as a signal that a new named Edge Kind may be warranted.
_Avoid_: using XREF as a permanent home for patterns that recur across codebases

## Example dialogue

> **Dev:** "I added a C# method that calls a Python service over a message bus. What edge kind should I emit?"
>
> **Domain expert:** "If it's a known pattern — Service Bus, RabbitMQ, SQS — emit CHANNEL_PUBLISH. The extractor should detect the framework and populate the detail field with the channel name. trace_flow will auto-cross to the CHANNEL_SUBSCRIBE on the Python side."
>
> **Dev:** "What if it's a custom IPC mechanism we haven't seen before?"
>
> **Domain expert:** "Start with XREF. That's the incubator — it'll show up in cross-language queries, but it signals that someone should look at whether this pattern deserves its own Edge Kind. If we see the same IPC pattern in a second codebase, promote it."
