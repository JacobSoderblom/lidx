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

**Resolver Marker**:
Typing data an extractor leaves for the resolver in a symbol's stored `signature` instead of a column of its own, on a line the read paths strip (`model::strip_internal_markers`, via `public_signature` and `display_signature`), so no response shows it. Rust: `#[nested(..)]` and `#[trait_impl]`, plus two that record where a type path points so a signature read in another file needs no scope: `#[rust_types(<written>=<origin>;..)]` on a fn or struct/enum signature (`~` for std or a dependency, else the absolute `crate::` path that may re-export a foreign type) and `#[rust_uses(<name>;<name>=<crate:: path>;*;*=<crate:: module>;..)]` on a module symbol (the names its `use`s bind, `*` an external glob such as `pub use walkdir::*;` and `*=` a glob of a module of the repo, read by `indexer::rust::follow_path` to follow a `pub use` through any number of re-exports, a cycle ending the walk), both written and read through `model::rust_types_marker`/`rust_types_data` and `rust_uses_marker`/`rust_uses_data`. Go: one `#[go_type(<data>)]` line, written by `model::go_type_marker` and read by `model::go_type_data` (the only writer and reader), whose data is a struct's field list or an interface's embedded interfaces (`{conn net.Conn; ~Embedded}`, `~` marking an embedded field) or a package variable's declared type text / `= <json GoValue>` initializer. A symbol whose signature is only a marker has none when read. A new marker is a named constant in `src/model.rs` shared by writer and reader, never a bare string.
_Avoid_: adding a column per language's typing data; showing a marker in outline or `explain_symbol`

**Go build twins**:
One Go declaration written in several files of a package under mutually exclusive build constraints (`//go:build !gore2regex` vs `gore2regex`, `_linux.go` vs `_windows.go`). The extractor records each file's constraint as `buildtag:<expr>` (plus `buildhost` when it holds) in `symbols.visibility` (`indexer::go_build`). A call binds to one twin: the one carrying the caller file's own constraint, else the one that holds on the **default host** (GOOS=linux, GOARCH=amd64, cgo off, no extra tags, go1.1..go1.24 release tags), else the unconstrained one; several that still tie stay ambiguous. Choosing the host rather than refusing keeps the call graph the one a default `go build` sees, which is also what the oracle benchmark compiles.
_Avoid_: refusing a twin as ambiguous; guessing among twins that tie

**Go interface dispatch**:
An interface's method specs are `method` symbols (`pkg.Iface.Method`); a call on an interface-typed receiver first binds to the spec (also through an interface a struct or interface embeds), then is narrowed by `Resolver::refine_go_interface_call`: (1) to the concrete method when the call site proves the receiver's dynamic type (`var d Doer = &Impl{}` or `s := &S{h: &Impl{}}; s.h.Do()`, with the variable or field assigned nowhere else in the function: `GoDeferred::dynamic`), else (2) to the one in-repo implementation when there is exactly one, else (3) it stays on the spec and is stored as a **`DISPATCH`** edge (`edge_views::DISPATCH`; a call whose callee is not one function is not a `CALLS` edge). Go has no `implements` clause, so `Db::rederive_go_implements` derives `IMPLEMENTS` edges (`edges.derived = 2`) from the method sets (name and normalized parameter/result types, promoted methods and known standard-library embeds included; an interface with an unknown embed has no implementers), and the existing dispatch machinery (`edges_for_symbols_with_dispatch`: `analyze_impact`, `trace_flow`, `subgraph`, `explain_symbol`) fans a `DISPATCH` edge out to the implementations. `edge_views::CALL_LIKE` is the set callers/callees code tests for.

**Local Function Symbol**:
A TypeScript/JavaScript function declared inside another function (`function g(){}`, `const f = () => ..`), indexed as a `function` symbol qualified under its parent (`mod.outer.f`) and marked with the `local` modifier in `symbols.visibility` (`graph_query::LOCAL_MODIFIER`). Bound only by a bare call or its parent's CONTAINS edge — never by a name-fallback tier or as a member of a same-named type, since `outer.f` also reads as member `f` of a type `outer`. Not a top-level symbol: `dead_symbols`, the `repo_map` key symbols, kind counts and module symbol counts leave it out, while `outline` nests it under its parent. Nothing calls a returned closure, so upstream traversal (`analyze_impact`, `trace_flow`, `explain_symbol` callers) hops from it to its CONTAINS parent over a synthetic edge labelled `containment`. A name declared twice in one function, or inside a parent whose qualname is declared twice, gets no symbol; `const f = function g(){}` stays the enclosing function's, as tsc keys its calls to `g`.
_Avoid_: nested function, closure symbol

**Workspace Crate**:
A Rust package of the indexed repository that another package names in a path (`grep_regex::RegexMatcher::new`). Every package's items are qualified `crate::...`, so which package a path means is decided from the Cargo manifests (`src/indexer/rust_workspace.rs`), seen from the *calling* package: its declared dependency (a path dependency is named by its `[lib] name`, a renamed one by its key), its own library, else the one undeclared sibling in its workspace; `pub use <crate> as <alias>;` in a library root lets a facade crate's path (`grep::cli::f`) reach the crate behind it. A dependency from outside the repo is external; a module declared in the calling scope (`mod regex;`) wins over a crate of the same name.
_Avoid_: member (only workspace `members` are members), package (a Cargo package may have no library and so no name in a path)

**Crate Pin**:
The marker a Rust extractor leaves in `import_candidates` for a path rooted at a **Workspace Crate**: `@<package dir>|<path in the crate>` (`@crates/regex|RegexMatcher::new`), or `~` for a dependency from outside the repo. The resolver binds a pin only to a public item of that package's library (not its tests, benches, examples or binaries), never to a same-named item of another package; a miss is unresolved, not a name guess. A receiver type imported from another package is pinned the same way (`@dir|Type`), and so is the trait of an `impl Trait for Type` (the pin binds only to a trait). A third marker, `^<crate:: path>`, sits in the `import_candidates` of a Rust type-level `IMPLEMENTS` edge whose impl is in another module than its type: each is a path the self type may be declared at (the impl module's own item of that name, then what its `use`s and globs bring in), followed through the re-exports to a type of the impl file's own package. Such an edge is written with no source, `Resolver::rust_impl_self` settles it at insert, and `Db::rederive_rust_impl_sources` settles it again after every sync, so a moved type or impl, or an edited re-export, gives what a fresh reindex gives. Limits: a glob of a workspace crate (`pub use grep_matcher::*;`) and a type generated by a macro other than `pin_project!` have no symbol to follow; an unresolved glob keeps the module's own guess.
_Avoid_: treating a pin as a qualname; it is a lookup instruction

**Manifest Fingerprint**:
A hash of what the Rust extractor read from the Cargo manifests and library roots (packages, library names, workspace membership, dependency sources, crate re-exports; registry versions excluded), stored in `meta` (`rust_workspace_fingerprint`). When it differs from the one the last index recorded, every Rust file is extracted again even if its hash is unchanged, so an incremental sync equals a fresh reindex after a manifest edit.
_Avoid_: manifest hash (it is not a hash of the files)

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
