use serde_json::json;
use std::collections::HashMap;

pub const CHANNEL_PUBLISH_KIND: &str = "CHANNEL_PUBLISH";
pub const CHANNEL_SUBSCRIBE_KIND: &str = "CHANNEL_SUBSCRIBE";

/// Known topic container prefixes (C# class names, Python enum names, etc.)
const TOPIC_CONTAINERS: &[&str] = &[
    "Topics",
    "TopicName",
    "TopicNames",
    "Topic",
    "Channels",
    "Channel",
    "Queues",
    "Queue",
    "QueueName",
    "QueueNames",
    "EventType",
    "EventTypes",
    "Subjects",
    "Subject",
];

/// Known bus receiver name patterns (last segment of receiver expression)
const BUS_RECEIVER_PATTERNS: &[&str] = &[
    "_bus",
    "_messages",
    "_messageBus",
    "bus",
    "Bus",
    "messageBus",
    "MessageBus",
    "_publisher",
    "publisher",
    "_eventBus",
    "eventBus",
    "_serviceBus",
    "serviceBus",
    "_queue",
    "_channel",
];

/// Known publish method names
const PUBLISH_METHODS: &[&str] = &[
    "PublishAsync",
    "Publish",
    "publish",
    "publish_async",
    "SendAsync",
    "Send",
    "send",
    "emit",
    "Emit",
    "dispatch",
    "Dispatch",
];

/// Known subscribe method names
const SUBSCRIBE_METHODS: &[&str] = &[
    "SubscribeAsync",
    "Subscribe",
    "subscribe",
    "subscribe_async",
    "on",
    "On",
    "AddHandler",
    "add_handler",
    "listen",
    "Listen",
];

/// Normalize a channel/topic name to a canonical form.
///
/// Strips the container prefix (Topics., TopicName., etc.), removes underscores,
/// and lowercases everything so that C# PascalCase and Python SCREAMING_SNAKE
/// produce identical keys.
///
/// # Examples
/// - `Topics.OrchestratorTriggers` → `channel://orchestratortriggers`
/// - `TopicName.ORCHESTRATOR_TRIGGERS` → `channel://orchestratortriggers`
/// - `Topics.DataProxyCommands` → `channel://dataproxycommands`
/// - `DATAPROXY_COMMANDS` → `channel://dataproxycommands`
pub fn normalize_channel_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !is_plausible_topic(trimmed) {
        return None;
    }

    // Strip known container prefix (Topics.X → X)
    let topic_part = strip_topic_container(trimmed);
    if topic_part.is_empty() {
        return None;
    }

    // Remove underscores and lowercase
    let normalized: String = topic_part
        .chars()
        .filter(|ch| *ch != '_')
        .flat_map(|ch| ch.to_lowercase())
        .collect();

    if normalized.is_empty() {
        return None;
    }

    Some(format!("channel://{normalized}"))
}

/// A topic name never contains quote, bracket, or call-syntax characters.
/// Source text like `"x"`, `Foo()` or `Arg.Any<string>()` is an expression,
/// not a topic, so it is rejected rather than turned into a fabricated name.
fn is_plausible_topic(s: &str) -> bool {
    !s.chars().any(|ch| {
        matches!(
            ch,
            '"' | '\'' | '`' | '(' | ')' | '<' | '>' | '{' | '}' | '[' | ']' | '\n' | '\r'
        )
    })
}

/// Value of a string-literal source text, for every quoting form the
/// supported languages allow: single/double quotes, Python triple quotes and
/// `r`/`u`/`b`/`f` prefixes, C# verbatim (`@"..."`), interpolated (`$"..."`)
/// and raw (`"""..."""`) strings, Go/JS backtick strings, Rust `r#"..."#`.
/// Interpolated/template strings with holes (`{`/`${`) are not static and
/// yield `None`, as does anything that is not a single complete literal.
pub fn string_literal_value(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let qpos = raw.find(['"', '\'', '`'])?;
    let prefix = &raw[..qpos];
    if !prefix.chars().all(|c| {
        matches!(
            c,
            'r' | 'R' | 'u' | 'U' | 'b' | 'B' | 'f' | 'F' | '$' | '@' | '#'
        )
    }) {
        return None;
    }
    let interpolated = prefix.contains(['f', 'F', '$']);
    let hashes = prefix.chars().filter(|c| *c == '#').count();
    let rest = &raw[qpos..];
    let q = rest.chars().next()?;
    let triple: String = std::iter::repeat_n(q, 3).collect();
    let delim = if q != '`' && rest.starts_with(&triple) && rest.len() >= 6 {
        triple
    } else {
        q.to_string()
    };
    let mut tail = rest.strip_suffix(&"#".repeat(hashes))?;
    if tail.len() < delim.len() * 2 {
        return None;
    }
    tail = tail.strip_suffix(delim.as_str())?;
    let body = &tail[delim.len()..];
    if body.is_empty() || body.contains(q) {
        return None;
    }
    if (interpolated || q == '`') && body.contains('{') {
        return None;
    }
    Some(body.to_string())
}

/// What the enclosing function says about a bare identifier used as a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalBinding {
    /// Not declared in the enclosing function: fall back to file constants.
    NotLocal,
    /// A parameter, reassigned, or otherwise not statically known.
    Unknown,
    /// Assigned exactly once, from this expression text.
    Value(String),
}

/// Same-file string constants by simple name. Shared by channel topics and
/// any other consumer that needs "this expression is really this string".
pub type StringConsts = HashMap<String, String>;

/// Resolve a topic expression to a normalized `channel://` name.
///
/// A topic is derived from a *value*: a string literal's content, or a
/// same-file constant's value. A call, parameter, local with unknown value,
/// interpolation with holes or mock matcher yields `None`; a name is never
/// derived from expression text. An unresolved dotted/PascalCase member
/// access (`Topics.Orders`, imported constant) keeps the container-prefix
/// behaviour; a lowercase/underscore-leading identifier is a variable and is
/// rejected.
pub fn resolve_topic(raw: &str, consts: &StringConsts, local: &LocalBinding) -> Option<String> {
    let raw = raw.trim();
    if let Some(value) = string_literal_value(raw) {
        return normalize_channel_name(&value);
    }
    if !is_identifier_path(raw) {
        return None;
    }
    let simple = !raw.contains('.');
    if simple {
        match local {
            LocalBinding::Unknown => return None,
            LocalBinding::Value(expr) => {
                let value = string_literal_value(expr).or_else(|| {
                    let e = expr.trim();
                    is_identifier_path(e)
                        .then(|| consts.get(e.rsplit('.').next().unwrap_or(e)).cloned())
                        .flatten()
                })?;
                return normalize_channel_name(&value);
            }
            LocalBinding::NotLocal => {}
        }
    }
    let last = raw.rsplit('.').next().unwrap_or(raw);
    if let Some(value) = consts.get(last) {
        return normalize_channel_name(value);
    }
    if strip_topic_container(raw).len() != raw.len() {
        return normalize_channel_name(raw);
    }
    if last.starts_with(|c: char| c.is_lowercase() || c == '_') {
        return None;
    }
    normalize_channel_name(raw)
}

fn is_identifier_path(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|seg| {
            let mut chars = seg.chars();
            chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
                && chars.all(|c| c.is_alphanumeric() || c == '_')
        })
}

/// Strip known topic container prefix from a dotted expression.
/// "Topics.Foo" → "Foo", "TopicName.FOO_BAR" → "FOO_BAR", "Foo" → "Foo"
fn strip_topic_container(raw: &str) -> &str {
    if let Some((prefix, suffix)) = raw.split_once('.') {
        let prefix = prefix.rsplit('.').next().unwrap_or(prefix);
        if TOPIC_CONTAINERS.contains(&prefix) {
            return suffix;
        }
    }
    raw
}

/// Check if a receiver expression looks like a message bus.
pub fn is_bus_receiver(receiver: &str) -> bool {
    let last = receiver.rsplit('.').next().unwrap_or(receiver).trim();
    if last.is_empty() {
        return false;
    }
    BUS_RECEIVER_PATTERNS.contains(&last)
}

/// Check if a method name is a publish method.
pub fn is_publish_method(name: &str) -> bool {
    PUBLISH_METHODS.contains(&name)
}

/// Check if a method name is a subscribe method.
pub fn is_subscribe_method(name: &str) -> bool {
    SUBSCRIBE_METHODS.contains(&name)
}

/// Check if a raw topic value looks like a topic container member access.
/// Returns the normalized channel name if it does.
pub fn topic_from_member_access(raw: &str) -> Option<String> {
    normalize_channel_name(raw)
}

pub fn build_publish_detail(channel: &str, raw: &str, framework: &str) -> String {
    json!({
        "channel": channel,
        "raw": raw,
        "framework": framework,
        "role": "publisher",
    })
    .to_string()
}

pub fn build_subscribe_detail(channel: &str, raw: &str, framework: &str) -> String {
    json!({
        "channel": channel,
        "raw": raw,
        "framework": framework,
        "role": "subscriber",
    })
    .to_string()
}

/// Bridge pair: given an edge kind, return the complementary kind(s) for traversal bridging.
///
/// `RPC_IMPL` bridges to *two* things: `RPC_CALL` (a cross-service caller
/// invoking this RPC) and `RPC_ROUTE` (the `.proto` definition this method
/// implements) -- unlike `HTTP_ROUTE`/`HTTP_CALL`, gRPC has a third edge
/// kind (the route/definition side) distinct from the call side, so it
/// needs both. `RPC_ROUTE` only bridges back to `RPC_IMPL`: tracing
/// downstream from a `.proto` rpc has nothing to reach via `RPC_CALL`
/// (nothing *calls* a route definition).
pub fn bridge_complement(kind: &str) -> Option<&'static [&'static str]> {
    match kind {
        "CHANNEL_PUBLISH" => Some(&["CHANNEL_SUBSCRIBE"]),
        "CHANNEL_SUBSCRIBE" => Some(&["CHANNEL_PUBLISH"]),
        "RPC_CALL" => Some(&["RPC_IMPL"]),
        "RPC_IMPL" => Some(&["RPC_CALL", "RPC_ROUTE"]),
        "RPC_ROUTE" => Some(&["RPC_IMPL"]),
        "HTTP_CALL" => Some(&["HTTP_ROUTE"]),
        "HTTP_ROUTE" => Some(&["HTTP_CALL"]),
        "CONFIG_SOURCE" => Some(&["CONFIG_READ"]),
        "CONFIG_READ" => Some(&["CONFIG_SOURCE"]),
        _ => None,
    }
}

/// Returns true for an edge kind that `Db::insert_edges` always keeps a live
/// edge for, even when unresolved -- issue #79's exemption from "every other
/// kind's unresolved reference lives only in `unresolved_references`". Not
/// one uniform reason: two different groups of consumers need the edge's
/// `target_qualname` (or `detail`) text to still be there at rest, not just
/// a resolved `target_symbol_id`:
///
/// - The three actual **Bridge Edge** pairs/triple (CONTEXT.md glossary):
///   `RPC_CALL`↔`RPC_IMPL` (plus `RPC_ROUTE`, the `.proto` definition side --
///   `channel::bridge_complement`'s doc), `CHANNEL_PUBLISH`↔`CHANNEL_SUBSCRIBE`,
///   `HTTP_CALL`↔`HTTP_ROUTE`. Their target is a cross-process join key, not
///   necessarily a symbol in this graph; `trace_flow`'s traversal bridging
///   (`traversal.rs`, `bridge_complement`) looks up the complementary side by
///   `target_qualname` text regardless of resolution.
/// - `CONFIG_SOURCE`/`CONFIG_READ`/`CONFIG_BIND`: not a Bridge Edge pair (no
///   `bridge_complement` entry for `CONFIG_BIND`, and traversal never crosses
///   these three to each other), but their target is a config key/secret URI
///   (`secret://...`, `env://...`), not a symbol either.
///   `graph_query::source_symbols_for_config_uri` looks these up by that URI
///   text so a config-URI-rooted `trace_flow`/`analyze_impact` can find
///   their source symbols before any symbol-based resolution applies.
/// - `XREF`: unlike the above, its target usually *is* a real, already-known
///   symbol qualname (`indexer::xref::collect_xref_edges` reads it from a
///   same-graph-version symbol index) -- but confidence-gated, evidence-based
///   consumers (`model::xref_is_traversable`, `context.rs`'s cross-ref
///   listing) read its `target_qualname`/`detail` text directly off the edge
///   itself regardless of whether `target_symbol_id` bound, so the edge (and
///   that text) must survive even an Unresolved/Ambiguous outcome.
pub fn is_bridge_edge_kind(kind: &str) -> bool {
    matches!(
        kind,
        "RPC_IMPL"
            | "RPC_CALL"
            | "RPC_ROUTE"
            | "HTTP_ROUTE"
            | "HTTP_CALL"
            | "CHANNEL_PUBLISH"
            | "CHANNEL_SUBSCRIBE"
            | "CONFIG_SOURCE"
            | "CONFIG_READ"
            | "CONFIG_BIND"
            | "XREF"
    )
}

/// Determine the boundary type string for a bridged edge kind.
pub fn boundary_type_for_kind(kind: &str) -> &'static str {
    match kind {
        "CHANNEL_PUBLISH" | "CHANNEL_SUBSCRIBE" => "message_bus",
        "RPC_CALL" | "RPC_IMPL" | "RPC_ROUTE" => "grpc",
        "HTTP_CALL" | "HTTP_ROUTE" => "http",
        "CONFIG_SOURCE" | "CONFIG_READ" => "config",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_csharp_pascal_case() {
        assert_eq!(
            normalize_channel_name("Topics.OrchestratorTriggers"),
            Some("channel://orchestratortriggers".to_string())
        );
    }

    #[test]
    fn normalize_python_screaming_snake() {
        assert_eq!(
            normalize_channel_name("TopicName.ORCHESTRATOR_TRIGGERS"),
            Some("channel://orchestratortriggers".to_string())
        );
    }

    #[test]
    fn normalize_csharp_data_proxy() {
        assert_eq!(
            normalize_channel_name("Topics.DataProxyCommands"),
            Some("channel://dataproxycommands".to_string())
        );
    }

    #[test]
    fn normalize_python_data_proxy() {
        assert_eq!(
            normalize_channel_name("TopicName.DATAPROXY_COMMANDS"),
            Some("channel://dataproxycommands".to_string())
        );
    }

    #[test]
    fn normalize_bare_name() {
        assert_eq!(
            normalize_channel_name("DataProxyCommands"),
            Some("channel://dataproxycommands".to_string())
        );
    }

    #[test]
    fn normalize_empty() {
        assert_eq!(normalize_channel_name(""), None);
    }

    #[test]
    fn normalize_rejects_expression_text() {
        for raw in ["\"orders\"", "Foo()", "Arg.Any<string>()", "'x'"] {
            assert_eq!(normalize_channel_name(raw), None, "{raw}");
        }
    }

    #[test]
    fn string_literal_forms() {
        for (raw, want) in [
            ("\"a\"", Some("a")),
            ("'a'", Some("a")),
            ("\"\"\"a\"\"\"", Some("a")),
            ("'''a'''", Some("a")),
            ("@\"a\"", Some("a")),
            ("$\"a\"", Some("a")),
            ("$\"a{x}\"", None),
            ("f\"a{x}\"", None),
            ("`a`", Some("a")),
            ("`a${x}`", None),
            ("r#\"a\"#", Some("a")),
            ("topic", None),
            ("f()", None),
            ("\"a\" \"b\"", None),
        ] {
            assert_eq!(string_literal_value(raw).as_deref(), want, "{raw}");
        }
    }

    #[test]
    fn bus_receiver_detection() {
        assert!(is_bus_receiver("_bus"));
        assert!(is_bus_receiver("self._messages"));
        assert!(is_bus_receiver("_messageBus"));
        assert!(!is_bus_receiver("_client"));
        assert!(!is_bus_receiver("httpClient"));
    }

    #[test]
    fn publish_subscribe_methods() {
        assert!(is_publish_method("PublishAsync"));
        assert!(is_publish_method("publish"));
        assert!(is_publish_method("emit"));
        assert!(!is_publish_method("SubscribeAsync"));

        assert!(is_subscribe_method("SubscribeAsync"));
        assert!(is_subscribe_method("subscribe"));
        assert!(is_subscribe_method("on"));
        assert!(!is_subscribe_method("PublishAsync"));
    }

    #[test]
    fn bridge_pairs() {
        assert_eq!(
            bridge_complement("CHANNEL_PUBLISH"),
            Some(&["CHANNEL_SUBSCRIBE"] as &[&str])
        );
        assert_eq!(
            bridge_complement("CHANNEL_SUBSCRIBE"),
            Some(&["CHANNEL_PUBLISH"] as &[&str])
        );
        assert_eq!(
            bridge_complement("RPC_CALL"),
            Some(&["RPC_IMPL"] as &[&str])
        );
        assert_eq!(
            bridge_complement("RPC_IMPL"),
            Some(&["RPC_CALL", "RPC_ROUTE"] as &[&str])
        );
        assert_eq!(
            bridge_complement("RPC_ROUTE"),
            Some(&["RPC_IMPL"] as &[&str])
        );
        assert_eq!(
            bridge_complement("HTTP_CALL"),
            Some(&["HTTP_ROUTE"] as &[&str])
        );
        assert_eq!(
            bridge_complement("HTTP_ROUTE"),
            Some(&["HTTP_CALL"] as &[&str])
        );
        assert_eq!(
            bridge_complement("CONFIG_SOURCE"),
            Some(&["CONFIG_READ"] as &[&str])
        );
        assert_eq!(
            bridge_complement("CONFIG_READ"),
            Some(&["CONFIG_SOURCE"] as &[&str])
        );
        assert_eq!(bridge_complement("CALLS"), None);
    }
}
