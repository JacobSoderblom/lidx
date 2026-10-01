use crate::indexer::string_consts::{LocalBinding, StringConsts};
use serde_json::json;

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

/// Resolve a topic argument to a normalized `channel://` name.
///
/// A topic is derived from a *value*: a string literal's content or a
/// same-file constant (see `string_consts`). A call, parameter, unknown
/// name, foreign member access, interpolation with holes or mock matcher
/// yields `None`; a name is never derived from expression text.
pub fn resolve_topic(raw: &str, consts: &StringConsts, local: &LocalBinding) -> Option<String> {
    normalize_channel_name(&consts.resolve_arg(raw, local)?)
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
