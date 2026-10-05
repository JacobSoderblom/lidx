//! Issue #220: a channel topic is derived from a *value* (a string literal's
//! content or a same-file constant), never from raw expression text.

use lidx::indexer::csharp::CSharpExtractor;
use lidx::indexer::extract::{EdgeInput, LanguageExtractor};
use lidx::indexer::go::GoExtractor;
use lidx::indexer::javascript::{JavascriptExtractor, TypescriptExtractor};
use lidx::indexer::python::PythonExtractor;
use lidx::indexer::rust::RustExtractor;

fn channel_edges(edges: Vec<EdgeInput>) -> Vec<(String, String)> {
    edges
        .into_iter()
        .filter(|e| e.kind == "CHANNEL_PUBLISH" || e.kind == "CHANNEL_SUBSCRIBE")
        .map(|e| (e.kind, e.target_qualname.unwrap_or_default()))
        .collect()
}

fn py(source: &str) -> Vec<(String, String)> {
    let mut x = PythonExtractor::new().unwrap();
    channel_edges(x.extract(source, "m").unwrap().edges)
}

fn cs(source: &str) -> Vec<(String, String)> {
    let mut x = CSharpExtractor::new().unwrap();
    channel_edges(x.extract(source, "M").unwrap().edges)
}

fn go(source: &str) -> Vec<(String, String)> {
    let mut x = GoExtractor::new().unwrap();
    channel_edges(x.extract(source, "main").unwrap().edges)
}

fn js(source: &str) -> Vec<(String, String)> {
    let mut x = JavascriptExtractor::new().unwrap();
    channel_edges(x.extract(source, "m").unwrap().edges)
}

fn ts(source: &str) -> Vec<(String, String)> {
    let mut x = TypescriptExtractor::new().unwrap();
    channel_edges(x.extract(source, "m").unwrap().edges)
}

fn rs(source: &str) -> Vec<(String, String)> {
    let mut x = RustExtractor::new().unwrap();
    channel_edges(x.extract(source, "m").unwrap().edges)
}

fn targets(edges: &[(String, String)]) -> Vec<&str> {
    edges.iter().map(|(_, t)| t.as_str()).collect()
}

#[test]
fn python_decorator_literal_has_no_quotes_for_every_quote_form() {
    for lit in [
        r#""orders""#,
        "'orders'",
        r#""""orders""""#,
        "'''orders'''",
        r#"r"orders""#,
    ] {
        let src = format!("@router.subscribe(topic={lit})\ndef handle(msg):\n    pass\n");
        assert_eq!(targets(&py(&src)), ["channel://orders"], "literal {lit}");
    }
    // Positional form too.
    let src = "@router.subscribe('orders')\ndef handle(msg):\n    pass\n";
    assert_eq!(targets(&py(src)), ["channel://orders"]);
}

#[test]
fn python_module_constant_resolves_and_bridges_with_literal_publisher() {
    let sub =
        "ORDERS = 'order-created'\n\n@router.subscribe(topic=ORDERS)\ndef handle(msg):\n    pass\n";
    let publ = "def send(msg):\n    _bus.publish(\"order-created\", msg)\n";
    let s = py(sub);
    let p = py(publ);
    assert_eq!(targets(&s), ["channel://ordercreated"]);
    assert_eq!(targets(&p), ["channel://ordercreated"]);
    assert_eq!(s[0].0, "CHANNEL_SUBSCRIBE");
    assert_eq!(p[0].0, "CHANNEL_PUBLISH");
}

#[test]
fn python_class_constant_and_local_binding() {
    let src = "class T:\n    ORDERS = 'orders'\n\ndef send(msg):\n    topic = T.ORDERS\n    _bus.publish(topic, msg)\n";
    assert_eq!(targets(&py(src)), ["channel://orders"]);
}

#[test]
fn python_non_literal_topics_emit_no_edge() {
    for src in [
        "def send(msg):\n    topic = topic_for()\n    _bus.publish(topic, msg)\n",
        "def send(topic, msg):\n    _bus.publish(topic, msg)\n",
        "def send(msg):\n    _bus.publish(topic_for(), msg)\n",
        "def send(msg, name):\n    _bus.publish(f'orders.{name}', msg)\n",
        "ORDERS = 'orders'\ndef send(ORDERS, msg):\n    _bus.publish(ORDERS, msg)\n",
    ] {
        assert!(
            py(src).is_empty(),
            "expected no edge for {src:?}: {:?}",
            py(src)
        );
    }
}

#[test]
fn csharp_literal_forms() {
    for lit in [
        r#""orders""#,
        r#"@"orders""#,
        r#"$"orders""#,
        r#"$@"orders""#,
    ] {
        let src = format!("class P {{ void Run() {{ _bus.PublishAsync({lit}, msg); }} }}");
        assert_eq!(targets(&cs(&src)), ["channel://orders"], "literal {lit}");
    }
}

#[test]
fn csharp_local_bound_to_class_constant_resolves() {
    let src = r#"
class P {
    private const string OrdersTopic = "orders";
    void Run() {
        var topic = OrdersTopic;
        _bus.PublishAsync(topic, msg);
    }
    void Direct() { _bus.PublishAsync(OrdersTopic, msg); }
}"#;
    let edges = cs(src);
    assert_eq!(targets(&edges), ["channel://orders", "channel://orders"]);
}

#[test]
fn csharp_literal_publisher_bridges_with_constant_subscriber() {
    let publ = r#"class P { void Run() { _bus.PublishAsync("order-created", msg); } }"#;
    let sub = r#"class S { const string T = "order-created"; void Run() { _bus.SubscribeAsync(T, h); } }"#;
    let p = cs(publ);
    let s = cs(sub);
    assert_eq!(
        p[0],
        ("CHANNEL_PUBLISH".into(), "channel://ordercreated".into())
    );
    assert_eq!(
        s[0],
        ("CHANNEL_SUBSCRIBE".into(), "channel://ordercreated".into())
    );
}

#[test]
fn csharp_computed_topics_emit_no_phantom_edge() {
    for src in [
        // Local assigned from a call: the real topic is computed.
        "class P { void Run() { var topic = TopicFor(); _bus.PublishAsync(topic, msg); } }",
        // Call expression directly.
        "class P { void Run() { _bus.PublishAsync(TopicFor(), msg); } }",
        // Parameter.
        "class P { void Run(string topic) { _bus.PublishAsync(topic, msg); } }",
        // Mock matchers.
        "class P { void Run() { _bus.PublishAsync(Arg.Any<string>(), null); } }",
        "class P { void Run() { _bus.PublishAsync(It.IsAny<string>(), null); } }",
        // Interpolation with holes.
        r#"class P { void Run(string id) { _bus.PublishAsync($"orders.{id}", msg); } }"#,
        // Subscribe side.
        "class P { void Run() { _bus.SubscribeAsync(TopicFor(), h); } }",
    ] {
        assert!(
            cs(src).is_empty(),
            "expected no edge for {src}: {:?}",
            cs(src)
        );
    }
}

#[test]
fn csharp_unresolvable_names_emit_no_phantom_edge() {
    for src in [
        // Member of a foreign object: not a topic, never `channel://order.topic`.
        "class P { void Run() { _bus.PublishAsync(order.Topic, msg); } }",
        "class P { void Run() { _bus.PublishAsync(settings.TOPIC, msg); } }",
        // Bare PascalCase / UPPER name defined in another file.
        "class P { void Run() { _bus.PublishAsync(OrdersTopic, msg); } }",
        "class P { void Run() { _bus.PublishAsync(ORDERS, msg); } }",
        // Container member whose class is not in this file.
        "class P { void Run() { _bus.PublishAsync(Topics.DataProxyStatus, msg); } }",
        // ... and a local bound to the same unresolvable member.
        "class P { void Run() { var t = Topics.DataProxyStatus; _bus.PublishAsync(t, msg); } }",
    ] {
        assert!(
            cs(src).is_empty(),
            "expected no edge for {src}: {:?}",
            cs(src)
        );
    }
}

#[test]
fn csharp_container_member_resolves_the_same_directly_and_via_local() {
    let src = r#"
static class Topics { public const string DataProxyStatus = "data-proxy-status"; }
class P {
    void Direct() { _bus.PublishAsync(Topics.DataProxyStatus, msg); }
    void ViaLocal() { var t = Topics.DataProxyStatus; _bus.PublishAsync(t, msg); }
}"#;
    assert_eq!(
        targets(&cs(src)),
        ["channel://dataproxystatus", "channel://dataproxystatus"]
    );
}

#[test]
fn csharp_this_class_and_nested_qualified_constants() {
    let src = r#"
class Outer {
    public const string A = "alpha";
    public class Inner { public const string B = "beta"; }
    void R() {
        _bus.PublishAsync(this.A, m);
        _bus.PublishAsync(Outer.A, m);
        _bus.PublishAsync(Outer.Inner.B, m);
        _bus.PublishAsync(Inner.B, m);
    }
}"#;
    assert_eq!(
        targets(&cs(src)),
        [
            "channel://alpha",
            "channel://alpha",
            "channel://beta",
            "channel://beta"
        ]
    );
}

#[test]
fn csharp_same_name_with_different_values_is_ambiguous() {
    let src = r#"
class A { public const string T = "one"; }
class B { public const string T = "two"; void R() { _bus.PublishAsync(T, m); _bus.PublishAsync(A.T, m); _bus.PublishAsync(B.T, m); } }
"#;
    // Bare `T` is ambiguous (dropped); the qualified forms still resolve.
    assert_eq!(targets(&cs(src)), ["channel://one", "channel://two"]);
}

#[test]
fn python_self_cls_class_qualified_and_foreign_receivers() {
    let src = r#"
class T:
    ORDERS = 'orders'

    def a(self):
        _bus.publish(self.ORDERS, m)

    @classmethod
    def b(cls):
        _bus.publish(cls.ORDERS, m)

def c():
    _bus.publish(T.ORDERS, m)
    _bus.publish(settings.ORDERS, m)
    _bus.publish(order.topic, m)
    _bus.publish(OrdersTopic, m)
"#;
    assert_eq!(
        targets(&py(src)),
        ["channel://orders", "channel://orders", "channel://orders"]
    );
}

#[test]
fn go_literals_and_constants() {
    let src = r#"
package main

const Orders = "orders"

func a() { bus.Publish("orders", e) }
func b() { bus.Subscribe(Orders, h) }
func c() { bus.Subscribe(`orders`, h) }
func d() { bus.Publish(topicFor(), e) }
func e(topic string) { bus.Publish(topic, e) }
"#;
    assert_eq!(
        targets(&go(src)),
        ["channel://orders", "channel://orders", "channel://orders"]
    );
}

#[test]
fn javascript_and_typescript_literals_and_constants() {
    let src = r#"
const ORDERS = 'orders';
function a() { bus.publish("orders", e); }
function b() { bus.subscribe(ORDERS, h); }
function c() { bus.subscribe(`orders`, h); }
function d(id) { bus.publish(`orders.${id}`, e); }
function e() { bus.publish(topicFor(), e); }
function f(topic) { bus.publish(topic, e); }
"#;
    for edges in [js(src), ts(src)] {
        assert_eq!(
            targets(&edges),
            ["channel://orders", "channel://orders", "channel://orders"]
        );
    }
}

#[test]
fn rust_literals_and_constants() {
    let src = r##"
const ORDERS: &str = "orders";
fn a() { bus.publish("orders", e); }
fn b() { bus.subscribe(ORDERS, h); }
fn c() { bus.subscribe(r#"orders"#, h); }
fn d() { bus.publish(topic_for(), e); }
fn e(topic: &str) { bus.publish(topic, e); }
"##;
    assert_eq!(
        targets(&rs(src)),
        ["channel://orders", "channel://orders", "channel://orders"]
    );
}

#[test]
fn no_channel_name_contains_quote_paren_or_angle_bracket() {
    let mut all = Vec::new();
    all.extend(py(
        "@r.subscribe(topic=\"a\")\ndef h(): pass\n@r.subscribe(f(1))\ndef g(): pass\n",
    ));
    all.extend(cs(
        "class P { void R() { _bus.PublishAsync(Arg.Any<string>(), null); _bus.PublishAsync(\"x\", 1); _bus.PublishAsync(T(), 1); } }",
    ));
    all.extend(go(
        "package main\nfunc a() { bus.Publish(\"x\", e); bus.Publish(f(), e) }\n",
    ));
    all.extend(js(
        "function a() { bus.publish(\"x\", e); bus.publish(f(), e); }",
    ));
    all.extend(rs("fn a() { bus.publish(\"x\", e); bus.publish(f(), e); }"));
    assert!(!all.is_empty());
    for (_, name) in &all {
        assert!(
            !name.contains(['"', '\'', '`', '(', ')', '<', '>']),
            "bad channel name {name}"
        );
    }
}

mod bridge {
    use lidx::indexer::Indexer;
    use lidx::rpc;
    use serde_json::Value;

    /// Issue #220 end to end: a literal-topic publisher and a subscriber whose
    /// topic is a same-file constant must be joined by `trace_flow`.
    #[test]
    fn trace_flow_crosses_bus_from_literal_publisher_to_constant_subscriber() {
        let tmp = tempfile::Builder::new()
            .prefix("lidx-channel-bridge-")
            .tempdir()
            .unwrap();
        let root = tmp.path().to_path_buf();
        std::fs::write(
            root.join("publisher.py"),
            "def send_order(msg):\n    _bus.publish(\"order-created\", msg)\n",
        )
        .unwrap();
        std::fs::write(
            root.join("subscriber.py"),
            "ORDER_TOPIC = 'order-created'\n\n@router.subscribe(topic=ORDER_TOPIC)\ndef handle_order(msg):\n    pass\n",
        )
        .unwrap();
        let db = root.join(".lidx").join(".lidx.sqlite");
        let mut indexer = Indexer::new(root.clone(), db.clone()).unwrap();
        indexer.reindex().unwrap();
        drop(indexer);

        let raw = rpc::call(
            root,
            db,
            "trace_flow".to_string(),
            r#"{"start_qualname":"publisher.send_order","direction":"downstream","max_hops":5}"#,
            "1",
        )
        .unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert!(
            raw.contains("subscriber.handle_order"),
            "trace did not cross the bus: {v}"
        );
    }
}
