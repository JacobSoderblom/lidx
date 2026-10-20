//! Broker SDK calls name the topic or queue they send to or receive from,
//! whatever the client variable is called: Azure Service Bus senders and
//! receivers in Python, .NET, JavaScript and Go, and Confluent's .NET
//! `ProduceAsync`. The topic still comes from a value (a literal or a
//! constant), never from expression text.

use lidx::indexer::csharp::CSharpExtractor;
use lidx::indexer::extract::{EdgeInput, LanguageExtractor};
use lidx::indexer::go::GoExtractor;
use lidx::indexer::javascript::TypescriptExtractor;
use lidx::indexer::python::PythonExtractor;

fn channel_edges(edges: Vec<EdgeInput>) -> Vec<(String, String, String)> {
    edges
        .into_iter()
        .filter(|e| e.kind == "CHANNEL_PUBLISH" || e.kind == "CHANNEL_SUBSCRIBE")
        .map(|e| {
            let framework =
                serde_json::from_str::<serde_json::Value>(e.detail.as_deref().unwrap_or("{}"))
                    .unwrap()["framework"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
            (e.kind, e.target_qualname.unwrap_or_default(), framework)
        })
        .collect()
}

fn own(v: &[(&str, &str, &str)]) -> Vec<(String, String, String)> {
    v.iter()
        .map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
        .collect()
}

#[test]
fn python_service_bus_senders_and_receivers_name_their_topic() {
    let src = "ORDERS = 'order-created'\n\ndef send(client):\n    with client.get_topic_sender(topic_name=ORDERS) as s:\n        s.send_messages([])\n\ndef jobs(client):\n    return client.get_queue_sender(queue_name='jobs')\n\ndef listen(client):\n    return client.get_subscription_receiver(topic_name='order-created', subscription_name='billing')\n\ndef work(client):\n    return client.get_queue_receiver('jobs')\n\ndef dynamic(client, topic):\n    return client.get_topic_sender(topic_name=topic)\n";
    let mut x = PythonExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(src, "m").unwrap().edges),
        own(&[
            (
                "CHANNEL_PUBLISH",
                "channel://ordercreated",
                "azure-service-bus"
            ),
            ("CHANNEL_PUBLISH", "channel://jobs", "azure-service-bus"),
            (
                "CHANNEL_SUBSCRIBE",
                "channel://ordercreated",
                "azure-service-bus"
            ),
            ("CHANNEL_SUBSCRIBE", "channel://jobs", "azure-service-bus"),
        ])
    );
}

#[test]
fn dotnet_service_bus_and_kafka_calls_name_their_topic() {
    let src = "class Bus {\n  const string Topic = \"order-created\";\n  void Send(ServiceBusClient client) { var s = client.CreateSender(Topic); }\n  void Listen(ServiceBusClient c) { var p = c.CreateProcessor(\"order-created\", \"billing\"); }\n  async Task Produce(IProducer<string, string> producer) { await producer.ProduceAsync(\"payments\", null); }\n  void Dynamic(ServiceBusClient c, string topic) { c.CreateSender(topic); }\n}\n";
    let mut x = CSharpExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(src, "M").unwrap().edges),
        own(&[
            (
                "CHANNEL_PUBLISH",
                "channel://ordercreated",
                "azure-service-bus"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "channel://ordercreated",
                "azure-service-bus"
            ),
            ("CHANNEL_PUBLISH", "channel://payments", "kafka"),
        ])
    );
}

#[test]
fn typescript_and_go_service_bus_clients_name_their_topic() {
    let ts = "const STATUS = 'job-status';\nexport function wire(sb: ServiceBusClient) {\n  const out = sb.createSender(STATUS);\n  const inbox = sb.createReceiver('job-requests', 'worker');\n  return [out, inbox];\n}\n";
    let mut x = TypescriptExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(ts, "m").unwrap().edges),
        own(&[
            (
                "CHANNEL_PUBLISH",
                "channel://jobstatus",
                "azure-service-bus"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "channel://jobrequests",
                "azure-service-bus"
            ),
        ])
    );
    let go = "package main\n\nfunc wire(c *azservicebus.Client) {\n\ts, _ := c.NewSender(\"job-status\", nil)\n\tr, _ := c.NewReceiverForSubscription(\"job-requests\", \"worker\", nil)\n\t_, _ = s, r\n}\n";
    let mut g = GoExtractor::new().unwrap();
    assert_eq!(
        channel_edges(g.extract(go, "main").unwrap().edges),
        own(&[
            (
                "CHANNEL_PUBLISH",
                "channel://jobstatus",
                "azure-service-bus"
            ),
            (
                "CHANNEL_SUBSCRIBE",
                "channel://jobrequests",
                "azure-service-bus"
            ),
        ])
    );
}

#[test]
fn an_unknown_method_on_an_unknown_receiver_names_no_topic() {
    let src = "def f(client):\n    return client.make_sender(topic_name='x')\n";
    let mut x = PythonExtractor::new().unwrap();
    assert!(channel_edges(x.extract(src, "m").unwrap().edges).is_empty());
}
