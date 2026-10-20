//! Broker SDK calls name the topic or queue they send to or receive from,
//! whatever the client variable is called: Azure Service Bus senders and
//! receivers in Python, .NET, JavaScript and Go, and Confluent's .NET
//! `ProduceAsync`. The topic still comes from a value (a literal or a
//! constant), never from expression text.

mod common;

use common::golden;
use lidx::indexer::Indexer;
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

#[test]
fn python_sdk_calls_with_a_computed_topic_name_none() {
    let src = "QUEUE = 'jobs'\n\nclass Settings:\n    TOPIC = 'orders'\n\ndef param(client, topic):\n    return client.get_topic_sender(topic_name=topic)\n\ndef call(client):\n    return client.get_queue_sender(queue_name=make_name())\n\ndef fstring(client, region):\n    return client.get_topic_sender(topic_name=f'orders-{region}')\n\ndef concat(client, suffix):\n    return client.get_queue_receiver('jobs-' + suffix)\n\ndef attr(client, settings):\n    return client.get_queue_receiver(settings.QUEUE)\n\ndef local(client):\n    name = build()\n    return client.get_queue_sender(queue_name=name)\n\ndef shadow(client, QUEUE):\n    return client.get_queue_sender(queue_name=QUEUE)\n\ndef fmt(client, n):\n    return client.get_queue_sender(queue_name='jobs-{}'.format(n))\n";
    let mut x = PythonExtractor::new().unwrap();
    assert_eq!(channel_edges(x.extract(src, "m").unwrap().edges), own(&[]));
}

#[test]
fn dotnet_sdk_calls_with_a_computed_topic_name_none() {
    let src = "class Bus {\n  const string Topic = \"orders\";\n  void Param(ServiceBusClient c, string topic) { c.CreateSender(topic); }\n  void Call(ServiceBusClient c) { c.CreateSender(GetName()); }\n  void Interp(ServiceBusClient c, int id) { c.CreateSender($\"orders-{id}\"); }\n  void Concat(ServiceBusClient c, string id) { c.CreateProcessor(\"orders-\" + id); }\n  async Task Opt(IProducer<string, string> p) { await p.ProduceAsync(_options.Topic, null); }\n  void Local(ServiceBusClient c) { var t = Build(); c.CreateReceiver(t); }\n  void Shadow(ServiceBusClient c, string Topic) { c.CreateSender(Topic); }\n  void Fmt(ServiceBusClient c, int n) { c.CreateSender(string.Format(\"orders-{0}\", n)); }\n}\n";
    let mut x = CSharpExtractor::new().unwrap();
    assert_eq!(channel_edges(x.extract(src, "M").unwrap().edges), own(&[]));
}

#[test]
fn typescript_sdk_calls_with_a_computed_topic_name_none() {
    let ts = "const STATUS = 'job-status';\nexport function param(sb: ServiceBusClient, topic: string) {\n  return sb.createSender(topic);\n}\nexport function call(sb: ServiceBusClient) {\n  return sb.createSender(name());\n}\nexport function template(sb: ServiceBusClient, id: string) {\n  return sb.createSender(`jobs-${id}`);\n}\nexport function concat(sb: ServiceBusClient, id: string) {\n  return sb.createReceiver('jobs-' + id);\n}\nexport function config(sb: ServiceBusClient, cfg: any) {\n  return sb.createSender(cfg.topic);\n}\nexport function local(sb: ServiceBusClient) {\n  const t = build();\n  return sb.createSender(t);\n}\nexport function shadow(sb: ServiceBusClient, STATUS: string) {\n  return sb.createSender(STATUS);\n}\n";
    let mut x = TypescriptExtractor::new().unwrap();
    assert_eq!(channel_edges(x.extract(ts, "m").unwrap().edges), own(&[]));
}

#[test]
fn go_sdk_calls_with_a_computed_topic_name_none() {
    let go = "package main\n\nconst Status = \"job-status\"\n\nfunc param(c *azservicebus.Client, topic string) {\n\tc.NewSender(topic, nil)\n}\n\nfunc call(c *azservicebus.Client) {\n\tc.NewSender(name(), nil)\n}\n\nfunc sprintf(c *azservicebus.Client, id string) {\n\tc.NewSender(fmt.Sprintf(\"jobs-%s\", id), nil)\n}\n\nfunc concat(c *azservicebus.Client, id string) {\n\tc.NewReceiverForQueue(\"jobs-\"+id, nil)\n}\n\nfunc config(c *azservicebus.Client, cfg Config) {\n\tc.NewReceiverForSubscription(cfg.Topic, \"worker\", nil)\n}\n\nfunc local(c *azservicebus.Client) {\n\tt := build()\n\tc.NewSender(t, nil)\n}\n\nfunc shadow(c *azservicebus.Client, Status string) {\n\tc.NewSender(Status, nil)\n}\n";
    let mut g = GoExtractor::new().unwrap();
    assert_eq!(
        channel_edges(g.extract(go, "main").unwrap().edges),
        own(&[])
    );
}

#[test]
fn a_local_bound_once_to_a_literal_names_the_topic_in_typescript_and_go() {
    // A file constant of the same name stands for none of the parameters,
    // loop variables or reassigned locals below.
    let ts = "const queue = 'file-queue';\nexport function wire(sb: ServiceBusClient) {\n  const queue = 'job-requests';\n  return sb.createReceiver(queue);\n}\nexport function moved(sb: ServiceBusClient) {\n  let queue = 'a';\n  queue = 'b';\n  return sb.createSender(queue);\n}\nexport function each(sb: ServiceBusClient, names: string[]) {\n  for (const queue of names) {\n    sb.createSender(queue);\n  }\n}\nexport function caught(sb: ServiceBusClient) {\n  try { run(); } catch (queue) { sb.createSender(queue); }\n}\nexport const arrow = (queue: string) => sb.createSender(queue);\nexport const bare = queue => sb.createSender(queue);\n";
    let mut x = TypescriptExtractor::new().unwrap();
    assert_eq!(
        channel_edges(x.extract(ts, "m").unwrap().edges),
        own(&[(
            "CHANNEL_SUBSCRIBE",
            "channel://jobrequests",
            "azure-service-bus"
        )])
    );
    let go = "package main\n\nconst queue = \"package-queue\"\n\nfunc wire(c *azservicebus.Client) {\n\tqueue := \"job-requests\"\n\tc.NewReceiverForQueue(queue, nil)\n}\n\nfunc moved(c *azservicebus.Client) {\n\tvar queue = \"a\"\n\tqueue = \"b\"\n\tc.NewSender(queue, nil)\n}\n\nfunc each(c *azservicebus.Client, names []string) {\n\tfor _, queue := range names {\n\t\tc.NewSender(queue, nil)\n\t}\n}\n\nfunc pair(c *azservicebus.Client) {\n\tqueue, err := lookup()\n\t_ = err\n\tc.NewSender(queue, nil)\n}\n\nfunc bump(c *azservicebus.Client) {\n\tqueue := \"a\"\n\tqueue += \"b\"\n\tc.NewSender(queue, nil)\n}\n\nfunc declared(c *azservicebus.Client) {\n\tvar queue = \"orders\"\n\tc.NewSender(queue, nil)\n}\n\nfunc swapped(c *azservicebus.Client) {\n\tother, queue := \"a\", \"b\"\n\t_ = other\n\tc.NewSender(queue, nil)\n}\n\nfunc kind(c *azservicebus.Client, v any) {\n\tswitch queue := v.(type) {\n\tcase string:\n\t\tc.NewSender(queue, nil)\n\t}\n}\n\nfunc (s *svc) method(queue string) {\n\ts.c.NewSender(queue, nil)\n}\n";
    let mut g = GoExtractor::new().unwrap();
    assert_eq!(
        channel_edges(g.extract(go, "main").unwrap().edges),
        own(&[
            (
                "CHANNEL_SUBSCRIBE",
                "channel://jobrequests",
                "azure-service-bus"
            ),
            ("CHANNEL_PUBLISH", "channel://orders", "azure-service-bus"),
        ])
    );
}

const BUS_PY: &str = "ORDERS = 'order-created'\n\ndef send(client):\n    return client.get_topic_sender(topic_name=ORDERS)\n";
const WORKER_PY: &str = "def listen(client):\n    return client.get_subscription_receiver(topic_name='order-created', subscription_name='billing')\n";
const APP_TS: &str =
    "export function wire(sb: ServiceBusClient) {\n  return sb.createSender('job-status');\n}\n";
const BUS_CS: &str = "class Bus {\n  async Task Produce(IProducer<string, string> producer) { await producer.ProduceAsync(\"payments\", null); }\n}\n";
const JOBS_GO: &str = "package main\n\nconst Requests = \"job-requests\"\n\nfunc wire(c *azservicebus.Client) {\n\tc.NewReceiverForQueue(Requests, nil)\n}\n";

/// `(file, kind, topic)` of every live channel edge in an index.
fn channel_rows(indexer: &Indexer) -> Vec<(String, String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT f.path, e.kind, e.target_qualname FROM edges e JOIN files f ON f.id = e.file_id
             WHERE e.kind LIKE 'CHANNEL_%' AND e.graph_version = ?1
             ORDER BY f.path, e.kind, e.target_qualname",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn incremental_sync_of_sdk_topic_calls_matches_a_fresh_index() {
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-sdk-sync-",
        &[
            ("bus.py", BUS_PY),
            ("worker.py", WORKER_PY),
            ("app.ts", APP_TS),
            ("Bus.cs", BUS_CS),
        ],
    );
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    // The constant's value changes, a literal becomes a parameter, a
    // subscriber goes and a Go receiver arrives.
    let bus = BUS_PY.replace("order-created", "order-paid");
    let app = "export function wire(sb: ServiceBusClient, topic: string) {\n  return sb.createSender(topic);\n}\n";
    common::write_files(
        &root,
        &[("bus.py", &bus), ("app.ts", app), ("jobs.go", JOBS_GO)],
    );
    std::fs::remove_file(root.join("worker.py")).unwrap();
    indexer
        .sync_rel_paths(&[
            "bus.py".to_string(),
            "app.ts".to_string(),
            "jobs.go".to_string(),
            "worker.py".to_string(),
        ])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let final_tree = [
        ("bus.py", bus.as_str()),
        ("app.ts", app),
        ("Bus.cs", BUS_CS),
        ("jobs.go", JOBS_GO),
    ];
    let (_fresh_tmp, fresh_root, fresh_db) = common::index_repo("lidx-sdk-fresh-", &final_tree);
    let fresh_ix = Indexer::new(fresh_root, fresh_db).unwrap();
    let fresh = golden::snapshot_edges(
        fresh_ix.db(),
        fresh_ix.db().current_graph_version().unwrap(),
    )
    .unwrap();
    let expected = own(&[
        ("Bus.cs", "CHANNEL_PUBLISH", "channel://payments"),
        ("bus.py", "CHANNEL_PUBLISH", "channel://orderpaid"),
        ("jobs.go", "CHANNEL_SUBSCRIBE", "channel://jobrequests"),
    ]);
    assert_eq!(channel_rows(&indexer), expected);
    assert_eq!(channel_rows(&fresh_ix), expected);
    common::assert_matches_fresh(&snapshot, &fresh);
}
