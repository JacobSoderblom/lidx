//! The resources on the `lidx graph` landing page: databases, caches,
//! brokers, secret stores, storage and external systems found from
//! manifests, imports, infrastructure files and index edges; the arrows from
//! apps to them with their verbs and evidence; flows through them; and where
//! the apps run.
mod common;

use common::glance_fixtures::{ASPIRE, CLI, K8S_NS, STACK};
use serde_json::{Value, json};
use std::path::Path;
use std::process::Command;

fn glance(files: &[(&str, &str)]) -> Value {
    let (_tmp, root, db) = common::index_repo("lidx-res-", files);
    glance_at(&root, &db)
}

fn glance_at(root: &Path, db: &Path) -> Value {
    let out = root.join("glance-out.html");
    let res = Command::new(env!("CARGO_BIN_EXE_lidx"))
        .arg("graph")
        .arg("--repo")
        .arg(root)
        .arg("--db")
        .arg(db)
        .arg("--out")
        .arg(&out)
        .output()
        .unwrap();
    assert!(
        res.status.success(),
        "{}",
        String::from_utf8_lossy(&res.stderr)
    );
    let html = std::fs::read_to_string(out).unwrap();
    lidx::graph_html::extract_document(&html).unwrap()["apps"]["glance"].clone()
}

fn name(g: &Value, i: &Value) -> String {
    g["nodes"][i.as_u64().unwrap() as usize]["name"]
        .as_str()
        .unwrap()
        .to_string()
}

/// `(name, place, lane, sentence)` of every resource node.
fn resources(g: &Value) -> Vec<(String, String, String, String)> {
    g["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|n| n["kind"] == "resource")
        .map(|n| {
            (
                n["name"].as_str().unwrap().to_string(),
                n["place"].as_str().unwrap().to_string(),
                n["lane"].as_str().unwrap().to_string(),
                n["sentence"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// `(from, to, family, label)` of every link to a resource and every
/// deployment wire.
fn res_links(g: &Value) -> Vec<(String, String, String, String)> {
    g["links"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|l| {
            matches!(
                l["family"].as_str(),
                Some("data" | "msg" | "secret" | "store" | "ext" | "wire")
            )
        })
        .map(|l| {
            (
                name(g, &l["from"]),
                name(g, &l["to"]),
                l["family"].as_str().unwrap().to_string(),
                l["label"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn link<'a>(g: &'a Value, from: &str, to: &str) -> &'a Value {
    g["links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| name(g, &l["from"]) == from && name(g, &l["to"]) == to)
        .unwrap_or_else(|| panic!("no link {from} -> {to}"))
}

fn node<'a>(g: &'a Value, n: &str) -> &'a Value {
    g["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["name"] == n)
        .unwrap_or_else(|| panic!("no node {n}"))
}

/// `(label, sentence, member names)` of every deployment group.
fn deploy(g: &Value) -> Vec<(String, String, Vec<String>)> {
    g["deploy"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["label"].as_str().unwrap().to_string(),
                d["sentence"].as_str().unwrap().to_string(),
                d["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| name(g, i))
                    .collect(),
            )
        })
        .collect()
}

fn own4(v: &[(&str, &str, &str, &str)]) -> Vec<(String, String, String, String)> {
    v.iter()
        .map(|(a, b, c, d)| (a.to_string(), b.to_string(), c.to_string(), d.to_string()))
        .collect()
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn a_compose_stack_shows_each_resource_category_with_verbs_and_evidence() {
    let g = glance(STACK);
    assert_eq!(g["mode"], "apps");
    assert_eq!(
        resources(&g),
        own4(&[
            (
                "PostgreSQL",
                "card",
                "data",
                "PostgreSQL database with 1 service (db); used by 1 app; declared in Docker Compose."
            ),
            (
                "Redis",
                "card",
                "data",
                "Redis cache with 1 service (cache); used by 1 app; declared in Docker Compose."
            ),
            (
                "RabbitMQ",
                "card",
                "data",
                "RabbitMQ message broker with 1 service (mq) and 1 topic (ordercreated); 1 app publishes; 1 app subscribes; declared in Docker Compose."
            ),
            (
                "Amazon SQS",
                "card",
                "data",
                "Amazon SQS message broker with 1 queue (refunds); declared in Terraform; no code here was found using it."
            ),
            (
                "Azure Key Vault",
                "card",
                "data",
                "Azure Key Vault secret store; used by 1 app; declared in Bicep."
            ),
            (
                "Amazon S3",
                "card",
                "data",
                "Amazon S3 object storage; used by 1 app."
            ),
            (
                "api.payhub.io",
                "card",
                "ext",
                "External API at api.payhub.io; used by 1 app."
            ),
        ])
    );
    assert_eq!(
        res_links(&g),
        own4(&[
            ("web", "api.payhub.io", "ext", "calls over HTTP ×1"),
            ("orders", "PostgreSQL", "data", "reads/writes"),
            ("orders", "Redis", "data", "uses"),
            ("orders", "RabbitMQ", "msg", "publishes ordercreated"),
            ("orders", "Amazon S3", "store", "stores objects"),
            ("worker", "RabbitMQ", "msg", "subscribes to ordercreated"),
            ("worker", "Azure Key Vault", "secret", "reads secrets"),
        ])
    );
    // The direct publisher -> subscriber link waits for a focus: the two
    // apps are drawn through the broker.
    assert_eq!(link(&g, "orders", "worker")["quiet"], true);
    // Evidence: the code first (the topic at its publisher, the import),
    // then the deployment, then the manifest.
    assert_eq!(
        link(&g, "orders", "RabbitMQ")["calls"][0],
        json!({ "key": "ordercreated", "from": { "qual": "orders.main.save", "file": "orders/orders/main.py", "line": 15 } })
    );
    assert_eq!(
        link(&g, "orders", "PostgreSQL")["calls"],
        json!([
            { "text": "import psycopg", "file": "orders/orders/main.py", "line": 1, "how": "import" },
            { "text": "deployment: orders depends on db", "file": "docker-compose.yml", "line": 5, "how": "deploy" },
            { "text": "deployment: DATABASE_URL=postgresql://app:pw@db:5432/orders", "file": "docker-compose.yml", "line": 8, "how": "deploy" },
            { "text": "declares dependencies = [\"fastapi\", \"psycopg\", \"redis\", \"pika\", \"boto3\"]", "file": "orders/pyproject.toml", "line": 3, "how": "manifest" },
        ])
    );
    assert_eq!(
        link(&g, "orders", "Amazon S3")["calls"][0]["text"],
        "s3 = boto3.client(\"s3\")"
    );
    // Declared where: the compose service, the Terraform resource.
    assert_eq!(
        node(&g, "PostgreSQL")["res"]["declared"],
        json!([{ "text": "db:", "file": "docker-compose.yml", "line": 14 }])
    );
    assert_eq!(
        node(&g, "Amazon SQS")["res"]["kinds"],
        json!([["aws_sqs_queue", 1]])
    );
    // A type no category covers stays in the detail of the chip of its
    // folder (Bicep and Terraform side by side in infra/).
    assert_eq!(
        node(&g, "Bicep templates")["other"]["types"],
        json!([["aws_iam_role", 1]])
    );
    // Cards say what they use.
    assert_eq!(
        node(&g, "orders")["sentence"],
        "Python web API serving 1 HTTP route; publishes 1 topic, uses Amazon S3, PostgreSQL, RabbitMQ and 1 more."
    );
    // A flow crosses the broker as a step of its own, and one reaches the
    // database the route's code writes to.
    let titles: Vec<&str> = g["flows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["title"].as_str().unwrap())
        .collect();
    assert_eq!(
        titles,
        [
            "on order-created → RabbitMQ → worker",
            "POST /orders → orders → PostgreSQL"
        ]
    );
    let steps = |i: usize| -> Vec<(String, String)> {
        g["flows"][i]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| (name(&g, &s["node"]), s["via"].as_str().unwrap().to_string()))
            .collect()
    };
    assert_eq!(
        steps(0),
        [
            ("orders", "start"),
            ("RabbitMQ", "publish"),
            ("worker", "bus")
        ]
        .map(|(a, b)| (a.to_string(), b.to_string()))
    );
    assert_eq!(
        steps(1),
        [
            ("orders", "start"),
            ("orders", "call"),
            ("PostgreSQL", "db")
        ]
        .map(|(a, b)| (a.to_string(), b.to_string()))
    );
    // The tour explains how the apps meet at the resources.
    let data = g["tour"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"] == "data")
        .unwrap();
    assert_eq!(data["title"], "Data and messaging");
    assert!(
        data["text"].as_str().unwrap().starts_with(
            "The apps keep data and pass messages through 6 shared resources: RabbitMQ (2 apps), "
        ),
        "{}",
        data["text"]
    );
    // Where it runs.
    assert_eq!(
        deploy(&g),
        vec![
            (
                "Docker Compose (docker-compose.yml)".to_string(),
                "Runs as 2 services in Docker Compose (docker-compose.yml).".to_string(),
                strs(&["orders", "worker"])
            ),
            (
                "Not deployed by files in this repository".to_string(),
                "1 app: no Dockerfile or deployment file here runs them.".to_string(),
                strs(&["web"])
            ),
        ]
    );
}

#[test]
fn a_broker_used_through_a_library_is_named_and_an_unnamed_one_stays_neutral() {
    // Both apps reach the broker client through the shared library they
    // depend on: the topic's broker is Kafka, not a guess.
    let files: &[(&str, &str)] = &[
        (
            "bus/pyproject.toml",
            "[project]\nname = \"bus\"\ndependencies = [\"confluent-kafka\"]\n",
        ),
        (
            "bus/bus/client.py",
            "from confluent_kafka import Producer\n\n\ndef send(topic, msg):\n    return Producer({}).produce(topic, msg)\n\n\ndef remind():\n    _bus.publish(\"invoice-due\", {})\n",
        ),
        (
            "api/pyproject.toml",
            "[project]\nname = \"api\"\ndependencies = [\"fastapi\", \"bus\"]\n",
        ),
        (
            "api/api/main.py",
            "from fastapi import FastAPI\nfrom bus.client import send\n\napp = FastAPI()\n\n\n@app.post(\"/pay\")\ndef pay():\n    send(\"x\", {})\n    _bus.publish(\"payment-made\", {})\n    return 1\n",
        ),
        (
            "mailer/pyproject.toml",
            "[project]\nname = \"mailer\"\ndependencies = [\"bus\"]\n\n[project.scripts]\nmailer = \"mailer.main:run\"\n",
        ),
        (
            "mailer/mailer/main.py",
            "@router.subscribe(\"payment-made\")\ndef on_paid(msg):\n    return 1\n\n\n@router.subscribe(\"invoice-due\")\ndef on_due(msg):\n    return 1\n\n\ndef run():\n    pass\n",
        ),
        // Another app on RabbitMQ: the repository has two brokers, so the
        // topics' broker comes from the library the apps depend on.
        (
            "jobs/pyproject.toml",
            "[project]\nname = \"jobs\"\ndependencies = [\"pika\"]\n\n[project.scripts]\njobs = \"jobs.main:run\"\n",
        ),
        (
            "jobs/jobs/main.py",
            "import pika\n\n\ndef run():\n    return 1\n",
        ),
    ];
    let g = glance(files);
    let kafka: Vec<_> = resources(&g).into_iter().map(|r| (r.0, r.3)).collect();
    assert_eq!(
        kafka,
        vec![
            ("RabbitMQ".to_string(), "RabbitMQ message broker; used by 1 app.".to_string()),
            (
                "Kafka".to_string(),
                "Kafka message broker with 2 topics (invoicedue and paymentmade); 1 app publishes; 1 app subscribes.".to_string()
            ),
        ]
    );
    assert_eq!(
        res_links(&g),
        own4(&[
            ("api", "Kafka", "msg", "publishes paymentmade"),
            ("jobs", "RabbitMQ", "msg", "uses"),
            ("mailer", "Kafka", "msg", "subscribes to 2 topics"),
        ])
    );
    // A topic no app on the map publishes (the library does) starts its
    // flow at the broker.
    let flows: Vec<Vec<(String, String)>> = g["flows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            f["steps"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| (name(&g, &s["node"]), s["via"].as_str().unwrap().to_string()))
                .collect()
        })
        .collect();
    assert!(
        flows.contains(&vec![
            ("Kafka".to_string(), "start".to_string()),
            ("mailer".to_string(), "bus".to_string())
        ]),
        "{flows:?}"
    );
    // The library's own use is evidence on the app's line.
    assert!(
        link(&g, "api", "Kafka")["calls"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["how"] == "via" && c["text"].as_str().unwrap().starts_with("through bus: ")),
        "{}",
        link(&g, "api", "Kafka")["calls"]
    );
}

#[test]
fn http_calls_out_of_the_repository_group_by_host() {
    let files: &[(&str, &str)] = &[
        (
            "web/package.json",
            "{ \"name\": \"web\", \"scripts\": { \"dev\": \"vite\" }, \"dependencies\": { \"react\": \"18.0.0\" } }\n",
        ),
        (
            "web/src/a.ts",
            "export function a() {\n  return fetch(\"https://api.weather.dev/v2/today\");\n}\n",
        ),
        (
            "web/src/b.ts",
            "export function b() {\n  return fetch(\"https://api.weather.dev/v2/week\");\n}\n\nexport function c() {\n  return fetch(\"http://localhost:8080/health\");\n}\n\nexport function d() {\n  return fetch(\"https://api.github.com/repos/x/y\");\n}\n",
        ),
        (
            "api/package.json",
            "{ \"name\": \"api\", \"scripts\": { \"start\": \"node server.js\" }, \"dependencies\": { \"express\": \"4.0.0\" } }\n",
        ),
        (
            "api/server.js",
            "const app = require('express')();\napp.get('/v2/today', (req, res) => res.send('x'));\n",
        ),
    ];
    let g = glance(files);
    let ext: Vec<(String, String)> = resources(&g).into_iter().map(|r| (r.0, r.2)).collect();
    assert_eq!(
        ext,
        vec![
            ("api.weather.dev".to_string(), "ext".to_string()),
            ("GitHub".to_string(), "ext".to_string()),
        ],
        "one node per host, a known host by its service; localhost is no external system"
    );
    assert_eq!(
        node(&g, "api.weather.dev")["res"]["items"],
        json!([
            ["GET api.weather.dev/v2/today", "call"],
            ["GET api.weather.dev/v2/week", "call"]
        ])
    );
    assert_eq!(
        link(&g, "web", "api.weather.dev")["label"],
        "calls over HTTP ×2"
    );
}

#[test]
fn an_aspire_app_host_declares_resources_and_wires_its_projects() {
    let g = glance(ASPIRE);
    assert_eq!(
        resources(&g),
        own4(&[
            (
                "RabbitMQ",
                "card",
                "data",
                "RabbitMQ message broker; used by 2 apps; declared in Aspire."
            ),
            (
                "PostgreSQL",
                "card",
                "data",
                "PostgreSQL database with 1 database (ordersdb); used by 1 app; declared in Aspire."
            ),
        ])
    );
    assert_eq!(
        res_links(&g),
        own4(&[
            ("Catalog.Api", "RabbitMQ", "msg", "uses"),
            ("Orders.Api", "RabbitMQ", "msg", "uses"),
            ("Orders.Api", "PostgreSQL", "data", "reads/writes"),
            (
                "Orders.Api",
                "Catalog.Api",
                "wire",
                "calls (set in deployment)"
            ),
        ])
    );
    assert_eq!(
        link(&g, "Orders.Api", "Catalog.Api")["calls"],
        json!([{ "text": "deployment: orders-api .WithReference(catalog)", "file": "src/AppHost/Program.cs", "line": 7 }])
    );
    // The app host is deployment, not a library: a chip, and the group
    // its projects run in.
    assert_eq!(node(&g, "Aspire app host")["place"], "chip");
    assert_eq!(
        deploy(&g),
        vec![(
            "Aspire app host AppHost".to_string(),
            "2 apps run together by the .NET Aspire app host AppHost.".to_string(),
            strs(&["Catalog.Api", "Orders.Api"])
        )]
    );
}

#[test]
fn kubernetes_groups_apps_by_namespace_and_env_hosts_wire_them() {
    let g = glance(K8S_NS);
    assert_eq!(
        resources(&g),
        own4(&[(
            "Redis",
            "card",
            "data",
            "Redis cache with 1 service (redis-cart); used by 1 app; declared in Kubernetes."
        )])
    );
    assert_eq!(
        res_links(&g),
        own4(&[
            ("cart", "Redis", "data", "uses"),
            ("front", "cart", "wire", "calls (set in deployment)"),
        ])
    );
    assert_eq!(
        link(&g, "cart", "Redis")["calls"],
        json!([{ "text": "deployment: REDIS_ADDR: redis-cart:6379", "file": "deploy/cart.yaml", "line": 16, "how": "deploy" }])
    );
    assert_eq!(
        deploy(&g),
        vec![(
            "Kubernetes namespace shop".to_string(),
            "Runs as 2 containers in Kubernetes namespace shop.".to_string(),
            strs(&["cart", "front"])
        )]
    );
    assert_eq!(node(&g, "cart")["deploy"], json!([0]));
}

#[test]
fn a_single_command_line_app_has_no_resources_and_no_empty_lane() {
    let g = glance(CLI);
    assert!(resources(&g).is_empty());
    let lanes: Vec<&str> = g["lanes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["id"].as_str().unwrap())
        .collect();
    assert_eq!(lanes, ["mod"]);
    assert_eq!(g["deploy"], json!([]));
    assert!(
        g["tour"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["id"] != "data"),
        "no data step without resources"
    );
}

#[test]
fn resources_past_the_cap_wait_behind_show_all() {
    // Ten distinct databases a compose file runs and one app connects to.
    let images = [
        "postgres",
        "mysql",
        "mariadb",
        "mongo",
        "cassandra",
        "neo4j",
        "redis",
        "memcached",
        "elasticsearch",
        "rabbitmq",
        "nats",
    ];
    let mut compose = String::from("services:\n  app:\n    build: ./app\n    depends_on:\n");
    for i in &images {
        compose.push_str(&format!("      - {i}\n"));
    }
    for i in &images {
        compose.push_str(&format!("  {i}:\n    image: {i}:latest\n"));
    }
    let files: Vec<(&str, &str)> = vec![
        ("docker-compose.yml", compose.as_str()),
        (
            "app/package.json",
            "{ \"name\": \"app\", \"scripts\": { \"start\": \"node index.js\" } }\n",
        ),
        ("app/index.js", "function main() {}\nmain();\n"),
    ];
    let g = glance(&files);
    let all = resources(&g);
    assert_eq!(all.len(), 11);
    let on_map = all.iter().filter(|r| r.1 == "card").count();
    assert_eq!(on_map, 8, "the cap");
    assert_eq!(g["hidden"]["resources"], 3);
    // Ties go by name: the first eight alphabetically stay.
    let hidden: Vec<&str> = all
        .iter()
        .filter(|r| r.1 == "off")
        .map(|r| r.0.as_str())
        .collect();
    assert_eq!(hidden, ["PostgreSQL", "Redis", "RabbitMQ"], "{all:?}");
}
