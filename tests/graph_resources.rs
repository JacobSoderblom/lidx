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
    glance_with(root, db, &[])
}

fn glance_with(root: &Path, db: &Path, extra: &[&str]) -> Value {
    let out = root.join("glance-out.html");
    let res = Command::new(env!("CARGO_BIN_EXE_lidx"))
        .arg("graph")
        .args(extra)
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

/// Names of the resource nodes, in the order the page lists them.
fn res_names(g: &Value) -> Vec<String> {
    resources(g).into_iter().map(|r| r.0).collect()
}

/// A Node API whose only resource is MongoDB. Redis, PostgreSQL, Kafka and
/// RabbitMQ appear only where tests, dev tooling, lockfiles and comments
/// name them.
const NODE_TEST_ONLY: &[(&str, &str)] = &[
    (
        "package.json",
        "{\n  \"name\": \"shop\",\n  \"scripts\": { \"start\": \"node src/server.js\" },\n  \"dependencies\": { \"express\": \"4\", \"mongodb\": \"6\" },\n  \"devDependencies\": { \"ioredis\": \"5\", \"pg\": \"8\", \"jest\": \"29\" }\n}\n",
    ),
    (
        "src/server.js",
        "const express = require('express');\nconst { MongoClient } = require('mongodb');\n// const Redis = require('ioredis');\n// import { Kafka } from 'kafkajs';\n/* import amqp from 'amqplib'; */\n/**\n * import { Client } from 'pg';\n */\nconst app = express();\napp.get('/orders', (req, res) => res.send(new MongoClient('x')));\napp.listen(3000);\n",
    ),
    (
        "test/cache.test.js",
        "const Redis = require('ioredis');\ntest('cache', () => new Redis());\n",
    ),
    (
        "src/__tests__/db.spec.ts",
        "import { Client } from 'pg';\nit('db', () => new Client());\n",
    ),
    ("src/orders.test.ts", "import { Kafka } from 'kafkajs';\n"),
    (
        "src/billing.test.js",
        "test('charge', async () => {\n  bus.publish('order-created', {});\n  await fetch('https://api.stripe.com/v1/charges', { method: 'POST' });\n});\n",
    ),
    (
        "spec/support/broker.js",
        "const amqp = require('amqplib');\nmodule.exports = amqp;\n",
    ),
    (
        "package-lock.json",
        "{ \"name\": \"shop\", \"lockfileVersion\": 3, \"packages\": { \"node_modules/ioredis\": {}, \"node_modules/kafkajs\": {} } }\n",
    ),
    (
        "yarn.lock",
        "ioredis@^5:\n  version \"5.0.0\"\nkafkajs@^2:\n  version \"2.0.0\"\n",
    ),
    (
        "pnpm-lock.yaml",
        "lockfileVersion: '6.0'\npackages:\n  /amqplib@0.10.0:\n    resolution: {integrity: x}\n  /pg@8.0.0:\n    resolution: {integrity: y}\n",
    ),
    (
        "docker-compose.yml",
        "services:\n  api:\n    build: .\n  mongo:\n    image: mongo:7\n#  redis:\n#    image: redis:7\n  # broker:\n  #   image: rabbitmq:3\n",
    ),
    (
        "docker-compose.test.yml",
        "services:\n  api:\n    depends_on:\n      - mongo\n      - redis\n  redis:\n    image: redis:7\n",
    ),
    (
        "docker-compose.ci.yml",
        "services:\n  pg:\n    image: postgres:16\n",
    ),
    (
        "tests/docker-compose.yml",
        "services:\n  mq:\n    image: rabbitmq:3\n",
    ),
];

#[test]
fn packages_images_and_imports_only_tests_or_tooling_use_are_not_resources() {
    let (_tmp, root, db) = common::index_repo("lidx-res-", NODE_TEST_ONLY);
    let g = glance_at(&root, &db);
    assert_eq!(res_names(&g), strs(&["MongoDB"]));
    // The code reads and writes it; the test stack's wiring is not a use.
    assert_eq!(uses_of(&g), own3(&[("src", "MongoDB", "reads/writes")]));
    let calls: Vec<(String, String)> = link(&g, "src", "MongoDB")["calls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["file"].as_str().unwrap().to_string(),
                c["how"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(calls, [("src/server.js".to_string(), "import".to_string())]);
    // Showing every file at first does not make test code a user of one.
    assert_eq!(
        res_names(&glance_with(&root, &db, &["--scope", "all"])),
        strs(&["MongoDB"])
    );
}

#[test]
fn python_test_and_dev_dependencies_are_not_resources() {
    let g = glance(&[
        (
            "pyproject.toml",
            "[project]\nname = \"api\"\ndependencies = [\"fastapi\", \"psycopg\"]\n\n[project.optional-dependencies]\ntest = [\"redis\", \"pytest\"]\ndev = [\"pymongo\"]\n\n[dependency-groups]\ntesting = [\"pika\"]\n\n[tool.poetry.group.dev.dependencies]\nkafka-python = \"*\"\n\n[tool.poetry.dev-dependencies]\nelasticsearch = \"*\"\n",
        ),
        ("requirements-dev.txt", "boto3\nredis\n"),
        ("dev-requirements.txt", "pymysql\n"),
        (
            "poetry.lock",
            "[[package]]\nname = \"redis\"\nversion = \"5.0.0\"\n",
        ),
        (
            "uv.lock",
            "[[package]]\nname = \"pymongo\"\nversion = \"4.0.0\"\n",
        ),
        (
            "api/main.py",
            "import psycopg\nfrom fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/orders\")\ndef orders():\n    return psycopg.connect(\"\")\n",
        ),
        ("conftest.py", "import pymongo\n"),
        ("tests/conftest.py", "import redis\n"),
        (
            "tests/test_cache.py",
            "import redis\n\n\ndef test_cache():\n    redis.Redis()\n",
        ),
        ("api/test_search.py", "import elasticsearch\n"),
        ("api/broker_test.py", "import pika\n"),
    ]);
    assert_eq!(res_names(&g), strs(&["PostgreSQL"]));
}

#[test]
fn a_go_module_only_tests_import_is_not_a_resource() {
    let g = glance(&[
        (
            "go.mod",
            "module example.com/orders\n\ngo 1.22\n\nrequire (\n\tgithub.com/redis/go-redis/v9 v9.5.0\n\tgithub.com/jackc/pgx/v5 v5.5.0\n\tgithub.com/segmentio/kafka-go v0.4.47\n)\n",
        ),
        (
            "go.sum",
            "go.mongodb.org/mongo-driver v1.15.0 h1:x\ngithub.com/redis/go-redis/v9 v9.5.0 h1:y\n",
        ),
        (
            "main.go",
            "package main\n\nimport (\n\t\"net/http\"\n\n\t\"github.com/segmentio/kafka-go\"\n\t// \"go.mongodb.org/mongo-driver/mongo\"\n)\n\nfunc main() {\n\t_ = kafka.Writer{}\n\thttp.HandleFunc(\"/orders\", nil)\n\thttp.ListenAndServe(\":8080\", nil)\n}\n",
        ),
        (
            "cache_test.go",
            "package main\n\nimport (\n\t\"testing\"\n\n\t\"github.com/redis/go-redis/v9\"\n)\n\nfunc TestCache(t *testing.T) { _ = redis.NewClient(nil) }\n",
        ),
        (
            "store_test.go",
            "package main\n\nimport \"github.com/jackc/pgx/v5\"\n\nvar _ = pgx.Connect\n",
        ),
    ]);
    assert_eq!(res_names(&g), strs(&["Kafka"]));
}

#[test]
fn rust_dev_dependencies_and_dotnet_test_projects_are_not_resources() {
    let g = glance(&[
        (
            "svc/Cargo.toml",
            "[package]\nname = \"svc\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\naxum = \"0.7\"\nrdkafka = \"0.36\"\n\n[dev-dependencies]\nredis = \"0.25\"\ntokio-postgres = \"0.7\"\n",
        ),
        (
            "svc/src/main.rs",
            "use rdkafka::config::ClientConfig;\n\nfn main() {}\n",
        ),
        ("svc/tests/it.rs", "use redis::Client;\n\nfn helper() {}\n"),
        (
            "Cargo.lock",
            "[[package]]\nname = \"mongodb\"\nversion = \"2.0.0\"\n",
        ),
        (
            "src/Api/Api.csproj",
            "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n  <ItemGroup>\n    <PackageReference Include=\"Swashbuckle.AspNetCore\" Version=\"6.0.0\" />\n  </ItemGroup>\n</Project>\n",
        ),
        (
            "src/Api/Program.cs",
            "var app = WebApplication.Create();\napp.MapGet(\"/orders\", () => 1);\napp.Run();\n",
        ),
        (
            "src/Api/packages.lock.json",
            "{ \"version\": 1, \"dependencies\": { \"net8.0\": { \"StackExchange.Redis\": { \"type\": \"Transitive\" } } } }\n",
        ),
        (
            "tests/Api.Tests/Api.Tests.csproj",
            "<Project Sdk=\"Microsoft.NET.Sdk\">\n  <ItemGroup>\n    <PackageReference Include=\"xunit\" Version=\"2.0.0\" />\n    <PackageReference Include=\"StackExchange.Redis\" Version=\"2.0.0\" />\n    <PackageReference Include=\"Npgsql\" Version=\"8.0.0\" />\n  </ItemGroup>\n</Project>\n",
        ),
        (
            "tests/Api.Tests/CacheTests.cs",
            "using StackExchange.Redis;\nusing Npgsql;\n\npublic class CacheTests { }\n",
        ),
    ]);
    assert_eq!(res_names(&g), strs(&["Kafka"]));
}

/// `(from, to, label)` of every link to a resource.
fn uses_of(g: &Value) -> Vec<(String, String, String)> {
    res_links(g)
        .into_iter()
        .filter(|l| l.2 != "wire")
        .map(|(a, b, _, l)| (a, b, l))
        .collect()
}

fn own3(v: &[(&str, &str, &str)]) -> Vec<(String, String, String)> {
    v.iter()
        .map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
        .collect()
}

/// A second app, so the page maps apps rather than modules.
const WORKER_TOML: (&str, &str) = (
    "worker/pyproject.toml",
    "[project]\nname = \"worker\"\n\n[project.scripts]\nworker = \"worker.main:run\"\n",
);
const WORKER_PY: (&str, &str) = ("worker/worker/main.py", "def run():\n    pass\n");

const PY_API: &str = "from fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/orders\")\ndef orders():\n    return 1\n";

#[test]
fn each_compose_file_wires_its_own_services() {
    // Two stacks of one app: `db` is PostgreSQL in one file, MariaDB in
    // the other.
    let g = glance(&[
        (
            "deploy/compose.postgres.yml",
            "services:\n  api:\n    build: ../api\n    depends_on:\n      - db\n  db:\n    image: postgres:16\n",
        ),
        (
            "deploy/compose.mariadb.yml",
            "services:\n  api:\n    build: ../api\n    depends_on:\n      - db\n  db:\n    image: mariadb:11\n",
        ),
        (
            "api/pyproject.toml",
            "[project]\nname = \"orders\"\ndependencies = [\"fastapi\"]\n",
        ),
        ("api/orders/main.py", PY_API),
        WORKER_TOML,
        WORKER_PY,
    ]);
    assert_eq!(res_names(&g), strs(&["MariaDB", "PostgreSQL"]));
    assert_eq!(
        uses_of(&g),
        own3(&[
            ("orders", "MariaDB", "uses"),
            ("orders", "PostgreSQL", "uses")
        ])
    );
}

#[test]
fn a_redis_or_mysql_client_reaches_the_compatible_server_compose_runs() {
    let g = glance(&[
        (
            "docker-compose.yml",
            "services:\n  api:\n    build: ./api\n    depends_on:\n      - cache\n      - db\n  cache:\n    image: valkey/valkey:8\n  db:\n    image: mariadb:11\n",
        ),
        (
            "api/pyproject.toml",
            "[project]\nname = \"orders\"\ndependencies = [\"fastapi\", \"redis\", \"pymysql\"]\n",
        ),
        (
            "api/orders/main.py",
            "import pymysql\nimport redis\nfrom fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/orders\")\ndef orders():\n    return redis.Redis(), pymysql.connect()\n",
        ),
    ]);
    assert_eq!(res_names(&g), strs(&["Valkey", "MariaDB"]));
    assert_eq!(
        uses_of(&g),
        own3(&[
            ("orders", "Valkey", "uses"),
            ("orders", "MariaDB", "reads/writes")
        ])
    );
}

#[test]
fn a_celery_app_uses_the_broker_its_url_or_extra_names() {
    let g = glance(&[
        (
            "jobs/pyproject.toml",
            "[project]\nname = \"jobs\"\ndependencies = [\"celery[redis]>=5\"]\n\n[project.scripts]\njobs = \"jobs.main:run\"\n",
        ),
        (
            "jobs/jobs/main.py",
            "from celery import Celery\n\napp = Celery(\"jobs\")\n\n\ndef run():\n    app.start()\n",
        ),
        (
            "mail/pyproject.toml",
            "[project]\nname = \"mail\"\ndependencies = [\"celery\"]\n\n[project.scripts]\nmail = \"mail.main:run\"\n",
        ),
        (
            "mail/mail/main.py",
            "from celery import Celery\n\napp = Celery(\"mail\", broker=\"amqp://guest@mq//\")\n\n\ndef run():\n    app.start()\n",
        ),
    ]);
    assert_eq!(res_names(&g), strs(&["Redis", "RabbitMQ"]));
    let calls = |from: &str, to: &str| -> Vec<(String, String, String)> {
        link(&g, from, to)["calls"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["file"].as_str().unwrap().to_string(),
                    c["how"].as_str().unwrap().to_string(),
                    c["text"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    assert_eq!(
        calls("jobs", "Redis"),
        own3(&[(
            "jobs/pyproject.toml",
            "manifest",
            "declares dependencies = [\"celery[redis]>=5\"]"
        )])
    );
    assert_eq!(
        calls("mail", "RabbitMQ"),
        own3(&[(
            "mail/mail/main.py",
            "import",
            "app = Celery(\"mail\", broker=\"amqp://guest@mq//\")"
        )])
    );
    let redis = node(&g, "Redis");
    assert_eq!(redis["sentence"], "Redis cache; used by 1 app.", "{redis}");
}

#[test]
fn a_django_database_engine_names_the_database() {
    let g = glance(&[
        (
            "pyproject.toml",
            "[project]\nname = \"site\"\ndependencies = [\"django\"]\n",
        ),
        (
            "manage.py",
            "import os\nimport sys\n\nif __name__ == \"__main__\":\n    from django.core.management import execute_from_command_line\n    execute_from_command_line(sys.argv)\n",
        ),
        (
            "site/settings.py",
            "DATABASES = {\n    \"default\": {\n        \"ENGINE\": \"django.db.backends.postgresql\",\n        \"NAME\": \"site\",\n    },\n    \"geo\": {\"ENGINE\": \"django.contrib.gis.db.backends.spatialite\"},\n}\n# ENGINE: django.db.backends.mysql is not used\n",
        ),
    ]);
    assert_eq!(res_names(&g), strs(&["PostgreSQL", "SQLite"]));
}

#[test]
fn a_front_ends_own_event_bus_is_no_broker() {
    let g = glance(&[
        (
            "ui/package.json",
            "{\n  \"name\": \"viewer\",\n  \"scripts\": { \"dev\": \"vite\" },\n  \"dependencies\": { \"react\": \"18.0.0\" }\n}\n",
        ),
        (
            "ui/src/viewer.ts",
            "export function wire(eventBus: any) {\n  eventBus.on('pagerendered', () => 1);\n  eventBus.publish('find', {});\n}\n",
        ),
        (
            "worker/pyproject.toml",
            "[project]\nname = \"worker\"\ndependencies = [\"pika\"]\n\n[project.scripts]\nworker = \"worker.main:run\"\n",
        ),
        (
            "worker/worker/main.py",
            "import pika\n\n\n@router.subscribe(\"file-uploaded\")\ndef on_uploaded(msg):\n    return 1\n\n\ndef run():\n    pass\n",
        ),
    ]);
    assert_eq!(res_names(&g), strs(&["RabbitMQ"]));
    assert_eq!(
        uses_of(&g),
        own3(&[("worker", "RabbitMQ", "subscribes to fileuploaded")])
    );
}

#[test]
fn sql_reaches_the_one_sql_database_and_an_interface_lends_no_resources() {
    // A writer on PostgreSQL and a reader on MongoDB in one Go module. The
    // writer's `database/sql` import and its SQL against the migrations (a
    // folder of SQL of its own) reach PostgreSQL, the only SQL database. Its handler satisfies the
    // reader's interface, which uses none of the reader's MongoDB.
    let g = glance(&[
        (
            "shop/go.mod",
            "module example.com/shop\n\ngo 1.22\n\nrequire (\n\tgithub.com/jackc/pgx/v5 v5.5.0\n\tgo.mongodb.org/mongo-driver v1.15.0\n)\n",
        ),
        (
            "migrations/001_init.up.sql",
            "CREATE TABLE products (id int primary key, name text);\n",
        ),
        (
            "shop/cmd/writer/main.go",
            "package main\n\nimport \"example.com/shop/writer\"\n\nfunc main() {\n\twriter.Run()\n}\n",
        ),
        (
            "shop/cmd/reader/main.go",
            "package main\n\nimport \"example.com/shop/reader\"\n\nfunc main() {\n\treader.Run()\n}\n",
        ),
        (
            "shop/writer/run.go",
            "package writer\n\nimport (\n\t\"database/sql\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\nvar ErrNone = sql.ErrNoRows\n\nfunc Run() {\n\t_, _ = pgx.Connect(nil, \"\")\n\tsave()\n\tlookup()\n}\n\nfunc save() {\n\tq := \"INSERT INTO products (id, name) VALUES ($1, $2)\"\n\t_ = q\n}\n\nfunc lookup() string {\n\treturn handler{}.Handle(\"x\")\n}\n",
        ),
        (
            "shop/writer/handler.go",
            "package writer\n\ntype handler struct{}\n\nfunc (h handler) Handle(id string) string { return id }\n",
        ),
        (
            "shop/reader/run.go",
            "package reader\n\nimport \"go.mongodb.org/mongo-driver/mongo\"\n\ntype Handler interface {\n\tHandle(id string) string\n}\n\nfunc Run() {\n\t_, _ = mongo.Connect(nil)\n}\n",
        ),
    ]);
    assert_eq!(res_names(&g), strs(&["PostgreSQL", "MongoDB"]));
    assert_eq!(
        uses_of(&g),
        own3(&[
            // The packages, then the commands that run them.
            ("reader", "MongoDB", "reads/writes"),
            ("writer", "PostgreSQL", "reads/writes"),
            ("reader", "MongoDB", "reads/writes via shop"),
            ("writer", "PostgreSQL", "reads/writes via shop"),
        ])
    );
    // The folder of SQL is PostgreSQL's, not a database of no technology.
    let pg = &node(&g, "PostgreSQL")["res"];
    assert_eq!(
        pg["declared"],
        json!([{ "file": "migrations/", "line": 0, "text": "SQL in migrations/" }])
    );
    assert_eq!(pg["items"], json!([["products", "object"]]));
}

#[test]
fn a_test_stacks_wiring_is_no_use() {
    // Only the test stack wires the API to the cache the main stack runs.
    let g = glance(&[
        (
            "docker-compose.yml",
            "services:\n  api:\n    build: ./api\n  cache:\n    image: redis:7\n",
        ),
        (
            "docker-compose.test.yml",
            "services:\n  api:\n    depends_on:\n      - cache\n    environment:\n      REDIS_URL: redis://cache:6379\n",
        ),
        (
            "api/pyproject.toml",
            "[project]\nname = \"orders\"\ndependencies = [\"fastapi\"]\n",
        ),
        ("api/orders/main.py", PY_API),
        WORKER_TOML,
        WORKER_PY,
    ]);
    assert_eq!(res_names(&g), strs(&["Redis"]));
    assert_eq!(uses_of(&g), own3(&[]));
}

#[test]
fn what_infrastructure_declares_does_not_depend_on_the_scope_shown() {
    // Compose files of examples declare nothing; the system's own compose
    // file declares its resources whichever files the page shows first.
    let (_tmp, root, db) = common::index_repo(
        "lidx-res-",
        &[
            (
                "docker-compose.yml",
                "services:\n  api:\n    build: ./api\n  cache:\n    image: redis:7\n",
            ),
            (
                "examples/kafka/docker-compose.yml",
                "services:\n  broker:\n    image: apache/kafka:3.7\n",
            ),
            ("api/pyproject.toml", "[project]\nname = \"orders\"\ndependencies = [\"fastapi\"]\n"),
            ("api/orders/main.py", PY_API),
            WORKER_TOML,
            WORKER_PY,
        ],
    );
    for scope in ["code", "tests", "all"] {
        assert_eq!(
            res_names(&glance_with(&root, &db, &["--scope", scope])),
            strs(&["Redis"]),
            "{scope}"
        );
    }
}
