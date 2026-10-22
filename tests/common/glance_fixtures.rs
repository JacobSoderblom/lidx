//! Repositories of three shapes for the landing page of `lidx graph`,
//! shared by tests/graph_glance.rs and tests/graph_viewer_node.rs.

/// A React portal posting to an API, which publishes on a bus to a worker
/// that reads a key a Kubernetes Deployment provides from a secret.
pub const MULTI: &[(&str, &str)] = &[
    (
        "README.md",
        "# Uploads\n\n[![ci](https://x/badge.svg)](https://x)\n\nUploads takes partner files, checks them and stores the results. More text here.\n",
    ),
    (
        "ui/package.json",
        "{\n  \"name\": \"portal\",\n  \"description\": \"Partner upload UI\",\n  \"scripts\": { \"dev\": \"vite\" },\n  \"dependencies\": { \"react\": \"18.0.0\" },\n  \"devDependencies\": { \"vite\": \"5.0.0\" }\n}\n",
    ),
    (
        "ui/src/api.ts",
        "export async function sendUpload(body: string) {\n  const res = await fetch(\"/uploads\", { method: \"POST\", body });\n  return res.json();\n}\n",
    ),
    (
        "api/pyproject.toml",
        "[project]\nname = \"ingest-api\"\ndescription = \"Receives partner uploads and queues them.\"\ndependencies = [\"fastapi>=0.110\"]\n",
    ),
    (
        "api/ingest_api/main.py",
        "from fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.post(\"/uploads\")\ndef upload():\n    return store()\n\n\ndef store():\n    _bus.publish(\"file-uploaded\", {})\n    return 1\n",
    ),
    (
        "worker/pyproject.toml",
        "[project]\nname = \"validator\"\ndependencies = [\"pydantic\"]\n\n[project.scripts]\nvalidator = \"validator.main:run\"\n",
    ),
    (
        "worker/validator/main.py",
        "import os\n\n\n@router.subscribe(\"file-uploaded\")\ndef on_uploaded(msg):\n    return check(msg)\n\n\ndef check(msg):\n    secret = os.environ[\"DB_PASSWORD\"]\n    return save(msg, secret)\n\n\ndef save(msg, secret):\n    return 1\n\n\ndef run():\n    pass\n",
    ),
    (
        "deploy/k8s/validator.yaml",
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: validator\nspec:\n  template:\n    spec:\n      containers:\n        - name: validator\n          image: registry.example.com/team/validator:1.0\n          env:\n            - name: DB_PASSWORD\n              valueFrom:\n                secretKeyRef:\n                  name: db-password\n                  key: value\n",
    ),
];

/// One Rust binary: `main` calls `cli`, which calls `store` and `report`.
pub const CLI: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"tally\"\nversion = \"0.1.0\"\nedition = \"2021\"\ndescription = \"Counts the words in files.\"\n",
    ),
    (
        "src/main.rs",
        "mod cli;\nmod report;\nmod store;\n\nfn main() {\n    cli::run();\n}\n",
    ),
    (
        "src/cli/mod.rs",
        "pub fn run() {\n    let words = crate::store::load();\n    crate::report::print(words);\n}\n",
    ),
    (
        "src/store/mod.rs",
        "pub struct Words {\n    pub n: usize,\n}\n\npub fn load() -> Words {\n    Words { n: parse() }\n}\n\nfn parse() -> usize {\n    1\n}\n",
    ),
    (
        "src/report/mod.rs",
        "use crate::store::Words;\n\npub fn print(w: Words) {\n    show(&w);\n}\n\nfn show(w: &Words) {\n    let _ = w.n;\n}\n",
    ),
];

/// Three JavaScript packages and no way in: no route, command or `main`.
pub const LIBS: &[(&str, &str)] = &[
    (
        "package.json",
        "{ \"name\": \"kit\", \"private\": true, \"workspaces\": [\"packages/*\"] }\n",
    ),
    (
        "packages/core/package.json",
        "{ \"name\": \"@kit/core\", \"description\": \"Shared parsing primitives.\", \"main\": \"index.js\" }\n",
    ),
    (
        "packages/core/index.js",
        "export function tokenize(s) {\n  return s.split(\" \");\n}\n\nexport function trim(s) {\n  return s.trim();\n}\n",
    ),
    (
        "packages/text/package.json",
        "{ \"name\": \"@kit/text\", \"main\": \"index.js\", \"dependencies\": { \"@kit/core\": \"workspace:*\" } }\n",
    ),
    (
        "packages/text/index.js",
        "import { tokenize, trim } from \"@kit/core\";\n\nexport function words(s) {\n  return tokenize(trim(s));\n}\n\nexport function count(s) {\n  return words(s).length;\n}\n",
    ),
    (
        "packages/report/package.json",
        "{ \"name\": \"@kit/report\", \"main\": \"index.js\", \"dependencies\": { \"@kit/text\": \"workspace:*\", \"@kit/core\": \"workspace:*\" } }\n",
    ),
    (
        "packages/report/index.js",
        "import { count } from \"@kit/text\";\nimport { trim } from \"@kit/core\";\n\nexport function summary(s) {\n  return trim(s) + \" \" + count(s);\n}\n\nexport function headline(s) {\n  return summary(s).toUpperCase();\n}\n",
    ),
];

/// A compose stack: an orders API (Postgres, Redis, RabbitMQ through
/// `pika`, S3 through `boto3`) publishing a topic a worker subscribes to,
/// a web front end calling an outside API over HTTP, an app reading Key
/// Vault, and Terraform declaring a queue no code uses.
pub const STACK: &[(&str, &str)] = &[
    (
        "docker-compose.yml",
        "services:\n  orders:\n    build: ./orders\n    depends_on:\n      - db\n      - mq\n    environment:\n      DATABASE_URL: postgresql://app:pw@db:5432/orders\n      REDIS_URL: redis://cache:6379/0\n  worker:\n    build: ./worker\n    depends_on:\n      - mq\n  db:\n    image: postgres:16\n  cache:\n    image: redis:7-alpine\n  mq:\n    image: rabbitmq:3-management\n",
    ),
    (
        "orders/pyproject.toml",
        "[project]\nname = \"orders\"\ndependencies = [\"fastapi\", \"psycopg\", \"redis\", \"pika\", \"boto3\"]\n",
    ),
    (
        "orders/orders/main.py",
        "import psycopg\nimport redis\nimport boto3\nfrom fastapi import FastAPI\n\napp = FastAPI()\ns3 = boto3.client(\"s3\")\n\n\n@app.post(\"/orders\")\ndef create():\n    return save()\n\n\ndef save():\n    _bus.publish(\"order-created\", {})\n    return 1\n",
    ),
    (
        "worker/pyproject.toml",
        "[project]\nname = \"worker\"\ndependencies = [\"pika\", \"azure-keyvault-secrets\"]\n\n[project.scripts]\nworker = \"worker.main:run\"\n",
    ),
    (
        "worker/worker/main.py",
        "import pika\nfrom azure.keyvault.secrets import SecretClient\n\n\n@router.subscribe(\"order-created\")\ndef on_created(msg):\n    return bill(msg)\n\n\ndef bill(msg):\n    return 1\n\n\ndef run():\n    pass\n",
    ),
    (
        "web/package.json",
        "{\n  \"name\": \"web\",\n  \"scripts\": { \"dev\": \"vite\" },\n  \"dependencies\": { \"react\": \"18.0.0\" }\n}\n",
    ),
    (
        "web/src/pay.ts",
        "export async function pay(body: string) {\n  const res = await fetch(\"https://api.payhub.io/v1/charges\", { method: \"POST\", body });\n  return res.json();\n}\n",
    ),
    (
        "infra/main.tf",
        "resource \"aws_sqs_queue\" \"refunds\" {\n  name = \"refunds\"\n}\n\nresource \"aws_iam_role\" \"runner\" {\n  name = \"runner\"\n}\n",
    ),
    (
        "infra/vault.bicep",
        "resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {\n  name: 'shop-kv'\n}\n",
    ),
];

/// A .NET Aspire app host giving two web APIs a Postgres database and a
/// RabbitMQ broker, and one API a reference to the other.
pub const ASPIRE: &[(&str, &str)] = &[
    (
        "src/AppHost/AppHost.csproj",
        "<Project Sdk=\"Aspire.AppHost.Sdk/9.0.0\">\n  <PropertyGroup><OutputType>Exe</OutputType></PropertyGroup>\n</Project>\n",
    ),
    (
        "src/AppHost/Program.cs",
        "var builder = DistributedApplication.CreateBuilder(args);\nvar bus = builder.AddRabbitMQ(\"eventbus\");\nvar pg = builder.AddPostgres(\"postgres\");\nvar ordersDb = pg.AddDatabase(\"ordersdb\");\nvar catalog = builder.AddProject<Projects.Catalog_Api>(\"catalog-api\")\n    .WithReference(bus);\nbuilder.AddProject<Projects.Orders_Api>(\"orders-api\")\n    .WithReference(ordersDb)\n    .WithReference(bus)\n    .WithReference(catalog);\nbuilder.Build().Run();\n",
    ),
    (
        "src/Catalog.Api/Catalog.Api.csproj",
        "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n</Project>\n",
    ),
    (
        "src/Catalog.Api/Program.cs",
        "namespace Catalog.Api;\npublic static class Program { public static void Main() { } }\n",
    ),
    (
        "src/Orders.Api/Orders.Api.csproj",
        "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n  <ItemGroup><PackageReference Include=\"Npgsql\" Version=\"8.0.0\" /></ItemGroup>\n</Project>\n",
    ),
    (
        "src/Orders.Api/Program.cs",
        "using Npgsql;\nnamespace Orders.Api;\npublic static class Program { public static void Main() { } }\n",
    ),
];

/// Kubernetes in one namespace: a cart service whose env names the host
/// of a Redis deployment, and a front end deployed beside it.
pub const K8S_NS: &[(&str, &str)] = &[
    ("cart/go.mod", "module example.com/cart\n\ngo 1.22\n"),
    (
        "cart/main.go",
        "package main\n\nfunc main() {\n\tserve()\n}\n\nfunc serve() {}\n",
    ),
    ("cart/Dockerfile", "FROM golang:1.22\n"),
    ("front/go.mod", "module example.com/front\n\ngo 1.22\n"),
    ("front/main.go", "package main\n\nfunc main() {\n}\n"),
    ("front/Dockerfile", "FROM golang:1.22\n"),
    (
        "deploy/cart.yaml",
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: cart\n  namespace: shop\nspec:\n  template:\n    metadata:\n      labels:\n        app: cart\n    spec:\n      containers:\n        - name: cart\n          image: registry.example.com/cart:1.0\n          env:\n            - name: REDIS_ADDR\n              value: \"redis-cart:6379\"\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: redis-cart\n  namespace: shop\nspec:\n  template:\n    metadata:\n      labels:\n        app: redis-cart\n    spec:\n      containers:\n        - name: redis\n          image: redis:alpine\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: redis-cart\n  namespace: shop\nspec:\n  selector:\n    app: redis-cart\n",
    ),
    (
        "deploy/front.yaml",
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: front\n  namespace: shop\nspec:\n  template:\n    metadata:\n      labels:\n        app: front\n    spec:\n      containers:\n        - name: front\n          image: registry.example.com/front:1.0\n          env:\n            - name: CART_ADDR\n              value: \"cart:8080\"\n",
    ),
];

/// Three APIs sharing a RabbitMQ broker, one of them also using Redis on
/// its own, and a hub API using five resources no other app uses.
pub const SHARED: &[(&str, &str)] = &[
    (
        "docker-compose.yml",
        "services:\n  a:\n    build: ./a\n    depends_on:\n      - mq\n  b:\n    build: ./b\n    depends_on:\n      - mq\n  c:\n    build: ./c\n    depends_on:\n      - mq\n  mq:\n    image: rabbitmq:3-management\n",
    ),
    (
        "a/pyproject.toml",
        "[project]\nname = \"a\"\ndependencies = [\"fastapi\", \"pika\", \"redis\"]\n",
    ),
    (
        "a/a/main.py",
        "import pika\nimport redis\nfrom fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/a\")\ndef get():\n    return 1\n",
    ),
    (
        "b/pyproject.toml",
        "[project]\nname = \"b\"\ndependencies = [\"fastapi\", \"pika\"]\n",
    ),
    (
        "b/b/main.py",
        "import pika\nfrom fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/b\")\ndef get():\n    return 1\n",
    ),
    (
        "c/pyproject.toml",
        "[project]\nname = \"c\"\ndependencies = [\"fastapi\", \"pika\"]\n",
    ),
    (
        "c/c/main.py",
        "import pika\nfrom fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/c\")\ndef get():\n    return 1\n",
    ),
    (
        "hub/pyproject.toml",
        "[project]\nname = \"hub\"\ndependencies = [\"fastapi\", \"psycopg\", \"boto3\", \"elasticsearch\", \"stripe\", \"azure-keyvault-secrets\"]\n",
    ),
    (
        "hub/hub/main.py",
        "import psycopg\nimport boto3\nimport stripe\nfrom elasticsearch import Elasticsearch\nfrom azure.keyvault.secrets import SecretClient\nfrom fastapi import FastAPI\n\napp = FastAPI()\ns3 = boto3.client(\"s3\")\n\n\n@app.get(\"/hub\")\ndef get():\n    return 1\n",
    ),
];

/// `MULTI` plus what the overview leaves out or folds: a service whose code
/// is Java (not indexed, only its proto is) and a second Kubernetes folder.
#[allow(dead_code)]
pub fn explore() -> Vec<(&'static str, &'static str)> {
    let mut v = MULTI.to_vec();
    v.extend_from_slice(&[
        ("ads/Dockerfile", "FROM gradle\nCOPY . .\n"),
        ("ads/src/main/java/Ads.java", "class Ads {}\n"),
        (
            "ads/src/main/proto/ads.proto",
            "syntax = \"proto3\";\nservice Ads { rpc Get(Req) returns (Res); }\nmessage Req {}\nmessage Res {}\n",
        ),
        (
            "deploy/overlays/a/kustomization.yaml",
            "resources:\n- ../../k8s\n",
        ),
        (
            "deploy/overlays/b/kustomization.yaml",
            "resources:\n- ../../k8s\n",
        ),
    ]);
    v
}
