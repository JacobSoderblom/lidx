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
