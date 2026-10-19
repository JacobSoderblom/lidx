//! `lidx graph` application view: units found from manifests on synthetic
//! repositories, which files each owns, and every link family between them
//! with its weight.
mod common;

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

fn graph_cmd(root: &Path, db: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lidx"))
        .arg("graph")
        .arg("--repo")
        .arg(root)
        .arg("--db")
        .arg(db)
        .arg("--out")
        .arg(root.join("apps-out.html"))
        .args(args)
        .output()
        .unwrap()
}

fn page(root: &Path, db: &Path, args: &[&str]) -> Value {
    let res = graph_cmd(root, db, args);
    assert!(
        res.status.success(),
        "lidx graph {args:?} failed: {}",
        String::from_utf8_lossy(&res.stderr)
    );
    let html = std::fs::read_to_string(root.join("apps-out.html")).unwrap();
    lidx::graph_html::extract_document(&html).unwrap()
}

struct Apps<'a> {
    doc: &'a Value,
}

impl<'a> Apps<'a> {
    fn units(&self) -> &'a Vec<Value> {
        self.doc["apps"]["units"].as_array().unwrap()
    }
    fn unit(&self, name: &str) -> &'a Value {
        self.units()
            .iter()
            .find(|u| u["name"] == name)
            .unwrap_or_else(|| panic!("no unit {name}: {:?}", self.kinds()))
    }
    fn name(&self, id: &Value) -> String {
        self.units()[id.as_u64().unwrap() as usize]["name"]
            .as_str()
            .unwrap()
            .to_string()
    }
    /// name -> kind
    fn kinds(&self) -> BTreeMap<String, String> {
        self.units()
            .iter()
            .map(|u| {
                (
                    u["name"].as_str().unwrap().to_string(),
                    u["kind"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }
    /// `(from, to, family, weight, declared)` for every link.
    fn links(&self) -> BTreeSet<(String, String, String, u64, bool)> {
        self.doc["apps"]["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    self.name(&e["from"]),
                    self.name(&e["to"]),
                    e["family"].as_str().unwrap().to_string(),
                    e["weight"].as_u64().unwrap(),
                    e["declared"].as_bool().unwrap(),
                )
            })
            .collect()
    }
    fn link(&self, from: &str, to: &str, family: &str) -> &'a Value {
        self.doc["apps"]["edges"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| {
                self.name(&e["from"]) == from && self.name(&e["to"]) == to && e["family"] == family
            })
            .unwrap_or_else(|| panic!("no {family} link {from} -> {to}: {:?}", self.links()))
    }
    /// The unit each file node of the page belongs to.
    fn owner_of(&self, file: &str) -> String {
        let strings: Vec<&str> = self.doc["s"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap())
            .collect();
        let nodes = self.doc["nodes"].as_array().unwrap();
        let i = nodes
            .iter()
            .position(|n| {
                let f = n[4].as_i64().unwrap();
                f >= 0 && strings[f as usize] == file
            })
            .unwrap_or_else(|| panic!("no node for {file}"));
        let u = self.doc["apps"]["node_unit"][i].as_i64().unwrap();
        assert!(u >= 0, "{file} has no unit");
        self.name(&Value::from(u))
    }
}

/// A .NET web app on a .NET library, its test project, a FastAPI app on a
/// Python library, a Python worker on a bus topic the API publishes, a
/// TypeScript UI calling the API, a k8s Deployment of the web app's image,
/// a tooling package.json next to the web app's project, an example and
/// docs.
const FILES: &[(&str, &str)] = &[
    (
        "services/web/Web.csproj",
        concat!(
            "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n",
            "  <ItemGroup>\n",
            "    <PackageReference Include=\"Microsoft.EntityFrameworkCore.SqlServer\" Version=\"8.0.0\" />\n",
            "    <PackageReference Include=\"Azure.Messaging.ServiceBus\" Version=\"7.0.0\" />\n",
            "  </ItemGroup>\n",
            "  <ItemGroup>\n",
            "    <ProjectReference Include=\"..\\..\\libs\\Core\\Core.csproj\" />\n",
            "  </ItemGroup>\n",
            "</Project>\n",
        ),
    ),
    (
        "services/web/Program.cs",
        concat!(
            "using Core;\n\n",
            "namespace Web\n{\n",
            "    public class OrdersController\n    {\n",
            "        public int Get()\n        {\n",
            "            var repo = new Repo();\n",
            "            return repo.Load();\n",
            "        }\n    }\n}\n",
        ),
    ),
    (
        "services/web/package.json",
        "{ \"name\": \"web-tools\", \"private\": true, \"devDependencies\": { \"prettier\": \"3.0.0\" } }\n",
    ),
    (
        "services/web/Dockerfile",
        "FROM mcr.microsoft.com/dotnet/aspnet:8.0\nCOPY . /app\nENTRYPOINT [\"dotnet\", \"Web.dll\"]\n",
    ),
    (
        "libs/Core/Core.csproj",
        concat!(
            "<Project Sdk=\"Microsoft.NET.Sdk\">\n",
            "  <ItemGroup>\n",
            "    <PackageReference Include=\"Microsoft.EntityFrameworkCore\" Version=\"8.0.0\" />\n",
            "  </ItemGroup>\n",
            "</Project>\n",
        ),
    ),
    (
        "libs/Core/Repo.cs",
        concat!(
            "namespace Core\n{\n",
            "    public class Repo\n    {\n",
            "        public int Load()\n        {\n            return 1;\n        }\n",
            "    }\n}\n",
        ),
    ),
    // Uses Core's code without a ProjectReference: a resolver guess.
    (
        "libs/Extra/Extra.csproj",
        "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n",
    ),
    (
        "libs/Extra/Extra.cs",
        concat!(
            "using Core;\n\n",
            "namespace Extra\n{\n",
            "    public class Helper\n    {\n",
            "        public int Run()\n        {\n            return new Repo().Load();\n        }\n",
            "    }\n}\n",
        ),
    ),
    (
        "tests/Web.Tests/Web.Tests.csproj",
        concat!(
            "<Project Sdk=\"Microsoft.NET.Sdk\">\n",
            "  <ItemGroup>\n",
            "    <PackageReference Include=\"xunit\" Version=\"2.0.0\" />\n",
            "    <ProjectReference Include=\"..\\..\\services\\web\\Web.csproj\" />\n",
            "  </ItemGroup>\n",
            "</Project>\n",
        ),
    ),
    (
        "tests/Web.Tests/OrdersTests.cs",
        concat!(
            "using Web;\n\n",
            "namespace Web.Tests\n{\n",
            "    public class OrdersTests\n    {\n",
            "        public void Gets()\n        {\n            new OrdersController().Get();\n        }\n",
            "    }\n}\n",
        ),
    ),
    (
        "api/pyproject.toml",
        "[project]\nname = \"orders-api\"\ndependencies = [\"fastapi>=0.110\", \"shared-utils\"]\n",
    ),
    (
        "api/orders_api/main.py",
        concat!(
            "from fastapi import FastAPI\n",
            "from shared_utils.fmt import fmt_order\n\n",
            "app = FastAPI()\n\n\n",
            "@app.get(\"/orders\")\n",
            "def list_orders():\n",
            "    _bus.publish(\"order-created\", {})\n",
            "    return fmt_order(1)\n",
        ),
    ),
    (
        "shared/pyproject.toml",
        "[project]\nname = \"shared-utils\"\ndependencies = [\"pydantic\"]\n",
    ),
    (
        "shared/shared_utils/fmt.py",
        "def fmt_order(n):\n    return str(n)\n",
    ),
    (
        "workers/notify/pyproject.toml",
        concat!(
            "[project]\nname = \"notify\"\ndependencies = [\"pydantic\"]\n\n",
            "[project.scripts]\nnotify = \"notify.main:run\"\n",
        ),
    ),
    (
        "workers/notify/notify/main.py",
        "@router.subscribe('order-created')\ndef on_created(msg):\n    pass\n\n\ndef run():\n    pass\n",
    ),
    (
        "ui/package.json",
        concat!(
            "{\n  \"name\": \"orders-ui\",\n",
            "  \"scripts\": { \"dev\": \"vite\" },\n",
            "  \"dependencies\": { \"react\": \"18.0.0\" },\n",
            "  \"devDependencies\": { \"vite\": \"5.0.0\" }\n}\n",
        ),
    ),
    (
        "ui/src/api.ts",
        "export async function loadOrders() {\n  const res = await fetch(\"/orders\");\n  return res.json();\n}\n",
    ),
    (
        "deploy/k8s/web.yaml",
        concat!(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n",
            "spec:\n  template:\n    spec:\n      containers:\n        - name: web\n",
            "          image: registry.example.com/team/web:1.4.2\n",
        ),
    ),
    (
        "examples/demo/pyproject.toml",
        "[project]\nname = \"demo\"\ndependencies = [\"shared-utils\"]\n",
    ),
    (
        "examples/demo/run.py",
        "from shared_utils.fmt import fmt_order\n\n\ndef main():\n    return fmt_order(2)\n",
    ),
    ("docs/guide.md", "# Guide\n\nHow to run it.\n"),
];

#[test]
fn units_come_from_manifests_and_own_their_files() {
    let (_tmp, root, db) = common::index_repo("lidx-apps-", FILES);
    let doc = page(&root, &db, &[]);
    assert_eq!(doc["meta"]["level"], "app", "the app view is the default");
    let a = Apps { doc: &doc };
    let kinds = a.kinds();
    let expected: BTreeMap<String, String> = [
        ("orders-ui", "ui"),
        ("Web", "api"),
        ("orders-api", "api"),
        ("notify", "worker"),
        ("Core", "lib"),
        ("Extra", "lib"),
        ("shared-utils", "lib"),
        ("deploy", "infra"),
        ("Web.Tests", "test"),
        ("demo", "example"),
        ("docs/", "docs"),
    ]
    .iter()
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .collect();
    assert_eq!(kinds, expected);

    // The tooling package.json merged into the .NET project's unit.
    let web = a.unit("Web");
    assert_eq!(
        web["manifests"],
        serde_json::json!(["services/web/Web.csproj", "services/web/package.json"])
    );
    assert_eq!(
        web["dockerfiles"],
        serde_json::json!(["services/web/Dockerfile"])
    );
    assert_eq!(web["why"], "Microsoft.NET.Sdk.Web");
    assert_eq!(a.unit("orders-api")["why"], "FastAPI app");
    // Hidden by default: tests, examples, docs.
    for (name, hidden) in [
        ("Web.Tests", true),
        ("demo", true),
        ("docs/", true),
        ("Web", false),
        ("deploy", false),
    ] {
        assert_eq!(a.unit(name)["hidden"], hidden, "{name}");
    }

    // Membership: the innermost unit owns each file.
    for (file, unit) in [
        ("services/web/Program.cs", "Web"),
        ("libs/Core/Repo.cs", "Core"),
        ("tests/Web.Tests/OrdersTests.cs", "Web.Tests"),
        ("api/orders_api/main.py", "orders-api"),
        ("shared/shared_utils/fmt.py", "shared-utils"),
        ("workers/notify/notify/main.py", "notify"),
        ("ui/src/api.ts", "orders-ui"),
        ("deploy/k8s/web.yaml", "deploy"),
        ("examples/demo/run.py", "demo"),
        ("docs/guide.md", "docs/"),
    ] {
        assert_eq!(a.owner_of(file), unit, "{file}");
    }

    // What the cards say each unit exposes and consumes.
    let api = a.unit("orders-api");
    assert_eq!(api["exposes"]["http"]["items"][0][0], "/orders");
    assert_eq!(
        api["exposes"]["http"]["items"][0][2][0][1],
        "api/orders_api/main.py"
    );
    assert_eq!(api["consumes"]["topics"]["total"], 1);
    assert_eq!(a.unit("notify")["exposes"]["bus"]["total"], 1);
    assert_eq!(a.unit("notify")["entries"], serde_json::json!(["notify"]));
}

#[test]
fn every_link_family_has_its_weight_and_evidence() {
    let (_tmp, root, db) = common::index_repo("lidx-apps-links-", FILES);
    let doc = page(&root, &db, &[]);
    let a = Apps { doc: &doc };
    let s = |x: &str| x.to_string();
    let expected: BTreeSet<(String, String, String, u64, bool)> = [
        // Declared in manifests.
        (s("Web"), s("Core"), s("depends"), 1, true),
        (s("orders-api"), s("shared-utils"), s("depends"), 1, true),
        (s("Web.Tests"), s("Web"), s("depends"), 1, true),
        (s("demo"), s("shared-utils"), s("depends"), 1, true),
        // Code crossing units: `using Core`, `new Repo()`, `repo.Load()`.
        (s("Web"), s("Core"), s("code"), 3, true),
        (s("orders-api"), s("shared-utils"), s("code"), 3, true),
        (s("Web.Tests"), s("Web"), s("code"), 2, true),
        (s("demo"), s("shared-utils"), s("code"), 3, true),
        // No ProjectReference from Extra to Core: a guess, kept but flagged.
        (s("Extra"), s("Core"), s("code"), 2, false),
        // Cross-process, by distinct key.
        (s("orders-ui"), s("orders-api"), s("http"), 1, true),
        (s("orders-api"), s("notify"), s("bus"), 1, true),
        // The Deployment's image names the web app's folder.
        (s("deploy"), s("Web"), s("deploys"), 1, true),
    ]
    .into_iter()
    .collect();
    assert_eq!(a.links(), expected);

    assert_eq!(
        a.link("orders-ui", "orders-api", "http")["label"],
        "HTTP ×1"
    );
    assert_eq!(
        a.link("orders-ui", "orders-api", "http")["keys"][0],
        "/orders"
    );
    let bus = a.link("orders-api", "notify", "bus");
    assert!(
        bus["label"]
            .as_str()
            .unwrap()
            .starts_with("publishes to topic "),
        "{bus}"
    );
    assert_eq!(a.link("Web", "Core", "code")["label"], "calls ×3");
    assert_eq!(a.link("Web", "Core", "depends")["label"], "references");
    let deps_ev = &a.link("Web", "Core", "depends")["evidence"][0];
    assert_eq!(deps_ev[1], "services/web/Web.csproj");
    assert_eq!(deps_ev[2], 7);
    let code_ev = a.link("Web", "Core", "code")["evidence"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e[0].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert!(
        code_ev.iter().any(|e| e.starts_with("Core.Repo")),
        "{code_ev:?}"
    );
    let deploy_ev = &a.link("deploy", "Web", "deploys")["evidence"][0];
    assert_eq!(deploy_ev[1], "deploy/k8s/web.yaml");

    // Shared external packages: one chip per group two shown units declare.
    let chips: BTreeMap<String, BTreeSet<String>> = doc["apps"]["chips"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap().to_string(),
                c["users"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|u| a.name(u))
                    .collect(),
            )
        })
        .collect();
    let expect_chips: BTreeMap<String, BTreeSet<String>> = [
        (s("EF Core"), [s("Core"), s("Web")].into()),
        (s("pydantic"), [s("notify"), s("shared-utils")].into()),
    ]
    .into_iter()
    .collect();
    assert_eq!(chips, expect_chips);
    let web_pkgs = a.unit("Web")["packages"].clone();
    assert_eq!(
        web_pkgs,
        serde_json::json!([
            ["Azure.Messaging.ServiceBus", ["Azure.Messaging.ServiceBus"]],
            ["EF Core", ["Microsoft.EntityFrameworkCore.SqlServer"]]
        ])
    );
}

#[test]
fn levels_and_seeds_keep_working() {
    let (_tmp, root, db) = common::index_repo("lidx-apps-levels-", FILES);
    let doc = page(&root, &db, &["--level", "module"]);
    assert_eq!(doc["meta"]["level"], "module");
    assert!(
        doc["apps"]["links"].as_bool().unwrap(),
        "the switcher still has the app view"
    );

    // A seeded view carries each node's unit, for colouring.
    let doc = page(
        &root,
        &db,
        &["--seed", "Web.OrdersController.Get", "--depth", "1"],
    );
    assert_eq!(doc["meta"]["level"], "symbol");
    let a = Apps { doc: &doc };
    assert_eq!(a.owner_of("services/web/Program.cs"), "Web");
    assert_eq!(doc["apps"]["links"], false);

    let res = graph_cmd(
        &root,
        &db,
        &["--level", "app", "--seed", "Web.OrdersController.Get"],
    );
    assert!(!res.status.success());
    assert!(
        String::from_utf8_lossy(&res.stderr).contains("--level app is the whole-repo overview"),
        "{}",
        String::from_utf8_lossy(&res.stderr)
    );
}

/// Rust, Go and Tauri: a binary crate on a path-dependency library, a Go
/// module whose `cmd/` main package is its own app, and a Tauri app whose
/// `src-tauri` crate belongs to the app around it.
#[test]
fn crates_go_mains_and_tauri_apps() {
    let files: &[(&str, &str)] = &[
        ("Cargo.toml", "[workspace]\nmembers = [\"crates/*\"]\n"),
        (
            "crates/tool/Cargo.toml",
            "[package]\nname = \"tool\"\nversion = \"0.1.0\"\n\n[dependencies]\ncore-lib = { path = \"../core-lib\" }\nserde = \"1\"\n",
        ),
        (
            "crates/tool/src/main.rs",
            "fn main() {\n    core_lib::run();\n}\n",
        ),
        (
            "crates/core-lib/Cargo.toml",
            "[package]\nname = \"core-lib\"\nversion = \"0.1.0\"\n\n[dependencies]\nserde = \"1\"\n",
        ),
        ("crates/core-lib/src/lib.rs", "pub fn run() {}\n"),
        ("go/go.mod", "module example.com/scan\n\ngo 1.22\n"),
        (
            "go/cmd/scan/main.go",
            "package main\n\nimport \"example.com/scan/pkg/walk\"\n\nfunc main() {\n\twalk.Run()\n}\n",
        ),
        ("go/pkg/walk/walk.go", "package walk\n\nfunc Run() {}\n"),
        (
            "desk/package.json",
            "{\n  \"name\": \"desk\",\n  \"scripts\": { \"dev\": \"vite\" },\n  \"dependencies\": { \"@tauri-apps/api\": \"2.0.0\", \"react\": \"18.0.0\" }\n}\n",
        ),
        (
            "desk/src/main.tsx",
            "export function App() {\n  return null;\n}\n",
        ),
        (
            "desk/src-tauri/Cargo.toml",
            "[package]\nname = \"desk-shell\"\nversion = \"0.1.0\"\n\n[dependencies]\ntauri = \"2\"\n",
        ),
        ("desk/src-tauri/src/main.rs", "fn main() {}\n"),
    ];
    let (_tmp, root, db) = common::index_repo("lidx-apps-native-", files);
    let doc = page(&root, &db, &[]);
    let a = Apps { doc: &doc };
    let kinds = a.kinds();
    assert_eq!(
        kinds.get("tool").map(String::as_str),
        Some("cli"),
        "{kinds:?}"
    );
    assert_eq!(
        kinds.get("core-lib").map(String::as_str),
        Some("lib"),
        "{kinds:?}"
    );
    assert_eq!(
        kinds.get("scan (module)").map(String::as_str),
        Some("lib"),
        "{kinds:?}"
    );
    // The main package directory `cmd/scan/` is an app named after it.
    let mains: Vec<&Value> = a
        .units()
        .iter()
        .filter(|u| u["dir"] == "go/cmd/scan/")
        .collect();
    assert_eq!(mains.len(), 1, "{kinds:?}");
    assert_eq!(mains[0]["kind"], "cli");
    assert_eq!(
        kinds.get("desk").map(String::as_str),
        Some("desktop"),
        "{kinds:?}"
    );
    assert!(
        !kinds.contains_key("desk-shell"),
        "merged into desk: {kinds:?}"
    );
    assert_eq!(a.owner_of("desk/src-tauri/src/main.rs"), "desk");
    assert_eq!(a.owner_of("go/pkg/walk/walk.go"), "scan (module)");
    assert_eq!(a.owner_of("go/cmd/scan/main.go"), "scan");
    let links = a.links();
    assert!(
        links.contains(&("tool".into(), "core-lib".into(), "depends".into(), 1, true)),
        "{links:?}"
    );
    assert!(
        links.contains(&(
            "scan".into(),
            "scan (module)".into(),
            "depends".into(),
            1,
            true
        )),
        "the main package depends on its module: {links:?}"
    );
}
