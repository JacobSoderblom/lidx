//! The viewer's own logic, run under node against pages written by
//! `lidx graph` (tests/viewer_node/harness.js stubs the DOM). Skipped, with
//! a note on stderr, when `node` is not installed: the rest of the suite
//! has no node dependency, and this repository's CI does not run tests.
mod common;

use serde_json::Value;
use std::path::Path;
use std::process::Command;

fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn page(root: &Path, db: &Path, args: &[&str]) -> std::path::PathBuf {
    let out = root.join("viewer-test.html");
    let res = Command::new(env!("CARGO_BIN_EXE_lidx"))
        .arg("graph")
        .arg("--repo")
        .arg(root)
        .arg("--db")
        .arg(db)
        .arg("--out")
        .arg(&out)
        .args(args)
        .output()
        .unwrap();
    assert!(
        res.status.success(),
        "{}",
        String::from_utf8_lossy(&res.stderr)
    );
    out
}

fn run(page: &Path, scenario: &str, arg: &str) -> Value {
    run_at(page, scenario, arg, "")
}

/// `run` with the page opened at URL fragment `hash`.
fn run_at(page: &Path, scenario: &str, arg: &str, hash: &str) -> Value {
    let harness = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/viewer_node/harness.js");
    let out = Command::new("node")
        .env("HASH", hash)
        .arg(harness)
        .arg(page)
        .arg(scenario)
        .arg(arg)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "harness failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("harness prints JSON")
}

/// A symbol in a folder the visible budget folded is shown as itself once
/// searched for: the rebuild that opens its folders must not fold it again
/// and select the folder instead.
#[test]
fn a_searched_symbol_in_a_folded_folder_is_selected_as_itself() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let mut files: Vec<(String, String)> = Vec::new();
    for m in 0..60 {
        let mut src = String::new();
        if m + 1 < 60 {
            src.push_str(&format!("from mod{}.m import f_{}_0\n", m + 1, m + 1));
        }
        for f in 0..25 {
            src.push_str(&format!("\n\ndef f_{m}_{f}():\n"));
            if f + 1 < 25 {
                src.push_str(&format!("    f_{m}_{}()\n", f + 1));
            }
            if f == 0 && m + 1 < 60 {
                src.push_str(&format!("    f_{}_0()\n", m + 1));
            }
            src.push_str("    return 1\n");
        }
        files.push((format!("mod{m}/m.py"), src));
    }
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (_tmp, root, db) = common::index_repo("lidx-viewer-fold-", &refs);
    let p = page(
        &root,
        &db,
        &[
            "--seed",
            "f_0_0",
            "--depth",
            "200",
            "--direction",
            "down",
            "--kinds",
            "CALLS",
            "--max-nodes",
            "3000",
        ],
    );
    let r = run(&p, "focus", "");
    assert!(
        r["folded"].as_u64().unwrap() > 0,
        "the fixture must exceed the budget: {r}"
    );
    assert_ne!(r["before"], r["id"], "starts folded into its folder: {r}");
    assert_eq!(r["after"], r["id"], "drawn as itself after the search: {r}");
    assert_eq!(r["selected"], r["id"], "and selected, not its folder: {r}");
}

/// File-level bridges merged into folders count distinct keys: two files of
/// one folder sharing a key with another folder count it once.
#[test]
fn folder_bridges_in_the_viewer_count_distinct_keys() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo(
        "lidx-viewer-keys-",
        &[
            (
                "sender/a.py",
                "def send(msg):\n    _bus.publish(\"alpha\", msg)\n    _bus.publish(\"beta\", msg)\n",
            ),
            (
                "sender/b.py",
                "def send(msg):\n    _bus.publish(\"beta\", msg)\n    _bus.publish(\"gamma\", msg)\n",
            ),
            ("worker/util.py", "def helper():\n    return 1\n"),
            (
                "worker/w.py",
                "@router.subscribe('alpha')\ndef h0(msg):\n    pass\n\n\n@router.subscribe('beta')\ndef h1(msg):\n    pass\n\n\n@router.subscribe('gamma')\ndef h2(msg):\n    pass\n",
            ),
        ],
    );
    // A module-level page opened at the top folders: sender/ and worker/ boxes.
    let p = page(&root, &db, &["--level", "module"]);
    let lines = run(&p, "lines", "");
    let bus: Vec<&Value> = lines
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kinds"].get("VIA_BUS").is_some())
        .collect();
    assert_eq!(bus.len(), 1, "{lines}");
    assert_eq!(bus[0]["from"], "c:sender/");
    assert_eq!(bus[0]["to"], "c:worker/");
    assert_eq!(bus[0]["weight"], 3, "distinct keys, not 2 + 2: {lines}");
}

/// The app sheet: bands top to bottom (front ends, services, libraries,
/// infra), declared dependencies drawn downward, tests hidden, and the
/// Folders level one switch away. A seeded page colours by app.
#[test]
fn the_app_sheet_layers_by_role_and_switches_to_folders() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo(
        "lidx-viewer-apps-",
        &[
            (
                "web/Web.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n  <ItemGroup>\n    <ProjectReference Include=\"..\\core\\Core.csproj\" />\n  </ItemGroup>\n</Project>\n",
            ),
            (
                "web/Program.cs",
                "using Core;\n\nnamespace Web\n{\n    public class Api\n    {\n        public int Get() { return new Repo().Load(); }\n    }\n}\n",
            ),
            ("web/Dockerfile", "FROM scratch\n"),
            (
                "core/Core.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n",
            ),
            (
                "core/Repo.cs",
                "namespace Core\n{\n    public class Repo\n    {\n        public int Load() { return 1; }\n    }\n}\n",
            ),
            (
                "ui/package.json",
                "{ \"name\": \"ui\", \"scripts\": { \"dev\": \"vite\" }, \"dependencies\": { \"react\": \"18.0.0\" } }\n",
            ),
            (
                "ui/src/app.ts",
                "export function boot() {\n  return 1;\n}\n",
            ),
            (
                "deploy/web.yaml",
                "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  template:\n    spec:\n      containers:\n        - name: web\n          image: web:1\n",
            ),
            (
                "tests/Web.Tests/Web.Tests.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk\">\n  <ItemGroup>\n    <PackageReference Include=\"xunit\" Version=\"2.0.0\" />\n  </ItemGroup>\n</Project>\n",
            ),
            (
                "tests/Web.Tests/T.cs",
                "namespace Web.Tests\n{\n    public class T { }\n}\n",
            ),
        ],
    );
    let p = page(&root, &db, &[]);
    let r = run(&p, "apps", "Web");
    assert_eq!(r["level"], "app", "{r}");
    let cards = r["sheet"]["cards"].as_array().unwrap();
    let card = |name: &str| -> &Value {
        cards
            .iter()
            .find(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("no card {name}: {r}"))
    };
    let y = |name: &str| card(name)["y"].as_f64().unwrap();
    assert_eq!(card("ui")["band"], "front");
    assert_eq!(card("Web")["band"], "svc");
    assert_eq!(card("Core")["band"], "lib");
    assert_eq!(card("deploy")["band"], "infra");
    assert!(
        y("ui") < y("Web") && y("Web") < y("Core") && y("Core") < y("deploy"),
        "{r}"
    );
    assert!(
        !cards.iter().any(|c| c["name"] == "Web.Tests"),
        "test projects start hidden: {r}"
    );
    let links = r["sheet"]["links"].as_array().unwrap();
    let web_core = links
        .iter()
        .find(|l| l["from"] == card("Web")["key"] && l["to"] == card("Core")["key"])
        .unwrap_or_else(|| panic!("no Web -> Core link: {r}"));
    assert_eq!(web_core["up"], false, "dependencies flow down: {r}");
    assert_eq!(web_core["family"], "code", "{r}");
    assert!(
        web_core["label"].as_str().unwrap().contains("depends on"),
        "code and the declared reference share one line: {r}"
    );
    assert_eq!(r["selected"], true);
    assert_eq!(r["after"], "folder", "{r}");
    assert!(r["visible"].as_u64().unwrap() > 0, "{r}");

    let p = page(&root, &db, &["--seed", "Web.Api.Get", "--depth", "1"]);
    let c = run(&p, "colour", "");
    assert_eq!(c["colorBy"], "app", "{c}");
}

/// A code link no manifest declares is a resolver guess: off the sheet
/// until the legend turns guesses on.
#[test]
fn guessed_code_links_start_hidden_on_the_app_sheet() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo(
        "lidx-viewer-guesses-",
        &[
            (
                "core/Core.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n",
            ),
            (
                "core/Repo.cs",
                "namespace Core\n{\n    public class Repo\n    {\n        public int Load() { return 1; }\n    }\n}\n",
            ),
            (
                "extra/Extra.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n",
            ),
            (
                "extra/Extra.cs",
                "using Core;\n\nnamespace Extra\n{\n    public class Helper\n    {\n        public int Run() { return new Repo().Load(); }\n    }\n}\n",
            ),
        ],
    );
    let p = page(&root, &db, &[]);
    let r = run(&p, "guesses", "");
    let key = |name: &str| -> Value {
        r["cards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("no card {name}: {r}"))["key"]
            .clone()
    };
    let (extra, core) = (key("Extra"), key("Core"));
    let drawn = |links: &Value| -> Vec<String> {
        links
            .as_array()
            .unwrap()
            .iter()
            .filter(|l| l["from"] == extra && l["to"] == core)
            .map(|l| l["family"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(drawn(&r["before"]), Vec::<String>::new(), "{r}");
    assert_eq!(drawn(&r["after"]), vec!["code".to_string()], "{r}");
}

/// Two services and a library: the API calls the library and publishes two
/// topics the worker subscribes to.
const ZOOM_FILES: &[(&str, &str)] = &[
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
            "    _bus.publish(\"order-paid\", {})\n",
            "    return fmt_order(1)\n",
        ),
    ),
    (
        "shared/pyproject.toml",
        "[project]\nname = \"shared-utils\"\ndependencies = [\"pydantic\"]\n",
    ),
    (
        "shared/shared_utils/fmt.py",
        concat!(
            "def fmt_order(n):\n    return str(n)\n\n\n",
            "class Formatter:\n",
            "    def render(self, n):\n        return fmt_order(n)\n\n",
            "    def width(self):\n        return 3\n",
        ),
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
        concat!(
            "@router.subscribe('order-created')\ndef on_created(msg):\n    pass\n\n\n",
            "@router.subscribe('order-paid')\ndef on_paid(msg):\n    pass\n\n\n",
            "def run():\n    pass\n",
        ),
    ),
];

fn names(items: &Value, ty: &str) -> Vec<String> {
    items
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["type"] == ty)
        .map(|i| i["name"].as_str().unwrap().to_string())
        .collect()
}

/// A card opens in place: folders, then a file's class with its methods
/// listed inside and the call between them drawn; a method selected shows
/// its details; Escape walks back up and closes the card.
#[test]
fn an_app_card_zooms_down_to_its_symbols_and_back() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-zoom-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-drill", "shared-utils");
    assert_eq!(names(&r["top"]["items"], "dir"), ["shared_utils/"], "{r}");
    assert_eq!(r["top"]["crumbs"], serde_json::json!(["shared-utils"]));
    // Opening the card selects it: its panel is in the link.
    assert_eq!(r["top"]["hash"], "#zoom=shared-utils&panel=shared-utils");
    // The panel follows each level opened: the card, its folder, its file.
    let starts = |level: &str, with: &str| {
        let t = r[level]["panel"].as_str().unwrap();
        assert!(t.starts_with(with), "{level} panel: {t}");
    };
    starts("top", "shared-utils");
    starts("dir", "shared_utils/ folder");
    starts("file", "fmt.py file");
    assert_eq!(r["dirOpened"], true);
    assert_eq!(names(&r["dir"]["items"], "file"), ["fmt.py"], "{r}");
    assert_eq!(names(&r["dir"]["items"], "frame"), ["shared_utils/"], "{r}");
    assert_eq!(r["fileOpened"], true);
    let items = &r["file"]["items"];
    assert_eq!(names(items, "sym"), ["fmt_order", "Formatter"], "{r}");
    assert_eq!(
        names(items, "row"),
        ["render", "width"],
        "methods inside the class: {r}"
    );
    let render = items
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["name"] == "render")
        .unwrap();
    assert_eq!(render["kind"], "method");
    assert!(render["sig"].as_str().unwrap().contains("self, n"), "{r}");
    assert_eq!(
        r["file"]["crumbs"],
        serde_json::json!(["shared-utils", "shared_utils/", "fmt.py"])
    );
    // The call inside the file, between the two drawn symbols.
    let links = r["file"]["links"].as_array().unwrap();
    let call = links
        .iter()
        .find(|l| l["from"]["name"] == "render" && l["to"]["name"] == "fmt_order")
        .unwrap_or_else(|| panic!("no render -> fmt_order: {r}"));
    assert_eq!(call["weight"], 1);
    assert_eq!(call["kinds"]["CALLS"], 1);
    assert_eq!(call["line"], true, "inside the open file: a line");
    // The API's use of the library (an import of the file, an import of
    // fmt_order, a call to it) leaves every open level through one stub
    // named for the closed card, with the same weight at each depth.
    for (level, at, members) in [
        ("top", Value::Null, vec!["shared_utils/"]),
        ("dir", "shared_utils/".into(), vec!["fmt.py"]),
        ("file", "fmt.py".into(), vec!["fmt.py", "fmt_order"]),
    ] {
        let stubs = r[level]["stubs"].as_array().unwrap();
        assert_eq!(stubs.len(), 1, "{level}: {r}");
        let s = &stubs[0];
        assert_eq!(s["at"], at, "{level}: {s}");
        assert_eq!(s["target"], "orders-api", "{level}: {s}");
        assert_eq!(s["targetCard"], true, "{level}: {s}");
        assert_eq!(
            (&s["out"], &s["inn"]),
            (&Value::Bool(false), &Value::Bool(true)),
            "{level}: {s}"
        );
        assert_eq!(s["weight"], 3, "{level}: {s}");
        assert_eq!(
            s["kinds"],
            serde_json::json!({"CALLS": 1, "IMPORTS": 1, "IMPORTS_FILE": 1}),
            "{level}: {s}"
        );
        assert_eq!(s["members"], serde_json::json!(members), "{level}: {s}");
        assert_eq!(s["text"], "← orders-api ×3", "{level}: {s}");
        // No line runs from inside the card to the closed one.
        for l in r[level]["links"].as_array().unwrap() {
            if l["from"]["card"] == true || l["to"]["card"] == true {
                assert_eq!(l["line"], false, "{level}: {l}");
            }
        }
    }
    // Two shelves above the file (compact folder tag, frames) and the file's
    // two symbols with the class's two members.
    assert_eq!(r["file"]["rendered"], 6, "{r}");

    assert_eq!(r["selected"]["sel"]["name"], "render");
    let panel = r["selected"]["panel"].as_str().unwrap();
    assert!(panel.contains("shared/shared_utils/fmt.py:6"), "{panel}");
    assert!(panel.contains("lidx graph --seed"), "{panel}");
    assert!(
        r["selected"]["hash"]
            .as_str()
            .unwrap()
            .contains("&sel=shared-utils/"),
        "{r}"
    );
    assert_eq!(
        r["esc1"],
        serde_json::json!(["shared-utils", "shared_utils/"]),
        "Escape closes one level: {r}"
    );
    assert_eq!(r["esc3"]["open"], serde_json::json!([]), "{r}");
}

/// With both services open down to their files and both in view, the bus
/// links run function to function. With one closed, or out of view, they
/// leave through one stub per target on each open file's edge, weighted
/// and naming the boxes behind them.
#[test]
fn two_open_apps_show_a_cross_app_link_between_symbols() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-zoom2-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    let r = run(
        &p,
        "zoom-two",
        "orders-api,orders_api/main.py,notify,notify/main.py",
    );
    let stub = |state: &str, target: &str| -> Value {
        r[state]["stubs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["target"] == target)
            .cloned()
            .unwrap_or_else(|| panic!("{state}: no stub to {target}: {r}"))
    };
    let closed = stub("closed", "notify");
    assert_eq!(closed["at"], "main.py");
    assert_eq!(closed["out"], true);
    assert_eq!(closed["inn"], false);
    assert_eq!(closed["weight"], 2, "two topics: {r}");
    assert_eq!(closed["kinds"], serde_json::json!({"VIA_BUS": 2}));
    assert_eq!(closed["members"], serde_json::json!(["list_orders"]));
    let lib = stub("closed", "shared-utils");
    assert_eq!(lib["weight"], 3, "{r}");
    assert_eq!(
        lib["members"],
        serde_json::json!(["list_orders", "main.py"])
    );
    assert_eq!(r["closed"]["stubs"].as_array().unwrap().len(), 2, "{r}");
    assert_eq!(r["open"], serde_json::json!(["orders-api", "notify"]));

    let bus = |state: &str| -> Vec<(String, Value, Value)> {
        r[state]["links"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|l| l["kinds"].get("VIA_BUS").is_some())
            .map(|l| {
                assert_eq!(l["from"]["unit"], "orders-api", "{l}");
                assert_eq!(l["to"]["unit"], "notify", "{l}");
                assert_eq!(l["from"]["name"], "list_orders", "{l}");
                assert_eq!(l["kinds"], serde_json::json!({"VIA_BUS": 1}), "{l}");
                (
                    l["to"]["name"].as_str().unwrap().to_string(),
                    l["weight"].clone(),
                    l["line"].clone(),
                )
            })
            .collect()
    };
    let t = Value::Bool(true);
    let f = Value::Bool(false);
    let one = Value::from(1);
    assert_eq!(
        bus("both"),
        [
            ("on_created".to_string(), one.clone(), t.clone()),
            ("on_paid".to_string(), one.clone(), t.clone())
        ],
        "both in view, drawn end to end: {r}"
    );
    assert!(
        r["both"]["stubs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["targetUnit"] != "notify"),
        "{r}"
    );
    assert_eq!(
        bus("away"),
        [
            ("on_created".to_string(), one.clone(), f.clone()),
            ("on_paid".to_string(), one, f)
        ],
        "out of view, no lines: {r}"
    );
    let here = stub("away", "notify › main.py");
    assert_eq!(
        (here["unit"].as_str(), here["at"].as_str()),
        (Some("orders-api"), Some("main.py"))
    );
    assert_eq!(
        (here["out"].clone(), here["weight"].clone()),
        (t.clone(), Value::from(2))
    );
    assert_eq!(here["members"], serde_json::json!(["list_orders"]));
    let there = stub("away", "orders-api › main.py");
    assert_eq!(
        (there["unit"].as_str(), there["at"].as_str()),
        (Some("notify"), Some("main.py"))
    );
    assert_eq!(
        (
            there["inn"].clone(),
            there["out"].clone(),
            there["weight"].clone()
        ),
        (t.clone(), Value::Bool(false), Value::from(2))
    );
    assert_eq!(
        there["members"],
        serde_json::json!(["on_created", "on_paid"])
    );
    // Hovering the stub lights it (and only it); clicking lists its edges.
    let on = r["hover"]["on"].as_array().unwrap();
    assert_eq!(on.iter().filter(|x| **x == t).count(), 1, "{r}");
    let panel = r["panel"].as_str().unwrap();
    assert!(panel.contains("Edges · 2"), "{panel}");
    assert!(panel.contains("list_orders → on_created"), "{panel}");
    assert!(panel.contains("list_orders → on_paid"), "{panel}");
}

/// A file using six siblings: past the per-side cap the lightest stubs on a
/// side share one "+N elsewhere" stub, and no weight or member is lost.
#[test]
fn stubs_past_the_cap_share_one_and_keep_their_weight() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let mut files: Vec<(String, String)> = vec![(
        "lib/pyproject.toml".into(),
        "[project]\nname = \"hub\"\n".into(),
    )];
    let mut main = String::new();
    for i in 1..=6 {
        files.push((
            format!("lib/hub/m{i}.py"),
            format!("def f{i}():\n    return {i}\n"),
        ));
        main.push_str(&format!("from hub.m{i} import f{i}\n"));
    }
    main.push_str("\n\ndef run():\n");
    for i in 1..=6 {
        // f_i is called i times: six stubs of distinct weight.
        for _ in 0..i {
            main.push_str(&format!("    f{i}()\n"));
        }
    }
    files.push(("lib/hub/main.py".into(), main));
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (_tmp, root, db) = common::index_repo("lidx-viewer-zoomcap-", &refs);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-cap", "hub,hub/main.py,1");
    let before = r["before"].as_array().unwrap();
    let after = r["after"].as_array().unwrap();
    let total = |l: &[Value]| -> u64 { l.iter().map(|s| s["weight"].as_u64().unwrap()).sum() };
    // Each sibling m_i: the file's import of it, the import of f_i, i calls.
    assert_eq!(before.len(), 6, "{r}");
    for s in before {
        let i: u64 = s["target"].as_str().unwrap()[1..2].parse().unwrap();
        assert_eq!(s["weight"], i + 2, "{s}");
        assert_eq!(
            s["kinds"],
            serde_json::json!({"CALLS": i, "IMPORTS": 1, "IMPORTS_FILE": 1}),
            "{s}"
        );
    }
    assert_eq!(total(before), 33, "{r}");
    assert_eq!(total(after), total(before), "no weight lost: {r}");
    let mut sides: std::collections::BTreeMap<String, Vec<&Value>> = Default::default();
    for s in before {
        sides
            .entry(s["side"].as_str().unwrap().to_string())
            .or_default()
            .push(s);
    }
    for (side, group) in &sides {
        let now: Vec<&Value> = after
            .iter()
            .filter(|s| s["side"] == side.as_str())
            .collect();
        assert_eq!(now.len(), 1, "one stub per side at cap 1: {side} {r}");
        if group.len() == 1 {
            assert_eq!(now[0]["target"], group[0]["target"], "{r}");
        } else {
            assert_eq!(
                now[0]["target"],
                format!("+{} elsewhere", group.len()),
                "{r}"
            );
            let w: u64 = group.iter().map(|s| s["weight"].as_u64().unwrap()).sum();
            assert_eq!(now[0]["weight"], w, "{r}");
            assert_eq!(
                now[0]["members"],
                serde_json::json!(["main.py", "run"]),
                "{r}"
            );
        }
    }
    assert!(
        sides.values().any(|g| g.len() > 1),
        "six stubs on four sides share one: {r}"
    );
}

/// The panel tree filters to matches and their folders and classes; a click
/// zooms the sheet to the match and selects it, and the URL fragment
/// reopens the same state.
#[test]
fn the_panel_tree_filters_and_zooms_to_an_entry() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-zoom3-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-tree", "shared-utils,wid");
    let rows: Vec<(String, u64, bool)> = r["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            (
                x["name"].as_str().unwrap().to_string(),
                x["depth"].as_u64().unwrap(),
                x["hit"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("shared_utils/".to_string(), 0, false),
            ("fmt.py".to_string(), 1, false),
            ("Formatter".to_string(), 2, false),
            ("width".to_string(), 3, true),
        ],
        "{r}"
    );
    assert_eq!(
        names(&r["all"], "dir"),
        ["shared_utils/"],
        "unfiltered: top level only"
    );
    assert_eq!(
        r["crumbs"],
        serde_json::json!(["shared-utils", "shared_utils/", "fmt.py"])
    );
    assert_eq!(r["sel"]["name"], "width");
    // The panel shows the symbol, as a click on its box does.
    let panel = r["panel"].as_str().unwrap();
    assert!(
        panel.contains("shared_utils.fmt.Formatter.width") && panel.contains("Location"),
        "the symbol's own panel: {r}"
    );
    assert!(!panel.contains("Inside"), "not the unit's tree: {r}");
    let hash = r["hash"].as_str().unwrap();
    assert!(hash.contains("filter=wid"), "{hash}");
    assert!(!hash.contains("panel="), "{hash}");
    // Back to the unit: the tree, its filter kept.
    let back = &r["back"];
    assert!(back["text"].as_str().unwrap().contains("Inside"), "{r}");
    assert!(
        back["html"].as_str().unwrap().contains("value=\"wid\""),
        "{r}"
    );
    assert!(r["backHash"].as_str().unwrap().contains("panel="), "{r}");
    let again = run_at(&p, "zoom-hash", "", hash);
    assert_eq!(
        again["open"],
        serde_json::json!(["shared-utils"]),
        "{again}"
    );
    assert_eq!(again["crumbs"], r["crumbs"], "{again}");
    assert_eq!(again["sel"]["name"], "width", "{again}");
    assert!(
        again["panel"]
            .as_str()
            .unwrap()
            .contains("shared_utils.fmt.Formatter.width"),
        "{again}"
    );
}

/// File names and signatures are data: a crafted one is drawn as text on
/// the open card, in its tooltip, panel and breadcrumb, never as markup.
/// A crafted fragment cannot inject either, and one with broken
/// percent-encoding still opens what it names correctly.
#[test]
fn crafted_names_and_fragments_render_as_text() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    const EVIL: &str = "<img src=x onerror=alert(1)>";
    let file = format!("api/orders_api/{EVIL}.py");
    let body = "def handler(a=\"<svg onload=alert(2)>\"):\n    return a\n";
    let mut files: Vec<(&str, &str)> = ZOOM_FILES.to_vec();
    files.push((file.as_str(), body));
    let (_tmp, root, db) = common::index_repo("lidx-viewer-zoomxss-", &files);
    let p = page(&root, &db, &[]);
    let rel = format!("orders_api/{EVIL}.py");
    let r = run(&p, "zoom-html", &format!("orders-api,{rel},handler"));
    let names: Vec<&str> = r["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"handler"), "the crafted file opened: {r}");
    let html = r["html"].as_str().unwrap();
    assert!(
        html.contains("&lt;img src=x onerror=alert(1)&gt;"),
        "drawn as text"
    );
    assert!(
        html.contains("&lt;svg onload=alert(2)&gt;"),
        "the signature as text"
    );
    for raw in ["<img", "<svg onload"] {
        assert!(!html.contains(raw), "{raw} reached the markup: {html}");
    }

    // The fragment: a filter that tries to close its attribute, a selection
    // naming the crafted file, and one entry with broken encoding.
    let hash = format!(
        "#zoom=orders-api,%E0%A4&sel=orders-api/{}%23handler&panel=orders-api&filter=%22%3E%3Cimg%20src%3Dy%3E",
        rel.replace('<', "%3C")
            .replace('>', "%3E")
            .replace(' ', "%20")
            .replace('=', "%3D")
    );
    let again = run_at(&p, "zoom-hash", "", &hash);
    assert_eq!(again["open"], serde_json::json!(["orders-api"]), "{again}");
    assert_eq!(again["sel"]["name"], "handler", "{again}");
    let html = again["html"].as_str().unwrap();
    assert!(
        html.contains("value=\"&quot;&gt;&lt;img src=y&gt;\""),
        "the filter stays in its attribute: {html}"
    );
    assert!(!html.contains("<img"), "{html}");
}

/// A library with four folders that call each other, and files without
/// symbols beside them.
const LEVELS_FILES: &[(&str, &str)] = &[
    (
        "core/pyproject.toml",
        "[project]\nname = \"corelib\"\ndependencies = [\"pydantic\"]\n",
    ),
    ("core/corelib/__init__.py", ""),
    ("core/corelib/README.md", "Plain words, no headings.\n"),
    ("core/corelib/NOTES.md", "More words, no headings.\n"),
    (
        "core/corelib/alpha/a.py",
        "from corelib.beta.b import b\n\n\ndef a():\n    return b()\n",
    ),
    (
        "core/corelib/beta/b.py",
        "from corelib.gamma.c import c\n\n\ndef b():\n    return c()\n",
    ),
    ("core/corelib/gamma/c.py", "def c():\n    return 1\n"),
    (
        "core/corelib/gamma/hub.py",
        concat!(
            "class Hub:\n",
            "    def first(self):\n        return self.second() + self.third() + self.fourth()\n\n",
            "    def second(self):\n        return self.third() + self.fourth()\n\n",
            "    def third(self):\n        return self.fourth()\n\n",
            "    def fourth(self):\n        return 1\n",
        ),
    ),
    (
        "core/corelib/delta/d.py",
        "from corelib.alpha.a import a\nfrom corelib.gamma.c import c\n\n\ndef d():\n    return a() + c()\n",
    ),
];

/// Parses the `M x y h w v h h -w Z` cut-outs of the link layer's clip path.
fn clip_holes(svg: &str) -> Vec<[f64; 4]> {
    let at = svg
        .find("<clipPath id=\"zclip\">")
        .unwrap_or_else(|| panic!("no clip path on the link layer: {svg}"));
    let rest = &svg[at..];
    let d0 = rest.find(" d=\"").unwrap() + 4;
    let d = &rest[d0..d0 + rest[d0..].find('"').unwrap()];
    let mut holes = Vec::new();
    for part in d.split('M').skip(2) {
        let nums: Vec<f64> = part
            .split(['h', 'v', 'Z', ' '])
            .filter(|t| !t.is_empty())
            .map(|t| t.parse().unwrap())
            .collect();
        holes.push([nums[0], nums[1], nums[2], nums[3]]);
    }
    holes
}

/// Link lines never sit over a box: every drawn box is cut out of the clip
/// path the lines and their hit areas are drawn through, so a box's centre
/// takes the pointer (click, double-click, hover) and no line crosses its
/// face. The lines stay interactive between the boxes.
#[test]
fn link_hit_areas_never_cover_a_box() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-clip-", LEVELS_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-clip", "corelib,corelib/");
    let svg = r["svg"].as_str().unwrap();
    let lines = r["links"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|l| l["line"] == true)
        .count();
    assert!(lines >= 3, "folder-to-folder lines are drawn: {r}");
    // Every hit area is inside the clipped group.
    let group = svg
        .find("<g class=\"zlg\" clip-path=\"url(#zclip)\">")
        .unwrap_or_else(|| panic!("the lines are not clipped: {svg}"));
    let end = svg.find("<g class=\"zsg\">").unwrap();
    assert!(
        svg[group..end].matches("class=\"hit\"").count() >= lines,
        "{svg}"
    );
    assert!(
        !svg[..group].contains("class=\"hit\"") && !svg[end..].contains("class=\"hit\""),
        "{svg}"
    );
    assert!(svg.contains("clip-rule=\"evenodd\""), "{svg}");
    let holes = clip_holes(svg);
    let boxes = r["boxes"].as_array().unwrap();
    assert!(boxes.len() >= 4, "{r}");
    for b in boxes {
        let (x, y, w, h) = (
            b["x"].as_f64().unwrap(),
            b["y"].as_f64().unwrap(),
            b["w"].as_f64().unwrap(),
            b["h"].as_f64().unwrap(),
        );
        let (cx, cy) = (x + w / 2.0, y + h / 2.0);
        let covered = holes
            .iter()
            .filter(|o| cx > o[0] && cx < o[0] + o[2] && cy > o[1] && cy < o[1] + o[3])
            .count();
        assert_eq!(
            covered, 1,
            "the centre of {b} is cut out of the link layer: {holes:?}"
        );
        assert!(
            holes.iter().any(|o| (o[0] - x).abs() < 0.11
                && (o[1] - y).abs() < 0.11
                && (o[2] - w).abs() < 0.11
                && (o[3] - h).abs() < 0.11),
            "the whole face of {b} is cut out: {holes:?}"
        );
    }
}

/// The top search finds the symbols and files embedded for the zoom, ranked
/// by how well the name matches, and picking one opens its card down to
/// it, selected, with its panel.
#[test]
fn the_search_finds_embedded_symbols_and_opens_them() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-search-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-search", "Formatter;fmt;width");
    let formatter = &r["results"][0];
    assert_eq!(formatter[0]["type"], "symbol", "{r}");
    assert_eq!(formatter[0]["name"], "Formatter", "{r}");
    assert_eq!(formatter[0]["kind"], "class", "{r}");
    assert_eq!(formatter[0]["unit"], "shared-utils", "{r}");
    // `fmt`: names starting with it before names that only contain it.
    let fmt: Vec<(&str, &str)> = r["results"][1]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| (x["type"].as_str().unwrap(), x["name"].as_str().unwrap()))
        .collect();
    let at = |n: &str| {
        fmt.iter()
            .position(|x| x.1 == n)
            .unwrap_or_else(|| panic!("{n} not listed: {fmt:?}"))
    };
    assert!(
        at("fmt_order") < at("Formatter"),
        "a name prefix before a match inside: {fmt:?}"
    );
    assert!(fmt.len() <= 40, "{fmt:?}");
    // A method found by name, not only its file.
    let width = &r["results"][2];
    assert_eq!(width[0]["name"], "width", "{r}");
    assert_eq!(width[0]["qual"], "shared_utils.fmt.Formatter.width", "{r}");
    assert_eq!(r["level"], "app", "{r}");
    assert_eq!(
        r["crumbs"],
        serde_json::json!(["shared-utils", "shared_utils/", "fmt.py"]),
        "{r}"
    );
    assert_eq!(r["sel"]["name"], "width", "{r}");
    let panel = r["panel"].as_str().unwrap();
    assert!(
        panel.contains("shared_utils.fmt.Formatter.width"),
        "the symbol's panel: {panel}"
    );
    assert!(panel.contains("shared/shared_utils/fmt.py:9"), "{panel}");
}

/// A scroll that opens a folder ends with the folder framed, not zoomed on
/// past it to the clamp; Fit then frames the open folder again however far
/// in the camera went.
#[test]
fn a_wheel_open_settles_on_the_level_and_fit_frames_it() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-wheel-", LEVELS_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-wheel", "corelib,corelib/");
    assert_eq!(
        r["crumbs"],
        serde_json::json!(["corelib", "corelib/"]),
        "the folder opened: {r}"
    );
    let k = r["settled"]["k"].as_f64().unwrap();
    assert!(
        k <= 1.4 + 1e-9,
        "settled on the folder, not zoomed past it: {r}"
    );
    let fits = |l: &Value| {
        let (x, y, w, h) = (
            l["x"].as_f64().unwrap(),
            l["y"].as_f64().unwrap(),
            l["w"].as_f64().unwrap(),
            l["h"].as_f64().unwrap(),
        );
        x >= -0.5 && y >= -0.5 && x + w <= 1400.5 && y + h <= 900.5
    };
    assert!(fits(&r["level"]), "the open folder is in view: {r}");
    let fit = r["fit"]["k"].as_f64().unwrap();
    assert!((fit - k).abs() < 1e-6, "Fit frames the open folder: {r}");
    assert!(fits(&r["fitLevel"]), "{r}");
}

/// Camera fits use the part of the stage that can be seen: a panel opening
/// beside the stage as the camera moves, or laid over its foot on a phone,
/// does not hide part of what the fit framed.
#[test]
fn fits_leave_out_the_side_panel() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-panelfit-", LEVELS_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-panel-fit", "corelib,corelib/");
    let f = |v: &Value, k: &str| v[k].as_f64().unwrap();
    let level = &r["level"];
    assert!(
        f(level, "x") + f(level, "w") <= 1028.5,
        "the folder ends before the panel: {r}"
    );
    for k in ["k", "x", "y"] {
        assert!(
            (f(&r["moved"], k) - f(&r["refit"], k)).abs() < 0.5,
            "the moving fit was made again for the narrower stage ({k}): {r}"
        );
    }
    let sheet = &r["sheet"];
    assert!(
        f(sheet, "y") + f(sheet, "h") <= 500.5,
        "a phone's panel over the stage is left out too: {r}"
    );
    let mid = f(sheet, "y") + f(sheet, "h") / 2.0;
    assert!((mid - 250.0).abs() < 60.0, "{r}");
}

/// Stub ports keep off an open level's title, and stub labels off the
/// status line at the stage's foot.
#[test]
fn stubs_keep_off_the_title_and_the_status_line() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-stubui-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    let f = |v: &Value, i: usize| v[i].as_f64().unwrap();
    // The file's top edge just above the view: the ports are pushed down,
    // below its header rather than through its title.
    let r = run(
        &p,
        "zoom-stubs-ui",
        "shared-utils,shared_utils/fmt.py,2,30,",
    );
    let tops: Vec<&Value> = r["stubs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["side"] == "t")
        .collect();
    assert!(!tops.is_empty(), "{r}");
    for s in tops {
        let (port, b, head) = (&s["port"], &s["box"], &s["head"]);
        let (px, py) = (f(port, 0), f(port, 1));
        let top = b["y"].as_f64().unwrap();
        let (hx, hy) = (head["x1"].as_f64().unwrap(), head["y1"].as_f64().unwrap());
        let in_band = py > top - 4.0 && py < hy - 0.5;
        assert!(
            !(in_band && px - 4.0 < hx),
            "port {port} over the title (ends {hx}, {hy}): {s}"
        );
        if py > top + 0.5 {
            assert!(py >= hy - 1e-6, "pushed down, below the header: {s}");
        }
    }
    // The file's foot at the view's foot: the labels stay in the view. The
    // status line is a bar under the sheet (viewer.css), never over it.
    let r = run(
        &p,
        "zoom-stubs-ui",
        "orders-api,orders_api/main.py,1.2,,890",
    );
    let (k, cy) = (
        r["cam"]["k"].as_f64().unwrap(),
        r["cam"]["y"].as_f64().unwrap(),
    );
    let bottoms: Vec<&Value> = r["stubs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["side"] == "b")
        .collect();
    assert!(!bottoms.is_empty(), "{r}");
    for s in bottoms {
        let l = &s["label"];
        assert!(l.is_array(), "labelled: {s}");
        assert!(
            f(l, 3) * k + cy <= 900.0 + 0.5,
            "label out of the view: {s} {r}"
        );
    }
}

/// Files without symbols at one level share an "N other files" box, after
/// the folders and code files; it lists them when clicked and folds again.
#[test]
fn files_without_symbols_share_one_box() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-others-", LEVELS_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-others", "corelib,corelib/,corelib/alpha/");
    let shown = |items: &Value| -> Vec<(String, String)> {
        items
            .as_array()
            .unwrap()
            .iter()
            .filter(|i| i["depth"] == 1)
            .map(|i| {
                (
                    i["type"].as_str().unwrap().to_string(),
                    i["name"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    let before = shown(&r["before"]);
    let last = before.last().unwrap();
    assert_eq!(last.0, "others", "one box for them, last: {before:?}");
    assert!(last.1.ends_with("other files"), "{before:?}");
    for f in ["README.md", "NOTES.md"] {
        assert!(
            !before.iter().any(|x| x.1 == f),
            "{f} has no box of its own: {before:?}"
        );
    }
    assert_eq!(
        before.iter().filter(|x| x.0 == "dir").count(),
        4,
        "the four folders: {before:?}"
    );
    let listed: Vec<String> = r["listed"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["type"] == "orow")
        .map(|i| i["name"].as_str().unwrap().to_string())
        .collect();
    for f in ["README.md", "NOTES.md"] {
        assert!(
            listed.iter().any(|x| x == f),
            "listed once open: {listed:?}"
        );
    }
    assert!(r["panel"].as_str().unwrap().contains("README.md"), "{r}");
    assert!(
        !r["folded"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["type"] == "orow"),
        "folded again: {r}"
    );
    // Deeper in it is a name tag; activating it goes back up to its level, listed.
    assert_eq!(r["tag"]["type"], "others", "{r}");
    assert_eq!(
        r["backUp"]["crumbs"],
        serde_json::json!(["corelib", "corelib/"]),
        "{r}"
    );
    assert!(
        r["backUp"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["type"] == "orow" && i["name"] == "README.md"),
        "{r}"
    );
}

/// Labels on the sheet (stub labels and link labels) sit in free space:
/// inside the view, off every box, tag and header, and off one another.
fn assert_labels_clear(r: &Value) {
    let l = &r["labels"];
    let v = &l["view"];
    let (x0, y0, x1, y1) = (
        v["x0"].as_f64().unwrap(),
        v["y0"].as_f64().unwrap(),
        v["x1"].as_f64().unwrap(),
        v["y1"].as_f64().unwrap(),
    );
    let rect = |b: &Value| -> [f64; 4] {
        let a = b
            .as_array()
            .unwrap_or_else(|| panic!("a label was dropped: {r}"));
        [0, 1, 2, 3].map(|i| a[i].as_f64().unwrap())
    };
    let over =
        |a: &[f64; 4], b: &[f64; 4]| a[0] < b[2] && a[2] > b[0] && a[1] < b[3] && a[3] > b[1];
    let obst: Vec<[f64; 4]> = l["obstacles"]
        .as_array()
        .unwrap()
        .iter()
        .map(rect)
        .collect();
    let mut boxes: Vec<(String, [f64; 4])> = Vec::new();
    for key in ["stubs", "links"] {
        for s in l[key].as_array().unwrap() {
            boxes.push((s["text"].as_str().unwrap().to_string(), rect(&s["box"])));
        }
    }
    for (i, (t, b)) in boxes.iter().enumerate() {
        assert!(
            b[0] >= x0 - 0.01 && b[2] <= x1 + 0.01 && b[1] >= y0 - 0.01 && b[3] <= y1 + 0.01,
            "{t} {b:?} is outside the view: {r}"
        );
        for o in &obst {
            assert!(!over(b, o), "{t} {b:?} sits on a box or tag {o:?}: {r}");
        }
        for (u, c) in &boxes[i + 1..] {
            assert!(!over(b, c), "{t} {b:?} sits on the label {u} {c:?}: {r}");
        }
    }
}

/// With a folder open deeper in, the stub labels next to the sibling tags
/// of the levels above are placed in free space, not over the tags.
#[test]
fn stub_labels_keep_off_the_tags_of_the_levels_above() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-lbltags-", LEVELS_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-labels", "corelib,corelib/alpha/,,,1.6,,");
    assert!(!r["labels"]["stubs"].as_array().unwrap().is_empty(), "{r}");
    assert_labels_clear(&r);
}

/// With no room for labels beside a side (the left edge at the view's, or
/// a panel narrowing the stage to just past the right edge), the labels
/// are placed elsewhere, never dropped.
#[test]
fn stub_labels_without_room_beside_a_side_are_placed_elsewhere() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-lblroom-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    // The API's file at scale 2: its stub to the worker leaves on the left.
    let probe = run(&p, "zoom-labels", "orders-api,orders_api/main.py,,,2,,,");
    let sides: Vec<&str> = probe["labels"]["stubs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["side"].as_str().unwrap())
        .collect();
    assert!(sides.contains(&"l"), "{probe}");
    // Its left edge 12 px into the view: no room beside it.
    let r = run(&p, "zoom-labels", "orders-api,orders_api/main.py,,,2,,,12");
    assert_labels_clear(&r);
    // A panel beside the stage, the file's right edge 24 px from it.
    let lv = &probe["level"];
    let right = lv["x"].as_f64().unwrap() + lv["w"].as_f64().unwrap();
    let r = run(
        &p,
        "zoom-labels",
        &format!(
            "orders-api,orders_api/main.py,,{},2,,,",
            (right + 24.0).round()
        ),
    );
    assert_labels_clear(&r);
}

/// In an open file, the labels of the calls between a class's members are
/// whole (not cut by the class box), inside the view and apart.
#[test]
fn link_labels_in_a_file_are_whole_and_apart() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-lbllinks-", LEVELS_FILES);
    let p = page(&root, &db, &[]);
    let r = run(
        &p,
        "zoom-labels",
        "corelib,corelib/gamma/hub.py,corelib/gamma/hub.py#corelib.gamma.hub.Hub.first,,,,",
    );
    let links = r["labels"]["links"].as_array().unwrap();
    assert!(
        links.len() >= 3,
        "the selected method's calls are labelled: {r}"
    );
    assert_labels_clear(&r);
}

// ------------------------------------------------- the landing page

fn glance_page(files: &[(&str, &str)], prefix: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let (tmp, root, db) = common::index_repo(prefix, files);
    let p = page(&root, &db, &[]);
    (tmp, p)
}

fn field_list(v: &Value, key: &str) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x[key].as_str().unwrap().to_string())
        .collect()
}

/// The page opens on the overview: a few cards in lanes, every line with a
/// verb and a count and its label clear of every card and other label.
#[test]
fn the_landing_page_shows_a_small_labelled_map() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-glance-");
    let r = run(&p, "glance-landing", "");
    assert_eq!(r["level"], "glance", "{r}");
    let cards = r["cards"].as_array().unwrap();
    assert!(cards.len() <= 12);
    assert_eq!(
        field_list(&r["cards"], "name"),
        vec![
            "portal",
            "ingest-api",
            "validator",
            "Kubernetes manifests",
            "Message bus"
        ]
    );
    assert_eq!(
        field_list(&r["lanes"], "label"),
        vec![
            "Front ends",
            "Services",
            "Workers and tools",
            "Data and messaging",
            "Infrastructure"
        ]
    );
    let links: Vec<(String, bool)> = r["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| {
            (
                l["label"].as_str().unwrap().to_string(),
                l["shown"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        links,
        vec![
            ("calls over HTTP".to_string(), true),
            ("reads secret DB_PASSWORD".to_string(), true),
            ("publishes file-uploaded".to_string(), true),
            // Labels keep off other lines where they can, which leaves
            // this short line a spot of its own.
            ("subscribes to file-uploaded".to_string(), true),
        ]
    );
    // Labels keep off the cards and each other.
    let rect = |v: &Value| -> (f64, f64, f64, f64) {
        (
            v["x"].as_f64().unwrap(),
            v["y"].as_f64().unwrap(),
            v["w"].as_f64().unwrap(),
            v["h"].as_f64().unwrap(),
        )
    };
    let overlap = |a: (f64, f64, f64, f64), b: (f64, f64, f64, f64)| {
        a.0 < b.0 + b.2 && a.0 + a.2 > b.0 && a.1 < b.1 + b.3 && a.1 + a.3 > b.1
    };
    let boxes: Vec<_> = r["links"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|l| l["shown"].as_bool().unwrap())
        .map(|l| rect(&l["box"]))
        .collect();
    for (i, b) in boxes.iter().enumerate() {
        for c in cards {
            assert!(!overlap(*b, rect(c)), "label {i} on card {c}: {r}");
        }
        for (j, o) in boxes.iter().enumerate() {
            assert!(
                i == j || !overlap(*b, *o),
                "labels {i} and {j} overlap: {r}"
            );
        }
    }
    assert_eq!(r["strip"], serde_json::json!([]), "no library: no strip");
    let side = r["side"].as_str().unwrap();
    assert!(
        side.starts_with("Start hereWays iningest-api1 routeHTTPPOST /uploadsFollow ▸validator1 subscriptionbuson file-uploaded"),
        "{side}"
    );
    assert!(
        side.contains("Read firstREADME.mdwhat the project is"),
        "{side}"
    );
    assert_eq!(
        r["tours"],
        "ToursSystem tour 6 stepsPOST /uploads → ingest-api → Message bus → validatorMore questions ▾"
    );
    // Every string reaches the page escaped.
    let html = r["html"]["cards"].as_str().unwrap();
    assert!(html.contains("data-g=\"1\""), "{html}");
}

/// A flow steps with the arrow keys over the map, and Escape leaves it with
/// the part it had reached selected.
#[test]
fn a_flow_steps_with_the_arrow_keys_and_escape_keeps_the_selection() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gflow-");
    let r = run(&p, "glance-flow", "0");
    // A flow opens as a sequence.
    assert_eq!(r["opened"]["seq"], true, "{r}");
    assert_eq!(r["opened"]["step"], 0, "{r}");
    let steps: Vec<(Value, Value, Value)> = r["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["flow"].clone(), s["step"].clone(), s["sel"].clone()))
        .collect();
    use serde_json::json;
    assert_eq!(
        steps,
        vec![
            (json!(0), json!(0), Value::Null),
            (json!(0), json!(1), Value::Null),
            (json!(0), json!(2), Value::Null),
            (json!(0), json!(1), Value::Null),
            // Past the end it stays on the last step.
            (json!(0), json!(6), Value::Null),
            (Value::Null, json!(6), json!("Kubernetes manifests")),
        ],
        "{r}"
    );
    // Step 3: the symbol, its file and line, and the chain so far.
    let side = r["side"].as_str().unwrap();
    assert!(
        side.starts_with("POST /uploads → ingest-api → Message bus → validator3 store runs in ingest-api.ingest_api.main.storeapi/ingest_api/main.py:11"),
        "{side}"
    );
    // The message goes to the bus, then the worker takes it off.
    assert!(
        side.contains(
            "4file-uploadedMessage bus · publishes to5on_uploadedvalidator · receives the message"
        ),
        "{side}"
    );
    assert!(
        side.contains("7secret DB_PASSWORDKubernetes manifests · reads the secret"),
        "{side}"
    );
    // Numbered crossings on the map, the current one marked.
    let links = r["html"]["links"].as_str().unwrap();
    assert_eq!(links.matches("class=\"g-fl").count(), 4, "{links}");
    assert!(
        links.contains("class=\"g-fl past\" data-gstep=\"1\""),
        "{links}"
    );
    assert!(links.contains("class=\"g-fl\" data-gstep=\"3\""), "{links}");
    let tours = r["html"]["tours"].as_str().unwrap();
    assert!(tours.contains("Step 3 of 7"), "{tours}");
}

/// Flows that do not fit the tours strip move into its menu, each as
/// "Follow <title>"; Follow buttons say what they do.
#[test]
fn flows_past_the_strip_wait_in_the_menu_and_follow_says_what_it_does() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::CLI, "lidx-viewer-gidle-");
    let r = run(&p, "glance-idle-tours", "0");
    let all = r["all"].as_array().unwrap();
    assert_eq!(all.len(), 2, "{r}");
    let html = r["html"].as_str().unwrap();
    // No pill: both flows are menu items, before the questions.
    assert!(!html.contains("class=\"g-tour\" data-gflow"), "{html}");
    let first = format!(
        "role=\"menuitem\" data-gflow=\"0\" title=\"Follow this request through the system: {}\">Follow {}</button>",
        all[0].as_str().unwrap(),
        all[0].as_str().unwrap()
    );
    assert!(html.contains(&first), "{html}");
    assert!(html.contains("data-gflow=\"1\""), "{html}");
    let one = run(&p, "glance-idle-tours", "1");
    let html = one["html"].as_str().unwrap();
    assert!(html.contains("class=\"g-tour\" data-gflow=\"0\""), "{html}");
    assert!(
        html.contains("role=\"menuitem\" data-gflow=\"1\""),
        "{html}"
    );
    assert!(
        !html.contains("role=\"menuitem\" data-gflow=\"0\""),
        "{html}"
    );
    // Start here: the Follow chip's name says where it goes.
    let land = run(&p, "glance-landing", "");
    let side = land["html"]["side"].as_str().unwrap_or("");
    assert!(
        side.contains("title=\"Follow this request through the system\" aria-label=\"Follow tally main through the system\">Follow ▸"),
        "{side}"
    );
}

/// A flow that branches lists where, and which branch it follows.
#[test]
fn a_branching_flow_lists_its_branch_points() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::CLI, "lidx-viewer-gfan-");
    let r = run(&p, "glance-flow", "0");
    let side = r["side"].as_str().unwrap();
    assert!(
        side.contains("Where the request branchescli reaches 2 parts: this flow follows report; it also reaches store."),
        "{side}"
    );
}

/// A map too tall for its area scrolls each tour step's subject and each
/// flow step's part into view, smoothly unless motion is reduced.
#[test]
fn every_tour_and_flow_step_scrolls_its_subject_into_view() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-greveal-");
    let r = run(&p, "glance-reveal", "900,150");
    assert!(
        r["cam"]["h"].as_f64().unwrap() > 300.0,
        "the map must not fit: {r}"
    );
    let tour = r["tour"].as_array().unwrap();
    assert!(tour.len() >= 5, "{r}");
    for t in tour {
        for (n, s) in t["subject"]
            .as_array()
            .unwrap()
            .iter()
            .zip(t["seen"].as_array().unwrap())
        {
            // A subject card taller than the view cannot be wholly in it.
            if t["id"] != "lanes" {
                assert_eq!(s, true, "tour step {} hides {n}: {r}", t["id"]);
            }
        }
    }
    // The data step is about the resources, at the foot of the map: it
    // scrolls down to them.
    let data = tour.iter().find(|t| t["id"] == "data").unwrap();
    assert!(data["top"].as_f64().unwrap() > 0.0, "{r}");
    for f in r["flow"].as_array().unwrap() {
        assert_eq!(
            f["seen"], true,
            "flow step {} hides {}: {r}",
            f["step"], f["node"]
        );
    }
    let b = r["behavior"].as_array().unwrap();
    assert!(!b.is_empty(), "{r}");
    assert!(b.iter().all(|x| x == "smooth"), "{r}");
    // Reduced motion: the same scrolls, at once.
    let out = Command::new("node")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/viewer_node/harness.js"))
        .arg(&p)
        .arg("glance-reveal")
        .arg("900,150")
        .env("REDUCE", "1")
        .output()
        .unwrap();
    let rr: Value = serde_json::from_slice(&out.stdout).unwrap();
    let b = rr["behavior"].as_array().unwrap();
    assert!(!b.is_empty() && b.iter().all(|x| x == "auto"), "{rr}");
}

/// The sequence view: one lane per part in the order the flow reaches it.
#[test]
fn the_sequence_toggle_draws_the_flow_in_lanes() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gseq-");
    let r = run(&p, "glance-seq", "0");
    assert_eq!(r["on"]["state"]["seq"], true);
    assert_eq!(
        r["on"]["lanes"],
        serde_json::json!([
            "portal",
            "ingest-api",
            "Message bus",
            "validator",
            "Kubernetes manifests"
        ])
    );
    let svg = r["on"]["html"].as_str().unwrap();
    assert_eq!(svg.matches("class=\"sq-msg").count(), 7, "{svg}");
    assert!(
        svg.contains("class=\"sq-msg cur\" data-gstep=\"1\""),
        "{svg}"
    );
    assert!(svg.contains("2  upload  · calls over HTTP"), "{svg}");
    // The toggle: Sequence pressed, Map not.
    let tours = r["on"]["tours"].as_str().unwrap();
    assert!(tours.ends_with("SequenceMap"), "{tours}");
    // S: the map, the flow still open; S again: the sequence.
    assert_eq!(r["off"]["state"]["seq"], false);
    assert_eq!(r["off"]["state"]["flow"], 0);
    assert_eq!(r["off"]["state"]["step"], 1);
    assert_eq!(r["off"]["cards"], 5);
    assert_eq!(r["again"]["seq"], true);
    assert_eq!(r["again"]["step"], 1);
}

/// The system tour runs step by step, shows its progress, and exits with
/// the selection kept and a way back in.
#[test]
fn the_tour_runs_and_exits_keeping_the_selection() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gtour-");
    let r = run(&p, "glance-tour", "");
    assert_eq!(r["count"], 6);
    let seen = r["seen"].as_array().unwrap();
    let at: Vec<Value> = seen.iter().map(|s| s["state"]["tour"].clone()).collect();
    assert_eq!(
        at,
        serde_json::json!([0, 1, 2, 1, null])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(
        seen[0]["tours"],
        "System tourStep 1 of 6: These are the 3 apps, by role‹ BackNext ›Exit"
    );
    assert_eq!(seen[1]["state"]["sel"], "ingest-api");
    // The worker meets the API at the bus, not on a line of its own.
    assert_eq!(
        seen[1]["state"]["dim"],
        serde_json::json!(["Kubernetes manifests", "validator"])
    );
    assert!(
        seen[2]["side"].as_str().unwrap().starts_with("System tour · step 3 of 6Follow one requestPOST /uploads → ingest-api → Message bus → validator: 7 steps across 5 parts."),
        "{}",
        seen[2]["side"]
    );
    // Escape: out of the tour, at the part step 2 selected, with a resume chip.
    assert_eq!(seen[4]["state"]["sel"], "ingest-api");
    assert_eq!(seen[4]["state"]["resume"], 1);
    assert!(
        seen[4]["tours"]
            .as_str()
            .unwrap()
            .contains("Resume tour step 2"),
        "{}",
        seen[4]["tours"]
    );
}

/// Explore is one click away and keeps working; the overview comes back.
#[test]
fn explore_opens_the_app_view_and_comes_back() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gexp-");
    let r = run(&p, "glance-explore", "");
    assert_eq!(r["before"], "glance");
    assert_eq!(r["after"], "app");
    assert_eq!(r["open"], serde_json::json!([]));
    assert_eq!(r["back"], "glance");
}

/// Double-clicking a card opens that app in Explore, zoomed into it; a
/// module card opens its unit at that folder.
#[test]
fn double_clicking_a_card_opens_it_in_explore() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gdbl-");
    let r = run(&p, "glance-dblclick", "validator");
    assert_eq!(r["ok"], true);
    assert_eq!(r["level"], "app");
    assert_eq!(r["open"], serde_json::json!(["validator"]));
    assert_eq!(r["crumbs"], serde_json::json!(["validator"]));
    // The header's breadcrumb says where Explore is; back on the overview
    // it is emptied and hidden, never left stale.
    assert!(
        r["crumbsThere"].as_str().unwrap().contains("validator"),
        "{r}"
    );
    assert_eq!(r["crumbsBack"], "", "{r}");
    assert_eq!(r["crumbsHidden"], true, "{r}");
    let (_tmp2, p2) = glance_page(common::glance_fixtures::CLI, "lidx-viewer-gdblm-");
    let r = run(&p2, "glance-dblclick", "store");
    assert_eq!(r["level"], "app", "{r}");
    assert_eq!(r["open"], serde_json::json!(["tally"]), "{r}");
    assert_eq!(
        r["crumbs"],
        serde_json::json!(["tally", "src/", "store/"]),
        "{r}"
    );
}

/// A card shows its neighbours and fades the rest; a line shows the calls
/// behind it; a question its answer; Escape clears all three.
#[test]
fn a_card_a_line_and_a_question_each_explain_themselves() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gsel-");
    let r = run(&p, "glance-select", "validator");
    assert_eq!(r["card"]["state"]["sel"], "validator");
    assert_eq!(r["card"]["state"]["dim"], serde_json::json!(["portal"]));
    let side = r["card"]["side"].as_str().unwrap();
    assert!(
        side.starts_with("‹ Start herevalidator worker · PythonPython worker subscribed to 1 topic; reads 1 secret, uses a message bus.worker/Ways in1 subscriptionbuson file-uploaded"),
        "{side}"
    );
    assert!(
        // One row per verb, as the map says it.
        side.contains("Talks toreads secret DB_PASSWORD Kubernetes manifestssubscribes to file-uploaded Message bus"),
        "{side}"
    );
    // Where it runs: the deployment that names its image.
    assert!(side.contains("Runs inKubernetes (deploy/)"), "{side}");
    assert!(
        side.contains("Called bypublishes file-uploaded ingest-api"),
        "{side}"
    );
    assert!(
        !side.contains("Flows"),
        "a flow passing through is not one that starts here: {side}"
    );
    assert_eq!(r["link"]["state"]["link"], 0);
    assert_eq!(
        r["link"]["state"]["dim"],
        serde_json::json!(["Kubernetes manifests", "Message bus", "validator"])
    );
    let side = r["link"]["side"].as_str().unwrap();
    assert!(
        side.starts_with("‹ Start hereportal → ingest-apiCalls over HTTP.The calls behind it/uploadsfrom ui/src/api.sendUpload ui/src/api.ts:1to ingest_api.main.upload api/ingest_api/main.py:6"),
        "{side}"
    );
    assert_eq!(r["q"]["state"]["question"], 0);
    assert!(
        r["q"]["side"]
            .as_str()
            .unwrap()
            // A key a Kubernetes Secret sets is a secret, listed as one.
            .starts_with("‹ Start hereWhere do secrets come from?validator reads secret DB_PASSWORD from Kubernetes manifests.Secrets 1validator reads secret DB_PASSWORD from Kubernetes manifestsworker/validator/main.py:9"),
        "{}",
        r["q"]["side"]
    );
    assert_eq!(r["cleared"]["sel"], Value::Null);
    assert_eq!(r["cleared"]["question"], Value::Null);
    assert_eq!(r["cleared"]["dim"], serde_json::json!([]));
}

/// A crafted description reaches the landing page as text, never as markup.
#[test]
fn crafted_descriptions_render_as_text_on_the_landing_page() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let mut files: Vec<(&str, &str)> = common::glance_fixtures::MULTI
        .iter()
        .filter(|(p, _)| *p != "ui/package.json")
        .copied()
        .collect();
    files.push((
        "ui/package.json",
        "{\n  \"name\": \"portal\",\n  \"description\": \"<img src=x onerror=alert(1)> & co\",\n  \"scripts\": { \"dev\": \"vite\" },\n  \"dependencies\": { \"react\": \"18.0.0\" }\n}\n",
    ));
    let (_tmp, p) = glance_page(&files, "lidx-viewer-gesc-");
    let landing = run(&p, "glance-landing", "");
    let all: String = ["head", "tours", "cards", "links", "side", "seq", "strip"]
        .iter()
        .map(|k| landing["html"][k].as_str().unwrap_or("").to_string())
        .collect();
    assert!(!all.contains("<img"), "{all}");
    assert!(
        all.contains("&lt;img src=x onerror=alert(1)&gt; &amp; co"),
        "{all}"
    );
    let r = run(&p, "glance-select", "portal");
    let side = r["card"]["side"].as_str().unwrap();
    assert!(
        side.contains("<img src=x onerror=alert(1)> & co"),
        "as text: {side}"
    );
}

/// Every string the landing page shows from the repository is text: a
/// README summary, a route path and a manifest description, on
/// the map, the side column, flows, sequences, tour steps and questions.
#[test]
fn crafted_routes_and_readmes_render_as_text_everywhere_on_the_landing_page() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    // No spaces: a route with one is no longer matched across apps.
    const ROUTE: &str = "/uploads/<img/src=x/onerror=alert(1)>";
    let api = common::glance_fixtures::MULTI
        .iter()
        .find(|(p, _)| *p == "api/ingest_api/main.py")
        .unwrap()
        .1
        .replace("/uploads", ROUTE);
    let ui = common::glance_fixtures::MULTI
        .iter()
        .find(|(p, _)| *p == "ui/src/api.ts")
        .unwrap()
        .1
        .replace("/uploads", ROUTE);
    let mut files: Vec<(&str, &str)> = common::glance_fixtures::MULTI
        .iter()
        .filter(|(p, _)| {
            !matches!(
                *p,
                "README.md" | "api/ingest_api/main.py" | "ui/src/api.ts" | "api/pyproject.toml"
            )
        })
        .copied()
        .collect();
    files.push((
        "README.md",
        "# Uploads\n\nUploads checks that 1 < 2 & 3 > 2 for \"every\" partner's file before storing it.\n",
    ));
    files.push(("api/ingest_api/main.py", &api));
    files.push(("ui/src/api.ts", &ui));
    files.push((
        "api/pyproject.toml",
        "[project]\nname = \"ingest-api\"\ndescription = \"Queues <script>alert(3)</script> uploads.\"\ndependencies = [\"fastapi>=0.110\"]\n",
    ));
    let (_tmp, p) = glance_page(&files, "lidx-viewer-gesc2-");
    let r = run(&p, "glance-everything", "");
    let html = r["html"].as_str().unwrap();
    assert!(r["flows"].as_u64().unwrap() >= 1, "no flow to check: {r}");
    assert!(r["tour"].as_u64().unwrap() >= 1, "no tour to check: {r}");
    for raw in ["<img", "<script"] {
        assert!(!html.contains(raw), "{raw} as markup:\n{html}");
    }
    for text in [
        "1 &lt; 2 &amp; 3 &gt; 2 for &quot;every&quot; partner&#39;s file",
        "&lt;img/src=x/onerror=alert(1)&gt;",
        "&lt;script&gt;alert(3)&lt;/script&gt;",
    ] {
        assert!(html.contains(text), "{text} not shown:\n{html}");
    }
}

/// The cubic Bézier `p` at `t`.
fn bez(p: &Value, t: f64) -> (f64, f64) {
    let c = |i: usize, k: usize| p[i][k].as_f64().unwrap();
    let u = 1.0 - t;
    let at = |k| {
        u * u * u * c(0, k)
            + 3.0 * u * u * t * c(1, k)
            + 3.0 * u * t * t * c(2, k)
            + t * t * t * c(3, k)
    };
    (at(0), at(1))
}

/// Curves (`{from, to, p}`) that run through a box (`{name, x, y, w, h}`)
/// other than their own two, or do not start on `from` and end on `to`.
fn unclear_curves(cards: &Value, curves: &Value) -> Vec<String> {
    let boxes: Vec<(String, [f64; 4])> = cards
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            let f = |k: &str| c[k].as_f64().unwrap();
            (
                c["name"].as_str().unwrap().to_string(),
                [f("x"), f("y"), f("w"), f("h")],
            )
        })
        .collect();
    let find = |n: &str| boxes.iter().find(|(m, _)| m == n).unwrap().1;
    let on_edge = |(x, y): (f64, f64), b: [f64; 4]| {
        let inside =
            x >= b[0] - 1.5 && x <= b[0] + b[2] + 1.5 && y >= b[1] - 1.5 && y <= b[1] + b[3] + 1.5;
        let edge = (x - b[0]).abs() < 1.5
            || (x - b[0] - b[2]).abs() < 1.5
            || (y - b[1]).abs() < 1.5
            || (y - b[1] - b[3]).abs() < 1.5;
        inside && edge
    };
    let mut bad = Vec::new();
    for c in curves.as_array().unwrap() {
        let (from, to) = (c["from"].as_str().unwrap(), c["to"].as_str().unwrap());
        // A route down a gap is several curves end to end.
        let segs: Vec<Value> = match c["pp"].as_array() {
            Some(pp) => pp.clone(),
            None => vec![c["p"].clone()],
        };
        if !on_edge(bez(&segs[0], 0.0), find(from))
            || !on_edge(bez(&segs[segs.len() - 1], 1.0), find(to))
        {
            bad.push(format!("{from}>{to} does not start and end on its cards"));
        }
        'segs: for seg in &segs {
            for k in 1..100 {
                let (x, y) = bez(seg, k as f64 / 100.0);
                if let Some((n, _)) = boxes.iter().find(|(n, b)| {
                    n != from
                        && n != to
                        && x > b[0] + 2.0
                        && x < b[0] + b[2] - 2.0
                        && y > b[1] + 2.0
                        && y < b[1] + b[3] - 2.0
                }) {
                    bad.push(format!("{from}>{to} through {n}"));
                    break 'segs;
                }
            }
        }
    }
    bad
}

/// A line between two cards with a third between them goes round it, not
/// behind it: one leaving A for B skips M (below A, above B) and the card
/// N beside M, through the narrow gap between them.
#[test]
fn a_line_goes_round_the_cards_between_its_ends() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-groute-");
    let b = |x: f64, y: f64| serde_json::json!({ "x": x, "y": y, "w": 196.0, "h": 98.0 });
    let layouts = [
        // A, M and B in one column; N in the next, 40 px away.
        serde_json::json!({
            "boxes": { "A": b(260.0, 24.0), "M": b(260.0, 168.0), "B": b(260.0, 312.0), "N": b(24.0, 312.0) },
            "size": { "w": 480.0, "h": 434.0 }, "from": "A", "to": "B"
        }),
        // A row: A, M and B side by side, B two columns on.
        serde_json::json!({
            "boxes": { "A": b(24.0, 60.0), "M": b(260.0, 60.0), "B": b(496.0, 60.0), "N": b(260.0, 204.0) },
            "size": { "w": 716.0, "h": 326.0 }, "from": "A", "to": "B"
        }),
    ];
    for layout in layouts {
        let r = run(&p, "glance-route", &layout.to_string());
        let cards: Vec<Value> = layout["boxes"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(n, v)| {
                let mut v = v.clone();
                v["name"] = Value::String(n.clone());
                v
            })
            .collect();
        let curves = serde_json::json!([{ "from": "A", "to": "B", "p": r["p"] }]);
        assert_eq!(
            unclear_curves(&Value::Array(cards.clone()), &curves),
            Vec::<String>::new(),
            "{r}"
        );
        // The plain sides alone would have crossed M.
        let plain = r["plain"].as_array().unwrap();
        let side = |v: &Value| v.as_str().unwrap().to_string();
        assert_ne!(
            (side(&plain[0]), side(&plain[1])),
            (side(&r["side"][0]), side(&r["side"][1])),
            "{r}"
        );
    }
}

/// The landing map grows to use its area, up to a third larger than the
/// cards' own size, and no line or flow arrow runs through a card it does
/// not connect.
#[test]
fn the_landing_map_uses_its_area_and_its_lines_stay_clear() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    for (files, prefix) in [
        (common::glance_fixtures::MULTI, "lidx-viewer-gcurve-m-"),
        (common::glance_fixtures::CLI, "lidx-viewer-gcurve-c-"),
        (common::glance_fixtures::LIBS, "lidx-viewer-gcurve-l-"),
    ] {
        let (_tmp, p) = glance_page(files, prefix);
        let r = run(&p, "glance-curves", "");
        let k = r["landing"]["cam"]["k"].as_f64().unwrap();
        assert!(k > 1.0 && k <= 1.35, "{prefix}: scale {k}: {r}");
        assert_eq!(
            unclear_curves(&r["landing"]["cards"], &r["landing"]["curves"]),
            Vec::<String>::new(),
            "{prefix}: {r}"
        );
        for f in r["flows"].as_array().unwrap() {
            assert_eq!(
                unclear_curves(&f["cards"], &f["curves"]),
                Vec::<String>::new(),
                "{prefix}: {f}"
            );
        }
    }
}

/// Two front ends, a service calling another beside one that calls none,
/// and two workers: few lanes of few cards. The map lays each lane out as
/// one band, so it is drawn at the largest scale instead of as a tall stack.
const SHOP: &[(&str, &str)] = &[
    (
        "web/package.json",
        "{ \"name\": \"web\", \"scripts\": { \"dev\": \"vite\" }, \"dependencies\": { \"react\": \"18.0.0\" } }\n",
    ),
    (
        "web/src/api.ts",
        "export async function order() {\n  return fetch(\"/orders\", { method: \"POST\" });\n}\n",
    ),
    (
        "admin/package.json",
        "{ \"name\": \"admin\", \"scripts\": { \"dev\": \"vite\" }, \"dependencies\": { \"react\": \"18.0.0\" } }\n",
    ),
    (
        "admin/src/api.ts",
        "export async function stock() {\n  return fetch(\"/stock\");\n}\n",
    ),
    (
        "orders/pyproject.toml",
        "[project]\nname = \"orders\"\ndependencies = [\"fastapi\", \"requests\"]\n",
    ),
    (
        "orders/orders/main.py",
        "import requests\nfrom fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.post(\"/orders\")\ndef place():\n    return requests.get(\"/stock\")\n",
    ),
    (
        "stock/pyproject.toml",
        "[project]\nname = \"stock\"\ndependencies = [\"fastapi\"]\n",
    ),
    (
        "stock/stock/main.py",
        "from fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/stock\")\ndef level():\n    return 1\n",
    ),
    (
        "mailer/pyproject.toml",
        "[project]\nname = \"mailer\"\ndependencies = [\"fastapi\"]\n",
    ),
    (
        "mailer/mailer/main.py",
        "from fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.post(\"/mail\")\ndef send():\n    return 1\n",
    ),
    (
        "sync/pyproject.toml",
        "[project]\nname = \"sync\"\n\n[project.scripts]\nsync = \"sync.main:run\"\n",
    ),
    ("sync/sync/main.py", "def run():\n    return 1\n"),
    (
        "audit/pyproject.toml",
        "[project]\nname = \"audit\"\n\n[project.scripts]\naudit = \"audit.main:run\"\n",
    ),
    ("audit/audit/main.py", "def run():\n    return 1\n"),
];

#[test]
fn a_few_lanes_of_a_few_cards_are_drawn_as_bands_at_full_scale() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(SHOP, "lidx-viewer-gbands-");
    let r = run(&p, "glance-curves", "");
    let cards = r["landing"]["cards"].as_array().unwrap();
    let row = |name: &str| {
        cards
            .iter()
            .find(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("{name}: {r}"))["y"]
            .as_f64()
            .unwrap()
    };
    // One band per lane: the services side by side, the workers too.
    assert_eq!(row("orders"), row("stock"), "{r}");
    assert_eq!(row("stock"), row("mailer"), "{r}");
    assert_eq!(row("sync"), row("audit"), "{r}");
    assert!(
        row("web") < row("orders") && row("orders") < row("sync"),
        "{r}"
    );
    assert_eq!(r["landing"]["cam"]["k"], 1.35, "{r}");
    assert_eq!(
        unclear_curves(&r["landing"]["cards"], &r["landing"]["curves"]),
        Vec::<String>::new(),
        "{r}"
    );
}

/// The resources lane: compact resource cards under the apps, lines from
/// the apps to them clear of every card, the two apps that meet at the
/// broker drawn through it, on the landing page and in every flow.
#[test]
fn the_resources_lane_renders_and_lines_go_through_the_broker() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::STACK, "lidx-viewer-gres-");
    let r = run(&p, "glance-landing", "");
    assert_eq!(
        field_list(&r["lanes"], "label"),
        vec![
            "Front ends",
            "Services",
            "Workers and tools",
            "Data and messaging",
            "External systems",
            "Infrastructure"
        ]
    );
    let lane_of = |n: &str| {
        r["cards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == n)
            .unwrap_or_else(|| panic!("{n}: {r}"))["lane"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(lane_of("RabbitMQ"), "data");
    assert_eq!(lane_of("api.payhub.io"), "ext");
    let pairs: Vec<(String, String)> = r["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| {
            (
                l["from"].as_str().unwrap().to_string(),
                l["to"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert!(
        pairs.contains(&("orders".into(), "RabbitMQ".into())),
        "{pairs:?}"
    );
    assert!(
        pairs.contains(&("worker".into(), "RabbitMQ".into())),
        "{pairs:?}"
    );
    assert!(
        !pairs.contains(&("orders".into(), "worker".into())),
        "the direct bus line waits for a focus: {pairs:?}"
    );
    let html = r["html"]["cards"].as_str().unwrap();
    assert!(html.contains("class=\"g-card g-res g-r-bus\""), "{html}");
    // A broker counts the topics its apps publish and subscribe to.
    assert!(html.contains("<p>1 topic · used by 2 apps</p>"), "{html}");
    let c = run(&p, "glance-curves", "");
    assert_eq!(
        unclear_curves(&c["landing"]["cards"], &c["landing"]["curves"]),
        Vec::<String>::new(),
        "{c}"
    );
    for f in c["flows"].as_array().unwrap() {
        assert_eq!(
            unclear_curves(&f["cards"], &f["curves"]),
            Vec::<String>::new(),
            "{f}"
        );
    }
    // The flow through the broker tells each hop.
    let fl = run(&p, "glance-flow", "0");
    let side = fl["side"].as_str().unwrap();
    assert!(
        side.contains(
            "2order-createdRabbitMQ · publishes to3on_createdworker · receives the message"
        ),
        "{side}"
    );
    // A resource's card: who uses it and where it is declared.
    let s = run(&p, "glance-select", "RabbitMQ");
    let side = s["card"]["side"].as_str().unwrap();
    assert!(side.contains("RabbitMQ messaging"), "{side}");
    // Each topic from its publishers to its subscribers, then each side.
    assert!(
        side.contains("Topics 1orders publishes order-created → worker"),
        "{side}"
    );
    assert!(
        side.contains(
            "Publishersorders publishes order-createdSubscribersworker subscribes to order-created"
        ),
        "{side}"
    );
    assert!(!side.contains("Used by"), "{side}");
    assert!(
        side.contains("Declared indocker-compose.yml:18 mq:"),
        "{side}"
    );
}

/// Resources past the cap wait behind a "+N more" in their lane and come
/// back with "show all".
#[test]
fn resources_past_the_cap_show_as_more_in_their_lane() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
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
    let (_tmp, p) = glance_page(&files, "lidx-viewer-gcap-");
    let r = run(&p, "glance-showall", "");
    let more = |lanes: &Value| {
        lanes
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["id"] == "data")
            .unwrap()["more"]
            .as_u64()
            .unwrap()
    };
    assert_eq!(more(&r["before"]["lanes"]), 3, "{r}");
    assert_eq!(more(&r["after"]["lanes"]), 0, "{r}");
    assert_eq!(
        r["after"]["cards"].as_u64().unwrap(),
        r["before"]["cards"].as_u64().unwrap() + 3
    );
    let html = run(&p, "glance-landing", "")["html"]["cards"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        html.contains("data-gall=\"1\" title=\"Show every resource\">+3 more</button>"),
        "{html}"
    );
}

/// "Where it runs" regroups the apps by the deployment that runs them,
/// frames each group, lists them in the side column, and goes back.
#[test]
fn the_deployment_overlay_groups_apps_by_where_they_run() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::STACK, "lidx-viewer-gdep-");
    let r = run(&p, "glance-deploy", "");
    assert!(
        r["before"]["head"]
            .as_str()
            .unwrap()
            .contains("data-gdeploy=\"1\" aria-pressed=\"false\""),
        "{r}"
    );
    assert_eq!(r["on"]["state"]["deploy"], true);
    assert_eq!(
        field_list(&r["on"]["lanes"], "label"),
        vec![
            "Docker Compose (docker-compose.yml)",
            "Not deployed by files in this repository",
            "Data and messaging",
            "External systems",
            "Infrastructure"
        ]
    );
    let html = r["on"]["html"].as_str().unwrap();
    assert_eq!(html.matches("class=\"g-group\"").count(), 2, "{html}");
    let side = r["on"]["side"].as_str().unwrap();
    assert!(
        side.starts_with("Where it runsThe apps grouped by the files that deploy them."),
        "{side}"
    );
    assert!(
        side.contains("Docker Compose (docker-compose.yml)Runs as 2 services in Docker Compose (docker-compose.yml).orders, worker"),
        "{side}"
    );
    assert_eq!(
        unclear_curves(
            &Value::Array(r["on"]["cards"].as_array().unwrap().clone()),
            &r["on"]["curves"]
        ),
        Vec::<String>::new(),
        "{r}"
    );
    assert_eq!(r["off"]["state"]["deploy"], false);
    assert_eq!(field_list(&r["off"]["lanes"], "label")[0], "Front ends");
    // One unit's modules: nothing deploys, no toggle.
    let (_tmp2, p2) = glance_page(common::glance_fixtures::CLI, "lidx-viewer-gdep2-");
    let head = run(&p2, "glance-landing", "")["html"]["head"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(!head.contains("data-gdeploy"), "{head}");
}

/// Resource names from infrastructure files and config reach the page as
/// text: a compose service, a Bicep resource, a topic and an external host.
#[test]
fn crafted_resource_names_render_as_text() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let files: &[(&str, &str)] = &[
        (
            "docker-compose.yml",
            "services:\n  app:\n    build: ./app\n    depends_on:\n      - \"<img src=x onerror=alert(1)>\"\n  \"<img src=x onerror=alert(1)>\":\n    image: postgres:16\n",
        ),
        (
            "app/package.json",
            "{ \"name\": \"app\", \"scripts\": { \"start\": \"node index.js\" } }\n",
        ),
        (
            "app/index.js",
            "function main() {\n  _bus.publish(\"</script><script>alert(2)</script>\", {});\n  return fetch(\"https://x\\\"><svg/onload=alert(3)>.evil.io/a\");\n}\nmain();\n",
        ),
        (
            "infra/main.bicep",
            "resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {\n  name: '<b onmouseover=alert(4)>vault</b>'\n}\nresource t 'Microsoft.ServiceBus/namespaces/topics@2022-10-01' = {\n  name: '<i>topic</i>'\n}\n",
        ),
    ];
    let (_tmp, p) = glance_page(files, "lidx-viewer-gesc3-");
    let r = run(&p, "glance-everything", "");
    let html = r["html"].as_str().unwrap();
    for bad in ["<img", "<script", "<svg", "<b ", "<i>"] {
        assert!(!html.contains(bad), "{bad} reached the page as markup");
    }
    assert!(
        html.contains("&lt;img src=x onerror=alert(1)&gt;"),
        "the compose service is shown, escaped"
    );
    assert!(
        html.contains("&lt;i&gt;topic&lt;/i&gt;"),
        "the Bicep topic is shown, escaped"
    );
    // And the embedded document cannot close its script element.
    let page = std::fs::read_to_string(&p).unwrap();
    let start = page.find("id=\"lidx-graph-data\"").unwrap();
    let end = start + page[start..].find("</script>").unwrap();
    assert!(
        !page[start..end].contains("<script"),
        "script context escaped"
    );
}

/// A line to a resource below a full row of cards, with a card beside its
/// target, has no clear curve: it runs down a gap (or the gutter) and in
/// along the gap above the target's row.
#[test]
fn a_line_with_no_clear_curve_runs_down_a_gap() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gvia-");
    let card = |x: f64, y: f64| serde_json::json!({ "x": x, "y": y, "w": 196.0, "h": 98.0 });
    let res = |x: f64, y: f64| serde_json::json!({ "x": x, "y": y, "w": 184.0, "h": 60.0 });
    let layout = serde_json::json!({
        "boxes": {
            "A": card(80.0, 30.0),
            "C1": card(80.0, 170.0), "C2": card(286.0, 170.0), "C3": card(492.0, 170.0),
            "R": res(80.0, 330.0), "B": res(296.0, 330.0),
        },
        "size": { "w": 800.0, "h": 420.0 }, "from": "A", "to": "B"
    });
    let r = run(&p, "glance-route", &layout.to_string());
    assert!(!r["via"].is_null(), "a route down a gap: {r}");
    let cards: Vec<Value> = layout["boxes"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(n, v)| {
            let mut v = v.clone();
            v["name"] = Value::String(n.clone());
            v
        })
        .collect();
    let curves = serde_json::json!([{ "from": "A", "to": "B", "p": r["p"], "pp": r["pp"] }]);
    assert_eq!(
        unclear_curves(&Value::Array(cards), &curves),
        Vec::<String>::new(),
        "{r}"
    );
}

/// Cards (`{name, x, y, w, h}`) that overlap another, as "a/b" pairs.
fn overlapping(cards: &Value) -> Vec<String> {
    let c = cards.as_array().unwrap();
    let f = |v: &Value, k: &str| v[k].as_f64().unwrap();
    let mut bad = Vec::new();
    for (i, a) in c.iter().enumerate() {
        for b in &c[i + 1..] {
            if f(a, "x") < f(b, "x") + f(b, "w")
                && f(b, "x") < f(a, "x") + f(a, "w")
                && f(a, "y") < f(b, "y") + f(b, "h")
                && f(b, "y") < f(a, "y") + f(a, "h")
            {
                bad.push(format!("{}/{}", a["name"], b["name"]));
            }
        }
    }
    bad
}

/// A resource three apps use is a coloured tag on each of their cards, and
/// so are the resources of an app with more than four of its own: their
/// lines wait for the pointer (or the selection) on one end. A resource
/// with one or two users of a quieter app keeps its line.
#[test]
fn shared_and_hub_resources_are_tags_whose_lines_wait_for_a_focus() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::SHARED, "lidx-viewer-gtag-");
    let r = run(&p, "glance-hover", "a,RabbitMQ,,hub,");
    let steps = r["steps"].as_array().unwrap();
    let links = |i: usize| -> Vec<String> {
        let mut v: Vec<String> = steps[i]["links"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l.as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };
    // At rest: only the line a quiet app keeps to a resource of its own.
    assert_eq!(links(0), vec!["a>Redis"], "{r}");
    // On a: its line to the shared broker too, not the other users'.
    assert_eq!(links(1), vec!["a>RabbitMQ", "a>Redis"], "{r}");
    // On the broker: every user's line.
    assert_eq!(
        links(2),
        vec!["a>RabbitMQ", "a>Redis", "b>RabbitMQ", "c>RabbitMQ"],
        "{r}"
    );
    // Off every card: back to rest.
    assert_eq!(links(3), vec!["a>Redis"], "{r}");
    // On the hub: its five resources.
    assert_eq!(
        links(4),
        vec![
            "a>Redis",
            "hub>Amazon S3",
            "hub>Azure Key Vault",
            "hub>Elasticsearch",
            "hub>PostgreSQL",
            "hub>Stripe"
        ],
        "{r}"
    );
    assert_eq!(links(5), vec!["a>Redis"], "{r}");
    // Lines drawn for a focus stay clear of the cards too.
    for s in steps {
        assert_eq!(
            unclear_curves(&s["cards"], &s["curves"]),
            Vec::<String>::new(),
            "{s}"
        );
    }
    // The tags: each in its resource's colour, the broker on all three
    // users, and the hub's (with what does not fit as "+N").
    let html = r["html"].as_str().unwrap();
    let tags = |name: &str| -> Vec<String> {
        let start = html
            .find(&format!("<h3>{name}</h3>"))
            .unwrap_or_else(|| panic!("{name}: {html}"));
        let card = &html[start
            ..start
                + html[start..]
                    .find("<div class=\"g-card")
                    .unwrap_or(html.len() - start)];
        let mut out = Vec::new();
        let mut rest = card;
        while let Some(i) = rest.find("<span class=\"g-badge ") {
            rest = &rest[i + 21..];
            let cls = &rest[..rest.find('"').unwrap()];
            let name = &rest[rest.find('>').unwrap() + 1..rest.find("</span>").unwrap()];
            out.push(format!("{cls}:{name}"));
        }
        if let Some(i) = card.find("<span class=\"g-badge-more\">") {
            let more = &card[i + 27..];
            out.push(more[..more.find("</span>").unwrap()].to_string());
        }
        out
    };
    assert_eq!(tags("a"), vec!["g-r-bus:RabbitMQ"]);
    assert_eq!(tags("b"), vec!["g-r-bus:RabbitMQ"]);
    assert_eq!(tags("c"), vec!["g-r-bus:RabbitMQ"]);
    // The hub's: as many as fit two lines, then how many more.
    assert_eq!(
        tags("hub"),
        vec![
            "g-r-secrets:Azure Key Vault",
            "g-r-search:Elasticsearch",
            "+3"
        ]
    );
    assert!(
        html.contains("title=\"Uses Azure Key Vault, Elasticsearch, PostgreSQL, Amazon S3, Stripe. Point at the card to draw these lines.\""),
        "{html}"
    );
    // The lane says what the tags are, once.
    assert_eq!(
        html.matches("Coloured tags on a card stand for its lines here")
            .count(),
        1,
        "{html}"
    );
}

/// Four front ends each calling one of eight services, in the reverse of
/// their name order. Every layout puts the callers in the order of what
/// they call, so no two lines cross. Where the map fits, it is drawn larger
/// than 1:1; on an area too short for it at 1:1 (card text at its own 11px
/// and up), it scrolls instead of shrinking: as wide as the area, its
/// lanes in rows of the area's width, each caller over its service.
#[test]
fn a_big_map_stays_readable_and_its_lines_do_not_cross() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let mut owned: Vec<(String, String)> = Vec::new();
    for i in 0..8 {
        owned.push((
            format!("s{i:02}/pyproject.toml"),
            format!("[project]\nname = \"s{i:02}\"\ndependencies = [\"fastapi\"]\n"),
        ));
        owned.push((
            format!("s{i:02}/s{i:02}/main.py"),
            format!("from fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/s{i:02}\")\ndef get():\n    return 1\n"),
        ));
    }
    for (f, s) in [(0, 7), (1, 6), (2, 5), (3, 4)] {
        owned.push((
            format!("f{f}/package.json"),
            format!("{{ \"name\": \"f{f}\", \"scripts\": {{ \"dev\": \"vite\" }}, \"dependencies\": {{ \"react\": \"18.0.0\" }} }}\n"),
        ));
        owned.push((
            format!("f{f}/src/api.ts"),
            format!("export async function go() {{\n  return fetch(\"/s{s:02}\");\n}}\n"),
        ));
    }
    let files: Vec<(&str, &str)> = owned
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (_tmp, p) = glance_page(&files, "lidx-viewer-gbig-");
    let calls = [("f0", "s07"), ("f1", "s06"), ("f2", "s05"), ("f3", "s04")];
    let card = |r: &Value, n: &str| {
        r["cards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == n)
            .unwrap_or_else(|| panic!("{n}: {r}"))
            .clone()
    };
    let mid = |c: &Value, x: &str, w: &str| c[x].as_f64().unwrap() + c[w].as_f64().unwrap() / 2.0;
    // The map's area in a 1400×900 window, beside the side column: left
    // to right, larger than 1:1.
    let r = run(&p, "glance-area", "1040,650");
    assert_eq!(r["cam"]["dir"], "h", "{r}");
    assert!(r["cam"]["k"].as_f64().unwrap() > 1.0, "{r}");
    // Callers top to bottom in the order of what they call: no crossing.
    let order = |names: Vec<&str>| {
        let mut v: Vec<(f64, usize)> = names
            .iter()
            .enumerate()
            .map(|(i, n)| (mid(&card(&r, n), "y", "h"), i))
            .collect();
        v.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        v.into_iter().map(|(_, i)| i).collect::<Vec<_>>()
    };
    assert_eq!(
        order(calls.iter().map(|c| c.0).collect()),
        order(calls.iter().map(|c| c.1).collect()),
        "{r}"
    );
    assert_eq!(overlapping(&r["cards"]), Vec::<String>::new());
    assert_eq!(
        unclear_curves(&r["cards"], &r["curves"]),
        Vec::<String>::new(),
        "{r}"
    );
    // An area too short for it: 1:1, scrolling.
    let r = run(&p, "glance-area", "1040,420");
    let cam = &r["cam"];
    assert_eq!(cam["k"].as_f64().unwrap(), 1.0, "{r}");
    let w = cam["w"].as_f64().unwrap();
    assert!(w <= 1040.0 && w > 900.0, "as wide as the area: {w}");
    assert!(cam["h"].as_f64().unwrap() > 420.0, "taller: it scrolls");
    assert_eq!(overlapping(&r["cards"]), Vec::<String>::new());
    assert_eq!(
        unclear_curves(&r["cards"], &r["curves"]),
        Vec::<String>::new(),
        "{r}"
    );
    for (f, s) in calls {
        let (a, b) = (card(&r, f), card(&r, s));
        assert!(
            (mid(&a, "x", "w") - mid(&b, "x", "w")).abs() < 1.0,
            "{f} over {s}: {r}"
        );
    }
    // Eight services in rows of four.
    let rows: std::collections::BTreeSet<i64> = r["cards"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["lane"] == "svc")
        .map(|c| c["y"].as_f64().unwrap() as i64)
        .collect();
    assert_eq!(rows.len(), 2, "{r}");
}

/// Names from deployment files and URLs reach the landing page as text in
/// every view: a Kubernetes namespace and a compose file's folder in the
/// "Where it runs" groups, compose service names in a shared resource
/// (shown as tags), with the pointer on each card. A URL whose host is not
/// a host name makes no resource at all.
#[test]
fn crafted_deployment_and_resource_names_render_as_text() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let mut owned: Vec<(String, String)> = vec![(
        "ops\"&'<x>/docker-compose.yml".to_string(),
        "services:\n  c:\n    build: ../c\n    depends_on:\n      - \"<i>pg</i>\"\n  \"<i>pg</i>\":\n    image: postgres:16\n".to_string(),
    )];
    for x in ["a", "b", "c"] {
        owned.push((
            format!("{x}/pyproject.toml"),
            format!("[project]\nname = \"{x}\"\ndependencies = [\"psycopg\", \"requests\"]\n\n[project.scripts]\n{x} = \"{x}.main:run\"\n"),
        ));
        owned.push((
            format!("{x}/{x}/main.py"),
            "import psycopg\nimport requests\n\n\ndef run():\n    return requests.get('https://q\"onmouseover=alert(6)&x.evil.io/a')\n".to_string(),
        ));
        owned.push((format!("{x}/Dockerfile"), "FROM python:3\n".to_string()));
    }
    for x in ["a", "b"] {
        owned.push((
            format!("deploy/{x}.yaml"),
            format!("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {x}\n  namespace: \"<u onmouseover=alert(5)>ns</u>\"\nspec:\n  template:\n    spec:\n      containers:\n        - name: {x}\n          image: registry.example.com/{x}:1.0\n"),
        ));
    }
    let files: Vec<(&str, &str)> = owned
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (_tmp, p) = glance_page(&files, "lidx-viewer-gesc4-");
    let r = run(&p, "glance-everything", "");
    let html = r["html"].as_str().unwrap();
    for bad in ["<u on", "<i>", "<x>", "q\"onmouseover", "ops\"", "&x.evil"] {
        assert!(!html.contains(bad), "{bad} reached the page as markup");
    }
    assert!(
        html.contains("Kubernetes namespace &lt;u onmouseover=alert(5)&gt;ns&lt;/u&gt;"),
        "the namespace is shown, escaped"
    );
    assert!(
        html.contains("Docker Compose (ops&quot;&amp;&#39;&lt;x&gt;/docker-compose.yml)"),
        "the compose file's folder is shown, escaped"
    );
    assert!(
        html.contains("&lt;i&gt;pg&lt;/i&gt;"),
        "the compose service is shown, escaped"
    );
    assert!(
        html.contains("<span class=\"g-badge g-r-db\" data-gb="),
        "the shared database is a tag"
    );
    assert!(
        !html.contains("evil.io"),
        "no resource for a host that is not one"
    );
    let page = std::fs::read_to_string(&p).unwrap();
    let start = page.find("id=\"lidx-graph-data\"").unwrap();
    let end = start + page[start..].find("</script>").unwrap();
    assert!(!page[start..end].contains("<u"), "script context escaped");
}

/// A service mesh: load → gateway, which calls orders and recs and four
/// of the services orders calls too; orders calls six; recs one. Ten
/// services in one lane.
fn mesh_page(prefix: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let calls: &[(&str, &[&str])] = &[
        ("loadgen", &["gateway"]),
        (
            "gateway",
            &["orders", "recs", "cart", "catalog", "shipping", "currency"],
        ),
        (
            "orders",
            &[
                "cart", "catalog", "shipping", "currency", "email", "payment",
            ],
        ),
        ("recs", &["catalog"]),
        ("cart", &[]),
        ("catalog", &[]),
        ("shipping", &[]),
        ("currency", &[]),
        ("email", &[]),
        ("payment", &[]),
    ];
    let mut owned: Vec<(String, String)> = Vec::new();
    for (svc, outs) in calls {
        owned.push((
            format!("{svc}/pyproject.toml"),
            format!("[project]\nname = \"{svc}\"\ndependencies = [\"fastapi\", \"requests\"]\n"),
        ));
        let mut body = format!(
            "import requests\nfrom fastapi import FastAPI\n\napp = FastAPI()\n\n\n@app.get(\"/{svc}\")\ndef handle():\n"
        );
        for o in *outs {
            body.push_str(&format!("    requests.get(\"/{o}\")\n"));
        }
        body.push_str("    return 1\n");
        owned.push((format!("{svc}/{svc}/main.py"), body));
    }
    let files: Vec<(&str, &str)> = owned
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    glance_page(&files, prefix)
}

fn link_names(step: &Value) -> Vec<String> {
    let mut v: Vec<String> = step["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| {
            format!(
                "{}>{}",
                l["from"].as_str().unwrap(),
                l["to"].as_str().unwrap()
            )
        })
        .collect();
    v.sort();
    v
}

/// The points of a route's curves.
fn seg_points(seg: &Value) -> Vec<(f64, f64)> {
    (0..4)
        .map(|i| (seg[i][0].as_f64().unwrap(), seg[i][1].as_f64().unwrap()))
        .collect()
}

/// The straight runs of a route that go across the map, as (y, x0, x1).
fn across_runs(curve: &Value) -> Vec<(f64, f64, f64)> {
    let mut out = Vec::new();
    for seg in curve["pp"].as_array().unwrap() {
        let p = seg_points(seg);
        if p.iter().all(|q| (q.1 - p[0].1).abs() < 0.5) && (p[3].0 - p[0].0).abs() > 1.0 {
            out.push((p[0].1, p[0].0.min(p[3].0), p[0].0.max(p[3].0)));
        }
    }
    out
}

/// Whether every piece of a route runs straight down, straight across, or
/// is a rounded corner between the two.
fn is_square(curve: &Value) -> bool {
    curve["pp"].as_array().unwrap().iter().all(|seg| {
        let p = seg_points(seg);
        let down = p.iter().all(|q| (q.0 - p[0].0).abs() < 0.5);
        let across = p.iter().all(|q| (q.1 - p[0].1).abs() < 0.5);
        let corner = (p[3].0 - p[0].0).abs() <= 8.5 && (p[3].1 - p[0].1).abs() <= 8.5;
        down || across || corner
    })
}

/// A dense mesh reads as a call hierarchy in a 1400×900 window: callers
/// over callees, one row per step of the longest call chain, a layer too
/// wide for its row lifting its overflow beside its caller, so the whole
/// map fits without scrolling. A call a longer chain already shows
/// (gateway → cart, as gateway → orders → cart) is a tag on the caller,
/// its line drawn with the pointer on it. Every line between rows runs
/// square, and two callers' tracks never run on top of each other; no
/// line crosses a card. A caller's lines with one verb share one label,
/// with no "×1"; no label sits on a card or another.
#[test]
fn a_service_mesh_is_drawn_as_a_call_hierarchy() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = mesh_page("lidx-viewer-gmesh-");
    let r = run(&p, "glance-area-hover", "1040,717,gateway,");
    let steps = r["steps"].as_array().unwrap();
    let (rest, hover, off) = (&steps[0], &steps[1], &steps[2]);
    // It fits at the readable scale: no scrolling.
    let cam = &rest["cam"];
    assert_eq!(cam["k"].as_f64().unwrap(), 1.0, "{cam}");
    assert!(cam["h"].as_f64().unwrap() <= 717.0, "fits: {cam}");
    let f = |c: &Value, k: &str| c[k].as_f64().unwrap();
    let card = |n: &str| {
        rest["cards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == n)
            .unwrap_or_else(|| panic!("{n}: {rest}"))
            .clone()
    };
    let mut ys: Vec<i64> = rest["cards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| f(c, "y") as i64)
        .collect();
    ys.sort();
    ys.dedup();
    let row = |n: &str| {
        ys.iter()
            .position(|&y| y == f(&card(n), "y") as i64)
            .unwrap()
    };
    // One row per step of the chain: load, gateway, orders and recs, the rest.
    assert_eq!(ys.len(), 4, "{rest}");
    assert_eq!(
        [
            "loadgen", "gateway", "orders", "recs", "catalog", "email", "payment", "shipping"
        ]
        .map(row),
        [0, 1, 2, 2, 3, 3, 3, 3],
        "{rest}"
    );
    // Two of orders' six callees lifted into its row, one either side.
    assert_eq!([row("cart"), row("currency")], [2, 2], "{rest}");
    let step = f(&card("orders"), "w") + 40.0;
    let side = |n: &str| ((f(&card(n), "x") - f(&card("orders"), "x")) / step).round() as i64;
    let mut sides = vec![side("cart"), side("currency")];
    sides.sort();
    assert_eq!(sides, vec![-1, 1], "{rest}");
    // At rest: the calls down the hierarchy, not the four it implies.
    assert_eq!(
        link_names(rest),
        vec![
            "gateway>orders",
            "gateway>recs",
            "loadgen>gateway",
            "orders>cart",
            "orders>catalog",
            "orders>currency",
            "orders>email",
            "orders>payment",
            "orders>shipping",
            "recs>catalog"
        ],
        "{rest}"
    );
    // Each drawn line steps one row down, or along a row to a neighbour.
    for l in rest["links"].as_array().unwrap() {
        let (a, b) = (l["from"].as_str().unwrap(), l["to"].as_str().unwrap());
        match row(b) as i64 - row(a) as i64 {
            1 => {}
            0 => assert!(
                (f(&card(a), "x") - f(&card(b), "x")).abs() < step + 1.0,
                "{a}>{b} along its row skips a card: {rest}"
            ),
            d => panic!("{a}>{b} spans {d} rows: {rest}"),
        }
    }
    // The implied calls: tags on the gateway's card (as many as fit, then
    // how many more), named in full in their title; their lines on hover.
    let html = rest["html"].as_str().unwrap();
    let start = html.find("<h3>gateway</h3>").unwrap();
    let gw = &html[start
        ..start
            + html[start..]
                .find("<div class=\"g-card")
                .unwrap_or(html.len() - start)];
    let tags = gw.matches("<span class=\"g-badge g-r-app\"").count();
    let pat = "<span class=\"g-badge-more\">+";
    let more = gw
        .find(pat)
        .map(|i| {
            let s = &gw[i + pat.len()..];
            s[..s.find('<').unwrap()].parse::<usize>().unwrap()
        })
        .unwrap_or(0);
    assert!(tags >= 1 && tags + more == 4, "{gw}");
    assert!(
        gw.contains("Also calls cart, catalog, currency, shipping directly."),
        "{gw}"
    );
    let mut on = link_names(hover);
    on.retain(|l| !link_names(rest).contains(l));
    assert_eq!(
        on,
        vec![
            "gateway>cart",
            "gateway>catalog",
            "gateway>currency",
            "gateway>shipping"
        ],
        "{hover}"
    );
    assert_eq!(link_names(off), link_names(rest));
    for s in [rest, hover] {
        assert_eq!(
            unclear_curves(&s["cards"], &s["curves"]),
            Vec::<String>::new(),
            "{s}"
        );
        // Lines between rows run square; two callers' tracks never run on
        // top of each other.
        let mut runs: Vec<(String, (f64, f64, f64))> = Vec::new();
        for c in s["curves"].as_array().unwrap() {
            let (a, b) = (c["from"].as_str().unwrap(), c["to"].as_str().unwrap());
            if row(a) != row(b) {
                assert!(is_square(c), "{a}>{b} is not square: {c}");
            }
            for run in across_runs(c) {
                runs.push((a.to_string(), run));
            }
        }
        for (i, (a, x)) in runs.iter().enumerate() {
            for (b, y) in &runs[i + 1..] {
                assert!(
                    a == b || (x.0 - y.0).abs() >= 1.0 || x.2 <= y.1 + 2.0 || y.2 <= x.1 + 2.0,
                    "{a} and {b} share a track at {}: {s}",
                    x.0
                );
            }
        }
        // Labels: one per caller and verb, no count of one, clear.
        let shown: Vec<&Value> = s["links"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|l| l["shown"].as_bool().unwrap())
            .collect();
        let mut seen = std::collections::BTreeSet::new();
        for l in &shown {
            assert_eq!(l["text"], "calls over HTTP", "{l}");
            assert!(
                seen.insert(l["from"].as_str().unwrap()),
                "two labels for {l}: {s}"
            );
        }
        assert_eq!(
            seen.into_iter().collect::<Vec<_>>(),
            vec!["gateway", "loadgen", "orders", "recs"],
            "{s}"
        );
        let rect = |b: &Value| [f(b, "x"), f(b, "y"), f(b, "w"), f(b, "h")];
        let over = |a: [f64; 4], b: [f64; 4]| {
            a[0] < b[0] + b[2] && a[0] + a[2] > b[0] && a[1] < b[1] + b[3] && a[1] + a[3] > b[1]
        };
        for (i, l) in shown.iter().enumerate() {
            let b = rect(&l["box"]);
            for c in s["cards"].as_array().unwrap() {
                assert!(!over(b, rect(c)), "label {l} on card {c}");
            }
            for m in &shown[i + 1..] {
                assert!(!over(b, rect(&m["box"])), "labels {l} and {m} overlap");
            }
        }
    }
}

/// Reading order of the landing cards: lane by lane (top to bottom, then
/// left to right), row by row, left to right.
fn reading_order(cards: &Value) -> Vec<String> {
    let list = cards.as_array().unwrap();
    let f = |c: &Value, k: &str| c[k].as_f64().unwrap();
    let mut lane_at: Vec<(String, f64, f64)> = Vec::new();
    for c in list {
        let lane = c["lane"].as_str().unwrap().to_string();
        match lane_at.iter_mut().find(|l| l.0 == lane) {
            Some(l) => {
                if (f(c, "y"), f(c, "x")) < (l.1, l.2) {
                    l.1 = f(c, "y");
                    l.2 = f(c, "x");
                }
            }
            None => lane_at.push((lane, f(c, "y"), f(c, "x"))),
        }
    }
    lane_at.sort_by(|a, b| (a.1, a.2).partial_cmp(&(b.1, b.2)).unwrap());
    let rank = |c: &Value| {
        lane_at
            .iter()
            .position(|l| l.0 == c["lane"].as_str().unwrap())
            .unwrap()
    };
    let mut sorted: Vec<&Value> = list.iter().collect();
    sorted.sort_by(|a, b| {
        rank(a).cmp(&rank(b)).then_with(|| {
            if (f(a, "y") - f(b, "y")).abs() > 8.0 {
                f(a, "y").partial_cmp(&f(b, "y")).unwrap()
            } else {
                f(a, "x").partial_cmp(&f(b, "x")).unwrap()
            }
        })
    });
    sorted
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect()
}

/// A landing card is one button (selected = pressed), the Tab order is the
/// order the eye reads the map, every action says something meaningful in
/// the live region, and the legend names exactly the line styles and
/// resource kinds this map draws.
#[test]
fn the_landing_cards_are_buttons_in_reading_order_and_the_legend_keys_the_map() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-ga11y-");
    let r = run(&p, "glance-a11y", "validator");
    let html = r["order"].as_str().unwrap();
    assert!(
        !html.contains("role=\"listitem\"") && !html.contains("<article"),
        "{html}"
    );
    let cards = r["cards"].as_array().unwrap().len();
    assert!(cards >= 4, "{r}");
    assert_eq!(
        html.matches("role=\"button\" aria-pressed=\"false\"")
            .count(),
        cards,
        "{html}"
    );
    assert_eq!(html.matches("tabindex=\"0\"").count(), cards, "{html}");
    // Tab order = reading order.
    let in_dom: Vec<String> = html
        .split("<h3>")
        .skip(1)
        .map(|t| t[..t.find("</h3>").unwrap()].to_string())
        .collect();
    assert_eq!(in_dom, reading_order(&r["cards"]), "{r}");
    // Selecting presses the card, and says what it is.
    let sel = r["card"]["html"].as_str().unwrap();
    assert_eq!(sel.matches("aria-pressed=\"true\"").count(), 1, "{sel}");
    assert!(
        r["card"]["live"]
            .as_str()
            .unwrap()
            .starts_with("validator: Python worker"),
        "{}",
        r["card"]["live"]
    );
    let line = r["link"]["live"].as_str().unwrap();
    assert!(
        line.starts_with("Line from ")
            && line.contains(" to ")
            && line.contains(": ")
            && (line.ends_with(" call") || line.ends_with(" calls")),
        "{line}"
    );
    assert_ne!(line, "depth 1");
    assert!(
        r["question"]["live"]
            .as_str()
            .unwrap()
            .starts_with("Question: "),
        "{r}"
    );
    // The legend: closed first, then a flat key of this map's own styles.
    assert_eq!(r["closed"]["hidden"], true);
    assert_eq!(r["closed"]["key"], "");
    let head = r["closed"]["head"].as_str().unwrap();
    assert!(
        head.contains("data-gkey=\"1\" aria-expanded=\"false\" aria-controls=\"g-key\""),
        "{head}"
    );
    assert_eq!(r["open"]["hidden"], false);
    assert_eq!(r["open"]["state"]["key"], true);
    assert!(
        r["open"]["head"]
            .as_str()
            .unwrap()
            .contains("aria-expanded=\"true\"")
    );
    let key = r["open"]["key"].as_str().unwrap();
    // Only the styles this map draws, each with its own dash class and a word.
    for (class, word) in [
        ("f-http", "HTTP request"),
        ("f-bus", "to a broker, arrow the way messages go"),
        ("f-config", "reads config"),
    ] {
        assert!(
            key.contains(&format!("<g class=\"g-ln {class}\" style=\"--w:2\"><path class=\"ln\" d=\"M1 5H37\"/></g></svg>{word}</li>")),
            "{class}: {key}"
        );
    }
    for absent in [
        "RPC call",
        "calls code",
        "reads secrets",
        "reads or writes data",
        "outside system",
    ] {
        assert!(!key.contains(absent), "{absent}: {key}");
    }
    assert!(
        key.contains(
            "<span class=\"g-sw-res g-r-bus\" aria-hidden=\"true\"></span>message bus</li>"
        ),
        "{key}"
    );
    assert_eq!(key.matches("g-sw-res").count(), 1, "{key}");
    assert!(key.contains("<span class=\"g-sw-frame\""), "{key}");
    assert_eq!(r["shut"]["hidden"], true);
    assert_eq!(r["shut"]["key"], "");
}

/// On the Explore sheet a closed card is one button (no button nested in
/// it), an open card is a group of its own controls, and the Tab order is
/// the sheet's reading order, not the order the units were found in.
#[test]
fn explore_cards_are_buttons_and_tab_in_sheet_order() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo(
        "lidx-viewer-aa11y-",
        &[
            (
                "web/Web.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n  <ItemGroup>\n    <ProjectReference Include=\"..\\core\\Core.csproj\" />\n  </ItemGroup>\n</Project>\n",
            ),
            (
                "web/Program.cs",
                "using Core;\n\nnamespace Web\n{\n    public class Api\n    {\n        public int Get() { return new Repo().Load(); }\n    }\n}\n",
            ),
            ("web/Dockerfile", "FROM scratch\n"),
            (
                "core/Core.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n",
            ),
            (
                "core/Repo.cs",
                "namespace Core\n{\n    public class Repo\n    {\n        public int Load() { return 1; }\n    }\n}\n",
            ),
            (
                "ui/package.json",
                "{ \"name\": \"ui\", \"scripts\": { \"dev\": \"vite\" }, \"dependencies\": { \"react\": \"18.0.0\" } }\n",
            ),
            (
                "ui/src/app.ts",
                "export function boot() {\n  return 1;\n}\n",
            ),
        ],
    );
    let p = page(&root, &db, &[]);
    let r = run(&p, "apps-a11y", "Web");
    let closed = r["closed"].as_str().unwrap();
    let n = r["apps"]["cards"].as_array().unwrap().len();
    assert!(n >= 3, "{r}");
    assert!(
        !closed.contains("<article") && !closed.contains("role=\"listitem\""),
        "{closed}"
    );
    assert_eq!(
        closed
            .matches("role=\"button\" aria-expanded=\"false\" aria-pressed=\"false\"")
            .count(),
        n,
        "{closed}"
    );
    assert!(
        !closed.contains("<button"),
        "no button nested in a button: {closed}"
    );
    if closed.contains("zexp") {
        assert!(
            closed.contains("<span class=\"zexp\"") && closed.contains("aria-hidden=\"true\""),
            "{closed}"
        );
    }
    // Reading order: row by row (top to bottom), left to right.
    let mut cards: Vec<(String, f64, f64)> = r["apps"]["cards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap().to_string(),
                c["y"].as_f64().unwrap(),
                c["x"].as_f64().unwrap(),
            )
        })
        .collect();
    cards.sort_by(|a, b| {
        if (a.1 - b.1).abs() > 8.0 {
            a.1.partial_cmp(&b.1).unwrap()
        } else {
            a.2.partial_cmp(&b.2).unwrap()
        }
    });
    let expect: Vec<String> = cards.into_iter().map(|c| c.0).collect();
    assert_eq!(field_strings(&r["apps"]["tabOrder"]), expect, "{r}");
    // Open, the card holds buttons of its own: a group, not a button.
    let open = r["open"].as_str().unwrap();
    assert!(open.contains("role=\"group\""), "{open}");
    assert_eq!(
        open.matches("role=\"button\"").count(),
        n - 1,
        "the open card is no longer a button: {open}"
    );
}

fn field_strings(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect()
}

/// Explore counts and bands units the way the overview does: the same apps
/// and tools, a band for what the overview leaves out (each saying why),
/// infrastructure folded into one card per tool until it is shown.
#[test]
fn explore_counts_and_bands_follow_the_overview() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let files = common::glance_fixtures::explore();
    let (_tmp, p) = glance_page(&files, "lidx-viewer-exsheet-");
    let r = run(&p, "explore-sheet", "Kubernetes manifests");
    let meta = r["first"]["meta"].as_str().unwrap();
    assert!(
        meta.starts_with(
            "Explore · 3 apps · Kubernetes manifests (2 folders) · +1 not on the overview: ads (Java, not indexed) · 1 test, docs or tooling folder hidden · "
        ),
        "{meta}"
    );
    let band = |sheet: &Value, name: &str| -> Option<String> {
        sheet["cards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .map(|c| c["band"].as_str().unwrap().to_string())
    };
    let first = &r["first"]["sheet"];
    assert_eq!(band(first, "ads").as_deref(), Some("off"), "{first}");
    assert_eq!(
        band(first, "Kubernetes manifests").as_deref(),
        Some("infra")
    );
    assert_eq!(
        band(first, "deploy/k8s"),
        None,
        "folded into its tool: {first}"
    );
    assert_eq!(band(first, "deploy/overlays"), None);
    assert_eq!(band(first, "validator").as_deref(), Some("svc"));
    // A folded tool's folders link as its card: its deployment of the
    // validator starts at the tool, no line runs from a hidden folder.
    let links = first["links"].as_array().unwrap();
    assert!(
        links
            .iter()
            .any(|l| l["from"] == "g0" && l["family"] == "deploys"),
        "{first}"
    );
    let k8s = first["cards"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["key"] != "u4");
    assert!(k8s, "{first}");
    assert!(
        links.iter().all(|l| first["cards"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["key"] == l["from"])),
        "every link starts at a drawn card: {first}"
    );
    assert_eq!(r["first"]["cam"]["k"], 1.0, "opens at 1:1: {r}");
    let open = &r["open"]["sheet"];
    assert_eq!(band(open, "Kubernetes manifests").as_deref(), Some("infra"));
    assert_eq!(band(open, "deploy/k8s").as_deref(), Some("infra"), "{open}");
    assert_eq!(band(open, "deploy/overlays").as_deref(), Some("infra"));
    let panel = r["open"]["panel"].as_str().unwrap();
    assert!(
        panel.starts_with("Kubernetes manifests infrastructure"),
        "{panel}"
    );
    assert!(
        panel.contains("Folders · 2") && panel.contains("deploy/overlays"),
        "{panel}"
    );
    assert_eq!(band(&r["folded"], "deploy/k8s"), None);
    // Going to a folder of a folded tool shows the tool's folders.
    let name = r["revealedName"].as_str().unwrap();
    assert_eq!(band(&r["revealed"], name).as_deref(), Some("infra"), "{r}");
}

/// With nothing chosen, Explore opens at 1:1 on the top left of a sheet too
/// big for the view (band titles in view); Fit still frames all of it.
#[test]
fn explore_opens_at_a_readable_scale() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-exhome-");
    // Wide enough for the rows, too short for the bands.
    let r = run(&p, "explore-home", "800,300");
    let full = r["sheet"]["w"].as_f64().unwrap() + 168.0;
    assert!(r["sheet"]["h"].as_f64().unwrap() > 260.0, "{r}");
    assert_eq!(r["home"]["k"], 1.0, "{r}");
    let x = ((800.0 - full) / 2.0).max(16.0) + 168.0;
    assert_eq!(r["home"]["x"].as_f64().unwrap(), x.round(), "{r}");
    assert_eq!(r["home"]["y"], 44.0, "room above for arcs: {r}");
    // A phone: the top left, close under the bar.
    let r = run(&p, "explore-home", "500,300");
    assert_eq!(r["home"]["k"], 1.0, "{r}");
    assert_eq!(r["home"]["x"], 16.0 + 168.0, "{r}");
    assert_eq!(r["home"]["y"], 12.0, "{r}");
    assert!(
        r["fit"]["k"].as_f64().unwrap() < 1.0,
        "Fit frames it all: {r}"
    );
    // A card double-clicked on the overview opens at 1:1 too.
    let r = run(&p, "glance-dblclick", "validator");
    assert_eq!(r["cam"]["k"], 1.0, "{r}");
    assert!(r["panel"].as_str().unwrap().starts_with("validator"), "{r}");
}

/// One click on a folder or file opens it and the panel follows it; the
/// second click of a double-click and the double-click itself do nothing
/// more; a click on a symbol selects it; Escape goes back up a level.
#[test]
fn one_click_opens_a_box_and_the_panel_follows() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-zclick-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-click", "shared-utils");
    let crumbs = |v: &Value| serde_json::to_string(v).unwrap();
    let starts = |v: &Value, with: &str| v.as_str().unwrap().starts_with(with);
    assert_eq!(crumbs(&r["top"]["crumbs"]), r#"["shared-utils"]"#, "{r}");
    assert_eq!(crumbs(&r["second"]), r#"["shared-utils"]"#, "{r}");
    assert_eq!(crumbs(&r["dbl"]), r#"["shared-utils"]"#, "{r}");
    assert_eq!(
        crumbs(&r["opened"]["crumbs"]),
        r#"["shared-utils","shared_utils/"]"#,
        "{r}"
    );
    assert!(starts(&r["opened"]["panel"], "shared_utils/ folder"), "{r}");
    assert_eq!(r["opened"]["sel"], Value::Null, "{r}");
    assert_eq!(
        crumbs(&r["fileOpened"]["crumbs"]),
        r#"["shared-utils","shared_utils/","fmt.py"]"#,
        "{r}"
    );
    assert!(starts(&r["fileOpened"]["panel"], "fmt.py file"), "{r}");
    assert_eq!(r["symSel"]["sel"]["type"], "sym", "{r}");
    let sym = r["symSel"]["sel"]["name"].as_str().unwrap();
    assert!(starts(&r["symSel"]["panel"], sym), "{r}");
    assert_eq!(
        crumbs(&r["esc"]["crumbs"]),
        r#"["shared-utils","shared_utils/"]"#,
        "{r}"
    );
    assert!(starts(&r["esc"]["panel"], "shared_utils/ folder"), "{r}");
}

/// Stubs are drawn neutral; only the one clicked is marked selected.
#[test]
fn only_the_clicked_stub_is_marked() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, root, db) = common::index_repo("lidx-viewer-zstubsel-", ZOOM_FILES);
    let p = page(&root, &db, &[]);
    let r = run(&p, "zoom-stub-sel", "shared-utils");
    let before = r["before"].as_str().unwrap();
    let after = r["after"].as_str().unwrap();
    assert!(before.contains("class=\"zstub conn"), "{before}");
    assert!(!before.contains(" sel\""), "{before}");
    assert_eq!(after.matches(" on sel\"").count(), 1, "{after}");
}

/// A sheet label with no free spot waits for a focus: those shown never
/// overlap.
#[test]
fn crowded_sheet_labels_wait_for_a_focus() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-crowd-");
    let one = run(&p, "labels-crowd", "1");
    assert_eq!(one[0]["crowded"], false, "{one}");
    let r = run(&p, "labels-crowd", "30");
    let all = r.as_array().unwrap();
    let shown: Vec<&Value> = all.iter().filter(|l| l["crowded"] == false).collect();
    assert!(shown.len() > 1 && shown.len() < all.len(), "{r}");
    let b = |v: &Value, i: usize| v["box"][i].as_f64().unwrap();
    for (i, x) in shown.iter().enumerate() {
        for y in &shown[i + 1..] {
            let apart = b(x, 2) <= b(y, 0)
                || b(y, 2) <= b(x, 0)
                || b(x, 3) <= b(y, 1)
                || b(y, 3) <= b(x, 1);
            assert!(apart, "{x} and {y} overlap");
        }
    }
}

/// Explore's stylesheet keeps the rules the critique found broken: the
/// status line under the sheet, no map text below 11px, stubs and lines
/// that touch the selection in ink, orange only for the one picked.
#[test]
fn explore_styles_keep_the_design_rules() {
    let css = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/graph_html/viewer.css"),
    )
    .unwrap();
    let rule = |sel: &str| -> String {
        let at = css
            .find(&format!("{sel} {{"))
            .unwrap_or_else(|| panic!("no rule {sel}"));
        css[at..at + css[at..].find('}').unwrap()].to_string()
    };
    assert!(rule(".stage.app-mode .apps").contains("bottom: 28px"));
    assert!(rule(".stage.app-mode .status").contains("height: 28px"));
    for sel in [".card .k", ".card dt", ".zi .zk", ".z-others .tw"] {
        assert!(
            rule(sel).contains("font-size: 11px"),
            "{sel}: {}",
            rule(sel)
        );
    }
    assert!(rule(".z-row .g, .vt-row .g").contains("11px var(--mono)"));
    for sel in [
        ".zedges .zstub .pbg",
        ".zedges .zstub.on .pbg",
        ".zedges .zl.on .ln",
        ".zi.zhl",
    ] {
        assert!(
            !rule(sel).contains("--focus"),
            "{sel} is orange: {}",
            rule(sel)
        );
    }
    assert!(rule(".zedges .zstub.sel .pbg").contains("--focus"));
    assert!(rule(".zedges .zl.sel .ln").contains("--focus"));
}

/// A double-click on an overview card as a browser sends it: the first
/// click selects (and re-draws the cards), the second click, detail 2,
/// opens that card in Explore at 1:1.
#[test]
fn a_second_click_on_a_card_opens_it_in_explore() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::MULTI, "lidx-viewer-gclick2-");
    let r = run(&p, "glance-click2", "validator");
    assert_eq!(r["first"]["level"], "glance", "{r}");
    assert_eq!(r["first"]["sel"], "validator", "{r}");
    assert_eq!(r["level"], "app", "{r}");
    assert_eq!(r["open"], serde_json::json!(["validator"]), "{r}");
    assert_eq!(r["k"], 1.0, "{r}");
}

/// Services that talk through integration events: the map draws who
/// publishes to whom, each line to the broker points the way its messages
/// go, the broker's card lists each event from publisher to subscribers,
/// the tour shows how events flow, and a flow says which event it crosses
/// the bus on.
#[test]
fn an_event_driven_system_shows_who_publishes_and_who_reacts() {
    if !node_available() {
        eprintln!("skipped: node is not installed");
        return;
    }
    let (_tmp, p) = glance_page(common::glance_fixtures::EVENTS, "lidx-viewer-events-");
    let r = run(&p, "glance-landing", "");
    let drawn: Vec<String> = r["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| {
            format!(
                "{}>{}",
                l["from"].as_str().unwrap(),
                l["to"].as_str().unwrap()
            )
        })
        .collect();
    // Publisher to subscriber, drawn at rest; the broker's lines are tags.
    assert_eq!(
        drawn,
        vec!["Catalog>Ordering", "Ordering>Basket", "Ordering>Web"],
        "{r}"
    );
    let svg = r["html"]["links"].as_str().unwrap();
    assert!(
        svg.contains("<title>Ordering → Basket: publishes OrderStartedIntegrationEvent</title>"),
        "{svg}"
    );
    // The broker selected: its lines, an arrow into each subscriber, two
    // heads on a line both ways.
    let s = run(&p, "glance-select", "RabbitMQ");
    let side = s["card"]["side"].as_str().unwrap();
    assert!(
        side.contains("Events 3Ordering publishes OrderPaidIntegrationEvent → Catalog, WebOrdering publishes OrderStartedIntegrationEvent → BasketCatalog publishes StockLowIntegrationEvent → Ordering"),
        "{side}"
    );
    assert!(
        side.contains("PublishersOrdering publishes 2 eventsOrderPaidIntegrationEvent, OrderStartedIntegrationEventCatalog publishes StockLowIntegrationEvent"),
        "{side}"
    );
    assert!(
        side.contains("SubscribersBasket subscribes to OrderStartedIntegrationEvent"),
        "{side}"
    );
    assert!(!side.contains("Used by"), "{side}");
    let all = run(&p, "glance-everything", "")["html"]
        .as_str()
        .unwrap()
        .to_string();
    // The arrowheads drawn on the line a title names: one per head.
    let heads = |title: &str| -> usize {
        let at = all.find(&format!("<title>{title}</title>")).expect(title);
        let ah = all[..at].rfind("class=\"ah\" d=\"").unwrap();
        let d = &all[ah..at];
        d[..d.find("\"/>").unwrap()].matches('Z').count()
    };
    assert_eq!(
        heads("RabbitMQ → Basket: Basket subscribes to OrderStartedIntegrationEvent"),
        1
    );
    // A subscriber's head sits where the line leaves the app, not at the
    // broker: its tip is the line's first point.
    let tip_at_start = |title: &str| -> bool {
        let at = all.find(&format!("<title>{title}</title>")).expect(title);
        let ah = all[..at].rfind("class=\"ah\" d=\"M").unwrap();
        let ln = all[..ah].rfind("class=\"ln\" d=\"M").unwrap();
        let first = |i: usize| {
            let t = &all[i..];
            let t = &t[t.find("d=\"M").unwrap() + 4..];
            t[..t.find(['L', 'C', 'H', 'V', 'Q']).unwrap()]
                .trim()
                .to_string()
        };
        first(ah) == first(ln)
    };
    assert!(tip_at_start(
        "RabbitMQ → Basket: Basket subscribes to OrderStartedIntegrationEvent"
    ));
    assert!(!tip_at_start(
        "Ordering → Basket: publishes OrderStartedIntegrationEvent"
    ));
    assert_eq!(
        heads("Ordering ⇄ RabbitMQ: publishes 2 events, subscribes to 1"),
        2
    );
    assert_eq!(
        heads("Ordering → Basket: publishes OrderStartedIntegrationEvent"),
        1
    );
    // The tour step on the broker.
    let t = run(&p, "glance-tour-at", "events");
    assert_eq!(t["state"]["sel"], "RabbitMQ", "{t}");
    let side = t["side"].as_str().unwrap();
    assert!(side.contains("How events flow"), "{side}");
    assert!(
        side.contains("Ordering publishes OrderPaidIntegrationEvent → Catalog, Web"),
        "{side}"
    );
    // A flow names the event at each crossing.
    let f = run(&p, "glance-flow", "0");
    let side = f["side"].as_str().unwrap();
    assert!(
        side.contains("3 Catalog picks OrderPaidIntegrationEvent up from RabbitMQ."),
        "{side}"
    );
}
