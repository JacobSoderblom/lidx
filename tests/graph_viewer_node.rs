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
        web_core["label"].as_str().unwrap().contains("references"),
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
    assert_eq!(r["top"]["hash"], "#zoom=shared-utils");
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
/// side share one "+N more" stub, and no weight or member is lost.
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
            assert_eq!(now[0]["target"], format!("+{} more", group.len()), "{r}");
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
    // The file's foot just above the status line: the labels stay clear of it.
    let r = run(
        &p,
        "zoom-stubs-ui",
        "orders-api,orders_api/main.py,1.2,,830",
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
            f(l, 3) * k + cy <= 900.0 - 36.0 + 0.5,
            "label over the status line: {s} {r}"
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
