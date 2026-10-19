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
    // The API's call into the library ends at the card's edge, named.
    let into = links
        .iter()
        .find(|l| l["from"]["unit"] == "orders-api")
        .unwrap_or_else(|| panic!("no link from the closed API card: {r}"));
    assert_eq!(into["from"]["card"], true);
    assert_eq!(into["to"]["name"], "fmt_order");
    assert_eq!(into["port"], "orders-api");
    assert!(r["file"]["rendered"].as_u64().unwrap() >= 4, "{r}");

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

/// With both services open down to their files, the bus link is drawn
/// function to function; with one closed it ends at a port named for it.
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
    let port = r["closed"]["ports"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["other"] == "notify")
        .unwrap_or_else(|| panic!("no port to notify: {r}"));
    assert_eq!(port["out"], true);
    assert_eq!(port["weight"], 2, "two topics: {r}");
    assert_eq!(r["open"], serde_json::json!(["orders-api", "notify"]));
    let links = r["links"].as_array().unwrap();
    for target in ["on_created", "on_paid"] {
        let l = links
            .iter()
            .find(|l| l["from"]["name"] == "list_orders" && l["to"]["name"] == target)
            .unwrap_or_else(|| panic!("no list_orders -> {target}: {r}"));
        assert_eq!(l["from"]["unit"], "orders-api");
        assert_eq!(l["to"]["unit"], "notify");
        assert_eq!(l["kinds"]["VIA_BUS"], 1, "{r}");
        assert!(l["port"].is_null(), "drawn end to end: {r}");
    }
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
    assert!(
        r["panel"].as_str().unwrap().contains("Inside"),
        "the panel keeps the tree: {r}"
    );
    let hash = r["hash"].as_str().unwrap();
    assert!(hash.contains("filter=wid"), "{hash}");
    let again = run_at(&p, "zoom-hash", "", hash);
    assert_eq!(
        again["open"],
        serde_json::json!(["shared-utils"]),
        "{again}"
    );
    assert_eq!(again["crumbs"], r["crumbs"], "{again}");
    assert_eq!(again["sel"]["name"], "width", "{again}");
}
