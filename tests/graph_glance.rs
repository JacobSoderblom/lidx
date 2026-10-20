//! `lidx graph` landing page ("System at a glance"): the map, the entry
//! points, the flows picked from `trace_flow`, card sentences, descriptions
//! from manifests and READMEs, and the tour, on synthetic repositories of
//! three shapes: several services that talk across processes, one
//! command-line app, and a workspace of libraries with no way in.
mod common;

use common::glance_fixtures::{CLI, LIBS, MULTI};

use serde_json::Value;
use std::path::Path;
use std::process::Command;

fn glance(root: &Path, db: &Path) -> Value {
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
    lidx::graph_html::extract_document(&html).unwrap()["apps"].clone()
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

struct G<'a>(&'a Value);

impl G<'_> {
    fn g(&self) -> &Value {
        &self.0["glance"]
    }
    fn name(&self, i: &Value) -> String {
        self.g()["nodes"][i.as_u64().unwrap() as usize]["name"]
            .as_str()
            .unwrap()
            .to_string()
    }
    /// `(name, place, lane, sentence)` per node.
    fn nodes(&self) -> Vec<(String, String, String, String)> {
        self.g()["nodes"]
            .as_array()
            .unwrap()
            .iter()
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
    /// `(from, to, label, quiet)` per link.
    fn links(&self) -> Vec<(String, String, String, bool)> {
        self.g()["links"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| {
                (
                    self.name(&l["from"]),
                    self.name(&l["to"]),
                    l["label"].as_str().unwrap().to_string(),
                    l["quiet"].as_bool().unwrap(),
                )
            })
            .collect()
    }
    /// `(node, kind, label)` per entry point.
    fn entries(&self) -> Vec<(String, String, String)> {
        self.g()["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    self.name(&e["node"]),
                    e["kind"].as_str().unwrap().to_string(),
                    e["label"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }
    fn titles(&self) -> Vec<String> {
        self.g()["flows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["title"].as_str().unwrap().to_string())
            .collect()
    }
    /// `(node, via, qualname)` per step of flow `i`.
    fn steps(&self, i: usize) -> Vec<(String, String, String)> {
        self.g()["flows"][i]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (
                    self.name(&s["node"]),
                    s["via"].as_str().unwrap().to_string(),
                    s["qual"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }
    /// `(id, title)` per tour step.
    fn tour(&self) -> Vec<(String, String)> {
        self.g()["tour"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                (
                    t["id"].as_str().unwrap().to_string(),
                    t["title"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }
}

fn own(v: &[(&str, &str, &str, &str)]) -> Vec<(String, String, String, String)> {
    v.iter()
        .map(|(a, b, c, d)| (a.to_string(), b.to_string(), c.to_string(), d.to_string()))
        .collect()
}

fn own3(v: &[(&str, &str, &str)]) -> Vec<(String, String, String)> {
    v.iter()
        .map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
        .collect()
}

#[test]
fn several_services_follow_one_request_across_http_the_bus_and_config() {
    let (_tmp, root, db) = common::index_repo("lidx-glance-multi-", MULTI);
    let apps = glance(&root, &db);
    let g = G(&apps);
    assert_eq!(g.g()["mode"], "apps");
    assert_eq!(
        g.g()["summary"]["text"],
        "Uploads takes partner files, checks them and stores the results."
    );
    assert_eq!(g.g()["summary"]["from"], "README.md");
    assert_eq!(
        g.nodes(),
        own(&[
            ("portal", "card", "front", "Partner upload UI"),
            (
                "ingest-api",
                "card",
                "svc",
                "Receives partner uploads and queues them."
            ),
            (
                "validator",
                "card",
                "work",
                "Python worker subscribed to 1 topic; reads 1 config key, uses a message bus."
            ),
            (
                "Kubernetes manifests",
                "chip",
                "infra",
                "Kubernetes manifests in 1 folder providing 1 config key to 1 app."
            ),
            // No broker named anywhere: a plain message bus, not a guess.
            (
                "Message bus",
                "card",
                "data",
                "Message bus (the repository names no broker) with 1 topic (fileuploaded); 1 app publishes; 1 app subscribes."
            ),
        ])
    );
    // Verbs and counts; nothing in-process between apps. The two apps meet
    // at the bus: their direct link waits for a focus.
    assert_eq!(
        g.links(),
        vec![
            (
                "portal".into(),
                "ingest-api".into(),
                "calls over HTTP ×1".into(),
                false
            ),
            (
                "ingest-api".into(),
                "validator".into(),
                "publishes fileuploaded".into(),
                true
            ),
            (
                "validator".into(),
                "Kubernetes manifests".into(),
                "reads config DB_PASSWORD".into(),
                false
            ),
            (
                "ingest-api".into(),
                "Message bus".into(),
                "publishes fileuploaded".into(),
                false
            ),
            (
                "validator".into(),
                "Message bus".into(),
                "subscribes to fileuploaded".into(),
                false
            ),
        ]
    );
    assert_eq!(
        g.entries(),
        own3(&[
            ("ingest-api", "http", "POST /uploads"),
            ("validator", "bus", "on file-uploaded"),
        ])
    );
    // One flow: the request starts at the portal's call, and every hop says
    // how it crossed. The bus entry's own path is the tail of this one.
    // The message crosses the bus as a step of its own.
    assert_eq!(
        g.titles(),
        vec!["POST /uploads → ingest-api → Message bus → validator → Kubernetes manifests"]
    );
    assert_eq!(
        g.steps(0),
        own3(&[
            ("portal", "start", "ui/src/api.sendUpload"),
            ("ingest-api", "http", "ingest_api.main.upload"),
            ("ingest-api", "call", "ingest_api.main.store"),
            ("Message bus", "publish", "fileuploaded"),
            ("validator", "bus", "validator.main.on_uploaded"),
            ("validator", "call", "validator.main.check"),
            ("Kubernetes manifests", "config", "env://DB_PASSWORD"),
        ])
    );
    let f = &g.g()["flows"][0];
    assert_eq!(f["caller"], true);
    assert_eq!(f["label"], "POST /uploads");
    assert_eq!(f["probable"], false);
    assert_eq!(f["steps"][3]["file"], "api/ingest_api/main.py");
    assert_eq!(f["steps"][6]["file"], "deploy/k8s/validator.yaml");
    // The entry point's card knows its flow.
    assert_eq!(g.g()["nodes"][1]["flows"], serde_json::json!([0]));
    assert_eq!(g.g()["entries"][0]["flow"], 0);
    assert_eq!(
        g.tour(),
        vec![
            ("lanes".into(), "These are the 3 apps, by role".into()),
            ("busiest".into(), "Most traffic enters ingest-api".into()),
            ("flow".into(), "Follow one request".into()),
            ("talk".into(), "How the apps talk".into()),
            ("data".into(), "Data and messaging".into()),
            ("config".into(), "Where config and secrets come from".into()),
        ]
    );
    assert_eq!(
        g.g()["tour"][0]["text"],
        "Front ends: portal. Services: ingest-api. Workers and tools: validator. Data and messaging: Message bus. Infrastructure: Kubernetes manifests."
    );
    assert_eq!(
        g.g()["tour"][4]["text"],
        "The apps keep data and pass messages through 1 shared resource: Message bus (2 apps). Click a resource to see who uses it and how."
    );
    assert_eq!(
        g.g()["tour"][3]["text"],
        "Across processes the apps use 1 message-bus link and 1 HTTP link. Click a line for the calls behind it."
    );
    let qs: Vec<String> = g.g()["questions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["q"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        qs,
        vec![
            "Where do secrets come from?",
            "What uses Message bus?",
            "What talks to ingest-api?",
            "What talks to validator?"
        ]
    );
    assert_eq!(
        g.g()["questions"][1]["a"],
        "ingest-api publishes fileuploaded; validator subscribes to fileuploaded."
    );
    assert_eq!(g.g()["questions"][2]["a"], "portal calls over HTTP ×1.");
    assert_eq!(
        g.g()["read_first"],
        serde_json::json!([{ "path": "README.md", "note": "what the project is" }])
    );
}

#[test]
fn a_single_command_line_app_is_shown_as_its_modules() {
    let (_tmp, root, db) = common::index_repo("lidx-glance-cli-", CLI);
    let apps = glance(&root, &db);
    let g = G(&apps);
    assert_eq!(g.g()["mode"], "modules");
    // No README: the manifest's description.
    assert_eq!(g.g()["summary"]["text"], "Counts the words in files.");
    assert_eq!(g.g()["summary"]["from"], "Cargo.toml");
    assert_eq!(
        g.nodes(),
        own(&[
            ("src/*", "card", "mod", "Rust module of 1 file."),
            ("cli", "card", "mod", "Rust module of 1 file."),
            ("report", "card", "mod", "Rust module of 1 file."),
            ("store", "card", "mod", "Rust module of 1 file."),
        ])
    );
    assert_eq!(
        strs(&serde_json::json!(
            g.g()["lanes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|l| l["id"].clone())
                .collect::<Vec<_>>()
        )),
        vec!["mod"],
        "no empty lanes"
    );
    assert_eq!(
        g.links(),
        vec![
            ("src/*".into(), "cli".into(), "calls ×1".into(), false),
            ("cli".into(), "report".into(), "calls ×1".into(), false),
            ("cli".into(), "store".into(), "calls ×1".into(), false),
            ("report".into(), "store".into(), "imports ×2".into(), false),
        ]
    );
    assert_eq!(g.entries(), own3(&[("src/*", "main", "tally main")]));
    // One way in: several paths from it, across modules.
    assert_eq!(
        g.titles(),
        vec![
            "tally main → src/* → cli → report",
            "tally main → src/* → cli → store"
        ]
    );
    assert_eq!(
        g.steps(1),
        own3(&[
            ("src/*", "start", "crate::main"),
            ("cli", "call", "crate::cli::run"),
            ("store", "call", "crate::store::load"),
        ])
    );
    assert_eq!(
        g.tour(),
        vec![
            (
                "lanes".into(),
                "This repository is one unit in 4 modules".into()
            ),
            ("busiest".into(), "Most traffic enters src/*".into()),
            ("flow".into(), "Follow one request".into()),
            ("talk".into(), "How the parts depend on each other".into()),
        ]
    );
}

#[test]
fn a_library_workspace_without_ways_in_starts_its_flows_from_central_functions() {
    let (_tmp, root, db) = common::index_repo("lidx-glance-libs-", LIBS);
    let apps = glance(&root, &db);
    let g = G(&apps);
    assert_eq!(g.g()["mode"], "packages");
    assert_eq!(
        g.g()["summary"]["text"],
        "A JavaScript repository of 3 packages."
    );
    assert_eq!(g.g()["summary"]["from"], "template");
    assert_eq!(
        g.nodes(),
        own(&[
            ("@kit/core", "card", "lib", "Shared parsing primitives."),
            (
                "@kit/report",
                "card",
                "lib",
                "JavaScript library of 1 file."
            ),
            (
                "@kit/text",
                "card",
                "lib",
                "JavaScript library; used by 1 unit."
            ),
        ])
    );
    assert_eq!(
        g.links(),
        vec![
            (
                "@kit/report".into(),
                "@kit/core".into(),
                "calls ×2".into(),
                false
            ),
            (
                "@kit/report".into(),
                "@kit/text".into(),
                "calls ×2".into(),
                false
            ),
            (
                "@kit/text".into(),
                "@kit/core".into(),
                "calls ×3".into(),
                false
            ),
        ]
    );
    // No way in: the honest note, and functions that call into other parts.
    assert_eq!(g.g()["central"], true);
    assert_eq!(
        g.entries(),
        own3(&[
            ("@kit/report", "central", "summary"),
            ("@kit/text", "central", "words"),
        ])
    );
    assert_eq!(
        g.titles(),
        vec![
            "summary → @kit/report → @kit/text → @kit/core",
            "summary → @kit/report → @kit/core"
        ]
    );
    let t = g.tour();
    assert_eq!(t[1], ("busiest".into(), "No entry points found".into()));
    assert_eq!(
        g.g()["tour"][1]["text"],
        "The index has no HTTP routes, RPC implementations, topic subscriptions, commands or main functions here, so the flows start from the functions that are called most and call into other parts."
    );
    for n in g.g()["nodes"].as_array().unwrap() {
        assert_eq!(
            n["ways"],
            serde_json::json!({}),
            "central functions are no ways in"
        );
    }
}

/// A monorepo past the card cap: the most connected packages are cards, the
/// rest wait behind "show all", and the page says how many.
#[test]
fn a_large_monorepo_caps_its_map_and_counts_what_is_hidden() {
    let mut owned: Vec<(String, String)> = vec![
        (
            "package.json".into(),
            "{ \"name\": \"mono\", \"private\": true, \"workspaces\": [\"packages/*\"] }\n".into(),
        ),
        (
            "packages/core/package.json".into(),
            "{ \"name\": \"@m/core\", \"main\": \"index.js\" }\n".into(),
        ),
        (
            "packages/core/index.js".into(),
            "export function base(s) {\n  return s;\n}\n".into(),
        ),
    ];
    for i in 0..14 {
        owned.push((
            format!("packages/p{i:02}/package.json"),
            format!(
                "{{ \"name\": \"@m/p{i:02}\", \"main\": \"index.js\", \"dependencies\": {{ \"@m/core\": \"workspace:*\" }} }}\n"
            ),
        ));
        // Later packages call core more often: they rank higher.
        let calls = "  base(s);\n".repeat(i + 1);
        owned.push((
            format!("packages/p{i:02}/index.js"),
            format!("import {{ base }} from \"@m/core\";\n\nexport function run{i}(s) {{\n{calls}  return s;\n}}\n"),
        ));
    }
    let files: Vec<(&str, &str)> = owned
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (_tmp, root, db) = common::index_repo("lidx-glance-mono-", &files);
    let apps = glance(&root, &db);
    let g = G(&apps);
    assert_eq!(g.g()["mode"], "packages");
    let nodes = g.nodes();
    assert_eq!(nodes.len(), 15);
    let cards: Vec<&str> = nodes
        .iter()
        .filter(|n| n.1 == "card")
        .map(|n| n.0.as_str())
        .collect();
    let off: Vec<&str> = nodes
        .iter()
        .filter(|n| n.1 == "off")
        .map(|n| n.0.as_str())
        .collect();
    assert_eq!(cards.len(), 12, "{nodes:?}");
    // Equal packages: those the flows pass through stay, then name order.
    assert_eq!(off, vec!["@m/p06", "@m/p07", "@m/p08"], "{nodes:?}");
    assert!(cards.contains(&"@m/core"));
    assert_eq!(g.g()["hidden"]["cards"], 3);
    for f in g.g()["flows"].as_array().unwrap() {
        for nd in f["nodes"].as_array().unwrap() {
            assert!(
                !off.contains(&g.name(nd).as_str()),
                "a flow's part is on the map: {f}"
            );
        }
    }
    assert!(!g.g()["flows"].as_array().unwrap().is_empty());
    // The "show all" layout still places every package.
    for n in g.g()["nodes"].as_array().unwrap() {
        assert!(n["all_layer"].as_i64().unwrap() >= 0, "{n}");
    }
}

/// Descriptions come from every manifest kind, else the README in the
/// unit's folder; a README's first sentence skips badges, HTML and
/// headings.
#[test]
fn card_descriptions_come_from_manifests_then_readmes() {
    let (_tmp, root, db) = common::index_repo(
        "lidx-glance-desc-",
        &[
            (
                "svc/Billing.csproj",
                "<Project Sdk=\"Microsoft.NET.Sdk.Web\">\n  <PropertyGroup>\n    <Description>Bills customers   every month.</Description>\n  </PropertyGroup>\n</Project>\n",
            ),
            (
                "svc/Program.cs",
                "public class Program { public static void Main() {} }\n",
            ),
            (
                "py/pyproject.toml",
                "[project]\nname = \"reports\"\ndescription = \"Builds the monthly reports.\"\n",
            ),
            ("py/reports/a.py", "def build():\n    return 1\n"),
            (
                "rs/Cargo.toml",
                "[package]\nname = \"pricing\"\nversion = \"0.1.0\"\ndescription = \"Prices orders.\"\n",
            ),
            ("rs/src/lib.rs", "pub fn price() -> u32 {\n    1\n}\n"),
            (
                "web/package.json",
                "{ \"name\": \"shop\", \"description\": \"The web shop.\", \"main\": \"index.js\" }\n",
            ),
            ("web/index.js", "export function go() {\n  return 1;\n}\n"),
            ("gomod/go.mod", "module example.com/fetcher\n\ngo 1.22\n"),
            (
                "gomod/README.md",
                "<p align=\"center\"><img src=\"logo.png\"></p>\n\n# fetcher\n\n[![build](https://x/b.svg)](https://x)\n\nRun the following to build it:\n\n```\ngo build\n```\n\n**Fetcher** pulls feeds from [partners](https://example.com) on a schedule, e.g. hourly. It retries.\n",
            ),
            (
                "gomod/f.go",
                "package fetcher\n\nfunc Pull() int {\n\treturn 1\n}\n",
            ),
        ],
    );
    let apps = glance(&root, &db);
    let desc = |name: &str| -> Value {
        apps["units"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["name"] == name)
            .unwrap_or_else(|| panic!("no unit {name}"))["description"]
            .clone()
    };
    assert_eq!(desc("Billing"), "Bills customers every month.");
    assert_eq!(desc("reports"), "Builds the monthly reports.");
    assert_eq!(desc("pricing"), "Prices orders.");
    assert_eq!(desc("shop"), "The web shop.");
    assert_eq!(
        desc("fetcher"),
        "Fetcher pulls feeds from partners on a schedule, e.g. hourly."
    );
    // And the card says it in those words.
    let card = apps["glance"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "Billing")
        .unwrap();
    assert_eq!(card["sentence"], "Bills customers every month.");
    assert_eq!(card["sentence_from"], "description");
}
