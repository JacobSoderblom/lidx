//! `lidx graph` landing page ("System at a glance"): the map, the entry
//! points, the flows picked from `trace_flow`, card sentences, descriptions
//! from manifests and READMEs, and the tour, on synthetic repositories of
//! three shapes: several services that talk across processes, one
//! command-line app, and a workspace of libraries with no way in.
mod common;

use common::glance_fixtures::{CLI, EVENTS, LIBS, MULTI};

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
                "Python worker subscribed to 1 topic; reads 1 secret, uses a message bus."
            ),
            (
                "Kubernetes manifests",
                "chip",
                "infra",
                "Kubernetes manifests in 1 folder providing 1 secret to 1 app."
            ),
            // No broker named anywhere: a plain message bus, not a guess.
            (
                "Message bus",
                "card",
                "data",
                "Message bus (the repository names no broker) with 1 topic (file-uploaded); 1 app publishes; 1 app subscribes."
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
                "calls over HTTP".into(),
                false
            ),
            (
                "ingest-api".into(),
                "validator".into(),
                "publishes file-uploaded".into(),
                true
            ),
            (
                "validator".into(),
                "Kubernetes manifests".into(),
                "reads secret DB_PASSWORD".into(),
                false
            ),
            (
                "ingest-api".into(),
                "Message bus".into(),
                "publishes file-uploaded".into(),
                false
            ),
            (
                "validator".into(),
                "Message bus".into(),
                "subscribes to file-uploaded".into(),
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
        // Named by where it goes: the manifests it reads config from are a
        // step, not a destination; the broker and its subscriber are one
        // branch, not two.
        vec!["POST /uploads → ingest-api → Message bus → validator"]
    );
    assert_eq!(
        g.steps(0),
        own3(&[
            ("portal", "start", "ui/src/api.sendUpload"),
            ("ingest-api", "http", "ingest_api.main.upload"),
            ("ingest-api", "call", "ingest_api.main.store"),
            ("Message bus", "publish", "file-uploaded"),
            ("validator", "bus", "validator.main.on_uploaded"),
            ("validator", "call", "validator.main.check"),
        ])
    );
    let f = &g.g()["flows"][0];
    assert_eq!(f["caller"], true);
    assert_eq!(f["label"], "POST /uploads");
    assert_eq!(f["probable"], false);
    assert_eq!(f["steps"][3]["file"], "api/ingest_api/main.py");
    // The secret the manifests set is where a value comes from, not a hop
    // at run time: the flow's `reads`, its manifests not one of its parts.
    assert_eq!(f["reads"].as_array().unwrap().len(), 1);
    assert_eq!(f["reads"][0]["name"], "secret DB_PASSWORD");
    assert_eq!(f["reads"][0]["qual"], "env://DB_PASSWORD");
    assert_eq!(f["reads"][0]["file"], "deploy/k8s/validator.yaml");
    let parts: Vec<&str> = f["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| {
            g.g()["nodes"][n.as_u64().unwrap() as usize]["name"]
                .as_str()
                .unwrap()
        })
        .collect();
    assert_eq!(
        parts,
        vec!["portal", "ingest-api", "Message bus", "validator"]
    );
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
        "The apps keep data and pass messages through 1 resource: Message bus (2 apps). Click a resource to see who uses it and how."
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
            "What reacts to file-uploaded?",
            "What uses Message bus?",
            "What talks to ingest-api?",
            "What talks to validator?"
        ]
    );
    assert_eq!(
        g.g()["questions"][1]["a"],
        "validator subscribes to file-uploaded on Message bus; ingest-api publishes it."
    );
    assert_eq!(
        g.g()["questions"][2]["a"],
        "ingest-api publishes file-uploaded; validator subscribes to file-uploaded."
    );
    assert_eq!(g.g()["questions"][3]["a"], "portal calls over HTTP.");
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
            ("src/*".into(), "cli".into(), "calls".into(), false),
            ("cli".into(), "report".into(), "calls".into(), false),
            ("cli".into(), "store".into(), "calls".into(), false),
            ("report".into(), "store".into(), "imports ×2".into(), false),
        ]
    );
    assert_eq!(g.entries(), own3(&[("src/*", "main", "tally main")]));
    // One way in: several paths from it, across modules.
    assert_eq!(
        g.titles(),
        vec![
            // The request branches at cli: each flow says so, and which
            // branch it follows.
            "tally main → cli calls 2 parts; following report",
            "tally main → cli calls 2 parts; following store"
        ]
    );
    // The branch point and every part it reaches.
    let name = |v: &serde_json::Value| -> String {
        g.g()["nodes"][v.as_u64().unwrap() as usize]["name"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let fan = &g.g()["flows"][0]["fan"];
    assert_eq!(fan.as_array().unwrap().len(), 1, "{fan}");
    assert_eq!(name(&fan[0]["node"]), "cli");
    let mut to: Vec<String> = fan[0]["to"].as_array().unwrap().iter().map(name).collect();
    to.sort();
    assert_eq!(to, vec!["report".to_string(), "store".to_string()]);
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
            "summary → @kit/report calls 2 parts; following @kit/text → @kit/core",
            "summary → @kit/report calls 2 parts; following @kit/core"
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

/// Each unit carries the overview's class for Explore to count by, and a
/// unit the overview leaves out says why: its code is in a language lidx
/// does not index.
#[test]
fn units_carry_the_overview_class_and_why_one_is_left_out() {
    let files = common::glance_fixtures::explore();
    let (_tmp, root, db) = common::index_repo("lidx-glance-class-", &files);
    let apps = glance(&root, &db);
    let units = apps["units"].as_array().unwrap();
    let class = &apps["glance"]["unit_class"];
    let of = |name: &str| -> (String, usize) {
        let u = units
            .iter()
            .find(|u| u["name"] == name)
            .unwrap_or_else(|| panic!("no {name}: {apps}"));
        let id = u["id"].as_u64().unwrap() as usize;
        (class[id].as_str().unwrap().to_string(), id)
    };
    assert_eq!(of("portal").0, "app");
    assert_eq!(of("ingest-api").0, "app");
    assert_eq!(of("validator").0, "app");
    assert_eq!(of("deploy/k8s").0, "infra");
    assert_eq!(of("deploy/overlays").0, "infra");
    assert_eq!(of("./").0, "hidden");
    let (ads, id) = of("ads");
    assert_eq!(ads, "off");
    assert_eq!(
        apps["glance"]["left_out"],
        serde_json::json!([[id, "Java, not indexed"]])
    );
    assert_eq!(class.as_array().unwrap().len(), units.len());
}

/// Services that talk only through integration events on one broker: the
/// map draws who publishes to whom, the broker's lines point the way the
/// messages go, the tour explains how events flow, and every flow names the
/// event it crosses the bus on.
#[test]
fn services_that_talk_through_events_draw_who_publishes_to_whom() {
    let (_tmp, root, db) = common::index_repo("lidx-glance-events-", EVENTS);
    let apps = glance(&root, &db);
    let g = G(&apps);
    assert_eq!(
        g.nodes()[4],
        (
            "RabbitMQ".to_string(),
            "card".to_string(),
            "data".to_string(),
            "RabbitMQ message broker with 3 events (OrderPaidIntegrationEvent, OrderStartedIntegrationEvent and StockLowIntegrationEvent); 2 apps publish; 4 apps subscribe.".to_string()
        )
    );
    assert_eq!(
        g.nodes()[2].3,
        "C# web API serving 1 HTTP route and subscribed to 1 event; publishes 2 events, uses RabbitMQ."
    );
    // Event links drawn publisher to subscriber; of two apps that message
    // each other the busier direction (a tie: the first).
    let l = |a: &str, b: &str, label: &str, quiet: bool| {
        (a.to_string(), b.to_string(), label.to_string(), quiet)
    };
    assert_eq!(
        g.links(),
        vec![
            l(
                "Catalog",
                "Ordering",
                "publishes StockLowIntegrationEvent",
                false
            ),
            l(
                "Ordering",
                "Basket",
                "publishes OrderStartedIntegrationEvent",
                false
            ),
            l(
                "Ordering",
                "Catalog",
                "publishes OrderPaidIntegrationEvent",
                true
            ),
            l(
                "Ordering",
                "Web",
                "publishes OrderPaidIntegrationEvent",
                false
            ),
            l(
                "Basket",
                "RabbitMQ",
                "subscribes to OrderStartedIntegrationEvent",
                false
            ),
            l(
                "Catalog",
                "RabbitMQ",
                "publishes 1 event, subscribes to 1",
                false
            ),
            l(
                "Ordering",
                "RabbitMQ",
                "publishes 2 events, subscribes to 1",
                false
            ),
            l(
                "Web",
                "RabbitMQ",
                "subscribes to OrderPaidIntegrationEvent",
                false
            ),
        ]
    );
    let dirs: Vec<Value> = g.g()["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["dir"].clone())
        .collect();
    assert_eq!(
        dirs,
        serde_json::json!([null, null, null, null, "in", "both", "both", "in"])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(g.g()["event_brokers"], serde_json::json!([4]));
    assert_eq!(
        g.g()["nodes"][4]["res"]["events"],
        serde_json::json!([
            { "name": "OrderPaidIntegrationEvent", "pubs": [2], "subs": [1, 3], "at": [
                { "node": 1, "role": "sub", "qual": "Catalog.OrderPaidHandler.Handle", "file": "catalog/Handlers.cs", "line": 8 },
                { "node": 2, "role": "pub", "qual": "Ordering.Api.Pay", "file": "ordering/Api.cs", "line": 14 },
                { "node": 3, "role": "sub", "qual": "Web.OrderPaidHandler.Handle", "file": "web/Handlers.cs", "line": 7 },
            ] },
            { "name": "OrderStartedIntegrationEvent", "pubs": [2], "subs": [0], "at": [
                { "node": 0, "role": "sub", "qual": "Basket.OrderStartedHandler.Handle", "file": "basket/Handlers.cs", "line": 7 },
                { "node": 2, "role": "pub", "qual": "Ordering.Api.Pay", "file": "ordering/Api.cs", "line": 14 },
            ] },
            { "name": "StockLowIntegrationEvent", "pubs": [1], "subs": [2], "at": [
                { "node": 1, "role": "pub", "qual": "Catalog.OrderPaidHandler.Handle", "file": "catalog/Handlers.cs", "line": 8 },
                { "node": 2, "role": "sub", "qual": "Ordering.StockLowHandler.Handle", "file": "ordering/Api.cs", "line": 23 },
            ] },
        ])
    );
    assert_eq!(
        g.tour().into_iter().map(|t| t.0).collect::<Vec<_>>(),
        vec!["lanes", "busiest", "flow", "talk", "events", "data"]
    );
    let t = &g.g()["tour"][4];
    assert_eq!(t["title"], "How events flow");
    assert_eq!(
        t["text"],
        "RabbitMQ carries 3 events between 4 apps. Publishing: Ordering (2) and Catalog (1). Subscribing: Basket (1), Catalog (1), Ordering (1) and Web (1). The most widely heard: OrderPaidIntegrationEvent from Ordering to Catalog and Web."
    );
    assert_eq!(t["nodes"], serde_json::json!([4, 0, 1, 2, 3]));
    assert_eq!(t["links"], serde_json::json!([0, 1, 3, 4, 5, 6, 7]));
    assert_eq!(t["select"], 4);
    assert_eq!(t["panel"], "events");
    let qs: Vec<(String, String)> = g.g()["questions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|q| q["id"].as_str().unwrap().starts_with("reacts-"))
        .map(|q| {
            (
                q["q"].as_str().unwrap().to_string(),
                q["a"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        qs,
        vec![
            (
                "What reacts to OrderPaidIntegrationEvent?".to_string(),
                "Catalog and Web subscribe to OrderPaidIntegrationEvent on RabbitMQ; Ordering publishes it.".to_string()
            ),
            (
                "What reacts to OrderStartedIntegrationEvent?".to_string(),
                "Basket subscribes to OrderStartedIntegrationEvent on RabbitMQ; Ordering publishes it.".to_string()
            ),
            (
                "What reacts to StockLowIntegrationEvent?".to_string(),
                "Ordering subscribes to StockLowIntegrationEvent on RabbitMQ; Catalog publishes it.".to_string()
            ),
        ]
    );
    // A flow across the bus names each event, at the broker and where it
    // is picked up.
    assert_eq!(
        g.titles(),
        vec![
            // It ends back in Ordering, which hears StockLowIntegrationEvent:
            // named by where it really ends.
            "on OrderPaidIntegrationEvent → RabbitMQ → Catalog → Ordering",
            "on OrderStartedIntegrationEvent → RabbitMQ → Basket"
        ]
    );
    let topics: Vec<(String, String, Value)> = g.g()["flows"][0]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                g.name(&s["node"]),
                s["via"].as_str().unwrap().to_string(),
                s["topic"].clone(),
            )
        })
        .collect();
    let st = |n: &str, v: &str, t: Value| (n.to_string(), v.to_string(), t);
    assert_eq!(
        topics,
        vec![
            st("Ordering", "start", Value::Null),
            st("RabbitMQ", "publish", "OrderPaidIntegrationEvent".into()),
            st("Catalog", "bus", "OrderPaidIntegrationEvent".into()),
            st("RabbitMQ", "publish", "StockLowIntegrationEvent".into()),
            st("Ordering", "bus", "StockLowIntegrationEvent".into()),
        ]
    );
}

/// Two Python apps: an API with a health probe, a route, a complex handler
/// no code calls and a complex helper it calls, types it uses from a
/// generated file and from its own; a worker reading a plain port and a
/// password a Kubernetes Secret provides.
const TRUTH: &[(&str, &str)] = &[
    (
        "api/pyproject.toml",
        "[project]\nname = \"api\"\ndependencies = [\"fastapi>=0.110\"]\n",
    ),
    (
        "api/api/main.py",
        "from fastapi import FastAPI\nfrom api.models import Order\nfrom api.gen import Item\n\napp = FastAPI()\n\n\n@app.get(\"/healthz\")\ndef health():\n    return 1\n\n\n@app.get(\"/items\")\ndef items(a, b, c):\n    if a:\n        return 1\n    elif b:\n        return 2\n    elif c:\n        return 3\n    for x in a:\n        if x:\n            return x\n    return rank(a, b)\n\n\ndef rank(a, b):\n    if a and b:\n        return 1\n    if a or b:\n        return 2\n    for x in a:\n        if x > 1:\n            return x\n    while b:\n        b = b - 1\n    return Order(Item())\n",
    ),
    (
        "api/api/report.py",
        "from api.models import Order\nfrom api.gen import Item\n\n\ndef total(o: Order, i: Item):\n    return Order(Item())\n",
    ),
    (
        "api/api/models.py",
        "class Order:\n    def __init__(self, item):\n        self.item = item\n",
    ),
    (
        "api/api/gen.py",
        "# Code generated by a tool. DO NOT EDIT.\n\nclass Item:\n    pass\n",
    ),
    (
        "worker/pyproject.toml",
        "[project]\nname = \"worker\"\ndependencies = [\"pydantic\", \"requests\"]\n\n[project.scripts]\nworker = \"worker.main:run\"\n",
    ),
    (
        "worker/worker/main.py",
        "import os\nimport requests\n\n\ndef run():\n    port = os.environ[\"PORT\"]\n    password = os.environ[\"DB_PASSWORD\"]\n    return requests.get(\"/items\"), port, password\n",
    ),
    (
        "deploy/worker.yaml",
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: worker\nspec:\n  template:\n    spec:\n      containers:\n        - name: worker\n          image: registry.example.com/team/worker:1.0\n          env:\n            - name: PORT\n              value: \"8080\"\n            - name: DB_PASSWORD\n              valueFrom:\n                secretKeyRef:\n                  name: db-password\n                  key: value\n",
    ),
];

/// What the guided path says is only what the index supports: secrets
/// apart from plain config, no generated type among the core types, no
/// code nobody calls among the risky, no health probe among the ways in.
#[test]
fn the_guided_path_tells_secrets_generated_code_and_callers_apart() {
    let (_tmp, root, db) = common::index_repo("lidx-glance-truth-", TRUTH);
    let apps = glance(&root, &db);
    let g = G(&apps);
    // Ways in: the route, not the probe.
    let ways: Vec<(String, String, String)> =
        g.entries().into_iter().filter(|e| e.0 == "api").collect();
    assert_eq!(ways, own3(&[("api", "http", "GET /items")]));
    // Core types: the app's own, never the generated one.
    let core: Vec<String> = g.g()["core_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect();
    assert!(core.contains(&"Order".to_string()), "{core:?}");
    assert!(!core.contains(&"Item".to_string()), "{core:?}");
    // Careful here: the helper something calls, not the uncalled handler.
    let hot: Vec<(String, u64)> = g.g()["hotspots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            (
                h["name"].as_str().unwrap().to_string(),
                h["uses"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(hot, vec![("rank".to_string(), 1)]);
    // Secrets: the password from the Secret, apart from the plain port.
    let q = g.g()["questions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["id"] == "secrets")
        .unwrap()
        .clone();
    let groups: Vec<(String, Vec<String>)> = q["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|gr| {
            (
                gr["title"].as_str().unwrap().to_string(),
                gr["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| i["text"].as_str().unwrap().to_string())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        groups,
        vec![
            (
                "Secrets".to_string(),
                vec!["worker reads secret DB_PASSWORD from Kubernetes manifests".to_string()]
            ),
            (
                "Plain config keys, not secrets".to_string(),
                vec!["worker reads PORT from Kubernetes manifests".to_string()]
            ),
        ]
    );
    assert_eq!(
        q["a"],
        "worker reads 1 secret and 1 config key from Kubernetes manifests. 1 other key read is plain config, not secrets: listed apart below."
    );
    let config = g.g()["tour"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"] == "config")
        .unwrap()
        .clone();
    assert_eq!(
        config["text"],
        "Config keys are set in Kubernetes manifests (read by 1 app). Secrets come from Kubernetes manifests (read by 1 app)."
    );
    let worker = g.nodes().into_iter().find(|n| n.0 == "worker").unwrap();
    assert_eq!(
        worker.3,
        "Python command-line app installing `worker`; calls 1 app over HTTP, reads 1 config key, reads 1 secret."
    );
    let chip = g
        .nodes()
        .into_iter()
        .find(|n| n.0 == "Kubernetes manifests")
        .unwrap();
    assert_eq!(
        chip.3,
        "Kubernetes manifests in 1 folder providing 1 secret and 1 config key to 1 app."
    );
}
