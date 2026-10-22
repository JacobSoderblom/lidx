//! `lidx graph` app view zoom data: every unit's files and symbols with
//! their nesting, the weighted relations between symbols (bridges through
//! shared keys included), and the `--max-symbols` cap with its note.
mod common;

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

const FILES: &[(&str, &str)] = &[
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
            "    _bus.publish(\"order-paid\", {})\n",
            "    fmt_order(0)\n",
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

fn page(root: &Path, db: &Path, args: &[&str]) -> (Value, String) {
    let out = root.join("zoom-out.html");
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
        "lidx graph {args:?} failed: {}",
        String::from_utf8_lossy(&res.stderr)
    );
    let html = std::fs::read_to_string(&out).unwrap();
    (
        lidx::graph_html::extract_document(&html).unwrap(),
        String::from_utf8_lossy(&res.stderr).into_owned(),
    )
}

/// (file, kind, name, qualname, line, parent, signature)
type Sym = (usize, String, String, String, i64, i64, Option<String>);

/// The zoom section decoded: units by name, files, symbols, edges.
struct Zoom {
    units: Vec<String>,
    files: Vec<(String, String, u64, bool)>,
    syms: Vec<Sym>,
    /// (from, to, kind, weight, keys)
    edges: Vec<(i64, i64, String, u64, Vec<String>)>,
    z: Value,
}

impl Zoom {
    fn new(doc: &Value) -> Zoom {
        let z = doc["apps"]["zoom"].clone();
        let s: Vec<String> = z["s"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect();
        let col = |k: &str| -> Vec<i64> {
            z[k].as_array()
                .unwrap_or_else(|| panic!("no column {k}"))
                .iter()
                .map(|v| v.as_i64().unwrap())
                .collect()
        };
        let units = doc["apps"]["units"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| u["name"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        let (fu, fp, fs, fe) = (col("f_unit"), col("f_path"), col("f_syms"), col("f_emb"));
        let files = (0..fp.len())
            .map(|i| {
                (
                    units[fu[i] as usize].clone(),
                    s[fp[i] as usize].clone(),
                    fs[i] as u64,
                    fe[i] == 1,
                )
            })
            .collect();
        let (yf, yk, yn, yq, yl, yp, yg) = (
            col("y_file"),
            col("y_kind"),
            col("y_name"),
            col("y_qual"),
            col("y_line"),
            col("y_parent"),
            col("y_sig"),
        );
        let syms = (0..yf.len())
            .map(|i| {
                (
                    yf[i] as usize,
                    s[yk[i] as usize].clone(),
                    s[yn[i] as usize].clone(),
                    // The qualname: its prefix plus the name, or whole.
                    if yq[i] >= 0 {
                        format!("{}{}", s[yq[i] as usize], s[yn[i] as usize])
                    } else {
                        s[(-1 - yq[i]) as usize].clone()
                    },
                    yl[i],
                    yp[i],
                    (yg[i] >= 0).then(|| s[yg[i] as usize].clone()),
                )
            })
            .collect();
        let e = col("e");
        let bridge_from = z["bridge_from"].as_u64().unwrap() as usize;
        let edges = e
            .chunks(4)
            .enumerate()
            .map(|(j, q)| {
                let keys = if j >= bridge_from {
                    z["keys"][j - bridge_from]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|k| k.as_str().unwrap().to_string())
                        .collect()
                } else {
                    Vec::new()
                };
                (q[0], q[1], s[q[2] as usize].clone(), q[3] as u64, keys)
            })
            .collect();
        Zoom {
            units,
            files,
            syms,
            edges,
            z,
        }
    }

    fn sym(&self, name: &str) -> i64 {
        self.syms
            .iter()
            .position(|y| y.2 == name)
            .unwrap_or_else(|| panic!("no symbol {name}: {:?}", self.syms)) as i64
    }

    fn end(&self, i: i64) -> String {
        if i >= 0 {
            self.syms[i as usize].2.clone()
        } else {
            format!("file:{}", self.files[(-1 - i) as usize].1)
        }
    }

    /// `(from, to, kind) -> weight` by name.
    fn weights(&self) -> BTreeMap<(String, String, String), u64> {
        self.edges
            .iter()
            .map(|(a, b, k, w, _)| ((self.end(*a), self.end(*b), k.clone()), *w))
            .collect()
    }

    /// Symbol names per unit.
    fn by_unit(&self) -> BTreeMap<String, BTreeSet<String>> {
        let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for y in &self.syms {
            out.entry(self.files[y.0].0.clone())
                .or_default()
                .insert(y.2.clone());
        }
        out
    }
}

#[test]
fn every_unit_embeds_its_symbols_with_their_nesting() {
    let (_tmp, root, db) = common::index_repo("lidx-zoom-", FILES);
    let (doc, stderr) = page(&root, &db, &[]);
    assert!(!stderr.contains("not embedded"), "{stderr}");
    let z = Zoom::new(&doc);
    assert!(z.units.contains(&"orders-api".to_string()), "{:?}", z.units);
    let by_unit = z.by_unit();
    let names = |u: &str| -> Vec<&str> { by_unit[u].iter().map(String::as_str).collect() };
    assert_eq!(
        names("shared-utils"),
        ["Formatter", "fmt_order", "render", "width"]
    );
    assert!(names("orders-api").contains(&"list_orders"), "{by_unit:?}");
    assert_eq!(names("notify"), ["on_created", "on_paid", "run"]);

    // Methods sit in their class; module-level symbols at file level.
    let formatter = z.sym("Formatter");
    for m in ["render", "width"] {
        let y = &z.syms[z.sym(m) as usize];
        assert_eq!(y.5, formatter, "{m} is inside Formatter");
        assert_eq!(y.1, "method");
    }
    assert_eq!(z.syms[formatter as usize].5, -1);
    assert_eq!(z.syms[z.sym("fmt_order") as usize].5, -1);
    let render = &z.syms[z.sym("render") as usize];
    assert_eq!(render.4, 6, "start line");
    // Qualnames come back whole from their shared prefix.
    assert_eq!(render.3, "shared_utils.fmt.Formatter.render");
    assert_eq!(z.syms[formatter as usize].3, "shared_utils.fmt.Formatter");
    let quals = z.z["y_qual"].as_array().unwrap();
    assert_eq!(
        quals[z.sym("render") as usize],
        quals[z.sym("width") as usize],
        "members share one prefix string"
    );
    assert!(
        render.6.as_deref().unwrap_or("").contains("self, n"),
        "{render:?}"
    );

    // Every owned file is listed with its symbol count, all embedded.
    let fmt = z
        .files
        .iter()
        .find(|f| f.1 == "shared/shared_utils/fmt.py")
        .unwrap();
    assert_eq!(fmt, &("shared-utils".to_string(), fmt.1.clone(), 4, true));
    let paths: Vec<&str> = z.files.iter().map(|f| f.1.as_str()).collect();
    assert_eq!(
        paths,
        [
            "api/orders_api/main.py",
            "shared/shared_utils/fmt.py",
            "workers/notify/notify/main.py"
        ]
    );
    assert!(z.files.iter().all(|f| f.3), "{:?}", z.files);
    assert_eq!(z.z["omitted"], serde_json::json!([]));
}

#[test]
fn relations_are_aggregated_per_symbol_pair_with_exact_weights() {
    let (_tmp, root, db) = common::index_repo("lidx-zoom-edges-", FILES);
    let (doc, _) = page(&root, &db, &[]);
    let z = Zoom::new(&doc);
    let w = z.weights();
    let get = |a: &str, b: &str, k: &str| w.get(&(a.into(), b.into(), k.into())).copied();
    // Two calls from one function to another: one edge, weight 2.
    assert_eq!(get("list_orders", "fmt_order", "CALLS"), Some(2), "{w:?}");
    assert_eq!(get("render", "fmt_order", "CALLS"), Some(1), "{w:?}");
    // Across processes through the bus: weight = distinct topics, so the
    // second publish of order-paid adds nothing.
    let bus: Vec<_> = z
        .edges
        .iter()
        .filter(|e| e.2 == "VIA_BUS")
        .map(|e| (z.end(e.0), z.end(e.1), e.3, e.4.clone()))
        .collect();
    assert_eq!(
        bus,
        [
            (
                "list_orders".to_string(),
                "on_created".to_string(),
                1,
                vec!["channel://order-created".to_string()]
            ),
            (
                "list_orders".to_string(),
                "on_paid".to_string(),
                1,
                vec!["channel://order-paid".to_string()]
            ),
        ],
        "{w:?}"
    );
    // Imports from a file's module scope start at the file.
    assert_eq!(
        get("file:api/orders_api/main.py", "fmt_order", "IMPORTS"),
        Some(1),
        "{w:?}"
    );
    // Structure is not a relation.
    assert!(z.edges.iter().all(|e| e.2 != "CONTAINS"), "{w:?}");
    // Every endpoint is a symbol or a listed file.
    for e in &z.edges {
        for end in [e.0, e.1] {
            assert!(
                (end >= 0 && (end as usize) < z.syms.len())
                    || (end < 0 && ((-1 - end) as usize) < z.files.len()),
                "{e:?}"
            );
        }
    }
}

#[test]
fn max_symbols_keeps_whole_files_and_says_what_it_left_out() {
    let (_tmp, root, db) = common::index_repo("lidx-zoom-cap-", FILES);
    // 3 units of 2, 4 and 3 symbols, one file each. An even share of 3 is 1
    // symbol, which no whole file fits; what that leaves goes round, and
    // the API's 2-symbol file is the one that fits.
    let (doc, stderr) = page(&root, &db, &["--max-symbols", "3"]);
    let z = Zoom::new(&doc);
    assert_eq!(z.z["max_symbols"], 3);
    let by_unit = z.by_unit();
    let names: Vec<(&str, Vec<&str>)> = by_unit
        .iter()
        .map(|(u, s)| (u.as_str(), s.iter().map(String::as_str).collect()))
        .collect();
    assert_eq!(
        names,
        [("orders-api", vec!["app", "list_orders"])],
        "{by_unit:?}"
    );
    // No file is half embedded: all of its symbols, or none.
    for (fi, f) in z.files.iter().enumerate() {
        let n = z.syms.iter().filter(|y| y.0 == fi).count() as u64;
        assert_eq!(n, if f.3 { f.2 } else { 0 }, "{f:?}");
    }
    let fmt = z
        .files
        .iter()
        .find(|f| f.1 == "shared/shared_utils/fmt.py")
        .unwrap();
    assert_eq!((fmt.2, fmt.3), (4, false), "listed, counted, not embedded");
    // Per unit: (unit, symbols, files) left out, and one honest note.
    let omitted: Vec<(String, u64, u64)> = z.z["omitted"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| {
            (
                z.units[o[0].as_u64().unwrap() as usize].clone(),
                o[1].as_u64().unwrap(),
                o[2].as_u64().unwrap(),
            )
        })
        .collect();
    let mut omitted = omitted;
    omitted.sort();
    assert_eq!(
        omitted,
        [
            ("notify".to_string(), 3, 1),
            ("shared-utils".to_string(), 4, 1)
        ],
    );
    let note = z.z["note"].as_str().unwrap();
    assert!(
        note.starts_with("7 symbols in 2 files not embedded") && note.contains("--max-symbols 3"),
        "{note}"
    );
    assert!(note.contains("lidx graph --seed"), "{note}");
    assert!(stderr.contains(note), "the note is printed too: {stderr}");
    // A budget of 5 spends all of it: the API's 2 and the worker's 3.
    let (doc, _) = page(&root, &db, &["--max-symbols", "5"]);
    let z5 = Zoom::new(&doc);
    assert_eq!(z5.syms.len(), 5);
    assert_eq!(
        z5.by_unit().keys().map(String::as_str).collect::<Vec<_>>(),
        ["notify", "orders-api"]
    );
    assert_eq!(z5.z["omitted"].as_array().unwrap().len(), 1);
    // Edges only between what is embedded (or listed files).
    for e in &z.edges {
        assert!(e.0 < z.syms.len() as i64 && e.1 < z.syms.len() as i64);
    }
}

#[test]
fn seeded_and_folder_pages_carry_no_zoom_section() {
    let (_tmp, root, db) = common::index_repo("lidx-zoom-none-", FILES);
    let (doc, _) = page(&root, &db, &["--level", "module"]);
    assert!(doc["apps"].get("zoom").is_none());
    // Asked for where it does nothing, the cap says so instead of vanishing.
    let (_, stderr) = page(&root, &db, &["--level", "module", "--max-symbols", "10"]);
    assert!(
        stderr.contains("--max-symbols applies to the whole-repo app view only"),
        "{stderr}"
    );
    let (_, stderr) = page(&root, &db, &["--max-symbols", "10"]);
    assert!(!stderr.contains("applies to the whole-repo"), "{stderr}");
    let (doc, _) = page(&root, &db, &["--seed", "fmt_order", "--depth", "1"]);
    assert!(doc["apps"].get("zoom").is_none());
}

#[test]
fn a_unit_inside_another_owns_its_own_files() {
    let mut files: Vec<(&str, &str)> = FILES.to_vec();
    files.push((
        "api/plugins/pyproject.toml",
        "[project]\nname = \"orders-plugins\"\ndependencies = [\"shared-utils\"]\n",
    ));
    files.push((
        "api/plugins/orders_plugins/hook.py",
        "from shared_utils.fmt import fmt_order\n\n\ndef hook():\n    return fmt_order(2)\n",
    ));
    let (_tmp, root, db) = common::index_repo("lidx-zoom-nested-", &files);
    let (doc, _) = page(&root, &db, &[]);
    let z = Zoom::new(&doc);
    let by_unit = z.by_unit();
    assert!(by_unit["orders-plugins"].contains("hook"), "{by_unit:?}");
    assert!(!by_unit["orders-api"].contains("hook"), "{by_unit:?}");
    let hook = z
        .files
        .iter()
        .find(|f| f.1 == "api/plugins/orders_plugins/hook.py")
        .unwrap();
    assert_eq!(hook.0, "orders-plugins", "the innermost unit owns the file");
    // Its call is counted once, from its own unit's symbol.
    let w = z.weights();
    assert_eq!(
        w.get(&("hook".into(), "fmt_order".into(), "CALLS".into())),
        Some(&1),
        "{w:?}"
    );
}

#[test]
fn a_method_declared_away_from_its_type_sits_in_its_own_file() {
    let files: &[(&str, &str)] = &[
        ("svc/go.mod", "module example.com/svc\n\ngo 1.22\n"),
        (
            "svc/a.go",
            "package svc\n\ntype Store struct{}\n\nfunc (s Store) Get() int { return 1 }\n",
        ),
        (
            "svc/b.go",
            "package svc\n\nfunc (s Store) Put(v int) int { return v }\n",
        ),
    ];
    let (_tmp, root, db) = common::index_repo("lidx-zoom-gomethod-", files);
    let (doc, _) = page(&root, &db, &[]);
    let z = Zoom::new(&doc);
    let store = z.sym("Store");
    let file_of = |i: i64| z.files[z.syms[i as usize].0].1.as_str();
    let get = &z.syms[z.sym("Get") as usize];
    assert_eq!(
        (file_of(z.sym("Get")), get.5),
        ("svc/a.go", store),
        "{:?}",
        z.syms
    );
    let put = &z.syms[z.sym("Put") as usize];
    assert_eq!(file_of(z.sym("Put")), "svc/b.go");
    assert_eq!(
        put.5, -1,
        "not parented to a symbol in another file: {:?}",
        z.syms
    );
    assert_eq!(get.3, "svc/a.Store.Get");
}
