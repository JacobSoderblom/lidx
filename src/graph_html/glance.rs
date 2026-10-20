//! "System at a glance": the landing page of the whole-repo view, as the
//! `apps.glance` section of the viewer document.
//!
//! Everything here is computed from the index at generate time, with
//! templates only (no model writes any text):
//!
//! - a one-sentence repo summary (README, else a manifest, else a template);
//! - a small map: at most [`MAX_CARDS`] cards in lanes, libraries as a
//!   "used by N apps" strip, infrastructure as one chip per tool, and only
//!   links that cross a process (HTTP, RPC, bus, config) between apps;
//! - a sentence, ways in, key types and flows for every card;
//! - "Start here": ways in, core types, docs to read first, hotspots;
//! - flows: `trace_flow` from each entry point, scored by the units it
//!   crosses and the infrastructure it reaches;
//! - a data-driven system tour and pre-computed answers to a few questions.
//!
//! A repository with fewer than two apps is shown as its packages (a
//! workspace of libraries) or, with one unit, as that unit's modules, so the
//! map is never one lonely card.

use super::apps::{LinkView, SymRef, UnitUse, is_probe, is_secret, short_key};
use super::build::{Ctx, RepoFile, RepoRow, RouteCanon};
use super::resources::{self, Cat, Found, How, Verb};
use super::units::{self, Kind, Owners, Unit};
use crate::model::TraceHop;
use crate::traversal::{TraceConfig, trace_flow};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

/// Cards on the map before "show all".
pub const MAX_CARDS: usize = 12;
/// Flows kept.
pub const MAX_FLOWS: usize = 6;
/// Entry points traced.
const MAX_TRACES: usize = 80;
/// Entry points traced per node, so one big API cannot use them all up.
const TRACES_PER_NODE: usize = 12;
/// Share of the code (percent of symbols) one unit holds for the map to
/// show its modules rather than the packages.
const DOMINANT_PCT: i64 = 75;
/// Paths kept per entry point when there are few of them.
const PATHS_PER_ENTRY: usize = 3;
/// Hops a flow's trace follows.
const TRACE_HOPS: usize = 7;
/// Entry points listed per node.
const ENTRIES_PER_NODE: usize = 40;
/// Evidence lines per link.
const MAX_CALLS: usize = 12;
/// Drawn links in a module or package map before the rest wait for a focus.
const MAX_DRAWN: usize = 12;
/// Data and messaging resources on the map before "show all".
pub const MAX_RESOURCES: usize = 8;
/// External systems on the map before "show all".
pub const MAX_EXTERNAL: usize = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Two or more apps that talk across processes: apps in lanes.
    Apps,
    /// A workspace of libraries (and at most a few apps): its packages.
    Packages,
    /// One unit: its modules.
    Modules,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Apps => "apps",
            Mode::Packages => "packages",
            Mode::Modules => "modules",
        }
    }
}

/// Which map a repository gets, from its visible code units and the links
/// between apps that cross a process.
/// `dominant`: one unit holds most of the code, so its modules say more
/// than a row of small packages around it.
pub fn mode_for(apps: usize, libs: usize, process_links: usize, dominant: bool) -> Mode {
    if apps >= 2 && (process_links > 0 || (apps >= libs && !dominant)) {
        Mode::Apps
    } else if libs >= 2 && apps + libs >= 3 && !dominant {
        Mode::Packages
    } else {
        Mode::Modules
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Place {
    Card,
    Chip,
    Strip,
    /// Past the card cap: shown with "show all", or in a flow that needs it.
    Off,
}

impl Place {
    fn as_str(self) -> &'static str {
        match self {
            Place::Card => "card",
            Place::Chip => "chip",
            Place::Strip => "strip",
            Place::Off => "off",
        }
    }
}

pub(super) struct Input<'a> {
    pub repo_root: &'a Path,
    pub units: &'a [Unit],
    pub owners: &'a Owners,
    pub uses: &'a [UnitUse],
    pub links: &'a [LinkView],
    pub langs: &'a [BTreeMap<String, i64>],
    pub file_count: &'a [i64],
    pub sym_count: &'a [i64],
    pub files: &'a [RepoFile],
    pub rows: &'a [RepoRow],
}

struct Node {
    name: String,
    /// Unit kind, `infra` for a chip, `module` for a module.
    kind: String,
    lane: &'static str,
    units: Vec<usize>,
    /// A module's folder, or a unit's root.
    dir: String,
    place: Place,
    rank: f64,
    files: i64,
    symbols: i64,
    langs: Vec<String>,
    sentence: String,
    sentence_from: &'static str,
    /// A resource's detail: category, items, declarations.
    res: Option<Value>,
}

#[derive(Clone, Debug)]
struct Sym {
    id: i64,
    name: String,
    qual: String,
    file: String,
    line: i64,
}

#[derive(Clone, Debug)]
struct EntryPoint {
    node: usize,
    unit: usize,
    /// `http`, `rpc`, `bus`, `cli` or `main`.
    kind: &'static str,
    label: String,
    key: Option<String>,
    sym: Sym,
    prominence: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Step {
    pub node: usize,
    pub name: String,
    pub qual: String,
    pub file: String,
    pub line: i64,
    /// The edge kind that reached it (`ENTRY` for the first step).
    pub kind: String,
    /// `start`, `call`, `http`, `rpc`, `bus`, `config` or `db`.
    pub via: &'static str,
    /// Reached over an edge bound by name only.
    pub probable: bool,
}

/// One scored path from an entry point.
#[derive(Clone, Debug)]
pub(crate) struct Candidate {
    pub entry: usize,
    pub steps: Vec<Step>,
    pub score: i64,
    /// Nodes in the order the path first reaches them.
    pub seq: Vec<usize>,
    pub unresolved_end: bool,
}

/// In-process links between two nodes: edge kinds counted, and sample
/// call sites `(text, file, line)`.
type CodeLinks = BTreeMap<(usize, usize), (BTreeMap<String, usize>, Vec<(String, String, i64)>)>;

#[derive(Clone, Debug)]
struct OutLink {
    from: usize,
    to: usize,
    family: &'static str,
    label: String,
    count: usize,
    quiet: bool,
    calls: Vec<Value>,
}

/// A lane of code (apps, libraries, modules), not of infrastructure or
/// resources.
fn is_code_lane(lane: &str) -> bool {
    !matches!(lane, "infra" | "data" | "ext")
}

fn lane_label(lane: &str) -> &'static str {
    match lane {
        "front" => "Front ends",
        "svc" => "Services",
        "work" => "Workers and tools",
        "apps" => "Apps",
        "lib" => "Libraries",
        "mod" => "Modules",
        "data" => "Data and messaging",
        "ext" => "External systems",
        "infra" => "Infrastructure",
        _ => "Other",
    }
}

const LANG_NAME: &[(&str, &str)] = &[
    ("csharp", "C#"),
    ("python", "Python"),
    ("typescript", "TypeScript"),
    ("tsx", "TypeScript"),
    ("javascript", "JavaScript"),
    ("rust", "Rust"),
    ("go", "Go"),
    ("proto", "Protobuf"),
    ("sql", "SQL"),
    ("postgres", "PostgreSQL"),
    ("lua", "Lua"),
    ("bicep", "Bicep"),
    ("yaml", "YAML"),
    ("markdown", "Markdown"),
];

fn lang_name(l: &str) -> &str {
    LANG_NAME
        .iter()
        .find(|(k, _)| *k == l)
        .map(|(_, v)| *v)
        .unwrap_or(l)
}

fn is_code_lang(l: &str) -> bool {
    !matches!(
        l,
        "markdown" | "yaml" | "json" | "bicep" | "proto" | "external"
    )
}

/// `n one` / `n many`.
fn count(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// Names joined as prose: `a`, `a and b`, `a, b and c`.
fn and_list(items: &[String]) -> String {
    match items.len() {
        0 => String::new(),
        1 => items[0].clone(),
        n => format!("{} and {}", items[..n - 1].join(", "), items[n - 1]),
    }
}

/// The code languages of a unit, most files first.
fn code_langs(langs: &BTreeMap<String, i64>) -> Vec<String> {
    let mut l: Vec<(&String, &i64)> = langs.iter().filter(|(k, _)| is_code_lang(k)).collect();
    l.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    let mut out: Vec<String> = Vec::new();
    for (k, _) in l {
        let n = lang_name(k).to_string();
        if !out.contains(&n) {
            out.push(n);
        }
    }
    out
}

/// A UI framework named the way people write it.
fn framework_name(why: &str) -> Option<&'static str> {
    Some(match why {
        "react" | "react-dom" => "React",
        "vue" => "Vue",
        "@angular/core" => "Angular",
        "svelte" | "@sveltejs/kit" => "Svelte",
        "next" => "Next.js",
        "nuxt" => "Nuxt",
        "solid-js" => "Solid",
        "preact" => "Preact",
        "lit" => "Lit",
        _ => return None,
    })
}

/// What a card's sentence says about a unit, beyond its own words.
#[derive(Default, Debug, Clone)]
pub struct Facts {
    pub lang: Option<String>,
    pub kind: String,
    pub why: String,
    pub http_routes: usize,
    pub rpc_methods: usize,
    pub rpc_services: usize,
    pub topics_in: usize,
    pub topics_out: usize,
    pub calls_http: usize,
    pub calls_rpc: usize,
    pub config_keys: usize,
    pub secrets: usize,
    pub used_by: usize,
    pub files: i64,
    pub command: Option<String>,
    /// Databases, brokers, storage and external systems it uses.
    pub resources: Vec<String>,
}

/// The template sentence of an app or library card:
/// "C# web API exposing 274 RPC methods in 82 services; reads 19 config keys."
pub fn card_sentence(f: &Facts) -> String {
    let noun = match f.kind.as_str() {
        "ui" => match framework_name(&f.why) {
            Some(fw) => format!("{fw} front end"),
            None => "front end".to_string(),
        },
        "desktop" => "desktop app".to_string(),
        "api" => "web API".to_string(),
        "service" => "service".to_string(),
        "worker" => "worker".to_string(),
        "cli" => "command-line app".to_string(),
        "lib" => "library".to_string(),
        "loose" => "shared folder".to_string(),
        "data" => "database project".to_string(),
        "module" => "module".to_string(),
        other => other.to_string(),
    };
    let mut lead = match &f.lang {
        Some(l) => format!("{l} {noun}"),
        None => {
            let mut c = noun.chars();
            match c.next() {
                Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
                None => noun,
            }
        }
    };
    let mut exposes: Vec<String> = Vec::new();
    if f.http_routes > 0 {
        exposes.push(format!(
            "serving {}",
            count(f.http_routes, "HTTP route", "HTTP routes")
        ));
    }
    if f.rpc_methods > 0 {
        exposes.push(format!(
            "exposing {} in {}",
            count(f.rpc_methods, "RPC method", "RPC methods"),
            count(f.rpc_services, "service", "services")
        ));
    }
    if f.topics_in > 0 {
        exposes.push(format!(
            "subscribed to {}",
            count(f.topics_in, "topic", "topics")
        ));
    }
    if let Some(cmd) = &f.command {
        exposes.push(format!("installing `{cmd}`"));
    }
    if !exposes.is_empty() {
        lead = format!("{lead} {}", and_list(&exposes));
    }
    let mut rest: Vec<String> = Vec::new();
    if f.calls_http > 0 {
        rest.push(format!(
            "calls {} over HTTP",
            count(f.calls_http, "app", "apps")
        ));
    }
    if f.calls_rpc > 0 {
        rest.push(format!(
            "calls {} over RPC",
            count(f.calls_rpc, "app", "apps")
        ));
    }
    if f.topics_out > 0 {
        rest.push(format!(
            "publishes {}",
            count(f.topics_out, "topic", "topics")
        ));
    }
    if f.config_keys > 0 {
        rest.push(format!(
            "reads {}",
            count(f.config_keys, "config key", "config keys")
        ));
    }
    if f.secrets > 0 {
        rest.push(format!("reads {}", count(f.secrets, "secret", "secrets")));
    }
    if !f.resources.is_empty() {
        let mut names: Vec<String> = f.resources.iter().take(3).cloned().collect();
        if f.resources.len() > 3 {
            names.push(format!("{} more", f.resources.len() - 3));
        }
        rest.push(format!("uses {}", and_list(&names)));
    }
    if f.used_by > 0 {
        rest.push(format!("used by {}", count(f.used_by, "unit", "units")));
    }
    if rest.is_empty() && exposes.is_empty() && f.files > 0 {
        lead = format!("{lead} of {}", count(f.files as usize, "file", "files"));
    }
    if rest.is_empty() {
        format!("{lead}.")
    } else {
        format!("{lead}; {}.", rest.join(", "))
    }
}

/// The sentence of a module card: "Rust module of 24 files; most used: Db, Symbol."
pub fn module_sentence(lang: Option<&str>, files: i64, key_types: &[String]) -> String {
    let lead = match lang {
        Some(l) => format!("{l} module of {}", count(files as usize, "file", "files")),
        None => format!("Module of {}", count(files as usize, "file", "files")),
    };
    if key_types.is_empty() {
        format!("{lead}.")
    } else {
        format!("{lead}; most used: {}.", key_types.join(", "))
    }
}

/// The sentence of an infrastructure chip.
fn chip_sentence(
    tool: &str,
    folders: usize,
    keys: usize,
    secrets: usize,
    readers: usize,
) -> String {
    let mut s = format!(
        "{} in {}",
        tool_name(tool),
        count(folders, "folder", "folders")
    );
    let mut provides: Vec<String> = Vec::new();
    if secrets > 0 {
        provides.push(count(secrets, "secret", "secrets"));
    }
    if keys > 0 {
        provides.push(count(keys, "config key", "config keys"));
    }
    if !provides.is_empty() {
        s = format!("{s} providing {}", and_list(&provides));
        if readers > 0 {
            s = format!("{s} to {}", count(readers, "app", "apps"));
        }
    }
    format!("{s}.")
}

fn tool_name(tool: &str) -> String {
    match tool {
        "Kubernetes" => "Kubernetes manifests".to_string(),
        "Helm" => "Helm charts".to_string(),
        "Bicep" => "Bicep templates".to_string(),
        "Terraform" => "Terraform".to_string(),
        "compose" => "Compose files".to_string(),
        "SQL" => "SQL database projects".to_string(),
        "Aspire" => "Aspire app host".to_string(),
        "Skaffold" => "Skaffold config".to_string(),
        "Serverless" => "Serverless Framework".to_string(),
        "CloudFormation" => "CloudFormation templates".to_string(),
        t => t.to_string(),
    }
}

/// Modules of one unit: folders under `root`, the biggest split while it
/// holds at least half the symbols and there are fewer than six, so a
/// `src/` that holds everything opens into its own folders. Files directly
/// in a split folder form a module of their own (`src/` itself).
pub fn split_modules(files: &[(String, i64)], root: &str) -> Vec<(String, i64, usize)> {
    fn group(files: &[(String, i64)], base: &str) -> BTreeMap<String, (i64, usize)> {
        let mut g: BTreeMap<String, (i64, usize)> = BTreeMap::new();
        for (p, s) in files {
            let Some(rest) = p.strip_prefix(base) else {
                continue;
            };
            let key = match rest.find('/') {
                Some(i) => format!("{base}{}", &rest[..=i]),
                None => base.to_string(),
            };
            let e = g.entry(key).or_default();
            e.0 += s;
            e.1 += 1;
        }
        g
    }
    let mut mods = group(files, root);
    for _ in 0..6 {
        if mods.len() >= 6 {
            break;
        }
        let total: i64 = mods.values().map(|v| v.0).sum();
        let Some((big, (syms, _))) = mods
            .iter()
            .filter(|(k, _)| k.as_str() != root)
            .max_by(|a, b| a.1.0.cmp(&b.1.0).then_with(|| b.0.cmp(a.0)))
            .map(|(k, v)| (k.clone(), *v))
        else {
            break;
        };
        if syms * 2 < total {
            break;
        }
        let inner: Vec<(String, i64)> = files
            .iter()
            .filter(|(p, _)| p.starts_with(&big))
            .cloned()
            .collect();
        let split = group(&inner, &big);
        if split.len() < 2 {
            break;
        }
        mods.remove(&big);
        mods.extend(split);
    }
    mods.into_iter().map(|(k, (s, f))| (k, s, f)).collect()
}

/// The module (from `mods`) a file belongs to: the longest key it starts
/// with, where a key naming a folder's own files only takes files directly
/// in it.
fn module_of(path: &str, mods: &[String]) -> Option<usize> {
    let dir = match path.rfind('/') {
        Some(i) => &path[..=i],
        None => "",
    };
    let mut best: Option<usize> = None;
    for (i, m) in mods.iter().enumerate() {
        let fits = if mods.iter().any(|o| o != m && o.starts_with(m.as_str())) {
            // A folder some other module is inside: its own files only.
            dir == m
        } else {
            path.starts_with(m.as_str())
        };
        if fits && best.is_none_or(|b| mods[b].len() < m.len()) {
            best = Some(i);
        }
    }
    best
}

/// The name a module card shows: its last folder, or `src/ files` for the
/// files directly in a folder that was split.
fn module_name(key: &str, split_parent: bool) -> String {
    if key.is_empty() {
        return "top-level files".to_string();
    }
    if split_parent {
        return format!("{key}*");
    }
    key.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(key)
        .to_string()
}

/// Score of one path: units (or modules) crossed weigh most, then bridges
/// crossed, then the infrastructure reached, then the entry point.
pub fn score_path(
    distinct: usize,
    bridges: usize,
    config: bool,
    db: bool,
    prominence: i64,
    steps: usize,
) -> i64 {
    distinct as i64 * 10
        + bridges as i64 * 4
        + if config { 3 } else { 0 }
        + if db { 4 } else { 0 }
        + prominence
        - (steps as i64) / 4
}

/// The flows kept: best first, one per entry point, none whose node
/// sequence another already shows, and none sharing most of its symbols
/// with one kept.
pub(crate) fn select_flows(
    mut cands: Vec<Candidate>,
    max: usize,
    per_entry: usize,
) -> Vec<Candidate> {
    cands.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.seq.len().cmp(&b.seq.len()).reverse())
            .then_with(|| a.steps[0].qual.cmp(&b.steps[0].qual))
            .then_with(|| a.entry.cmp(&b.entry))
    });
    let syms =
        |c: &Candidate| -> HashSet<String> { c.steps.iter().map(|s| s.qual.clone()).collect() };
    let mut out: Vec<Candidate> = Vec::new();
    for c in cands {
        if out.len() >= max {
            break;
        }
        if c.seq.len() < 2 {
            continue;
        }
        // The same nodes again, or the start of a longer path kept from the
        // same entry point, tells nothing new.
        if out
            .iter()
            .any(|o| o.seq == c.seq || (o.entry == c.entry && o.seq.starts_with(&c.seq)))
            || out.iter().filter(|o| o.entry == c.entry).count() >= per_entry
        {
            continue;
        }
        // Paths of one entry share their start; against other entries, most
        // symbols in common is the same flow told twice.
        let mine = syms(&c);
        if out.iter().filter(|o| o.entry != c.entry).any(|o| {
            let theirs = syms(o);
            let common = mine.intersection(&theirs).count();
            common * 2 > mine.len().min(theirs.len())
        }) {
            continue;
        }
        out.push(c);
    }
    out
}

/// A flow's title: the entry point, then the nodes it reaches in order;
/// a long one keeps its first two and its last.
pub fn flow_title(label: &str, names: &[String]) -> String {
    let names: Vec<String> = if names.len() > 4 {
        vec![
            names[0].clone(),
            names[1].clone(),
            "…".to_string(),
            names[names.len() - 1].clone(),
        ]
    } else {
        names.to_vec()
    };
    if names.is_empty() {
        label.to_string()
    } else {
        format!("{label} → {}", names.join(" → "))
    }
}

/// Command-line commands declared in a source file: `(name, line,
/// decorator)`. A decorator's command is the function defined just below
/// it; any other is the function the declaration sits in. Cobra
/// (`Use: "scan"`), click/typer (`@cli.command("scan")`), commander and
/// yargs (`.command("scan")`) and System.CommandLine (`new Command("scan")`).
pub fn cli_commands(lang: &str, text: &str) -> Vec<(String, i64, bool)> {
    fn first_quoted(s: &str) -> Option<String> {
        let start = s.find(['"', '\''])?;
        let q = s.as_bytes()[start] as char;
        let rest = &s[start + 1..];
        let end = rest.find(q)?;
        let name: String = rest[..end]
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string();
        let ok = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | ':'));
        ok.then_some(name)
    }
    let mut out = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    for (i, raw) in lines.iter().enumerate() {
        let l = raw.trim();
        let line = i as i64 + 1;
        match lang {
            "go" => {
                if let Some(rest) = l.strip_prefix("Use:")
                    && let Some(n) = first_quoted(rest)
                {
                    out.push((n, line, false));
                }
            }
            "python" => {
                if l.starts_with('@') && (l.contains(".command(") || l.contains(".group(")) {
                    let args = &l[l.find('(').unwrap_or(0)..];
                    let name = args
                        .strip_prefix('(')
                        .filter(|a| a.trim_start().starts_with(['"', '\'']))
                        .and_then(first_quoted)
                        .or_else(|| {
                            // The function below: `def run_scan(` -> `run-scan`.
                            lines[i + 1..].iter().take(6).find_map(|n| {
                                let n = n.trim();
                                let def = n
                                    .strip_prefix("def ")
                                    .or_else(|| n.strip_prefix("async def "))?;
                                def.split('(').next().map(|f| f.replace('_', "-"))
                            })
                        });
                    if let Some(n) = name {
                        out.push((n, line, true));
                    }
                }
            }
            "javascript" | "typescript" | "tsx" => {
                if let Some(at) = l.find(".command(")
                    && let Some(n) = first_quoted(&l[at + ".command(".len()..])
                {
                    out.push((n, line, false));
                }
            }
            "csharp" => {
                if let Some(at) = l.find("new Command(")
                    && let Some(n) = first_quoted(&l[at + "new Command(".len()..])
                {
                    out.push((n, line, false));
                }
            }
            _ => {}
        }
    }
    out
}

/// The label of an entry point from its edge.
fn entry_label(kind: &str, key: &str, detail: Option<&Value>) -> String {
    match kind {
        "HTTP_ROUTE" => {
            let method = detail
                .and_then(|d| d.get("method"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let path = detail
                .and_then(|d| d.get("raw"))
                .and_then(Value::as_str)
                .filter(|p| p.starts_with('/'))
                .unwrap_or(key);
            if method.is_empty() || method.eq_ignore_ascii_case("ANY") {
                path.to_string()
            } else {
                format!("{} {path}", method.to_ascii_uppercase())
            }
        }
        "RPC_IMPL" => {
            let svc = detail
                .and_then(|d| d.get("service"))
                .and_then(Value::as_str);
            let rpc = detail.and_then(|d| d.get("rpc")).and_then(Value::as_str);
            match (svc, rpc) {
                (Some(s), Some(r)) => format!("{s}.{r}"),
                _ => key.trim_start_matches('/').replace('/', "."),
            }
        }
        _ => {
            // The topic as written (`"file-uploaded"`), not its matching key.
            // A constant's name (`TOPIC_NAME`) is not the topic: its key is.
            let quoted = |r: &str| r.starts_with(['"', '\'', '`']);
            let raw = detail
                .and_then(|d| d.get("raw"))
                .and_then(Value::as_str)
                .filter(|r| {
                    quoted(r)
                        || r.contains('-')
                        || (r.chars().any(|c| c.is_ascii_lowercase()) && !r.contains('.'))
                })
                .map(|r| r.trim_matches(['"', '\'', '`']))
                .filter(|r| !r.is_empty() && !r.contains(['(', ' ']));
            format!("on {}", raw.unwrap_or(short_key(key)))
        }
    }
}

struct Ix<'a> {
    ctx: &'a Ctx<'a>,
    inp: &'a Input<'a>,
}

impl Ix<'_> {
    fn symbols_named(&self, sql_where: &str, arg: &str) -> Result<Vec<Sym>> {
        let conn = self.ctx.db.read_conn()?;
        let sql = format!(
            "SELECT s.id, s.name, s.qualname, f.path, s.start_line
             FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?1 AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
               AND s.kind IN ('function', 'method') AND {sql_where}
             ORDER BY f.path, s.start_line LIMIT 50"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params![self.ctx.graph_version, arg], |r| {
            Ok(Sym {
                id: r.get(0)?,
                name: r.get(1)?,
                qual: r.get(2)?,
                file: r.get(3)?,
                line: r.get::<_, Option<i64>>(4)?.unwrap_or(0),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Functions and methods of a file with their line ranges.
    fn file_functions(&self, path: &str) -> Result<Vec<(Sym, i64)>> {
        let conn = self.ctx.db.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.id, s.name, s.qualname, f.path, s.start_line, s.end_line
             FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?1 AND f.path = ?2
               AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
               AND s.kind IN ('function', 'method')
             ORDER BY s.start_line",
        )?;
        let rows = stmt.query_map(rusqlite::params![self.ctx.graph_version, path], |r| {
            Ok((
                Sym {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    qual: r.get(2)?,
                    file: r.get(3)?,
                    line: r.get::<_, Option<i64>>(4)?.unwrap_or(0),
                },
                r.get::<_, Option<i64>>(5)?.unwrap_or(0),
            ))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

/// The `glance` section.
pub(super) fn glance_json(ctx: &Ctx, inp: &Input) -> Result<Value> {
    let ix = Ix { ctx, inp };
    let units = inp.units;
    let n = units.len();
    let code: Vec<bool> = (0..n)
        .map(|u| !units[u].kind.hidden() && inp.langs[u].keys().any(|l| is_code_lang(l)))
        .collect();
    let is_app = |u: usize| code[u] && units[u].kind.is_app();
    let is_lib = |u: usize| code[u] && matches!(units[u].kind, Kind::Lib | Kind::Loose);
    let apps: Vec<usize> = (0..n).filter(|&u| is_app(u)).collect();
    let libs: Vec<usize> = (0..n).filter(|&u| is_lib(u)).collect();
    let process_links = inp
        .links
        .iter()
        .filter(|l| matches!(l.family, "http" | "rpc" | "bus") && is_app(l.from) && is_app(l.to))
        .count();
    let code_syms: Vec<i64> = apps
        .iter()
        .chain(libs.iter())
        .map(|&u| inp.sym_count[u])
        .collect();
    let total: i64 = code_syms.iter().sum();
    let dominant = total > 0 && code_syms.iter().any(|&s| s * 100 >= total * DOMINANT_PCT);
    // ---- resources: databases, brokers, secret stores, storage, external
    let deps = units::resolve_deps(units);
    let in_scope = |p: &str| ctx.in_scope(p);
    let found = resources::find(&resources::Input {
        root: inp.repo_root,
        units,
        owners: inp.owners,
        files: inp.files,
        rows: inp.rows,
        uses: inp.uses,
        links: inp.links,
        in_scope: &in_scope,
        deps: &deps,
    });
    // Apps that meet at a resource talk across processes as much as two
    // that call each other.
    let shared = {
        let mut users: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        for x in &found.uses {
            if is_app(x.unit) && x.how != How::Manifest {
                users.entry(x.res).or_default().insert(x.unit);
            }
        }
        users.values().filter(|s| s.len() >= 2).count()
    };
    let mode = mode_for(apps.len(), libs.len(), process_links + shared, dominant);

    // ---- nodes
    let mut nodes: Vec<Node> = Vec::new();
    let mut unit_node: Vec<Option<usize>> = vec![None; n];
    // Modules mode: the unit split into modules, and its files' modules.
    let mut main_unit: Option<usize> = None;
    let mut mod_keys: Vec<String> = Vec::new();
    let mut mod_nodes: Vec<usize> = Vec::new();
    let unit_node_of = |u: usize,
                        units: &[Unit],
                        langs: &[BTreeMap<String, i64>],
                        lane: &'static str,
                        place: Place| Node {
        name: units[u].name.clone(),
        kind: units[u].kind.as_str().to_string(),
        lane,
        units: vec![u],
        dir: units[u].display_root(),
        place,
        rank: 0.0,
        files: inp.file_count[u],
        symbols: inp.sym_count[u],
        langs: code_langs(&langs[u]),
        sentence: String::new(),
        sentence_from: "template",
        res: None,
    };
    match mode {
        Mode::Apps => {
            for &u in &apps {
                let lane = match units[u].kind {
                    Kind::Ui | Kind::Desktop => "front",
                    Kind::Api | Kind::Service => "svc",
                    _ => "work",
                };
                unit_node[u] = Some(nodes.len());
                nodes.push(unit_node_of(u, units, inp.langs, lane, Place::Card));
            }
            for &u in &libs {
                unit_node[u] = Some(nodes.len());
                nodes.push(unit_node_of(u, units, inp.langs, "lib", Place::Strip));
            }
        }
        Mode::Packages => {
            for &u in apps.iter().chain(libs.iter()) {
                let lane = if is_app(u) { "apps" } else { "lib" };
                unit_node[u] = Some(nodes.len());
                nodes.push(unit_node_of(u, units, inp.langs, lane, Place::Card));
            }
        }
        Mode::Modules => {
            let main = apps
                .iter()
                .chain(libs.iter())
                .copied()
                .max_by(|&a, &b| {
                    inp.sym_count[a]
                        .cmp(&inp.sym_count[b])
                        .then_with(|| b.cmp(&a))
                })
                .or_else(|| {
                    (0..n)
                        .filter(|&u| !units[u].kind.hidden())
                        .max_by_key(|&u| inp.sym_count[u])
                });
            main_unit = main;
            if let Some(m) = main {
                let files: Vec<(String, i64)> = inp
                    .files
                    .iter()
                    .filter(|(p, l, s)| {
                        *s > 0
                            && is_code_lang(l)
                            && ctx.in_scope(p)
                            && !is_aux(p)
                            && inp.owners.owner(p) == Some(m)
                    })
                    .map(|(p, _, s)| (p.clone(), *s))
                    .collect();
                let mods = split_modules(&files, &units[m].root);
                let keys: Vec<String> = mods.iter().map(|(k, _, _)| k.clone()).collect();
                for (k, syms, nf) in &mods {
                    let split_parent = keys.iter().any(|o| o != k && o.starts_with(k.as_str()));
                    let mut langs: BTreeMap<String, i64> = BTreeMap::new();
                    for (p, l, _) in inp.files.iter() {
                        if module_of(p, &keys).is_some_and(|i| keys[i] == *k)
                            && inp.owners.owner(p) == Some(m)
                        {
                            *langs.entry(l.clone()).or_default() += 1;
                        }
                    }
                    mod_nodes.push(nodes.len());
                    nodes.push(Node {
                        name: module_name(k, split_parent),
                        kind: "module".to_string(),
                        lane: "mod",
                        units: vec![m],
                        dir: if k.is_empty() {
                            "./".to_string()
                        } else {
                            k.clone()
                        },
                        place: Place::Card,
                        rank: 0.0,
                        files: *nf as i64,
                        symbols: *syms,
                        langs: code_langs(&langs),
                        sentence: String::new(),
                        sentence_from: "template",
                        res: None,
                    });
                }
                mod_keys = keys;
                // Names unique: a clash shows the whole folder.
                let mut seen: HashMap<String, usize> = HashMap::new();
                for &i in &mod_nodes {
                    *seen.entry(nodes[i].name.clone()).or_default() += 1;
                }
                for &i in &mod_nodes {
                    if seen[&nodes[i].name] > 1 {
                        nodes[i].name = nodes[i].dir.clone();
                    }
                }
            }
            for &u in apps.iter().chain(libs.iter()) {
                if Some(u) == main {
                    continue;
                }
                let lane = if is_app(u) { "apps" } else { "lib" };
                unit_node[u] = Some(nodes.len());
                nodes.push(unit_node_of(u, units, inp.langs, lane, Place::Card));
            }
        }
    }
    // Infrastructure: one chip per tool.
    let mut chip_of_tool: BTreeMap<String, usize> = BTreeMap::new();
    for u in 0..n {
        if units[u].kind.hidden() || units[u].kind != Kind::Infra {
            continue;
        }
        let tool = units[u]
            .why
            .split(", ")
            .next()
            .unwrap_or("infra")
            .to_string();
        let id = *chip_of_tool.entry(tool.clone()).or_insert_with(|| {
            nodes.push(Node {
                name: tool_name(&tool),
                kind: "infra".to_string(),
                lane: "infra",
                units: Vec::new(),
                dir: String::new(),
                place: Place::Chip,
                rank: 0.0,
                files: 0,
                symbols: 0,
                langs: Vec::new(),
                sentence: String::new(),
                sentence_from: "template",
                res: None,
            });
            nodes.len() - 1
        });
        nodes[id].units.push(u);
        nodes[id].files += inp.file_count[u];
        unit_node[u] = Some(id);
    }

    // Resources: a node each, in the data and messaging lane or the
    // external systems lane. A SQL project is its database.
    let mut res_node: Vec<Option<usize>> = vec![None; found.res.len()];
    {
        let mut used: BTreeSet<usize> = found
            .uses
            .iter()
            .filter(|x| !units[x.unit].kind.hidden())
            .map(|x| x.res)
            .collect();
        used.extend(
            found
                .res
                .iter()
                .enumerate()
                .filter(|(_, r)| !r.declared.is_empty())
                .map(|(i, _)| i),
        );
        for i in used {
            let r = &found.res[i];
            res_node[i] = Some(nodes.len());
            for &u in &r.units {
                unit_node[u] = Some(nodes.len());
            }
            nodes.push(Node {
                name: r.name(),
                kind: "resource".to_string(),
                lane: r.cat.lane(),
                units: r.units.clone(),
                dir: String::new(),
                place: Place::Card,
                rank: 0.0,
                files: r.units.iter().map(|&u| inp.file_count[u]).sum(),
                symbols: 0,
                langs: Vec::new(),
                sentence: String::new(),
                sentence_from: "template",
                res: None,
            });
        }
    }

    // The node of a file: its module in modules mode, else its unit's.
    let node_of_file = |path: &str| -> Option<usize> {
        let u = inp.owners.owner(path)?;
        if Some(u) == main_unit {
            return module_of(path, &mod_keys).map(|i| mod_nodes[i]);
        }
        unit_node[u]
    };

    // ---- links
    let mut out_links: Vec<OutLink> = Vec::new();
    let store_secrets: HashSet<(usize, String)> = found
        .uses
        .iter()
        .filter(|x| x.verb == Verb::ReadsSecrets && x.how == How::Edge && res_node[x.res].is_some())
        .filter_map(|x| Some((x.unit, x.item.clone()?)))
        .collect();
    // Who reads what from each chip, for chip sentences.
    let mut chip_keys: HashMap<usize, (BTreeSet<String>, BTreeSet<usize>)> = HashMap::new();
    match mode {
        Mode::Apps => {
            let mut agg: BTreeMap<(usize, usize, &'static str), BTreeSet<String>> = BTreeMap::new();
            for l in inp.links {
                if !matches!(l.family, "http" | "rpc" | "bus" | "config") {
                    continue;
                }
                let (Some(a), Some(b)) = (unit_node[l.from], unit_node[l.to]) else {
                    continue;
                };
                if a == b || nodes[a].place == Place::Strip || nodes[b].place == Place::Strip {
                    continue;
                }
                // Infrastructure declaring a topic publishes nothing: the
                // broker's node holds what it declares.
                if l.family == "bus"
                    && (!is_code_lane(nodes[a].lane) || !is_code_lane(nodes[b].lane))
                {
                    continue;
                }
                // A secret read from a secret store is drawn to the store.
                let keys: Vec<&String> = l
                    .keys
                    .iter()
                    .filter(|k| !is_probe(k))
                    .filter(|k| {
                        !(l.family == "config"
                            && is_secret(k)
                            && store_secrets.contains(&(l.from, short_key(k).to_string())))
                    })
                    .collect();
                if keys.is_empty() {
                    continue;
                }
                agg.entry((a, b, l.family))
                    .or_default()
                    .extend(keys.into_iter().cloned());
            }
            let config_links = agg.keys().filter(|k| k.2 == "config").count();
            for ((a, b, family), keys) in agg {
                if family == "config" {
                    let e = chip_keys.entry(b).or_default();
                    e.0.extend(keys.iter().cloned());
                    e.1.insert(a);
                }
                let calls = link_calls(inp, &nodes, a, b, family, &keys);
                out_links.push(OutLink {
                    from: a,
                    to: b,
                    family,
                    label: process_label(family, &keys),
                    count: keys.len(),
                    // Every app reads config: drawn always, the arrows
                    // would bury the rest.
                    quiet: family == "config" && config_links > 4,
                    calls,
                });
            }
        }
        Mode::Packages | Mode::Modules => {
            // Code links between nodes; a package link needs a declared
            // dependency (no resolver guesses).
            let mut agg: CodeLinks = BTreeMap::new();
            if mode == Mode::Packages {
                for l in inp
                    .links
                    .iter()
                    .filter(|l| l.family == "code" && l.declared)
                {
                    let (Some(a), Some(b)) = (unit_node[l.from], unit_node[l.to]) else {
                        continue;
                    };
                    if a == b || !is_code_lane(nodes[a].lane) || !is_code_lane(nodes[b].lane) {
                        continue;
                    }
                    let e = agg.entry((a, b)).or_default();
                    for (k, c) in &l.kinds {
                        *e.0.entry(k.clone()).or_default() += c;
                    }
                    e.1.extend(l.evidence.iter().cloned());
                }
                for l in inp.links.iter().filter(|l| l.family == "depends") {
                    let (Some(a), Some(b)) = (unit_node[l.from], unit_node[l.to]) else {
                        continue;
                    };
                    if a != b && is_code_lane(nodes[a].lane) && is_code_lane(nodes[b].lane) {
                        agg.entry((a, b)).or_default();
                    }
                }
            } else {
                for r in inp.rows {
                    if matches!(r.kind.as_str(), "CONTAINS" | "MODULE_FILE")
                        || !ctx.in_scope(&r.src_path)
                    {
                        continue;
                    }
                    let Some(tp) = &r.tgt_path else { continue };
                    if r.tgt_lang.as_deref() == Some("external") {
                        continue;
                    }
                    let (Some(a), Some(b)) = (node_of_file(&r.src_path), node_of_file(tp)) else {
                        continue;
                    };
                    if a == b || !is_code_lane(nodes[a].lane) || !is_code_lane(nodes[b].lane) {
                        continue;
                    }
                    let e = agg.entry((a, b)).or_default();
                    *e.0.entry(r.kind.clone()).or_default() += 1;
                    if e.1.len() < MAX_CALLS {
                        e.1.push((
                            format!(
                                "{} → {}",
                                r.src_qual,
                                r.tgt_qual.clone().unwrap_or_else(|| tp.clone())
                            ),
                            r.ev_file.clone(),
                            r.ev_line.unwrap_or(r.src_line),
                        ));
                    }
                }
            }
            let mut list: Vec<OutLink> = agg
                .into_iter()
                .map(|((a, b), (kinds, ev))| {
                    let total: usize = kinds.values().sum();
                    OutLink {
                        from: a,
                        to: b,
                        family: "code",
                        label: code_label(&kinds),
                        count: total,
                        quiet: false,
                        calls: ev
                            .iter()
                            .take(MAX_CALLS)
                            .map(|(t, f, l)| json!({ "text": t, "file": f, "line": l }))
                            .collect(),
                    }
                })
                .collect();
            // Drawn: each node's two heaviest links out, at most MAX_DRAWN.
            let mut order: Vec<usize> = (0..list.len()).collect();
            order.sort_by(|&x, &y| list[y].count.cmp(&list[x].count).then_with(|| x.cmp(&y)));
            let mut per: HashMap<usize, usize> = HashMap::new();
            let mut drawn = 0;
            let mut pairs: HashSet<(usize, usize)> = HashSet::new();
            for &i in &order {
                // Two parts that use each other: the heavier direction is drawn.
                let (a, b) = (list[i].from, list[i].to);
                let mutual = pairs.contains(&(b, a));
                let c = per.entry(a).or_default();
                if *c < 2 && drawn < MAX_DRAWN && !mutual {
                    pairs.insert((a, b));
                    *c += 1;
                    drawn += 1;
                } else {
                    list[i].quiet = true;
                }
            }
            out_links = list;
        }
    }

    // ---- links to resources, with a verb and their evidence
    let mut ragg: BTreeMap<(usize, usize), ResAgg> = BTreeMap::new();
    for x in &found.uses {
        let Some(rn) = res_node[x.res] else { continue };
        if units[x.unit].kind.hidden() {
            continue;
        }
        let from = match mode {
            // A module is where the code is; the manifest of the unit the
            // modules split names none.
            Mode::Modules if Some(x.unit) == main_unit => {
                if matches!(x.how, How::Manifest | How::Deploy) {
                    None
                } else {
                    node_of_file(&x.ev.file)
                }
            }
            _ => unit_node[x.unit],
        };
        let Some(a) = from else { continue };
        if a == rn || nodes[a].place == Place::Strip || !is_code_lane(nodes[a].lane) {
            continue;
        }
        ragg.entry((a, rn)).or_default().add(x, units);
    }
    for ((a, rn), agg) in &ragg {
        let r = &found.res[found_index(&res_node, *rn)];
        out_links.push(OutLink {
            from: *a,
            to: *rn,
            family: r.cat.family(),
            label: agg.label(r.cat, &r.tech, units),
            count: agg.items.len().max(1),
            quiet: false,
            calls: agg.calls(units),
        });
    }
    // Apps a deployment file wires to each other (an env value naming the
    // other's host, an app host's reference), when no other link says so.
    let mut wired: BTreeMap<(usize, usize), Vec<&units::Evidence>> = BTreeMap::new();
    for (a, b, ev) in &found.app_wires {
        let (Some(na), Some(nb)) = (unit_node[*a], unit_node[*b]) else {
            continue;
        };
        let card = |i: usize| is_code_lane(nodes[i].lane) && nodes[i].place != Place::Strip;
        if na == nb || !card(na) || !card(nb) {
            continue;
        }
        if out_links
            .iter()
            .any(|l| (l.from == na && l.to == nb) || (l.from == nb && l.to == na))
        {
            continue;
        }
        wired.entry((na, nb)).or_default().push(ev);
    }
    for ((a, b), evs) in wired {
        let starts_after = evs.iter().all(|e| e.text.contains(" depends on "));
        out_links.push(OutLink {
            from: a,
            to: b,
            family: "wire",
            label: if starts_after {
                "starts after".to_string()
            } else {
                "calls (set in deployment)".to_string()
            },
            count: evs.len(),
            quiet: false,
            calls: evs
                .iter()
                .take(MAX_CALLS)
                .map(|e| json!({ "text": format!("deployment: {}", e.text), "file": e.file, "line": e.line }))
                .collect(),
        });
    }
    // Apps that meet at a broker are drawn through it: their direct bus
    // link waits for a focus.
    let brokered: HashSet<usize> = ragg
        .keys()
        .filter(|(_, rn)| {
            nodes[*rn].lane == "data" && found.res[found_index(&res_node, *rn)].cat == Cat::Bus
        })
        .map(|(a, _)| *a)
        .collect();
    for l in out_links.iter_mut() {
        if l.family == "bus" && brokered.contains(&l.from) && brokered.contains(&l.to) {
            l.quiet = true;
        }
    }
    // A resource nothing on the map links to and no file declares is
    // only in the detail.
    let linked_res: HashSet<usize> = ragg.keys().map(|(_, rn)| *rn).collect();
    for (i, nd) in res_node.iter().enumerate() {
        if let Some(rn) = *nd
            && !linked_res.contains(&rn)
            && found.res[i].declared.is_empty()
        {
            nodes[rn].place = Place::Off;
        }
    }

    // ---- entry points
    let mut entries = collect_entries(&ix, mode, &nodes, &node_of_file, main_unit)?;
    // Two `main`s (or commands) of one part read apart by their file.
    let mut same: HashMap<(usize, String), usize> = HashMap::new();
    for e in entries.iter().filter(|e| matches!(e.kind, "main" | "cli")) {
        *same.entry((e.node, e.label.clone())).or_default() += 1;
    }
    for e in entries.iter_mut() {
        if matches!(e.kind, "main" | "cli")
            && same.get(&(e.node, e.label.clone())).is_some_and(|&c| c > 1)
        {
            let file = e
                .sym
                .file
                .rsplit('/')
                .next()
                .unwrap_or(&e.sym.file)
                .to_string();
            e.label = format!("{} ({file})", e.label);
        }
    }
    // Prominence: callers from other units for routes and RPCs.
    let mut callers_of: HashMap<String, BTreeSet<usize>> = HashMap::new();
    for (u, x) in inp.uses.iter().enumerate() {
        for k in x.calls.keys.keys() {
            callers_of.entry(k.clone()).or_default().insert(u);
        }
    }
    for e in &mut entries {
        let callers = e
            .key
            .as_ref()
            .and_then(|k| callers_of.get(k))
            .map(|s| s.iter().filter(|&&u| u != e.unit).count())
            .unwrap_or(0);
        e.prominence += 2 * callers.min(3) as i64;
    }
    entries.sort_by(|a, b| {
        a.node
            .cmp(&b.node)
            .then_with(|| b.prominence.cmp(&a.prominence))
            .then_with(|| kind_rank(a.kind).cmp(&kind_rank(b.kind)))
            .then_with(|| a.label.cmp(&b.label))
            .then_with(|| a.sym.qual.cmp(&b.sym.qual))
    });

    // ---- flows
    let probable_kinds: Vec<&str> = crate::db::resolver::HEURISTIC_RESOLUTION_KINDS
        .iter()
        .copied()
        .chain([crate::indexer::config::CROSS_SERVICE_KIND])
        .collect();
    let counted = |node: usize| -> bool { nodes[node].place != Place::Strip };
    // Who reads which provided key: reader qualname -> the step to the
    // infrastructure that provides it (secrets first).
    let mut reads: HashMap<String, Step> = HashMap::new();
    let secret_node: HashMap<(usize, String), usize> = found
        .uses
        .iter()
        .filter(|x| x.verb == Verb::ReadsSecrets && x.how == How::Edge)
        .filter_map(|x| Some(((x.unit, x.item.clone()?), res_node[x.res]?)))
        .collect();
    if mode == Mode::Apps {
        let mut config_links: Vec<&LinkView> =
            inp.links.iter().filter(|l| l.family == "config").collect();
        config_links.sort_by_key(|l| (l.from, l.to));
        for l in config_links {
            let Some(chip) = unit_node[l.to] else {
                continue;
            };
            let mut keys: Vec<&String> = l.keys.iter().collect();
            keys.sort_by_key(|k| (!is_secret(k), k.to_string()));
            for k in keys {
                let readers = inp.uses[l.from]
                    .secrets
                    .keys
                    .get(k.as_str())
                    .or_else(|| inp.uses[l.from].config.keys.get(k.as_str()));
                // Several deployments may set one key: the one whose file
                // names the reading app, else the first.
                let aliases = &inp.units[l.from].aliases;
                let provider =
                    inp.uses[l.to]
                        .provides
                        .keys
                        .get(k.as_str())
                        .and_then(|(_, syms)| {
                            syms.iter()
                                .find(|(_, f, _)| {
                                    let path = units::norm(f);
                                    aliases
                                        .iter()
                                        .any(|a| a.len() >= 3 && path.contains(a.as_str()))
                                })
                                .or_else(|| syms.first())
                                .cloned()
                        });
                // A secret from a secret store comes from the store.
                let from = if is_secret(k) {
                    secret_node
                        .get(&(l.from, short_key(k).to_string()))
                        .copied()
                        .unwrap_or(chip)
                } else {
                    chip
                };
                for (q, _, _) in readers.map(|(_, s)| s.as_slice()).unwrap_or(&[]) {
                    reads.entry(q.clone()).or_insert_with(|| Step {
                        node: from,
                        name: format!(
                            "{} {}",
                            if is_secret(k) { "secret" } else { "config" },
                            short_key(k)
                        ),
                        qual: k.clone(),
                        file: provider.as_ref().map(|p| p.1.clone()).unwrap_or_default(),
                        line: provider.as_ref().map(|p| p.2).unwrap_or(0),
                        kind: "CONFIG_READ".to_string(),
                        via: "config",
                        probable: false,
                    });
                }
            }
        }
    }
    let config_of = |q: &str| -> Option<Step> { reads.get(q).cloned() };
    // A message crosses its broker: the topic's resource, at its publisher.
    let mut topic_at: HashMap<String, (usize, String, i64)> = HashMap::new();
    for x in &found.uses {
        if let (Some(t), Some(rn), Verb::Publishes | Verb::Subscribes) =
            (&x.item, res_node[x.res], x.verb)
        {
            let e = (rn, x.ev.file.clone(), x.ev.line);
            if x.verb == Verb::Publishes {
                topic_at.insert(t.clone(), e);
            } else {
                topic_at.entry(t.clone()).or_insert(e);
            }
        }
    }
    let mut publisher_of: HashMap<String, (String, String, i64)> = HashMap::new();
    for x in &found.uses {
        if let (Some(t), Some(q), Verb::Publishes) = (&x.item, &x.qual, x.verb) {
            publisher_of
                .entry(t.clone())
                .or_insert((q.clone(), x.ev.file.clone(), x.ev.line));
        }
    }
    let broker_of = |topic: &str| -> Option<Step> {
        let (rn, file, line) = topic_at.get(topic)?.clone();
        Some(Step {
            node: rn,
            name: topic.to_string(),
            qual: topic.to_string(),
            file,
            line,
            kind: "CHANNEL_PUBLISH".to_string(),
            via: "publish",
            probable: false,
        })
    };
    // The resource a file's code uses, as the last step of a flow.
    let mut file_line: HashMap<(&str, usize), i64> = HashMap::new();
    for x in &found.uses {
        file_line
            .entry((x.ev.file.as_str(), x.res))
            .or_insert(x.ev.line);
    }
    let rank_cat = |c: Cat| match c {
        Cat::Db => 0,
        Cat::Storage => 1,
        Cat::Cache => 2,
        Cat::Bus => 3,
        Cat::Search => 4,
        Cat::Ext => 5,
        _ => 9,
    };
    let res_of = |file: &str| -> Option<Step> {
        let set = found.file_res.get(file)?;
        let i = set
            .iter()
            .copied()
            .filter(|&i| res_node[i].is_some() && rank_cat(found.res[i].cat) < 9)
            .min_by(|&a, &b| {
                rank_cat(found.res[a].cat)
                    .cmp(&rank_cat(found.res[b].cat))
                    .then_with(|| found.res[a].name().cmp(&found.res[b].name()))
            })?;
        let r = &found.res[i];
        Some(Step {
            node: res_node[i]?,
            name: r.name(),
            qual: r.name(),
            file: file.to_string(),
            line: file_line.get(&(file, i)).copied().unwrap_or(0),
            kind: "USES".to_string(),
            via: if r.cat == Cat::Db { "db" } else { "res" },
            probable: false,
        })
    };
    let code_lanes: Vec<bool> = nodes.iter().map(|n| is_code_lane(n.lane)).collect();
    let code_node = |i: usize| code_lanes.get(i).copied().unwrap_or(false);
    let hooks = Hooks {
        config_of: &config_of,
        broker_of: &broker_of,
        res_of: &res_of,
        code_node: &code_node,
    };
    let cfg = TraceConfig {
        max_hops: TRACE_HOPS,
        // Apps cross processes a few calls deep, past generated client
        // stubs; one unit's modules sit near the top of the call tree.
        max_bytes: if mode == Mode::Modules {
            24_000
        } else {
            80_000
        },
        include_snippets: false,
        compact: true,
        ..TraceConfig::default()
    };
    let mut traced = 0usize;
    let mut cands: Vec<Candidate> = Vec::new();
    let mut seen_syms: HashSet<i64> = HashSet::new();
    // No entry point, or none that leads anywhere: start from the functions
    // called from most places instead, and say so.
    let mut central_from = entries.len();
    let mut flows: Vec<Candidate> = Vec::new();
    for pass in 0..2 {
        if pass == 1 {
            if !flows.is_empty() {
                break;
            }
            central_from = entries.len();
            entries.extend(central_entries(&ix, &nodes, &node_of_file)?);
        }
        // Round-robin over nodes, most prominent entry points first.
        let mut by_node: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (i, e) in entries
            .iter()
            .enumerate()
            .skip(if pass == 1 { central_from } else { 0 })
        {
            by_node.entry(e.node).or_default().push(i);
        }
        let mut queue: Vec<usize> = Vec::new();
        for round in 0..TRACES_PER_NODE {
            for list in by_node.values() {
                let mut sorted = list.clone();
                sorted.sort_by(|&a, &b| {
                    entries[b]
                        .prominence
                        .cmp(&entries[a].prominence)
                        .then_with(|| a.cmp(&b))
                });
                if let Some(&i) = sorted.get(round) {
                    queue.push(i);
                }
            }
        }
        let picked: Vec<usize> = queue
            .iter()
            .copied()
            .filter(|&ei| seen_syms.insert(entries[ei].sym.id))
            .take(MAX_TRACES.saturating_sub(traced))
            .collect();
        traced += picked.len();
        let results = trace_all(
            ctx,
            &cfg,
            &picked
                .iter()
                .map(|&ei| entries[ei].sym.id)
                .collect::<Vec<_>>(),
        );
        for (&ei, hops) in picked.iter().zip(results) {
            let e = &entries[ei];
            let Some(hops) = hops else {
                continue;
            };
            // A route or RPC another app calls starts at that caller.
            let caller = if mode == Mode::Apps && matches!(e.kind, "http" | "rpc") {
                e.key
                    .as_ref()
                    .and_then(|k| first_caller(inp, k, e.unit))
                    .and_then(|(u, s)| {
                        let node = unit_node[u]?;
                        (node != e.node && counted(node)).then_some(Step {
                            node,
                            name: s
                                .0
                                .rsplit(['.', ':', '/'])
                                .next()
                                .unwrap_or(&s.0)
                                .to_string(),
                            qual: s.0.clone(),
                            file: s.1.clone(),
                            line: s.2,
                            kind: "ENTRY".to_string(),
                            via: "start",
                            probable: false,
                        })
                    })
            } else if e.kind == "bus" {
                // A subscription starts where the topic is published.
                e.key
                    .as_ref()
                    .and_then(|k| publisher_of.get(short_key(k)))
                    .and_then(|(q, f, l)| {
                        let node = node_of_file(f)?;
                        (node != e.node && counted(node)).then(|| Step {
                            node,
                            name: q.rsplit(['.', ':', '/']).next().unwrap_or(q).to_string(),
                            qual: q.clone(),
                            file: f.clone(),
                            line: *l,
                            kind: "ENTRY".to_string(),
                            via: "start",
                            probable: false,
                        })
                    })
                    // No app on the map publishes it: the broker hands it over.
                    .or_else(|| {
                        let k = e.key.as_ref()?;
                        let mut b = broker_of(short_key(k))?;
                        b.via = "start";
                        b.kind = "ENTRY".to_string();
                        Some(b)
                    })
            } else {
                None
            };
            cands.extend(best_path(
                e,
                ei,
                &hops,
                caller,
                &node_of_file,
                &counted,
                ctx,
                &probable_kinds,
                &hooks,
            ));
        }
        // Few ways in (one `main`): several paths from each, so the page
        // still shows how the parts work together.
        let with_paths: HashSet<usize> = cands.iter().map(|c| c.entry).collect();
        let per_entry = if with_paths.len() >= 3 {
            1
        } else {
            PATHS_PER_ENTRY
        };
        flows = select_flows(cands.clone(), MAX_FLOWS, per_entry);
    }
    let central_used = entries.len() > central_from;
    // A node a flow passes through is on the map.
    for f in &flows {
        for &nd in &f.seq {
            if nodes[nd].place == Place::Off {
                nodes[nd].place = Place::Card;
            }
        }
    }

    // ---- core types, hotspots
    let core = core_types(&ix, &node_of_file)?;
    let hotspots = hotspots(&ix, &node_of_file)?;

    // ---- ranks and the card cap
    for (i, nd) in nodes.iter_mut().enumerate() {
        let ways = entries
            .iter()
            .filter(|e| e.node == i && e.kind != "central")
            .count() as f64;
        let degree: usize = out_links
            .iter()
            .filter(|l| l.from == i || l.to == i)
            .map(|l| l.count)
            .sum();
        nd.rank =
            ways.min(50.0) * 2.0 + (degree as f64).sqrt() * 3.0 + ((nd.symbols + 1) as f64).ln();
    }
    let flow_nodes: HashSet<usize> = flows.iter().flat_map(|f| f.seq.iter().copied()).collect();
    // Resources: their own row and cap, the most used first.
    let mut hidden_res = 0usize;
    for (lane, cap) in [("data", MAX_RESOURCES), ("ext", MAX_EXTERNAL)] {
        let mut list: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].lane == lane && nodes[i].place == Place::Card)
            .collect();
        let users = |i: usize| ragg.keys().filter(|(_, rn)| *rn == i).count();
        list.sort_by(|&a, &b| {
            flow_nodes
                .contains(&b)
                .cmp(&flow_nodes.contains(&a))
                .then_with(|| users(b).cmp(&users(a)))
                .then_with(|| nodes[a].name.cmp(&nodes[b].name))
        });
        let keep = cap.max(list.iter().filter(|c| flow_nodes.contains(c)).count());
        for &i in list.iter().skip(keep) {
            nodes[i].place = Place::Off;
            hidden_res += 1;
        }
    }
    let mut cards: Vec<usize> = (0..nodes.len())
        .filter(|&i| nodes[i].place == Place::Card && is_code_lane(nodes[i].lane))
        .collect();
    cards.sort_by(|&a, &b| {
        flow_nodes
            .contains(&b)
            .cmp(&flow_nodes.contains(&a))
            .then_with(|| nodes[b].rank.total_cmp(&nodes[a].rank))
            .then_with(|| nodes[a].name.cmp(&nodes[b].name))
    });
    let cap = MAX_CARDS.max(cards.iter().filter(|c| flow_nodes.contains(c)).count());
    let mut hidden_cards = 0usize;
    for &c in cards.iter().skip(cap) {
        nodes[c].place = Place::Off;
        hidden_cards += 1;
    }

    // ---- sentences
    let used_by: Vec<BTreeSet<usize>> = {
        let mut ub: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
        for l in inp.links.iter().filter(|l| {
            (l.family == "depends" || (l.family == "code" && l.declared)) && l.from != l.to
        }) {
            if !units[l.from].kind.hidden() {
                ub[l.to].insert(l.from);
            }
        }
        ub
    };
    let core_by_node = {
        let mut m: HashMap<usize, Vec<String>> = HashMap::new();
        for c in &core {
            let e = m.entry(c.node).or_default();
            if e.len() < 3 {
                e.push(c.sym.name.clone());
            }
        }
        m
    };
    for (i, nd) in nodes.iter_mut().enumerate() {
        if nd.kind == "module" {
            let s = module_sentence(
                nd.langs.first().map(String::as_str),
                nd.files,
                core_by_node.get(&i).map(Vec::as_slice).unwrap_or(&[]),
            );
            nd.sentence = s;
            continue;
        }
        if nd.kind == "resource" {
            let r = &found.res[found_index(&res_node, i)];
            let aggs: Vec<&ResAgg> = ragg
                .iter()
                .filter(|((_, rn), _)| *rn == i)
                .map(|(_, a)| a)
                .collect();
            // Libraries that use it directly, for a resource no app reaches.
            let libs: BTreeSet<String> = found
                .uses
                .iter()
                .filter(|x| {
                    res_node[x.res] == Some(i)
                        && matches!(units[x.unit].kind, Kind::Lib | Kind::Loose)
                })
                .map(|x| units[x.unit].name.clone())
                .collect();
            nd.sentence = resource_sentence(r, &aggs, mode == Mode::Modules, &libs);
            nd.res = Some(resource_json(r));
            continue;
        }
        if nd.kind == "infra" {
            let (keys, readers) = chip_keys.get(&i).cloned().unwrap_or_default();
            let secrets = keys.iter().filter(|k| is_secret(k)).count();
            let provided: usize = nd
                .units
                .iter()
                .map(|&u| inp.uses[u].provides.keys.len())
                .sum();
            let tool = chip_of_tool
                .iter()
                .find(|(_, v)| **v == i)
                .map(|(k, _)| k.clone())
                .unwrap_or_default();
            let s = chip_sentence(
                &tool,
                nd.units.len(),
                provided.max(keys.len()) - secrets.min(provided.max(keys.len())),
                secrets,
                readers.len(),
            );
            nd.sentence = s;
            continue;
        }
        let u = nd.units[0];
        if let Some(d) = &units[u].description {
            nd.sentence = d.clone();
            nd.sentence_from = "description";
            continue;
        }
        let x = &inp.uses[u];
        let calls_to = |fam: &str| -> usize {
            inp.links
                .iter()
                .filter(|l| l.family == fam && l.from == u && l.to != u)
                .map(|l| l.to)
                .collect::<BTreeSet<_>>()
                .len()
        };
        let facts = Facts {
            lang: nd.langs.first().cloned(),
            kind: nd.kind.clone(),
            why: units[u].why.clone(),
            http_routes: x.http.keys.keys().filter(|k| !is_probe(k)).count(),
            rpc_methods: x.rpc.keys.len(),
            rpc_services: x
                .rpc
                .keys
                .keys()
                .map(|k| rpc_service(k))
                .collect::<BTreeSet<_>>()
                .len(),
            topics_in: x.bus.keys.len(),
            topics_out: x.topics.keys.len(),
            calls_http: calls_to("http"),
            calls_rpc: calls_to("rpc"),
            config_keys: x.config.keys.len(),
            secrets: x.secrets.keys.len(),
            used_by: if units[u].kind.is_app() {
                0
            } else {
                used_by[u].len()
            },
            files: nd.files,
            command: (units[u].kind == Kind::Cli)
                .then(|| units[u].entries.iter().next().cloned())
                .flatten(),
            resources: {
                let mut names: BTreeSet<String> = ragg
                    .keys()
                    .filter(|(a, _)| *a == i)
                    .map(|(_, rn)| &found.res[found_index(&res_node, *rn)])
                    .filter(|r| !matches!(r.cat, Cat::Secrets | Cat::Config))
                    .map(|r| r.in_sentence())
                    .collect();
                if nd.place == Place::Strip {
                    names.extend(
                        found
                            .uses
                            .iter()
                            .filter(|x| x.unit == u && res_node[x.res].is_some())
                            .map(|x| &found.res[x.res])
                            .filter(|r| !matches!(r.cat, Cat::Secrets | Cat::Config))
                            .map(|r| r.in_sentence()),
                    );
                }
                names.into_iter().collect()
            },
        };
        nd.sentence = card_sentence(&facts);
    }

    // ---- layout: lanes, layers, order
    let lanes_order: &[&str] = match mode {
        Mode::Apps => &["front", "svc", "work", "data", "ext", "infra"],
        Mode::Packages => &["apps", "lib", "data", "ext", "infra"],
        Mode::Modules => &["apps", "mod", "lib", "data", "ext", "infra"],
    };
    let shown = |p: Place| matches!(p, Place::Card | Place::Chip);
    let drawn: Vec<(usize, usize)> = out_links
        .iter()
        .filter(|l| !l.quiet && shown(nodes[l.from].place) && shown(nodes[l.to].place))
        .map(|l| (l.from, l.to))
        .collect();
    let mut lanes: Vec<Value> = Vec::new();
    let mut layer_of: Vec<i64> = vec![-1; nodes.len()];
    let mut order_of: Vec<i64> = vec![-1; nodes.len()];
    let mut all_layer: Vec<i64> = vec![-1; nodes.len()];
    let mut all_order: Vec<i64> = vec![-1; nodes.len()];
    let all_edges: Vec<(usize, usize)> = out_links
        .iter()
        .filter(|l| !l.quiet)
        .map(|l| (l.from, l.to))
        .collect();
    for lane in lanes_order {
        let members: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].lane == *lane && shown(nodes[i].place))
            .collect();
        let everyone: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].lane == *lane && nodes[i].place != Place::Strip)
            .collect();
        for (set, edges, layer, order) in [
            (&members, &drawn, &mut layer_of, &mut order_of),
            (&everyone, &all_edges, &mut all_layer, &mut all_order),
        ] {
            let names: Vec<String> = set.iter().map(|&i| nodes[i].name.clone()).collect();
            let local: Vec<(usize, usize)> = edges
                .iter()
                .filter_map(|(a, b)| {
                    Some((
                        set.iter().position(|x| x == a)?,
                        set.iter().position(|x| x == b)?,
                    ))
                })
                .collect();
            let (ly, ord) = if *lane == "infra" {
                let mut idx: Vec<usize> = (0..set.len()).collect();
                idx.sort_by(|&a, &b| names[a].cmp(&names[b]));
                let mut o = vec![0usize; set.len()];
                for (k, &i) in idx.iter().enumerate() {
                    o[i] = k;
                }
                (vec![0usize; set.len()], o)
            } else if !is_code_lane(lane) {
                // Resources under the apps that use them: by the mean
                // position of their users, then by name.
                let mut idx: Vec<usize> = (0..set.len()).collect();
                let at = |i: usize| -> f64 {
                    let users: Vec<f64> = edges
                        .iter()
                        .filter(|(_, b)| *b == set[i])
                        .filter_map(|(a, _)| {
                            let o = order[*a];
                            (o >= 0).then_some(o as f64)
                        })
                        .collect();
                    if users.is_empty() {
                        f64::MAX
                    } else {
                        users.iter().sum::<f64>() / users.len() as f64
                    }
                };
                idx.sort_by(|&a, &b| {
                    at(a)
                        .total_cmp(&at(b))
                        .then_with(|| names[a].cmp(&names[b]))
                });
                let mut o = vec![0usize; set.len()];
                for (k, &i) in idx.iter().enumerate() {
                    o[i] = k;
                }
                (vec![0usize; set.len()], o)
            } else {
                layer_lane(&names, &local)
            };
            for (k, &i) in set.iter().enumerate() {
                layer[i] = ly[k] as i64;
                order[i] = ord[k] as i64;
            }
        }
        if !everyone.is_empty() {
            lanes.push(json!({ "id": lane, "label": lane_label(lane) }));
        }
    }

    // ---- read first, summary
    let read_first = read_first(inp.repo_root);
    let summary = summary(inp, mode, &nodes);

    // ---- JSON
    let entry_flow: HashMap<usize, usize> = flows
        .iter()
        .enumerate()
        .map(|(fi, f)| (f.entry, fi))
        .collect();
    let flows_json: Vec<Value> = flows
        .iter()
        .map(|f| {
            let e = &entries[f.entry];
            // The title starts at the entry point: a caller before it is
            // step 1, not part of the name.
            let caller = f.steps[0].qual != e.sym.qual;
            let mut names: Vec<String> = f
                .seq
                .iter()
                .skip(usize::from(caller))
                .map(|&nd| nodes[nd].name.clone())
                .collect();
            // A database reached inside a part is named too.
            if f.steps
                .iter()
                .any(|s| s.via == "db" && nodes[s.node].kind != "resource")
                && !names.iter().any(|n| n == "database")
            {
                names.push("database".to_string());
            }
            json!({
                "title": flow_title(&e.label, &names),
                "label": e.label,
                "caller": caller,
                "entry": f.entry,
                "score": f.score,
                "nodes": f.seq,
                "probable": f.steps.iter().any(|s| s.probable),
                "ends": if f.unresolved_end { "unresolved" } else { "" },
                "steps": f.steps.iter().map(|s| json!({
                    "node": s.node, "name": s.name, "qual": s.qual, "file": s.file, "line": s.line,
                    "kind": s.kind, "via": s.via, "probable": s.probable,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    let entries_json: Vec<Value> = {
        let mut per: HashMap<usize, usize> = HashMap::new();
        entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                let c = per.entry(e.node).or_default();
                *c += 1;
                *c <= ENTRIES_PER_NODE
            })
            .map(|(i, e)| {
                json!({
                    "id": i, "node": e.node, "kind": e.kind, "label": e.label,
                    "name": e.sym.name, "qual": e.sym.qual, "file": e.sym.file, "line": e.sym.line,
                    "flow": entry_flow.get(&i),
                })
            })
            .collect()
    };
    let ways = |i: usize| -> Value {
        // Distinct ways in: two handlers of one route are one route.
        let mut m: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for e in entries
            .iter()
            .filter(|e| e.node == i && e.kind != "central")
        {
            m.entry(e.kind).or_default().insert(e.label.as_str());
        }
        json!(
            m.into_iter()
                .map(|(k, v)| (k, v.len()))
                .collect::<BTreeMap<_, _>>()
        )
    };
    let deployed_by = |i: usize| -> Vec<String> {
        let us: HashSet<usize> = nodes[i].units.iter().copied().collect();
        let mut v: Vec<String> = inp
            .links
            .iter()
            .filter(|l| l.family == "deploys" && us.contains(&l.to))
            .filter_map(|l| l.evidence.first().map(|e| e.1.clone()))
            .collect();
        v.sort();
        v.dedup();
        v.truncate(4);
        v
    };
    // Infrastructure a chip declares beyond the resources on the map.
    let other_infra = |i: usize| -> Value {
        if nodes[i].kind != "infra" {
            return Value::Null;
        }
        let mut m: BTreeMap<String, usize> = BTreeMap::new();
        let mut runs: BTreeSet<&str> = BTreeSet::new();
        for u in &nodes[i].units {
            for (k, c) in found.other.get(u).into_iter().flatten() {
                *m.entry(k.clone()).or_default() += c;
            }
            runs.extend(found.runtimes.get(u).into_iter().flatten());
        }
        let mut v: Vec<(String, usize)> = m.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        json!({
            "types": v.iter().take(12).map(|(k, c)| json!([k, c])).collect::<Vec<_>>(),
            "total": v.iter().map(|(_, c)| c).sum::<usize>(),
            "runtimes": runs,
        })
    };
    let deploy = deploy_groups(inp, &found, &nodes, &unit_node, mode);
    let users_of = |i: usize| -> Vec<usize> {
        let mut v: BTreeSet<usize> = BTreeSet::new();
        for &u in &nodes[i].units {
            for &x in &used_by[u] {
                if let Some(nd) = unit_node[x]
                    && nd != i
                    && nodes[nd].lane != "lib"
                {
                    v.insert(nd);
                }
            }
        }
        v.into_iter().collect()
    };
    let nodes_json: Vec<Value> = nodes
        .iter()
        .enumerate()
        .map(|(i, nd)| {
            json!({
                "id": i, "name": nd.name, "kind": nd.kind, "lane": nd.lane, "units": nd.units,
                "dir": nd.dir, "place": nd.place.as_str(), "files": nd.files, "symbols": nd.symbols,
                "langs": nd.langs, "sentence": nd.sentence, "sentence_from": nd.sentence_from,
                "layer": layer_of[i], "order": order_of[i], "all_layer": all_layer[i], "all_order": all_order[i],
                "ways": ways(i),
                "key_types": core.iter().filter(|c| c.node == i).take(4).map(|c| c.to_json()).collect::<Vec<_>>(),
                "flows": flows.iter().enumerate().filter(|(_, f)| entries[f.entry].node == i).map(|(fi, _)| fi).collect::<Vec<_>>(),
                "deployed_by": deployed_by(i),
                "used_by": if nd.lane == "lib" || mode != Mode::Apps { json!(users_of(i)) } else { json!([]) },
                "res": nd.res,
                "other": other_infra(i),
                "deploy": deploy.of.get(&i).cloned().unwrap_or_default(),
            })
        })
        .collect();
    let links_json: Vec<Value> = out_links
        .iter()
        .map(|l| {
            json!({
                "from": l.from, "to": l.to, "family": l.family, "label": l.label, "count": l.count,
                "quiet": l.quiet, "calls": l.calls,
            })
        })
        .collect();
    let mut doc = json!({
        "mode": mode.as_str(),
        "summary": summary,
        "nodes": nodes_json,
        "lanes": lanes,
        "links": links_json,
        "entries": entries_json,
        "flows": flows_json,
        "core_types": core.iter().take(8).map(|c| c.to_json()).collect::<Vec<_>>(),
        "hotspots": hotspots.iter().map(|h| h.to_json()).collect::<Vec<_>>(),
        "read_first": read_first,
        "central": central_used,
        "deploy": deploy.groups,
        "hidden": {
            "cards": hidden_cards,
            "resources": hidden_res,
            "tests": units.iter().filter(|u| u.kind.hidden()).count(),
            "entries": entries.len().saturating_sub(entries_json_len(&entries)),
            "traced": traced,
        },
    });
    doc["tour"] = json!(tour(&doc));
    doc["questions"] = json!(questions(&doc));
    Ok(doc)
}

/// The resource a resource node stands for.
fn found_index(res_node: &[Option<usize>], node: usize) -> usize {
    res_node.iter().position(|n| *n == Some(node)).unwrap_or(0)
}

/// What a flow looks up as it is built: the config a symbol reads, the
/// broker of a topic, the resource a file's code uses.
struct Hooks<'a> {
    config_of: &'a dyn Fn(&str) -> Option<Step>,
    broker_of: &'a dyn Fn(&str) -> Option<Step>,
    res_of: &'a dyn Fn(&str) -> Option<Step>,
    /// A node of code (an app, library or module), not infrastructure.
    code_node: &'a dyn Fn(usize) -> bool,
}

/// What one node does with one resource: verbs, the items they name, and
/// the evidence.
#[derive(Default)]
struct ResAgg {
    verbs: BTreeSet<Verb>,
    /// Topics, secrets, SQL objects, calls: the verbs that name each.
    items: BTreeMap<String, BTreeSet<Verb>>,
    hows: BTreeSet<How>,
    vias: BTreeSet<usize>,
    ev: Vec<resources::Use>,
}

impl ResAgg {
    fn add(&mut self, x: &resources::Use, _units: &[Unit]) {
        self.verbs.insert(x.verb);
        self.hows.insert(x.how);
        if let Some(v) = x.via {
            self.vias.insert(v);
        }
        if let Some(item) = &x.item
            && x.how == How::Edge
        {
            self.items.entry(item.clone()).or_default().insert(x.verb);
        }
        self.ev.push(x.clone());
    }

    fn named(&self, verb: Verb) -> Vec<&String> {
        self.items
            .iter()
            .filter(|(_, v)| v.contains(&verb))
            .map(|(k, _)| k)
            .collect()
    }

    /// The verb of the line: "publishes order-created", "reads 4 secrets",
    /// "reads/writes", "stores blobs", "calls over HTTP ×3".
    fn label(&self, cat: Cat, tech: &str, units: &[Unit]) -> String {
        let has = |v: Verb| self.verbs.contains(&v);
        let code = self
            .hows
            .iter()
            .any(|h| matches!(h, How::Edge | How::Import | How::Via));
        let base = match cat {
            Cat::Bus => {
                let (p, s) = (self.named(Verb::Publishes), self.named(Verb::Subscribes));
                match (p.len(), s.len()) {
                    (0, 0) if has(Verb::Subscribes) => "subscribes".to_string(),
                    (0, 0) => "uses".to_string(),
                    (1, 0) => format!("publishes {}", p[0]),
                    (n, 0) => format!("publishes {n} topics"),
                    (0, 1) => format!("subscribes to {}", s[0]),
                    (0, n) => format!("subscribes to {n} topics"),
                    (a, b) => format!(
                        "publishes {a}, subscribes to {}",
                        count(b, "topic", "topics")
                    ),
                }
            }
            Cat::Secrets => {
                let s = self.named(Verb::ReadsSecrets);
                match s.len() {
                    0 if has(Verb::ReadsSecrets) && code => "reads secrets".to_string(),
                    0 => "uses".to_string(),
                    1 => format!("reads secret {}", s[0]),
                    n => format!("reads {n} secrets"),
                }
            }
            Cat::Config => "reads config".to_string(),
            Cat::Db if has(Verb::Subscribes) && !code => "reads the change stream".to_string(),
            Cat::Db if has(Verb::ReadsWrites) && code => "reads/writes".to_string(),
            Cat::Storage if has(Verb::Stores) && code => {
                if tech.contains("Blob") {
                    "stores blobs".to_string()
                } else {
                    "stores objects".to_string()
                }
            }
            Cat::Storage if has(Verb::Subscribes) => "is triggered by".to_string(),
            Cat::Search if code => "queries".to_string(),
            Cat::Ext => {
                let calls = self.named(Verb::Calls);
                if !calls.is_empty() {
                    format!("calls over HTTP ×{}", calls.len())
                } else if code {
                    "calls".to_string()
                } else {
                    "uses".to_string()
                }
            }
            _ => "uses".to_string(),
        };
        // Only through one library: say which.
        if self.hows.len() == 1
            && self.hows.contains(&How::Via)
            && let [lib] = self.vias.iter().collect::<Vec<_>>().as_slice()
        {
            return format!("{base} via {}", units[**lib].name);
        }
        base
    }

    /// The evidence behind the line, strongest first.
    fn calls(&self, units: &[Unit]) -> Vec<Value> {
        let mut ev: Vec<&resources::Use> = self.ev.iter().collect();
        ev.sort_by(|a, b| {
            b.how
                .cmp(&a.how)
                .then_with(|| a.ev.file.cmp(&b.ev.file))
                .then_with(|| a.ev.line.cmp(&b.ev.line))
        });
        let mut seen: HashSet<(String, i64, Option<String>)> = HashSet::new();
        let mut out = Vec::new();
        for x in ev {
            if out.len() >= MAX_CALLS {
                break;
            }
            if !seen.insert((x.ev.file.clone(), x.ev.line, x.item.clone())) {
                continue;
            }
            if let (Some(item), Some(q), How::Edge) = (&x.item, &x.qual, x.how) {
                out.push(json!({
                    "key": item,
                    "from": { "qual": q, "file": x.ev.file, "line": x.ev.line },
                }));
                continue;
            }
            let text = match x.how {
                How::Manifest => format!("declares {}", x.ev.text),
                How::Deploy => format!("deployment: {}", x.ev.text),
                How::Via => format!(
                    "through {}: {}",
                    x.via.map(|v| units[v].name.as_str()).unwrap_or("a library"),
                    x.ev.text
                ),
                _ => x.ev.text.clone(),
            };
            out.push(json!({ "text": text, "file": x.ev.file, "line": x.ev.line, "how": x.how.as_str() }));
        }
        out
    }
}

/// The sentence of a resource card: "PostgreSQL database with 4 databases
/// (catalogdb, identitydb and 2 more); used by 3 apps; declared in Aspire."
fn resource_sentence(
    r: &resources::Res,
    aggs: &[&ResAgg],
    modules: bool,
    libs: &BTreeSet<String>,
) -> String {
    let (one, many) = if modules {
        ("part", "parts")
    } else {
        ("app", "apps")
    };
    let noun = match r.cat {
        Cat::Db => "database",
        Cat::Cache => "cache",
        Cat::Search => "search index",
        Cat::Bus => "message broker",
        Cat::Secrets => "secret store",
        Cat::Config => "config store",
        Cat::Storage => "object storage",
        Cat::Ext => "external service",
    };
    let mut s = if r.tech.is_empty() {
        format!(
            "{} (the repository names no {})",
            r.name(),
            if r.cat == Cat::Bus {
                "broker"
            } else {
                "technology"
            }
        )
    } else if r.cat == Cat::Ext && r.tech.contains('.') {
        format!("External API at {}", r.tech)
    } else {
        format!("{} {noun}", r.tech)
    };
    // Named parts, by kind.
    let mut by_kind: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
    for (name, k) in &r.items {
        if *k != "call" && *k != "object" {
            by_kind.entry(k).or_default().push(name);
        }
    }
    let parts: Vec<String> = by_kind
        .iter()
        .map(|(k, names)| {
            let shown: Vec<String> = names.iter().take(3).map(|n| n.to_string()).collect();
            let mut list = shown.clone();
            if names.len() > 3 {
                list.push(format!("{} more", names.len() - 3));
            }
            let plural = match *k {
                "database" => "databases",
                "topic" => "topics",
                "queue" => "queues",
                "subscription" => "subscriptions",
                "secret" => "secrets",
                "container" => "containers",
                "table" => "tables",
                "stream" => "streams",
                "parameter" => "parameters",
                "service" => "services",
                "chart" => "charts",
                _ => "resources",
            };
            format!("{} ({})", count(names.len(), k, plural), and_list(&list))
        })
        .collect();
    if !parts.is_empty() {
        s = format!("{s} with {}", and_list(&parts));
    }
    let mut rest: Vec<String> = Vec::new();
    if r.cat == Cat::Bus {
        let p = aggs
            .iter()
            .filter(|a| a.verbs.contains(&Verb::Publishes))
            .count();
        let sub = aggs
            .iter()
            .filter(|a| a.verbs.contains(&Verb::Subscribes))
            .count();
        let other = aggs.len().saturating_sub(
            aggs.iter()
                .filter(|a| {
                    a.verbs.contains(&Verb::Publishes) || a.verbs.contains(&Verb::Subscribes)
                })
                .count(),
        );
        if p > 0 {
            rest.push(format!(
                "{} publish{}",
                count(p, one, many),
                if p == 1 { "es" } else { "" }
            ));
        }
        if sub > 0 {
            rest.push(format!(
                "{} subscribe{}",
                count(sub, one, many),
                if sub == 1 { "s" } else { "" }
            ));
        }
        if other > 0 && p + sub == 0 {
            rest.push(format!("used by {}", count(other, one, many)));
        } else if other > 0 {
            rest.push(format!(
                "{} more use{} it",
                count(other, one, many),
                if other == 1 { "s" } else { "" }
            ));
        }
    } else if !aggs.is_empty() {
        rest.push(format!("used by {}", count(aggs.len(), one, many)));
    }
    if aggs.is_empty() && !libs.is_empty() {
        let names: Vec<String> = libs.iter().take(3).cloned().collect();
        rest.push(format!(
            "used by the {} {}",
            if libs.len() == 1 {
                "library"
            } else {
                "libraries"
            },
            and_list(&names)
        ));
    }
    let tools: BTreeSet<&str> = r
        .declared
        .iter()
        .filter(|e| e.line > 0)
        .map(|e| match units::infra_kind(&e.file) {
            "Kubernetes" if e.file.ends_with(".cs") => "Aspire",
            "Kubernetes" if e.file.ends_with(".prisma") => "Prisma",
            "Kubernetes" if !(e.file.ends_with(".yaml") || e.file.ends_with(".yml")) => "code",
            t => t,
        })
        .collect();
    if !tools.is_empty() {
        let names: Vec<String> = tools
            .iter()
            .map(|t| match *t {
                "compose" => "Docker Compose".to_string(),
                "code" => "the app host".to_string(),
                t => t.to_string(),
            })
            .collect();
        rest.push(format!("declared in {}", and_list(&names)));
    }
    if aggs.is_empty() && libs.is_empty() {
        rest.push("no code here was found using it".to_string());
    }
    if rest.is_empty() {
        format!("{s}.")
    } else {
        format!("{s}; {}.", rest.join("; "))
    }
}

/// A resource's detail for the side column.
fn resource_json(r: &resources::Res) -> Value {
    json!({
        "cat": r.cat.as_str(),
        "noun": r.cat.noun(),
        "tech": r.tech,
        "items": r.items.iter().take(60).map(|(k, v)| json!([k, v])).collect::<Vec<_>>(),
        "total_items": r.items.len(),
        "declared": r.declared.iter().take(8).map(|e| json!({ "text": e.text, "file": e.file, "line": e.line })).collect::<Vec<_>>(),
        "kinds": r.kinds.iter().map(|(k, n)| json!([k, n])).collect::<Vec<_>>(),
    })
}

/// A deployment group being built: its tool, nodes and files.
type DeployGroup = (&'static str, BTreeSet<usize>, BTreeSet<String>);

/// Where the apps run: deployment groups for the overlay.
struct Deploy {
    groups: Vec<Value>,
    /// Node -> its groups, the one it is drawn in first.
    of: HashMap<usize, Vec<usize>>,
}

fn deploy_groups(
    inp: &Input,
    found: &Found,
    nodes: &[Node],
    unit_node: &[Option<usize>],
    mode: Mode,
) -> Deploy {
    let units = inp.units;
    let mut out = Deploy {
        groups: Vec::new(),
        of: HashMap::new(),
    };
    if mode == Mode::Modules {
        return out;
    }
    let app_node = |u: usize| -> Option<usize> {
        let nd = unit_node[u]?;
        (is_code_lane(nodes[nd].lane) && nodes[nd].place != Place::Strip).then_some(nd)
    };
    // label -> (tool, nodes, files)
    let mut groups: BTreeMap<String, DeployGroup> = BTreeMap::new();
    for (i, t, ev) in units::deployments(units) {
        let Some(nd) = app_node(t) else { continue };
        let infra = &units[i];
        let root = infra.display_root();
        let tool: &'static str = if infra.why == "Aspire" {
            "Aspire"
        } else {
            units::infra_kind(&ev.file)
        };
        let runtimes: Vec<&str> = found
            .runtimes
            .get(&i)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let label = match tool {
            "Kubernetes" | "Helm" => {
                let dir = ev.file.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
                let ns = infra
                    .decls
                    .iter()
                    .find(|d| {
                        d.source == "k8s"
                            && d.ev.file == ev.file
                            && d.span.0 <= ev.line
                            && ev.line <= d.span.1
                            && d.ns.is_some()
                    })
                    .or_else(|| {
                        infra.decls.iter().find(|d| {
                            d.what == "kind:Kustomization"
                                && dir.starts_with(
                                    d.ev.file.rsplit_once('/').map(|(x, _)| x).unwrap_or(""),
                                )
                        })
                    })
                    .and_then(|d| d.ns.clone());
                let chart = infra
                    .decls
                    .iter()
                    .find(|d| d.source == "helm" && d.what == "chart")
                    .map(|d| d.name.clone());
                match (tool, ns, chart) {
                    (_, Some(ns), _) => format!("Kubernetes namespace {ns}"),
                    ("Helm", None, Some(c)) => format!("Helm chart {c}"),
                    ("Helm", None, None) => format!("Helm ({root})"),
                    _ => format!("Kubernetes ({root})"),
                }
            }
            "compose" => format!("Docker Compose ({})", ev.file),
            "Aspire" => format!("Aspire app host {}", infra.name),
            "Bicep" | "Terraform" => match runtimes.first() {
                Some(rt) => format!("{rt} ({tool}, {root})"),
                None => format!("{tool} ({root})"),
            },
            other => format!("{other} ({root})"),
        };
        let g = groups
            .entry(label)
            .or_insert_with(|| (tool, BTreeSet::new(), BTreeSet::new()));
        g.1.insert(nd);
        g.2.insert(ev.file.clone());
    }
    for (u, runs) in &found.runs_on {
        let Some(nd) = app_node(*u) else { continue };
        for (rt, ev) in runs {
            let g = groups
                .entry(format!("{rt} (Serverless Framework)"))
                .or_insert_with(|| ("Serverless", BTreeSet::new(), BTreeSet::new()));
            g.1.insert(nd);
            g.2.insert(ev.file.clone());
        }
    }
    // Apps no deployment file names.
    let placed: HashSet<usize> = groups.values().flat_map(|g| g.1.iter().copied()).collect();
    for (u, unit) in units.iter().enumerate() {
        let Some(nd) = app_node(u) else { continue };
        if placed.contains(&nd) || !unit.kind.is_app() {
            continue;
        }
        let (label, tool) = if unit.dockerfiles.is_empty() {
            (
                "Not deployed by files in this repository".to_string(),
                "none",
            )
        } else {
            ("Container images (Dockerfile only)".to_string(), "docker")
        };
        let g = groups
            .entry(label)
            .or_insert_with(|| (tool, BTreeSet::new(), BTreeSet::new()));
        g.1.insert(nd);
        g.2.extend(unit.dockerfiles.iter().cloned());
    }
    let rank = |tool: &str| -> usize {
        match tool {
            "Kubernetes" => 0,
            "Helm" => 1,
            "compose" => 2,
            "Aspire" => 3,
            "Bicep" | "Terraform" => 4,
            "Serverless" => 5,
            "Skaffold" => 6,
            "docker" => 7,
            _ => 8,
        }
    };
    let list: Vec<(String, DeployGroup)> = groups.into_iter().collect();
    // Each node is drawn in one group: the first a deployment runs it in.
    let mut primary: HashMap<usize, usize> = HashMap::new();
    for (gi, (label, (tool, ns, _))) in list.iter().enumerate() {
        for &nd in ns {
            // The deployment that runs it (Kubernetes before a dev
            // compose file or a build tool), then the biggest group.
            let better = match primary.get(&nd) {
                None => true,
                Some(&o) => {
                    let (ol, (ot, ons, _)) = &list[o];
                    (
                        std::cmp::Reverse(rank(tool)),
                        ns.len(),
                        std::cmp::Reverse(label),
                    ) > (
                        std::cmp::Reverse(rank(ot)),
                        ons.len(),
                        std::cmp::Reverse(ol),
                    )
                }
            };
            if better {
                primary.insert(nd, gi);
            }
        }
    }
    for (gi, (label, (tool, ns, files))) in list.iter().enumerate() {
        let n = ns.len();
        let sentence = match *tool {
            "Kubernetes" => format!(
                "Runs as {} {}.",
                count(n, "container", "containers"),
                label
                    .strip_prefix("Kubernetes namespace ")
                    .map(|x| format!("in Kubernetes namespace {x}"))
                    .unwrap_or_else(|| "on Kubernetes".to_string())
            ),
            "Helm" => format!("Runs as {} from {label}.", count(n, "service", "services")),
            "compose" => format!("Runs as {} in {label}.", count(n, "service", "services")),
            "Aspire" => format!(
                "{} run together by the .NET {label}.",
                count(n, "app", "apps")
            ),
            "Bicep" | "Terraform" => format!("{} deployed with {label}.", count(n, "app", "apps")),
            "Skaffold" => format!("{} built and run by {label}.", count(n, "app", "apps")),
            "Serverless" => format!(
                "{} run as AWS Lambda functions (Serverless Framework).",
                count(n, "app", "apps")
            ),
            "docker" => format!(
                "{} built as container images; no deployment file here runs them.",
                count(n, "app", "apps")
            ),
            _ => format!(
                "{}: no Dockerfile or deployment file here runs them.",
                count(n, "app", "apps")
            ),
        };
        let mut members: Vec<usize> = ns.iter().copied().collect();
        members.sort_by_key(|nd| (primary.get(nd) != Some(&gi), *nd));
        for &nd in ns {
            let e = out.of.entry(nd).or_default();
            if primary.get(&nd) == Some(&gi) {
                e.insert(0, gi);
            } else {
                e.push(gi);
            }
        }
        out.groups.push(json!({
            "id": gi, "label": label, "tool": tool, "sentence": sentence,
            "nodes": members,
            "primary": ns.iter().filter(|nd| primary.get(nd) == Some(&gi)).collect::<Vec<_>>(),
            "files": files.iter().take(6).collect::<Vec<_>>(),
        }));
    }
    out
}

fn entries_json_len(entries: &[EntryPoint]) -> usize {
    let mut per: HashMap<usize, usize> = HashMap::new();
    entries
        .iter()
        .filter(|e| {
            let c = per.entry(e.node).or_default();
            *c += 1;
            *c <= ENTRIES_PER_NODE
        })
        .count()
}

fn kind_rank(k: &str) -> usize {
    match k {
        "http" => 0,
        "rpc" => 1,
        "bus" => 2,
        "cli" => 3,
        _ => 4,
    }
}

/// RPC service of a route key: `/pkg.Greeter/SayHello` -> `pkg.Greeter`.
fn rpc_service(key: &str) -> String {
    let k = key.trim_start_matches('/');
    match k.rsplit_once('/') {
        Some((svc, _)) => svc.to_string(),
        None => k.rsplit_once('.').map(|(s, _)| s).unwrap_or(k).to_string(),
    }
}

/// The verb of a link that crosses a process, with its count.
pub fn process_label(family: &str, keys: &BTreeSet<String>) -> String {
    let n = keys.len();
    let one = || {
        keys.iter()
            .next()
            .map(|k| short_key(k).to_string())
            .unwrap_or_default()
    };
    match family {
        "http" => format!("calls over HTTP ×{n}"),
        "rpc" => format!("calls RPC ×{n}"),
        "bus" if n == 1 => format!("publishes {}", one()),
        "bus" => format!("publishes {n} topics"),
        "config" => {
            let secrets = keys.iter().filter(|k| is_secret(k)).count();
            match (n, secrets) {
                (1, 1) => format!("reads secret {}", one()),
                (1, _) => format!("reads config {}", one()),
                (_, s) if s == n => format!("reads {n} secrets"),
                (_, 0) => format!("reads {n} config keys"),
                (_, s) => format!(
                    "reads {} and {}",
                    count(s, "secret", "secrets"),
                    count(n - s, "config key", "config keys")
                ),
            }
        }
        _ => format!("{family} ×{n}"),
    }
}

/// The verb of an in-process link from its edge kinds: the most common.
pub fn code_label(kinds: &BTreeMap<String, usize>) -> String {
    let total: usize = kinds.values().sum();
    if total == 0 {
        return "depends on".to_string();
    }
    let mut verbs: BTreeMap<&str, usize> = BTreeMap::new();
    for (k, c) in kinds {
        let v = match k.as_str() {
            "CALLS" | "DISPATCH" => "calls",
            "IMPORTS" => "imports",
            "EXTENDS" | "IMPLEMENTS" | "INHERITS" => "extends",
            _ => "references",
        };
        *verbs.entry(v).or_default() += c;
    }
    let verb = verbs
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
        .map(|(v, _)| *v)
        .unwrap_or("references");
    format!("{verb} ×{total}")
}

/// What lies behind a link: per key, the sending and receiving symbol.
fn link_calls(
    inp: &Input,
    nodes: &[Node],
    a: usize,
    b: usize,
    family: &str,
    keys: &BTreeSet<String>,
) -> Vec<Value> {
    let first = |units: &[usize],
                 pick: &dyn Fn(&UnitUse) -> &super::apps::Usage,
                 key: &str|
     -> Option<SymRef> {
        units.iter().find_map(|&u| {
            pick(&inp.uses[u])
                .keys
                .get(key)
                .and_then(|(_, s)| s.first().cloned())
        })
    };
    let send: &dyn Fn(&UnitUse) -> &super::apps::Usage = match family {
        "bus" => &|x: &UnitUse| &x.topics,
        "config" => &|x: &UnitUse| &x.config,
        _ => &|x: &UnitUse| &x.calls,
    };
    let recv: &dyn Fn(&UnitUse) -> &super::apps::Usage = match family {
        "http" => &|x: &UnitUse| &x.http,
        "rpc" => &|x: &UnitUse| &x.rpc,
        "bus" => &|x: &UnitUse| &x.bus,
        _ => &|x: &UnitUse| &x.provides,
    };
    keys.iter()
        .take(MAX_CALLS)
        .map(|k| {
            let s = first(&nodes[a].units, send, k)
                .or_else(|| first(&nodes[a].units, &|x: &UnitUse| &x.secrets, k));
            let r = first(&nodes[b].units, recv, k);
            let side =
                |s: Option<SymRef>| s.map(|(q, f, l)| json!({ "qual": q, "file": f, "line": l }));
            json!({ "key": short_key(k), "from": side(s), "to": side(r) })
        })
        .collect()
}

/// The first symbol of another unit that calls `key`: `(unit, symbol)`.
fn first_caller(inp: &Input, key: &str, not: usize) -> Option<(usize, SymRef)> {
    let mut best: Option<(usize, SymRef)> = None;
    for (u, x) in inp.uses.iter().enumerate() {
        if u == not || inp.units[u].kind.hidden() {
            continue;
        }
        if let Some((_, syms)) = x.calls.keys.get(key)
            && let Some(s) = syms.iter().min()
            && best.as_ref().is_none_or(|(bu, bs)| {
                (inp.units[u].name.as_str(), s) < (inp.units[*bu].name.as_str(), bs)
            })
        {
            best = Some((u, s.clone()));
        }
    }
    best
}

/// Layers and order of a lane's nodes: the longest path along its links
/// (callers before callees), then within a layer by where the nodes they
/// are reached from sit, then by name.
pub fn layer_lane(names: &[String], edges: &[(usize, usize)]) -> (Vec<usize>, Vec<usize>) {
    let n = names.len();
    let mut layer = vec![0usize; n];
    // Edges out of a cycle are ignored: only edges that keep the order of a
    // DFS from the nodes sorted by name.
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_by(|&a, &b| names[a].cmp(&names[b]));
    let mut state = vec![0u8; n];
    let mut keep: Vec<(usize, usize)> = Vec::new();
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(a, b) in edges {
        if a != b && !adj[a].contains(&b) {
            adj[a].push(b);
        }
    }
    for a in &mut adj {
        a.sort_by(|&x, &y| names[x].cmp(&names[y]));
    }
    fn dfs(v: usize, adj: &[Vec<usize>], state: &mut [u8], keep: &mut Vec<(usize, usize)>) {
        state[v] = 1;
        for &w in &adj[v] {
            if state[w] == 0 {
                keep.push((v, w));
                dfs(w, adj, state, keep);
            } else if state[w] == 2 {
                keep.push((v, w));
            }
        }
        state[v] = 2;
    }
    // Sources first, so a chain starts at its head.
    let has_in: Vec<bool> = (0..n).map(|v| adj.iter().any(|a| a.contains(&v))).collect();
    for &v in idx.iter().filter(|&&v| !has_in[v]).chain(idx.iter()) {
        if state[v] == 0 {
            dfs(v, &adj, &mut state, &mut keep);
        }
    }
    for _ in 0..n {
        let mut changed = false;
        for &(a, b) in &keep {
            if layer[b] < layer[a] + 1 {
                layer[b] = layer[a] + 1;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    let max = layer.iter().copied().max().unwrap_or(0);
    let mut order = vec![0usize; n];
    let mut pos = vec![0f64; n];
    for l in 0..=max {
        let mut members: Vec<usize> = (0..n).filter(|&v| layer[v] == l).collect();
        let bary = |v: usize| -> f64 {
            let preds: Vec<f64> = keep
                .iter()
                .filter(|(_, b)| *b == v)
                .map(|(a, _)| pos[*a])
                .collect();
            if preds.is_empty() || l == 0 {
                f64::MAX
            } else {
                preds.iter().sum::<f64>() / preds.len() as f64
            }
        };
        members.sort_by(|&a, &b| {
            bary(a)
                .total_cmp(&bary(b))
                .then_with(|| names[a].cmp(&names[b]))
        });
        for (k, &v) in members.iter().enumerate() {
            order[v] = k;
            pos[v] = k as f64;
        }
    }
    (layer, order)
}

#[allow(clippy::too_many_arguments)]
fn best_path(
    e: &EntryPoint,
    ei: usize,
    hops: &[TraceHop],
    caller: Option<Step>,
    node_of_file: &dyn Fn(&str) -> Option<usize>,
    counted: &dyn Fn(usize) -> bool,
    ctx: &Ctx,
    probable_kinds: &[&str],
    hooks: &Hooks,
) -> Vec<Candidate> {
    let config_of = hooks.config_of;
    let by_id: HashMap<i64, usize> = {
        let mut m = HashMap::new();
        for (i, h) in hops.iter().enumerate() {
            m.entry(h.symbol.id).or_insert(i);
        }
        m
    };
    let has_child: HashSet<i64> = hops.iter().map(|h| h.predecessor_id).collect();
    let start = Step {
        node: e.node,
        name: e.sym.name.clone(),
        qual: e.sym.qual.clone(),
        file: e.sym.file.clone(),
        line: e.sym.line,
        kind: if caller.is_some() {
            match e.kind {
                "http" => "HTTP_ROUTE".to_string(),
                "bus" => "CHANNEL_SUBSCRIBE".to_string(),
                _ => "RPC_IMPL".to_string(),
            }
        } else {
            "ENTRY".to_string()
        },
        via: if caller.is_some() {
            match e.kind {
                "http" => "http",
                "bus" => "bus",
                _ => "rpc",
            }
        } else {
            "start"
        },
        probable: false,
    };
    let mut best: BTreeMap<Vec<usize>, Candidate> = BTreeMap::new();
    let leaves: Vec<usize> = (0..hops.len())
        .filter(|&i| !has_child.contains(&hops[i].symbol.id))
        .collect();
    // Paths to every hop, not only leaves: a branch that wanders off after
    // crossing is trimmed back to its last crossing anyway.
    let ends: Vec<usize> = if leaves.len() > 400 {
        leaves
    } else {
        (0..hops.len()).collect()
    };
    let mut consider = |mut steps: Vec<Step>, unresolved: bool| {
        if let Some(c) = &caller {
            steps.insert(0, c.clone());
            // Publisher, broker, subscriber.
            if e.kind == "bus"
                && let Some(b) = e.key.as_ref().and_then(|k| (hooks.broker_of)(short_key(k)))
                && b.node != c.node
            {
                steps.insert(1, b);
            }
        }
        // Trim after the last step that adds a node or reaches infra.
        let mut seen: Vec<usize> = Vec::new();
        let mut last = 0;
        let mut bridges = 0;
        let mut config = false;
        let mut db = false;
        for (k, s) in steps.iter().enumerate() {
            let fresh = counted(s.node) && !seen.contains(&s.node);
            if fresh {
                seen.push(s.node);
            }
            let infra = matches!(
                s.via,
                "http" | "rpc" | "bus" | "config" | "db" | "publish" | "res"
            );
            if fresh || infra {
                last = k;
            }
            if matches!(s.via, "http" | "rpc" | "bus") {
                bridges += 1;
            }
            config |= s.via == "config";
            db |= s.via == "db";
        }
        // Config the last part reads, from the infrastructure that provides
        // it: a final step (config flows from its source, so a downstream
        // trace never crosses to it). Looked for in the last part's own
        // steps, before they are trimmed.
        let tail = steps[last].node;
        let seg_end = (last..steps.len())
            .take_while(|&i| steps[i].node == tail)
            .last()
            .unwrap_or(last);
        let found = (last..=seg_end)
            .rev()
            .find_map(|r| config_of(&steps[r].qual).map(|c| (r, c)));
        // Else the resource the last code reached uses (a database, a
        // store, an external API), as its own final step.
        let used = if found.is_none() {
            (last..steps.len())
                .rev()
                .find_map(|r| (hooks.res_of)(&steps[r].file).map(|s| (r, s)))
                .filter(|(_, s)| !seen.contains(&s.node))
        } else {
            None
        };
        let trimmed = last + 1 < steps.len() && found.is_none() && used.is_none();
        if let Some((r, cs)) = found {
            steps.truncate(r + 1);
            if !seen.contains(&cs.node) && counted(cs.node) {
                seen.push(cs.node);
            }
            config = true;
            steps.push(cs);
        } else if let Some((r, rs)) = used {
            steps.truncate(r + 1);
            if counted(rs.node) {
                seen.push(rs.node);
            }
            db = true;
            steps.push(rs);
        } else {
            steps.truncate(last + 1);
        }
        // A plain config read is only worth a flow after a real crossing:
        // on its own (a logger reading its level) it tells nothing.
        let real = seen
            .iter()
            .filter(|&&n| {
                steps
                    .iter()
                    .any(|s| s.node == n && (s.via != "config" || s.name.starts_with("secret ")))
            })
            .count();
        if seen.len() < 2 || real < 2 {
            return;
        }
        let score = score_path(seen.len(), bridges, config, db, e.prominence, steps.len());
        let cand = Candidate {
            entry: ei,
            steps,
            score,
            seq: seen,
            unresolved_end: unresolved && !trimmed,
        };
        // The best path for each sequence of nodes.
        let better = match best.get(&cand.seq) {
            None => true,
            Some(b) => {
                (cand.score, std::cmp::Reverse(cand.steps.len()))
                    > (b.score, std::cmp::Reverse(b.steps.len()))
            }
        };
        if better {
            best.insert(cand.seq.clone(), cand);
        }
    };
    for &end in &ends {
        // Walk back to the root.
        let mut chain: Vec<usize> = Vec::new();
        let mut cur = Some(end);
        let mut guard = 0;
        while let Some(i) = cur {
            chain.push(i);
            guard += 1;
            if guard > TRACE_HOPS * 3 {
                break;
            }
            let p = hops[i].predecessor_id;
            cur = if p == e.sym.id {
                None
            } else {
                by_id.get(&p).copied()
            };
            if cur.is_none() && p != e.sym.id {
                chain.clear();
                break;
            }
        }
        if chain.is_empty() {
            continue;
        }
        chain.reverse();
        let mut steps = vec![start.clone()];
        let mut ok = true;
        for &i in &chain {
            let h = &hops[i];
            if h.symbol.is_external() || !ctx.in_scope(&h.symbol.file_path) {
                break;
            }
            let Some(node) = node_of_file(&h.symbol.file_path) else {
                break;
            };
            let via = if h.bridge_direction.is_some() {
                match h.boundary_type.as_deref() {
                    Some("http") => "http",
                    Some("grpc") => "rpc",
                    Some("message_bus") => "bus",
                    Some("config") => "config",
                    _ => "call",
                }
            } else if matches!(h.language.as_str(), "sql" | "postgres") {
                "db"
            } else {
                "call"
            };
            // A crossing into infrastructure (a Bicep subscription, a route
            // a manifest declares) reaches no code: the flow ends before it.
            if matches!(via, "http" | "rpc" | "bus") && !(hooks.code_node)(node) {
                break;
            }
            // A message goes through its broker: a step of its own.
            if via == "bus"
                && let Some(topic) = h
                    .protocol_context
                    .as_ref()
                    .and_then(|p| p["channel"].as_str())
                && let Some(b) = (hooks.broker_of)(short_key(topic))
            {
                steps.push(b);
            }
            steps.push(Step {
                node,
                name: h.symbol.name.clone(),
                qual: h.symbol.qualname.clone(),
                file: h.symbol.file_path.clone(),
                line: h.symbol.start_line,
                kind: h.edge_kind.clone(),
                via,
                probable: h
                    .resolution_kind
                    .as_deref()
                    .is_some_and(|k| probable_kinds.contains(&k)),
            });
            if steps.len() > TRACE_HOPS + 2 {
                ok = false;
                break;
            }
        }
        if !ok && steps.len() < 2 {
            continue;
        }
        let unresolved = hops[end]
            .stop
            .as_ref()
            .is_some_and(|s| s.stop == crate::model::StopReason::UnresolvedCalls);
        consider(steps, unresolved);
    }
    if hops.is_empty() {
        consider(vec![start.clone()], false);
    }
    let mut out: Vec<Candidate> = best.into_values().collect();
    out.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.steps.len().cmp(&b.steps.len()))
            .then_with(|| a.seq.cmp(&b.seq))
    });
    out.truncate(PATHS_PER_ENTRY);
    out
}

/// `trace_flow` downstream from each id. The traces are independent reads,
/// so they run side by side; results come back in the order of `ids`, so the
/// page never depends on thread timing.
///
/// Two threads, not one per core: every connection shares SQLite's one
/// page-cache mutex (the bundled build enables memory management), so more
/// threads mostly wait on it. Measured on three repositories, two was the
/// fastest and eight took twice as long as two.
fn trace_all(ctx: &Ctx, cfg: &TraceConfig, ids: &[i64]) -> Vec<Option<Vec<TraceHop>>> {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 2);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let done: std::sync::Mutex<Vec<(usize, Option<Vec<TraceHop>>)>> =
        std::sync::Mutex::new(Vec::new());
    std::thread::scope(|s| {
        for _ in 0..threads.min(ids.len()) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(&id) = ids.get(i) else { break };
                    let hops = trace_flow(ctx.db, vec![id], None, None, ctx.graph_version, cfg)
                        .ok()
                        .map(|r| r.hops);
                    done.lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push((i, hops));
                }
            });
        }
    });
    let mut done = done.into_inner().unwrap_or_else(|e| e.into_inner());
    done.sort_by_key(|(i, _)| *i);
    done.into_iter().map(|(_, h)| h).collect()
}

/// The functions called from the most other functions, as starting points
/// when the repository declares no way in.
fn central_entries(
    ix: &Ix,
    nodes: &[Node],
    node_of_file: &dyn Fn(&str) -> Option<usize>,
) -> Result<Vec<EntryPoint>> {
    let ctx = ix.ctx;
    // Per function: who calls it, and which other parts it calls into. A
    // flow needs a start that reaches another part, so those come first.
    let mut callers: HashMap<i64, HashSet<i64>> = HashMap::new();
    let mut crosses: HashMap<i64, (HashSet<usize>, String)> = HashMap::new();
    for r in ix.inp.rows {
        if !matches!(r.kind.as_str(), "CALLS" | "DISPATCH") || !ctx.in_scope(&r.src_path) {
            continue;
        }
        let (Some(t), Some(tp)) = (r.tgt_id, r.tgt_path.as_ref()) else {
            continue;
        };
        if t == r.src_id || r.tgt_lang.as_deref() == Some("external") {
            continue;
        }
        callers.entry(t).or_default().insert(r.src_id);
        if let (Some(a), Some(b)) = (node_of_file(&r.src_path), node_of_file(tp))
            && a != b
        {
            crosses
                .entry(r.src_id)
                .or_insert_with(|| (HashSet::new(), r.src_path.clone()))
                .0
                .insert(b);
        }
    }
    let mut ranked: Vec<(usize, i64, String)> = crosses
        .into_iter()
        .filter(|(_, (_, p))| ctx.in_scope(p) && !is_generated(p) && !is_aux(p))
        .map(|(id, (parts, p))| {
            (
                callers.get(&id).map_or(0, HashSet::len) + 2 * parts.len(),
                id,
                p,
            )
        })
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let mut out = Vec::new();
    for (n, id, path) in ranked.into_iter().take(40) {
        let Some(nd) = node_of_file(&path) else {
            continue;
        };
        if !is_code_lane(nodes[nd].lane) {
            continue;
        }
        let Some(u) = ix.inp.owners.owner(&path) else {
            continue;
        };
        let Some(s) = ix.ctx.db.get_symbol_by_id(id)? else {
            continue;
        };
        if !matches!(s.kind.as_str(), "function" | "method") {
            continue;
        }
        out.push(EntryPoint {
            node: nd,
            unit: u,
            kind: "central",
            label: s.name.clone(),
            key: None,
            sym: Sym {
                id,
                name: s.name,
                qual: s.qualname,
                file: s.file_path,
                line: s.start_line,
            },
            prominence: (n as i64).min(20) / 4,
        });
        if out.len() >= 12 {
            break;
        }
    }
    Ok(out)
}

fn collect_entries(
    ix: &Ix,
    mode: Mode,
    nodes: &[Node],
    node_of_file: &dyn Fn(&str) -> Option<usize>,
    main_unit: Option<usize>,
) -> Result<Vec<EntryPoint>> {
    let ctx = ix.ctx;
    let inp = ix.inp;
    let mut out: Vec<EntryPoint> = Vec::new();
    let entry_node = |path: &str| -> Option<(usize, usize)> {
        let u = inp.owners.owner(path)?;
        if inp.units[u].kind.hidden() || !ctx.in_scope(path) || is_aux(path) {
            return None;
        }
        let nd = node_of_file(path)?;
        if !is_code_lane(nodes[nd].lane) {
            return None;
        }
        Some((u, nd))
    };
    // Routes, RPC implementations and subscriptions.
    {
        let conn = ctx.db.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT e.kind, e.target_qualname, e.detail, ss.id, ss.name, ss.qualname, sf.path, ss.start_line
             FROM edges e
             JOIN symbols ss ON e.source_symbol_id = ss.id
             JOIN files sf ON ss.file_id = sf.id
             WHERE e.graph_version = ?1
               AND (sf.deleted_version IS NULL OR sf.deleted_version > ?1)
               AND e.kind IN ('HTTP_ROUTE', 'RPC_IMPL', 'CHANNEL_SUBSCRIBE')
             ORDER BY sf.path, ss.start_line, e.target_qualname",
        )?;
        let rows: Vec<(String, Option<String>, Option<String>, Sym)> = stmt
            .query_map([ctx.graph_version], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    Sym {
                        id: r.get(3)?,
                        name: r.get(4)?,
                        qual: r.get(5)?,
                        file: r.get(6)?,
                        line: r.get::<_, Option<i64>>(7)?.unwrap_or(0),
                    },
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;
        drop(stmt);
        drop(conn);
        let mut routes = RouteCanon::default();
        let mut seen: HashSet<(String, i64)> = HashSet::new();
        for (kind, tq, detail, sym) in rows {
            let Some(tq) = tq.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()) else {
                continue;
            };
            let Some((u, nd)) = entry_node(&sym.file) else {
                continue;
            };
            let key = routes.canonical(ctx.db, ctx.graph_version, &kind, &tq);
            if kind == "HTTP_ROUTE" && is_probe(&key) {
                continue;
            }
            let detail: Option<Value> =
                detail.as_deref().and_then(|d| serde_json::from_str(d).ok());
            // The handler the route names, when it is registered elsewhere.
            let mut sym = sym;
            let handler = detail
                .as_ref()
                .and_then(|d| d.get("handler_candidates"))
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(Value::as_str)
                .map(|q| ("s.qualname = ?2", q.to_string()))
                .or_else(|| {
                    detail
                        .as_ref()
                        .and_then(|d| d.get("handler"))
                        .and_then(Value::as_str)
                        .map(|h| ("s.name = ?2", h.to_string()))
                });
            if let Some((clause, arg)) = handler
                && let Some(h) = ix
                    .symbols_named(clause, &arg)?
                    .into_iter()
                    .filter(|h| inp.owners.owner(&h.file) == Some(u))
                    .min_by_key(|h| (h.file != sym.file, h.file.clone(), h.line))
            {
                sym = h;
            }
            let label = entry_label(&kind, &key, detail.as_ref());
            if !seen.insert((label.clone(), sym.id)) {
                continue;
            }
            let (k, prominence): (&'static str, i64) = match kind.as_str() {
                "HTTP_ROUTE" => ("http", 3),
                "RPC_IMPL" => ("rpc", 3),
                _ => ("bus", 4),
            };
            out.push(EntryPoint {
                node: nd,
                unit: u,
                kind: k,
                label,
                key: Some(key),
                sym,
                prominence,
            });
        }
    }
    // `main` of each app (or of the unit a modules map splits).
    let wanted = |u: usize| -> bool {
        inp.units[u].kind.is_app() || (mode != Mode::Apps && Some(u) == main_unit)
    };
    for sym in ix.symbols_named("s.name IN ('main', 'Main', ?2)", "__main__")? {
        let Some((u, nd)) = entry_node(&sym.file) else {
            continue;
        };
        if !wanted(u) {
            continue;
        }
        let file = sym.file.rsplit('/').next().unwrap_or("");
        let primary = matches!(
            file,
            "main.rs"
                | "main.go"
                | "Program.cs"
                | "__main__.py"
                | "main.py"
                | "cli.py"
                | "index.ts"
                | "index.js"
                | "main.ts"
        );
        let label = format!(
            "{} main",
            inp.units[u]
                .entries
                .iter()
                .next()
                .cloned()
                .unwrap_or_else(|| inp.units[u].name.clone())
        );
        out.push(EntryPoint {
            node: nd,
            unit: u,
            kind: "main",
            label,
            key: None,
            sym,
            prominence: if primary { 4 } else { 2 },
        });
    }
    // Command-line commands, read from the sources of apps.
    let mut scanned = 0usize;
    for (path, lang, _) in inp.files.iter() {
        if !matches!(
            lang.as_str(),
            "go" | "python" | "javascript" | "typescript" | "tsx" | "csharp"
        ) {
            continue;
        }
        let Some((u, nd)) = entry_node(path) else {
            continue;
        };
        if !wanted(u) || scanned >= 2000 {
            continue;
        }
        scanned += 1;
        let Some(text) = read_source(inp.repo_root, path) else {
            continue;
        };
        let found = cli_commands(lang, &text);
        if found.is_empty() {
            continue;
        }
        let funcs = ix.file_functions(path)?;
        for (name, line, decorator) in found {
            let sym = if decorator {
                funcs
                    .iter()
                    .find(|(s, _)| s.line > line && s.line <= line + 6)
                    .map(|(s, _)| s.clone())
            } else {
                funcs
                    .iter()
                    .filter(|(s, end)| s.line <= line && *end >= line)
                    .max_by_key(|(s, _)| s.line)
                    .map(|(s, _)| s.clone())
            };
            let Some(sym) = sym else { continue };
            let root = inp.units[u]
                .entries
                .iter()
                .next()
                .cloned()
                .unwrap_or_else(|| inp.units[u].name.clone());
            let label = if name == root {
                root.clone()
            } else {
                format!("{root} {name}")
            };
            if out.iter().any(|e| e.sym.id == sym.id && e.kind == "cli") {
                continue;
            }
            out.push(EntryPoint {
                node: nd,
                unit: u,
                kind: "cli",
                label,
                key: None,
                sym,
                prominence: 3,
            });
        }
    }
    Ok(out)
}

fn read_source(root: &Path, rel: &str) -> Option<String> {
    use std::io::Read;
    let mut f = std::fs::File::open(root.join(rel)).ok()?;
    let mut buf = vec![0u8; 64 * 1024];
    let n = f.read(&mut buf).ok()?;
    buf.truncate(n);
    Some(String::from_utf8_lossy(&buf).into_owned())
}

struct Ranked {
    node: usize,
    sym: Sym,
    uses: usize,
    complexity: i64,
}

impl Ranked {
    fn to_json(&self) -> Value {
        json!({
            "node": self.node, "name": self.sym.name, "qual": self.sym.qual, "file": self.sym.file,
            "line": self.sym.line, "uses": self.uses, "complexity": self.complexity,
        })
    }
}

/// The types most used from other files: references to a type or any of
/// its members, by distinct referring symbol.
fn core_types(ix: &Ix, node_of_file: &dyn Fn(&str) -> Option<usize>) -> Result<Vec<Ranked>> {
    let ctx = ix.ctx;
    let conn = ctx.db.read_conn()?;
    let mut stmt = conn.prepare(
        "SELECT s.id, s.name, s.qualname, f.path, s.start_line
         FROM symbols s JOIN files f ON s.file_id = f.id
         WHERE s.graph_version = ?1 AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
           AND s.kind IN ('class', 'struct', 'interface', 'trait', 'enum', 'record', 'type')
           AND f.language != 'external'",
    )?;
    let types: Vec<Sym> = stmt
        .query_map([ctx.graph_version], |r| {
            Ok(Sym {
                id: r.get(0)?,
                name: r.get(1)?,
                qual: r.get(2)?,
                file: r.get(3)?,
                line: r.get::<_, Option<i64>>(4)?.unwrap_or(0),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    let by_qual: HashMap<&str, usize> = types
        .iter()
        .enumerate()
        .filter(|(_, t)| {
            ctx.in_scope(&t.file) && !is_aux(&t.file) && node_of_file(&t.file).is_some()
        })
        .map(|(i, t)| (t.qual.as_str(), i))
        .collect();
    let mut users: Vec<HashSet<i64>> = vec![HashSet::new(); types.len()];
    for r in ix.inp.rows {
        if matches!(r.kind.as_str(), "CONTAINS" | "MODULE_FILE") || !ctx.in_scope(&r.src_path) {
            continue;
        }
        let Some(q) = r.tgt_qual.as_deref() else {
            continue;
        };
        let ty = by_qual.get(q).copied().or_else(|| {
            let cut = q
                .rfind("::")
                .map(|i| &q[..i])
                .or_else(|| q.rfind('.').map(|i| &q[..i]))?;
            by_qual.get(cut).copied()
        });
        let Some(t) = ty else { continue };
        if r.tgt_path.as_deref() == Some(r.src_path.as_str()) {
            continue;
        }
        users[t].insert(r.src_id);
    }
    let mut out: Vec<Ranked> = users
        .iter()
        .enumerate()
        .filter(|(_, u)| u.len() >= 2)
        .filter_map(|(i, u)| {
            Some(Ranked {
                node: node_of_file(&types[i].file)?,
                sym: types[i].clone(),
                uses: u.len(),
                complexity: 0,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        b.uses
            .cmp(&a.uses)
            .then_with(|| a.sym.qual.cmp(&b.sym.qual))
    });
    // A type generated into several apps (one proto message per service)
    // is listed once, where it is used most.
    let mut seen: HashSet<String> = HashSet::new();
    out.retain(|r| seen.insert(r.sym.name.clone()));
    out.truncate(60);
    Ok(out)
}

/// Functions where a change is riskiest: complex and called from many
/// places (complexity × (1 + ln callers)).
fn hotspots(ix: &Ix, node_of_file: &dyn Fn(&str) -> Option<usize>) -> Result<Vec<Ranked>> {
    let ctx = ix.ctx;
    let conn = ctx.db.read_conn()?;
    let mut stmt = conn.prepare(
        "SELECT s.id, s.name, s.qualname, f.path, s.start_line, sm.complexity
         FROM symbol_metrics sm
         JOIN symbols s ON sm.symbol_id = s.id
         JOIN files f ON sm.file_id = f.id
         WHERE s.graph_version = ?1 AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
           AND s.kind IN ('function', 'method') AND sm.complexity >= 5
         ORDER BY sm.complexity DESC, s.id LIMIT 400",
    )?;
    let rows: Vec<(Sym, i64)> = stmt
        .query_map([ctx.graph_version], |r| {
            Ok((
                Sym {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    qual: r.get(2)?,
                    file: r.get(3)?,
                    line: r.get::<_, Option<i64>>(4)?.unwrap_or(0),
                },
                r.get(5)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let wanted: HashSet<i64> = rows.iter().map(|(s, _)| s.id).collect();
    let mut callers: HashMap<i64, HashSet<i64>> = HashMap::new();
    for r in ix.inp.rows {
        if let Some(t) = r.tgt_id
            && wanted.contains(&t)
            && matches!(r.kind.as_str(), "CALLS" | "DISPATCH")
            && r.src_id != t
        {
            callers.entry(t).or_default().insert(r.src_id);
        }
    }
    let mut out: Vec<(f64, Ranked)> = rows
        .into_iter()
        .filter(|(s, _)| ctx.in_scope(&s.file) && !is_generated(&s.file) && !is_aux(&s.file))
        .filter_map(|(s, cx)| {
            let node = node_of_file(&s.file)?;
            let c = callers.get(&s.id).map(HashSet::len).unwrap_or(0);
            let score = cx as f64 * (1.0 + (1.0 + c as f64).ln());
            Some((
                score,
                Ranked {
                    node,
                    sym: s,
                    uses: c,
                    complexity: cx,
                },
            ))
        })
        .collect();
    out.sort_by(|a, b| {
        b.0.total_cmp(&a.0)
            .then_with(|| a.1.sym.qual.cmp(&b.1.sym.qual))
    });
    // One copy of code vendored into several places.
    let mut seen: HashSet<(String, i64)> = HashSet::new();
    Ok(out
        .into_iter()
        .filter(|(_, r)| seen.insert((r.sym.name.clone(), r.complexity)))
        .take(6)
        .map(|(_, r)| r)
        .collect())
}

/// Benchmark and performance code: part of the repository, not of what it
/// does, so neither a module of the map nor a hotspot.
pub fn is_aux(path: &str) -> bool {
    let mut dirs: Vec<&str> = path.split('/').collect();
    dirs.pop();
    dirs.iter().any(|d| {
        let d = d.to_ascii_lowercase();
        let words: Vec<&str> = d.split(['-', '_', '.']).collect();
        words.iter().any(|w| {
            matches!(
                *w,
                "bench"
                    | "benches"
                    | "benchmark"
                    | "benchmarks"
                    | "perf"
                    | "test"
                    | "tests"
                    | "e2e"
            )
        })
    })
}

/// Code a generator wrote (protobuf and gRPC stubs, designer files): no
/// one edits it, so it is no hotspot.
pub fn is_generated(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    const SUFFIXES: &[&str] = &[
        ".pb.go",
        ".pb.gw.go",
        "_pb2.py",
        "_pb2_grpc.py",
        "_pb2.pyi",
        ".pb.cc",
        ".pb.h",
        "_pb.js",
        "_pb.ts",
        "_pb.d.ts",
        "_grpc_pb.js",
        "_grpc_pb.d.ts",
        ".g.cs",
        ".g.i.cs",
        ".designer.cs",
        ".gen.go",
        "_gen.go",
        ".generated.ts",
        ".generated.cs",
        "_generated.go",
        "_generated.rs",
    ];
    SUFFIXES.iter().any(|s| name.ends_with(s))
        || name.starts_with("zz_generated")
        || path
            .split('/')
            .any(|d| d == "generated" || d == "__generated__")
}

/// Docs a newcomer should open first, from what is on disk.
fn read_first(root: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some((name, _)) = units::root_readme(root) {
        out.push(json!({ "path": name, "note": "what the project is" }));
    }
    for (f, note) in [
        ("CONTEXT.md", "the domain's words"),
        ("ARCHITECTURE.md", "how it is built"),
        ("docs/ARCHITECTURE.md", "how it is built"),
        ("docs/architecture.md", "how it is built"),
        ("CONTRIBUTING.md", "how to work on it"),
    ] {
        if root.join(f).is_file() {
            out.push(json!({ "path": f, "note": note }));
        }
    }
    for d in [
        "docs/adr",
        "doc/adr",
        "docs/decisions",
        "adr",
        "docs/architecture/decisions",
    ] {
        let p = root.join(d);
        if let Ok(rd) = std::fs::read_dir(&p) {
            let n = rd
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "md"))
                .count();
            if n > 0 {
                out.push(json!({ "path": format!("{d}/"), "note": count(n, "decision record", "decision records") }));
                break;
            }
        }
    }
    if root.join("docs").is_dir()
        && !out
            .iter()
            .any(|v| v["path"].as_str().is_some_and(|p| p.starts_with("docs/")))
    {
        let n = std::fs::read_dir(root.join("docs"))
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().is_some_and(|x| x == "md"))
                    .count()
            })
            .unwrap_or(0);
        if n > 0 {
            out.push(json!({ "path": "docs/", "note": count(n, "page", "pages") }));
        }
    }
    out
}

/// One sentence on the whole repository.
fn summary(inp: &Input, mode: Mode, nodes: &[Node]) -> Value {
    if let Some((name, text)) = units::root_readme(inp.repo_root)
        && let Some(s) = units::readme_summary(&text)
    {
        return json!({ "text": s, "from": name });
    }
    if let Some(u) = inp
        .units
        .iter()
        .find(|u| u.root.is_empty() && u.description.is_some())
    {
        return json!({ "text": u.description, "from": u.manifests.first() });
    }
    let mut langs: BTreeMap<String, i64> = BTreeMap::new();
    for (u, l) in inp.langs.iter().enumerate() {
        if inp.units[u].kind.hidden() {
            continue;
        }
        for (k, c) in l {
            if is_code_lang(k) {
                *langs.entry(lang_name(k).to_string()).or_default() += c;
            }
        }
    }
    let mut ls: Vec<(String, i64)> = langs.into_iter().collect();
    ls.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let ls: Vec<String> = ls.into_iter().take(3).map(|(l, _)| l).collect();
    let count_of = |pred: &dyn Fn(&Node) -> bool| nodes.iter().filter(|n| pred(n)).count();
    let what = match mode {
        Mode::Apps => {
            let apps = count_of(&|n| matches!(n.lane, "front" | "svc" | "work"));
            let libs = count_of(&|n| n.lane == "lib");
            let mut parts = vec![count(apps, "app", "apps")];
            if libs > 0 {
                parts.push(count(libs, "shared library", "shared libraries"));
            }
            and_list(&parts)
        }
        Mode::Packages => count(count_of(&|n| is_code_lane(n.lane)), "package", "packages"),
        Mode::Modules => format!(
            "one unit in {}",
            count(count_of(&|n| n.kind == "module"), "module", "modules")
        ),
    };
    let infra: Vec<String> = nodes
        .iter()
        .filter(|n| n.lane == "infra")
        .map(|n| n.name.clone())
        .collect();
    let mut s = if ls.is_empty() {
        format!("A repository of {what}")
    } else {
        format!("A {} repository of {what}", and_list(&ls))
    };
    if !infra.is_empty() {
        s = format!("{s}, deployed with {}", and_list(&infra));
    }
    json!({ "text": format!("{s}."), "from": "template" })
}

/// The system tour: about six steps over the map, all from the data.
fn tour(doc: &Value) -> Vec<Value> {
    let nodes = doc["nodes"].as_array().cloned().unwrap_or_default();
    let links = doc["links"].as_array().cloned().unwrap_or_default();
    let flows = doc["flows"].as_array().cloned().unwrap_or_default();
    let mode = doc["mode"].as_str().unwrap_or("apps");
    let name = |i: &Value| -> String {
        nodes[i.as_u64().unwrap_or(0) as usize]["name"]
            .as_str()
            .unwrap_or("")
            .to_string()
    };
    let on_map = |n: &&Value| matches!(n["place"].as_str(), Some("card" | "chip"));
    let mut steps: Vec<Value> = Vec::new();
    // 1. The lanes.
    {
        let mut parts: Vec<String> = Vec::new();
        for lane in doc["lanes"].as_array().into_iter().flatten() {
            let id = lane["id"].as_str().unwrap_or("");
            let members: Vec<String> = nodes
                .iter()
                .filter(on_map)
                .filter(|n| n["lane"] == id)
                .map(|n| n["name"].as_str().unwrap_or("").to_string())
                .collect();
            if members.is_empty() {
                continue;
            }
            let shown: Vec<String> = members.iter().take(4).cloned().collect();
            let more = if members.len() > 4 {
                format!(" and {} more", members.len() - 4)
            } else {
                String::new()
            };
            parts.push(format!(
                "{}: {}{more}.",
                lane["label"].as_str().unwrap_or(""),
                shown.join(", ")
            ));
        }
        let cards: Vec<Value> = nodes
            .iter()
            .filter(on_map)
            .map(|n| n["id"].clone())
            .collect();
        let title = match mode {
            "apps" => format!(
                "These are the {} apps, by role",
                nodes
                    .iter()
                    .filter(|n| matches!(n["lane"].as_str(), Some("front" | "svc" | "work")))
                    .count()
            ),
            "packages" => format!(
                "This workspace has {} packages",
                nodes
                    .iter()
                    .filter(|n| n["lane"].as_str().is_some_and(is_code_lane))
                    .count()
            ),
            _ => format!(
                "This repository is one unit in {} modules",
                nodes.iter().filter(|n| n["kind"] == "module").count()
            ),
        };
        steps.push(json!({ "id": "lanes", "title": title, "text": parts.join(" "), "nodes": cards, "links": [] }));
    }
    // 2. Where most traffic enters.
    let busiest = nodes
        .iter()
        .filter(|n| n["lane"].as_str().is_some_and(is_code_lane))
        .map(|n| {
            let ways: u64 = n["ways"]
                .as_object()
                .map(|m| m.values().filter_map(Value::as_u64).sum())
                .unwrap_or(0);
            (ways, n)
        })
        .filter(|(w, _)| *w > 0)
        .max_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| b.1["id"].as_u64().cmp(&a.1["id"].as_u64()))
        });
    if let Some((_, n)) = busiest {
        let m = n["ways"].as_object().cloned().unwrap_or_default();
        let mut parts: Vec<String> = Vec::new();
        for (k, one, many) in [
            ("http", "HTTP route", "HTTP routes"),
            ("rpc", "RPC method", "RPC methods"),
            ("bus", "topic subscription", "topic subscriptions"),
            ("cli", "command", "commands"),
            ("main", "main function", "main functions"),
        ] {
            if let Some(c) = m.get(k).and_then(Value::as_u64) {
                parts.push(count(c as usize, one, many));
            }
        }
        let id = n["id"].clone();
        let neighbours: Vec<Value> = links
            .iter()
            .filter(|l| {
                !l["quiet"].as_bool().unwrap_or(false) && (l["from"] == id || l["to"] == id)
            })
            .map(|l| {
                if l["from"] == id {
                    l["to"].clone()
                } else {
                    l["from"].clone()
                }
            })
            .collect();
        let mut ns = vec![id.clone()];
        ns.extend(neighbours);
        steps.push(json!({
            "id": "busiest",
            "title": format!("Most traffic enters {}", n["name"].as_str().unwrap_or("")),
            "text": format!("{} has {}: more ways in than any other part. Its neighbours stay lit.", n["name"].as_str().unwrap_or(""), and_list(&parts)),
            "nodes": ns, "links": [], "select": id, "panel": "ways",
        }));
    } else {
        steps.push(json!({
            "id": "busiest",
            "title": "No entry points found",
            "text": if doc["central"].as_bool().unwrap_or(false) {
                if flows.is_empty() {
                    "The index has no HTTP routes, RPC implementations, topic subscriptions, commands or main functions here, and no path from one part to another was found either."
                } else {
                    "The index has no HTTP routes, RPC implementations, topic subscriptions, commands or main functions here, so the flows start from the functions that are called most and call into other parts."
                }
            } else {
                "The index has no HTTP routes, RPC implementations, topic subscriptions, commands or main functions here. The most used types under Start here are the best place to begin."
            },
            "nodes": [], "links": [],
        }));
    }
    // 3. One request.
    if let Some(f) = flows.first() {
        steps.push(json!({
            "id": "flow",
            "title": "Follow one request",
            "text": format!("{}: {} steps across {}.", f["title"].as_str().unwrap_or(""), f["steps"].as_array().map(Vec::len).unwrap_or(0), count(f["nodes"].as_array().map(Vec::len).unwrap_or(0), "part", "parts")),
            "nodes": f["nodes"], "links": [], "flow": 0,
        }));
    }
    // 4. How the parts talk.
    let talk: Vec<(usize, &Value)> = links
        .iter()
        .enumerate()
        .filter(|(_, l)| matches!(l["family"].as_str(), Some("http" | "rpc" | "bus")))
        .collect();
    if !talk.is_empty() {
        let mut by: BTreeMap<&str, usize> = BTreeMap::new();
        for (_, l) in &talk {
            *by.entry(l["family"].as_str().unwrap_or("")).or_default() += 1;
        }
        let parts: Vec<String> = by
            .iter()
            .map(|(f, c)| match *f {
                "http" => count(*c, "HTTP link", "HTTP links"),
                "rpc" => count(*c, "RPC link", "RPC links"),
                _ => count(*c, "message-bus link", "message-bus links"),
            })
            .collect();
        let mut ns: BTreeSet<u64> = BTreeSet::new();
        for (_, l) in &talk {
            ns.insert(l["from"].as_u64().unwrap_or(0));
            ns.insert(l["to"].as_u64().unwrap_or(0));
        }
        steps.push(json!({
            "id": "talk",
            "title": "How the apps talk",
            "text": format!("Across processes the apps use {}. Click a line for the calls behind it.", and_list(&parts)),
            "nodes": ns.into_iter().collect::<Vec<_>>(),
            "links": talk.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        }));
    } else if mode != "apps" {
        let heavy: Vec<(usize, &Value)> = links
            .iter()
            .enumerate()
            .filter(|(_, l)| !l["quiet"].as_bool().unwrap_or(false) && l["family"] == "code")
            .collect();
        if let Some((_, top)) = heavy
            .iter()
            .max_by_key(|(i, l)| (l["count"].as_u64().unwrap_or(0), std::cmp::Reverse(*i)))
        {
            let ns: BTreeSet<u64> = heavy
                .iter()
                .flat_map(|(_, l)| {
                    [
                        l["from"].as_u64().unwrap_or(0),
                        l["to"].as_u64().unwrap_or(0),
                    ]
                })
                .collect();
            steps.push(json!({
                "id": "talk",
                "title": "How the parts depend on each other",
                "text": format!("The lines are in-process links. The heaviest: {} {} {}.", name(&top["from"]), top["label"].as_str().unwrap_or(""), name(&top["to"])),
                "nodes": ns.into_iter().collect::<Vec<_>>(),
                "links": heavy.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            }));
        }
    }
    // 5. Data and messaging: the resources the apps share.
    if let Some(step) = data_step(&nodes, &links, mode) {
        steps.push(step);
    }
    // 6. Config and secrets.
    let cfg: Vec<(usize, &Value)> = links
        .iter()
        .enumerate()
        .filter(|(_, l)| matches!(l["family"].as_str(), Some("config" | "secret")))
        .collect();
    if !cfg.is_empty() {
        let mut providers: BTreeMap<String, usize> = BTreeMap::new();
        for (_, l) in &cfg {
            *providers.entry(name(&l["to"])).or_default() += 1;
        }
        let parts: Vec<String> = providers
            .iter()
            .map(|(p, c)| format!("{p} (read by {})", count(*c, "app", "apps")))
            .collect();
        let mut ns: BTreeSet<u64> = BTreeSet::new();
        for (_, l) in &cfg {
            ns.insert(l["from"].as_u64().unwrap_or(0));
            ns.insert(l["to"].as_u64().unwrap_or(0));
        }
        steps.push(json!({
            "id": "config",
            "title": "Where config and secrets come from",
            "text": format!("Config keys and secrets are set in {}.", and_list(&parts)),
            "nodes": ns.into_iter().collect::<Vec<_>>(),
            "links": cfg.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        }));
    }
    // 6. Shared code.
    let shared: Vec<&Value> = {
        let mut v: Vec<&Value> = nodes
            .iter()
            .filter(|n| {
                n["lane"] == "lib" && n["used_by"].as_array().is_some_and(|u| !u.is_empty())
            })
            .collect();
        v.sort_by(|a, b| {
            b["used_by"]
                .as_array()
                .map(Vec::len)
                .cmp(&a["used_by"].as_array().map(Vec::len))
                .then_with(|| a["name"].as_str().cmp(&b["name"].as_str()))
        });
        v
    };
    let core: Vec<String> = doc["core_types"]
        .as_array()
        .into_iter()
        .flatten()
        .take(3)
        .map(|c| c["name"].as_str().unwrap_or("").to_string())
        .collect();
    if !shared.is_empty() || !core.is_empty() {
        let mut text = String::new();
        if !shared.is_empty() {
            let parts: Vec<String> = shared
                .iter()
                .take(3)
                .map(|n| {
                    format!(
                        "{} (used by {})",
                        n["name"].as_str().unwrap_or(""),
                        count(
                            n["used_by"].as_array().map(Vec::len).unwrap_or(0),
                            "unit",
                            "units"
                        )
                    )
                })
                .collect();
            text.push_str(&format!("Shared libraries: {}. ", and_list(&parts)));
        }
        if !core.is_empty() {
            text.push_str(&format!("The most used types: {}.", and_list(&core)));
        }
        let mut ns: Vec<Value> = shared.iter().take(3).map(|n| n["id"].clone()).collect();
        for c in doc["core_types"].as_array().into_iter().flatten().take(3) {
            if !ns.contains(&c["node"]) {
                ns.push(c["node"].clone());
            }
        }
        steps.push(json!({
            "id": "shared", "title": "Shared code everyone depends on", "text": text.trim_end(),
            "nodes": ns, "links": [], "panel": "core",
        }));
    }
    // 7. Hotspots.
    let hot: Vec<&Value> = doc["hotspots"]
        .as_array()
        .into_iter()
        .flatten()
        .take(3)
        .collect();
    if !hot.is_empty() {
        let parts: Vec<String> = hot
            .iter()
            .map(|h| {
                format!(
                    "{} in {} (complexity {}, {})",
                    h["name"].as_str().unwrap_or(""),
                    name(&h["node"]),
                    h["complexity"],
                    count(
                        h["uses"].as_u64().unwrap_or(0) as usize,
                        "caller",
                        "callers"
                    )
                )
            })
            .collect();
        let mut ns: Vec<Value> = Vec::new();
        for h in &hot {
            if !ns.contains(&h["node"]) {
                ns.push(h["node"].clone());
            }
        }
        steps.push(json!({
            "id": "careful", "title": "Be careful here",
            "text": format!("The most complex code, weighed by how many places call it: {}. Change it with tests at hand.", parts.join("; ")),
            "nodes": ns, "links": [], "panel": "hotspots",
        }));
    }
    steps
}

/// Families of links to a resource.
fn is_res_family(f: &str) -> bool {
    matches!(f, "data" | "msg" | "secret" | "store" | "ext")
}

/// The tour step on the resources: who keeps data where, who passes
/// messages through what, and which apps meet only there.
fn data_step(nodes: &[Value], links: &[Value], mode: &str) -> Option<Value> {
    let on_map = |i: u64| {
        nodes
            .get(i as usize)
            .is_some_and(|n| matches!(n["place"].as_str(), Some("card" | "chip")))
    };
    let res_links: Vec<(usize, &Value)> = links
        .iter()
        .enumerate()
        .filter(|(_, l)| {
            l["family"].as_str().is_some_and(is_res_family)
                && on_map(l["from"].as_u64().unwrap_or(0))
                && on_map(l["to"].as_u64().unwrap_or(0))
        })
        .collect();
    if res_links.is_empty() {
        return None;
    }
    let (one, many) = if mode == "apps" {
        ("app", "apps")
    } else {
        ("part", "parts")
    };
    let name = |i: u64| nodes[i as usize]["name"].as_str().unwrap_or("").to_string();
    let mut users: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for (_, l) in &res_links {
        users
            .entry(l["to"].as_u64().unwrap_or(0))
            .or_default()
            .insert(l["from"].as_u64().unwrap_or(0));
    }
    let mut order: Vec<(&u64, &BTreeSet<u64>)> = users.iter().collect();
    order.sort_by(|a, b| {
        b.1.len()
            .cmp(&a.1.len())
            .then_with(|| name(*a.0).cmp(&name(*b.0)))
    });
    let parts: Vec<String> = order
        .iter()
        .take(5)
        .map(|(r, u)| format!("{} ({})", name(**r), count(u.len(), one, many)))
        .collect();
    let mut text = format!(
        "The {many} keep data and pass messages through {}: {}",
        count(users.len(), "shared resource", "shared resources"),
        and_list(&parts)
    );
    if users.len() > 5 {
        text.push_str(&format!(" and {} more", users.len() - 5));
    }
    text.push('.');
    // Pairs that never call each other and meet only at a resource.
    let direct: BTreeSet<(u64, u64)> = links
        .iter()
        .filter(|l| !l["family"].as_str().is_some_and(is_res_family))
        .map(|l| {
            let (a, b) = (
                l["from"].as_u64().unwrap_or(0),
                l["to"].as_u64().unwrap_or(0),
            );
            (a.min(b), a.max(b))
        })
        .collect();
    let mut only: Vec<(u64, u64, u64)> = Vec::new();
    for (r, u) in &order {
        let list: Vec<u64> = u.iter().copied().collect();
        for (i, a) in list.iter().enumerate() {
            for b in &list[i + 1..] {
                if !direct.contains(&((*a).min(*b), (*a).max(*b)))
                    && !only.iter().any(|(x, y, _)| (x, y) == (a, b))
                {
                    only.push((*a, *b, **r));
                }
            }
        }
    }
    if let Some((a, b, r)) = only.first() {
        text.push_str(&format!(
            " {} meet only here, such as {} and {} through {}.",
            count(
                only.len(),
                &format!("pair of {many} that never call each other"),
                &format!("pairs of {many} that never call each other")
            ),
            name(*a),
            name(*b),
            name(*r)
        ));
    }
    text.push_str(" Click a resource to see who uses it and how.");
    let mut ns: BTreeSet<u64> = BTreeSet::new();
    for (_, l) in &res_links {
        ns.insert(l["from"].as_u64().unwrap_or(0));
        ns.insert(l["to"].as_u64().unwrap_or(0));
    }
    Some(json!({
        "id": "data",
        "title": "Data and messaging",
        "text": text,
        "nodes": ns.into_iter().collect::<Vec<_>>(),
        "links": res_links.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
    }))
}

/// Questions with answers worked out in advance.
fn questions(doc: &Value) -> Vec<Value> {
    let nodes = doc["nodes"].as_array().cloned().unwrap_or_default();
    let links = doc["links"].as_array().cloned().unwrap_or_default();
    let name = |i: &Value| -> String {
        nodes[i.as_u64().unwrap_or(0) as usize]["name"]
            .as_str()
            .unwrap_or("")
            .to_string()
    };
    let mut out: Vec<Value> = Vec::new();
    // Secrets.
    {
        let mut items: Vec<Value> = Vec::new();
        let mut ns: BTreeSet<u64> = BTreeSet::new();
        for l in links
            .iter()
            .filter(|l| matches!(l["family"].as_str(), Some("config" | "secret")))
        {
            for c in l["calls"].as_array().into_iter().flatten() {
                let Some(key) = c["key"].as_str() else {
                    continue;
                };
                items.push(json!({
                    "text": format!("{} reads {key} from {}", name(&l["from"]), name(&l["to"])),
                    "file": c["from"]["file"], "line": c["from"]["line"],
                }));
            }
            ns.insert(l["from"].as_u64().unwrap_or(0));
            ns.insert(l["to"].as_u64().unwrap_or(0));
        }
        let secrets: Vec<&Value> = links
            .iter()
            .filter(|l| {
                matches!(l["family"].as_str(), Some("config" | "secret"))
                    && l["label"].as_str().is_some_and(|s| s.contains("secret"))
            })
            .collect();
        let a = if links
            .iter()
            .all(|l| !matches!(l["family"].as_str(), Some("config" | "secret")))
        {
            "No secrets or config keys found: no code reads a key that the repository's infrastructure provides.".to_string()
        } else if secrets.is_empty() {
            "No code reads a secret. Config keys come from the infrastructure listed below."
                .to_string()
        } else {
            let parts: Vec<String> = secrets
                .iter()
                .map(|l| {
                    format!(
                        "{} {} from {}",
                        name(&l["from"]),
                        l["label"].as_str().unwrap_or(""),
                        name(&l["to"])
                    )
                })
                .collect();
            format!("{}.", parts.join("; "))
        };
        items.truncate(40);
        out.push(json!({ "id": "secrets", "q": "Where do secrets come from?", "a": a, "nodes": ns.into_iter().collect::<Vec<_>>(), "items": items }));
    }
    // What talks to each card.
    let mut cards: Vec<&Value> = nodes
        .iter()
        .filter(|n| matches!(n["place"].as_str(), Some("card")))
        .collect();
    cards.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    for n in cards {
        let id = &n["id"];
        let inc: Vec<&Value> = links.iter().filter(|l| &l["to"] == id).collect();
        if inc.is_empty() {
            continue;
        }
        let parts: Vec<String> = inc
            .iter()
            .map(|l| format!("{} {}", name(&l["from"]), l["label"].as_str().unwrap_or("")))
            .collect();
        let mut ns = vec![id.clone()];
        ns.extend(inc.iter().map(|l| l["from"].clone()));
        let resource = n["kind"] == "resource";
        // A resource's answer lists its evidence: where each user touches it.
        let mut items: Vec<Value> = Vec::new();
        if resource {
            for l in &inc {
                for c in l["calls"].as_array().into_iter().flatten().take(4) {
                    let (text, file, line) = match c["key"].as_str() {
                        Some(k) => (
                            format!(
                                "{} {}: {k}",
                                name(&l["from"]),
                                l["label"].as_str().unwrap_or("")
                            ),
                            c["from"]["file"].clone(),
                            c["from"]["line"].clone(),
                        ),
                        None => (
                            format!("{}: {}", name(&l["from"]), c["text"].as_str().unwrap_or("")),
                            c["file"].clone(),
                            c["line"].clone(),
                        ),
                    };
                    items.push(json!({ "text": text, "file": file, "line": line }));
                }
            }
            items.truncate(40);
        }
        out.push(json!({
            "id": format!("{}-{}", if resource { "uses" } else { "talks" }, id),
            "q": if resource {
                format!("What uses {}?", n["name"].as_str().unwrap_or(""))
            } else {
                format!("What talks to {}?", n["name"].as_str().unwrap_or(""))
            },
            "a": format!("{}.", parts.join("; ")),
            "nodes": ns, "items": items,
        }));
    }
    // Which apps share a library.
    let mut libs: Vec<&Value> = nodes
        .iter()
        .filter(|n| n["lane"] == "lib" && n["used_by"].as_array().is_some_and(|u| u.len() >= 2))
        .collect();
    libs.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    for n in libs {
        let users: Vec<String> = n["used_by"]
            .as_array()
            .into_iter()
            .flatten()
            .map(&name)
            .collect();
        let mut ns = vec![n["id"].clone()];
        ns.extend(n["used_by"].as_array().cloned().unwrap_or_default());
        out.push(json!({
            "id": format!("shares-{}", n["id"]),
            "q": format!("Which apps share {}?", n["name"].as_str().unwrap_or("")),
            "a": format!("{} is used by {}.", n["name"].as_str().unwrap_or(""), and_list(&users)),
            "nodes": ns, "items": [],
        }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(node: usize, qual: &str) -> Step {
        Step {
            node,
            name: qual.rsplit('.').next().unwrap().to_string(),
            qual: qual.to_string(),
            file: String::new(),
            line: 0,
            kind: "CALLS".to_string(),
            via: "call",
            probable: false,
        }
    }

    fn cand(entry: usize, score: i64, steps: &[(usize, &str)]) -> Candidate {
        let mut seq: Vec<usize> = Vec::new();
        for (n, _) in steps {
            if !seq.contains(n) {
                seq.push(*n);
            }
        }
        Candidate {
            entry,
            steps: steps.iter().map(|(n, q)| step(*n, q)).collect(),
            score,
            seq,
            unresolved_end: false,
        }
    }

    #[test]
    fn the_map_follows_the_shape_of_the_repository() {
        // Apps that talk across processes; apps that do not, outnumbering
        // the libraries; a workspace of libraries; one unit.
        assert_eq!(mode_for(3, 2, 1, false), Mode::Apps);
        assert_eq!(mode_for(12, 0, 0, false), Mode::Apps);
        assert_eq!(mode_for(2, 10, 0, false), Mode::Packages);
        assert_eq!(mode_for(0, 9, 0, false), Mode::Packages);
        // One package holding most of the code is shown as its modules.
        assert_eq!(mode_for(0, 9, 0, true), Mode::Modules);
        assert_eq!(mode_for(2, 1, 0, true), Mode::Modules);
        // A dominant app still shows the apps when they talk.
        assert_eq!(mode_for(3, 1, 2, true), Mode::Apps);
        assert_eq!(mode_for(1, 1, 0, false), Mode::Modules);
        assert_eq!(mode_for(1, 0, 0, false), Mode::Modules);
    }

    #[test]
    fn card_sentences_say_what_a_unit_does_in_one_line() {
        let api = Facts {
            lang: Some("C#".into()),
            kind: "api".into(),
            rpc_methods: 274,
            rpc_services: 82,
            config_keys: 19,
            ..Facts::default()
        };
        assert_eq!(
            card_sentence(&api),
            "C# web API exposing 274 RPC methods in 82 services; reads 19 config keys."
        );
        let svc = Facts {
            lang: Some("Go".into()),
            kind: "service".into(),
            http_routes: 11,
            calls_rpc: 4,
            config_keys: 13,
            ..Facts::default()
        };
        assert_eq!(
            card_sentence(&svc),
            "Go service serving 11 HTTP routes; calls 4 apps over RPC, reads 13 config keys."
        );
        let worker = Facts {
            lang: Some("Python".into()),
            kind: "worker".into(),
            topics_in: 1,
            topics_out: 2,
            secrets: 1,
            ..Facts::default()
        };
        assert_eq!(
            card_sentence(&worker),
            "Python worker subscribed to 1 topic; publishes 2 topics, reads 1 secret."
        );
        let ui = Facts {
            lang: Some("TypeScript".into()),
            kind: "ui".into(),
            why: "react".into(),
            calls_http: 1,
            ..Facts::default()
        };
        assert_eq!(
            card_sentence(&ui),
            "TypeScript React front end; calls 1 app over HTTP."
        );
        let cli = Facts {
            lang: Some("Rust".into()),
            kind: "cli".into(),
            command: Some("rg".into()),
            ..Facts::default()
        };
        assert_eq!(
            card_sentence(&cli),
            "Rust command-line app installing `rg`."
        );
        let lib = Facts {
            lang: Some("Go".into()),
            kind: "lib".into(),
            used_by: 3,
            files: 12,
            ..Facts::default()
        };
        assert_eq!(card_sentence(&lib), "Go library; used by 3 units.");
        let bare = Facts {
            kind: "lib".into(),
            files: 1,
            ..Facts::default()
        };
        assert_eq!(card_sentence(&bare), "Library of 1 file.");
        assert_eq!(
            module_sentence(Some("Rust"), 24, &["Db".into(), "Symbol".into()]),
            "Rust module of 24 files; most used: Db, Symbol."
        );
        assert_eq!(module_sentence(None, 1, &[]), "Module of 1 file.");
        assert_eq!(
            chip_sentence("Bicep", 3, 12, 4, 7),
            "Bicep templates in 3 folders providing 4 secrets and 12 config keys to 7 apps."
        );
        assert_eq!(
            chip_sentence("Helm", 1, 0, 0, 0),
            "Helm charts in 1 folder."
        );
    }

    #[test]
    fn links_name_their_verb_and_count() {
        let keys = |k: &[&str]| -> BTreeSet<String> { k.iter().map(|s| s.to_string()).collect() };
        assert_eq!(
            process_label("http", &keys(&["/a", "/b"])),
            "calls over HTTP ×2"
        );
        assert_eq!(process_label("rpc", &keys(&["/s/m"])), "calls RPC ×1");
        assert_eq!(process_label("bus", &keys(&["orders"])), "publishes orders");
        assert_eq!(
            process_label("bus", &keys(&["a", "b", "c"])),
            "publishes 3 topics"
        );
        assert_eq!(
            process_label("config", &keys(&["secret://db"])),
            "reads secret db"
        );
        assert_eq!(
            process_label("config", &keys(&["secret://a", "secret://b"])),
            "reads 2 secrets"
        );
        assert_eq!(
            process_label("config", &keys(&["env://A", "env://B", "secret://c"])),
            "reads 1 secret and 2 config keys"
        );
        let kinds = |k: &[(&str, usize)]| -> BTreeMap<String, usize> {
            k.iter().map(|(a, b)| (a.to_string(), *b)).collect()
        };
        assert_eq!(code_label(&kinds(&[("CALLS", 5), ("USES", 2)])), "calls ×7");
        assert_eq!(
            code_label(&kinds(&[("IMPORTS", 3), ("CALLS", 1)])),
            "imports ×4"
        );
        assert_eq!(code_label(&kinds(&[("USES", 4)])), "references ×4");
        assert_eq!(code_label(&kinds(&[])), "depends on");
    }

    #[test]
    fn a_folder_that_holds_most_of_the_code_opens_into_its_folders() {
        let files: Vec<(String, i64)> = [
            ("src/main.rs", 5),
            ("src/db/mod.rs", 40),
            ("src/db/query.rs", 30),
            ("src/cli/mod.rs", 20),
            ("src/rpc/mod.rs", 25),
            ("build.rs", 2),
        ]
        .iter()
        .map(|(p, s)| (p.to_string(), *s))
        .collect();
        assert_eq!(
            split_modules(&files, ""),
            vec![
                ("".to_string(), 2, 1),
                ("src/".to_string(), 5, 1),
                ("src/cli/".to_string(), 20, 1),
                ("src/db/".to_string(), 70, 2),
                ("src/rpc/".to_string(), 25, 1),
            ]
        );
        let mods: Vec<String> = ["", "src/", "src/cli/", "src/db/", "src/rpc/"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(module_of("src/db/query.rs", &mods), Some(3));
        assert_eq!(module_of("src/main.rs", &mods), Some(1));
        assert_eq!(module_of("build.rs", &mods), Some(0));
        assert_eq!(module_name("src/", true), "src/*");
        assert_eq!(module_name("src/db/", false), "db");
        assert_eq!(module_name("", true), "top-level files");
    }

    #[test]
    fn flows_are_scored_by_parts_crossed_then_infrastructure() {
        // Each part crossed is worth most; then each bridge, the database,
        // config and the entry point's prominence; long paths cost a little.
        assert!(score_path(3, 0, false, false, 0, 4) > score_path(2, 0, false, false, 0, 4));
        assert!(score_path(2, 1, false, false, 0, 4) > score_path(2, 0, false, false, 0, 4));
        assert!(score_path(2, 0, false, true, 0, 4) > score_path(2, 0, true, false, 0, 4));
        assert!(score_path(2, 0, false, false, 0, 4) > score_path(2, 0, false, false, 0, 12));
        assert_eq!(score_path(3, 0, false, false, 0, 4), 29);
        assert_eq!(score_path(2, 2, true, true, 6, 4), 40);
        assert_eq!(score_path(2, 1, false, false, 3, 4), 26);
        assert_eq!(score_path(3, 2, true, true, 4, 8), 47);
    }

    #[test]
    fn flow_selection_keeps_distinct_stories() {
        let cands = vec![
            // 0: the best.
            cand(0, 50, &[(0, "a.start"), (1, "b.handle"), (2, "c.save")]),
            // Same nodes as 0 from another entry: told already.
            cand(1, 45, &[(0, "a.other"), (1, "b.other"), (2, "c.other")]),
            // The start of 0 from the same entry: nothing new.
            cand(0, 40, &[(0, "a.start"), (1, "b.handle")]),
            // Shares most of its symbols with 0.
            cand(2, 38, &[(1, "b.handle"), (2, "c.save"), (3, "d.x")]),
            // Distinct.
            cand(3, 30, &[(3, "d.run"), (1, "b.read")]),
            // One part only: not a flow.
            cand(4, 99, &[(5, "e.a"), (5, "e.b")]),
        ];
        let kept = select_flows(cands.clone(), 6, 3);
        let picked: Vec<(usize, i64)> = kept.iter().map(|c| (c.entry, c.score)).collect();
        assert_eq!(picked, vec![(0, 50), (3, 30)]);
        // At most `max`, best first.
        assert_eq!(
            select_flows(cands, 1, 3)
                .iter()
                .map(|c| c.score)
                .collect::<Vec<_>>(),
            vec![50]
        );
        // One path per entry when there are many entries.
        let many = vec![
            cand(0, 50, &[(0, "a.x"), (1, "b.x")]),
            cand(0, 45, &[(0, "a.x"), (2, "c.y")]),
            cand(1, 40, &[(3, "d.z"), (4, "e.z")]),
        ];
        let one: Vec<(usize, i64)> = select_flows(many.clone(), 6, 1)
            .iter()
            .map(|c| (c.entry, c.score))
            .collect();
        assert_eq!(one, vec![(0, 50), (1, 40)]);
        let three: Vec<(usize, i64)> = select_flows(many, 6, 3)
            .iter()
            .map(|c| (c.entry, c.score))
            .collect();
        assert_eq!(three, vec![(0, 50), (0, 45), (1, 40)]);
    }

    #[test]
    fn flow_titles_start_at_the_entry_point() {
        assert_eq!(
            flow_title("GET /a", &["x".into(), "y".into()]),
            "GET /a → x → y"
        );
        assert_eq!(
            flow_title(
                "m",
                &["a".into(), "b".into(), "c".into(), "d".into(), "e".into()]
            ),
            "m → a → b → … → e"
        );
        assert_eq!(flow_title("solo", &[]), "solo");
    }

    #[test]
    fn command_line_commands_are_found_in_their_sources() {
        let go = "func Scan() *cobra.Command {\n\tcmd := &cobra.Command{\n\t\tUse:   \"scan [SOURCE]\",\n\t\tShort: \"x\",\n\t}\n}\n";
        assert_eq!(cli_commands("go", go), vec![("scan".to_string(), 3, false)]);
        let py = "@cli.command(\"export\")\ndef run_export():\n    pass\n\n@app.command()\nasync def sync_all():\n    pass\n";
        assert_eq!(
            cli_commands("python", py),
            vec![
                ("export".to_string(), 1, true),
                ("sync-all".to_string(), 5, true)
            ]
        );
        let js = "program\n  .command('build <dir>')\n  .action(build);\n";
        assert_eq!(
            cli_commands("javascript", js),
            vec![("build".to_string(), 2, false)]
        );
        let cs = "var root = new Command(\"migrate\", \"Runs migrations\");\n";
        assert_eq!(
            cli_commands("csharp", cs),
            vec![("migrate".to_string(), 1, false)]
        );
        // Not a command: a decorator without `.command(`, a `Use` in prose.
        assert!(cli_commands("python", "@app.get(\"/x\")\ndef x():\n    pass\n").is_empty());
        assert!(cli_commands("rust", "Use: \"x\"").is_empty());
    }

    #[test]
    fn lanes_are_layered_callers_first() {
        let names: Vec<String> = ["api", "db", "ui", "worker"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        // ui -> api -> worker -> db, and api -> db.
        let (layer, order) = layer_lane(&names, &[(2, 0), (0, 3), (3, 1), (0, 1)]);
        assert_eq!(layer, vec![1, 3, 0, 2]);
        assert_eq!(order, vec![0, 0, 0, 0]);
        // A cycle does not loop: its back edge is left out.
        let (layer, _) = layer_lane(&names[..2], &[(0, 1), (1, 0)]);
        assert_eq!(layer, vec![0, 1]);
        // Unlinked nodes share layer 0 in name order.
        let (layer, order) = layer_lane(&names, &[]);
        assert_eq!(layer, vec![0, 0, 0, 0]);
        assert_eq!(order, vec![0, 1, 2, 3]);
    }

    #[test]
    fn generated_and_benchmark_code_is_no_hotspot() {
        for p in [
            "src/genproto/demo.pb.go",
            "proto/a_pb2.py",
            "x/Form1.Designer.cs",
            "pkg/zz_generated.deepcopy.go",
            "web/src/__generated__/types.ts",
        ] {
            assert!(is_generated(p), "{p}");
        }
        for p in ["src/main.go", "src/pb.rs", "lib/generator.py"] {
            assert!(!is_generated(p), "{p}");
        }
        for p in [
            "benches/a.rs",
            "benchmarks/run.py",
            "runtime-tests/x/index.ts",
            "perf-measures/a.ts",
            "e2e/t.ts",
        ] {
            assert!(is_aux(p), "{p}");
        }
        for p in ["src/perfect.rs", "src/testing_utils.rs", "src/bench.rs"] {
            assert!(!is_aux(p), "{p}");
        }
    }

    #[test]
    fn entry_labels_read_like_requests() {
        let d = |s: &str| -> Value { serde_json::from_str(s).unwrap() };
        assert_eq!(
            entry_label(
                "HTTP_ROUTE",
                "/uploads/{}",
                Some(&d(r#"{"method":"POST","raw":"/uploads/{id}"}"#))
            ),
            "POST /uploads/{id}"
        );
        assert_eq!(
            entry_label("HTTP_ROUTE", "/cart", Some(&d(r#"{"method":"ANY"}"#))),
            "/cart"
        );
        assert_eq!(
            entry_label(
                "RPC_IMPL",
                "/hipstershop.cartservice/getcart",
                Some(&d(r#"{"service":"CartService","rpc":"GetCart"}"#))
            ),
            "CartService.GetCart"
        );
        assert_eq!(entry_label("RPC_IMPL", "/pkg.s/m", None), "pkg.s.m");
        assert_eq!(
            entry_label(
                "CHANNEL_SUBSCRIBE",
                "fileuploaded",
                Some(&d(r#"{"raw":"\"file-uploaded\""}"#))
            ),
            "on file-uploaded"
        );
        assert_eq!(
            entry_label("CHANNEL_SUBSCRIBE", "topic://jobs", None),
            "on jobs"
        );
    }
}
