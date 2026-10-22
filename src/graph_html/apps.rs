//! The application view: the units [`units::detect`] finds and the links
//! between them, as the `apps` section of the viewer document.
//!
//! Every link is one family with a weight and its evidence:
//!
//! - `depends`: declared in a manifest (project reference, workspace or path
//!   dependency); weight = declarations.
//! - `code`: calls, imports and uses from one unit's symbols into another's;
//!   weight = edges, evidence = the most used targets.
//! - `http`, `rpc`, `bus`, `config`, `declares`: cross-process links through
//!   a shared key (route, topic, config key or secret), attributed to the
//!   units that own each side; weight = distinct keys.
//! - `deploys`: an infra unit whose image or build directory names an app.
//!
//! Shared external packages are chips (`chips`) a unit links to; each unit
//! also lists every package group it declares.

use super::build::{
    AggInput, Ctx, Graph, Leaf, RepoFile, RepoRow, RouteCanon, derive_leaf_bridges,
};
use super::units::{self, Kind, Owners, Unit};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

/// Symbols listed per key, keys listed per list, evidence lines per link.
const MAX_SYMS: usize = 12;
const MAX_KEYS: usize = 400;
const MAX_EVIDENCE: usize = 8;

pub(super) type SymRef = (String, String, i64);
/// A code target: uses, and a sample call site (file, line, snippet).
type Target = (usize, String, i64, String);

/// Keys (routes, topics, config keys) a unit uses, with the symbols that
/// use each.
#[derive(Default)]
pub(super) struct Usage {
    pub(super) keys: BTreeMap<String, (usize, Vec<SymRef>)>,
}

impl Usage {
    fn add(&mut self, key: &str, sym: SymRef) {
        let e = self.keys.entry(key.to_string()).or_default();
        e.0 += 1;
        if e.1.len() < MAX_SYMS && !e.1.contains(&sym) {
            e.1.push(sym);
        }
    }

    /// `{ total, items: [[key, uses, [[qualname, file, line]], external?]] }`.
    fn to_json(&self, external: Option<&HashSet<String>>) -> Value {
        let items: Vec<Value> = self
            .keys
            .iter()
            .take(MAX_KEYS)
            .map(|(k, (n, syms))| {
                let syms: Vec<Value> = syms.iter().map(|(q, f, l)| json!([q, f, l])).collect();
                match external {
                    Some(ext) => json!([k, n, syms, ext.contains(k)]),
                    None => json!([k, n, syms]),
                }
            })
            .collect();
        json!({ "total": self.keys.len(), "items": items })
    }

    fn first_sym(&self, key: &str) -> Option<&SymRef> {
        self.keys.get(key).and_then(|(_, s)| s.first())
    }
}

#[derive(Default)]
pub(super) struct UnitUse {
    pub(super) http: Usage,
    pub(super) rpc: Usage,
    pub(super) bus: Usage,
    pub(super) provides: Usage,
    declares: Usage,
    pub(super) config: Usage,
    pub(super) secrets: Usage,
    pub(super) topics: Usage,
    pub(super) calls: Usage,
    /// Keys it provides from a secret: a Kubernetes env var set by
    /// `secretKeyRef`. Read as plain config, they are secrets all the same.
    pub(super) secret_sourced: BTreeSet<String>,
}

/// A link of the app view as the glance page reads it.
pub(super) struct LinkView {
    pub(super) from: usize,
    pub(super) to: usize,
    pub(super) family: &'static str,
    pub(super) keys: Vec<String>,
    pub(super) evidence: Vec<(String, String, i64)>,
    /// A code link both manifests declare (or a non-code link).
    pub(super) declared: bool,
    pub(super) kinds: BTreeMap<String, usize>,
}

/// A link between two units being built.
#[derive(Default)]
struct Link {
    weight: usize,
    keys: BTreeSet<String>,
    kinds: BTreeMap<String, usize>,
    /// Code: target qualname -> (uses, a sample call site).
    targets: HashMap<String, Target>,
    evidence: Vec<(String, String, i64)>,
}

pub(super) fn is_secret(key: &str) -> bool {
    let scheme = key.split("://").next().unwrap_or("");
    key.contains("://") && scheme.to_ascii_lowercase().contains("secret")
}

/// Health and metrics routes every service serves: no link between two
/// services because both answer `/health`.
pub(super) fn is_probe(key: &str) -> bool {
    let path = short_key(key).to_ascii_lowercase();
    let last = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
    path.contains("/health")
        || matches!(
            last,
            "healthz" | "livez" | "readyz" | "ready" | "live" | "metrics" | "ping"
        )
}

/// Edge kinds that link two units through a shared key (route, topic,
/// config key) rather than a resolved target.
pub(super) fn is_key_link(kind: &str) -> bool {
    matches!(
        kind,
        "HTTP_ROUTE"
            | "RPC_IMPL"
            | "CHANNEL_SUBSCRIBE"
            | "CONFIG_SOURCE"
            | "RPC_ROUTE"
            | "CONFIG_READ"
            | "CHANNEL_PUBLISH"
            | "HTTP_CALL"
            | "RPC_CALL"
    )
}

/// The part of a key people read: `secret://db-conn` -> `db-conn`.
pub(super) fn short_key(key: &str) -> &str {
    key.split_once("://").map(|(_, r)| r).unwrap_or(key)
}

/// RPC service of a route key: `/pkg.Greeter/SayHello` -> `pkg.Greeter`.
fn rpc_service(key: &str) -> String {
    let k = key.trim_start_matches('/');
    match k.rsplit_once('/') {
        Some((svc, _)) => svc.to_string(),
        None => k.rsplit_once('.').map(|(s, _)| s).unwrap_or(k).to_string(),
    }
}

fn label(family: &str, link: &Link) -> String {
    let n = link.weight;
    let one = |s: &str| -> String {
        link.keys
            .iter()
            .next()
            .map(|k| format!("{s} {}", short_key(k)))
            .unwrap_or_default()
    };
    match family {
        "code" => {
            let calls = link.kinds.get("CALLS").copied().unwrap_or(0);
            if calls * 2 >= n {
                format!("calls ×{n}")
            } else {
                format!("references ×{n}")
            }
        }
        "depends" => "depends on".to_string(),
        "http" => format!("calls over HTTP ×{n}"),
        "rpc" => format!("calls RPC ×{n}"),
        "declares" => format!("declares RPC ×{n}"),
        "bus" if n == 1 => one("publishes to topic"),
        "bus" => format!(
            "publishes to {n} {}",
            super::glance::topic_noun(link.keys.iter().map(|k| k.as_str())).1
        ),
        "config" => {
            let secrets = link.keys.iter().filter(|k| is_secret(k)).count();
            match (n, secrets) {
                (1, 1) => one("reads secret"),
                (1, _) => one("reads config"),
                (_, s) if s == n => format!("reads {n} secrets"),
                (_, 0) => format!("reads {n} config keys"),
                _ => format!("reads {n} config keys and secrets"),
            }
        }
        "deploys" => "deploys".to_string(),
        _ => format!("{family} ×{n}"),
    }
}

/// The `apps` section for `graph`. With `rows` (a whole-repo view) it holds
/// the links and what each unit exposes and consumes; without (a seeded
/// view) only the units and which node belongs to which.
pub(crate) fn apps_json(
    ctx: &Ctx,
    repo_root: &Path,
    graph: &Graph,
    files: Option<&[RepoFile]>,
    rows: Option<&[RepoRow]>,
    max_symbols: Option<usize>,
) -> Result<Value> {
    let mut indexed: Vec<(String, String)> = ctx
        .file_lang
        .iter()
        .filter(|(_, l)| l.as_str() != "external")
        .map(|(p, l)| (p.clone(), l.clone()))
        .collect();
    indexed.sort();
    let list = units::detect(repo_root, &indexed);
    let owners = Owners::new(&list);
    let n = list.len();

    // Sizes, languages and top folders.
    let symbols: HashMap<&str, i64> = files
        .unwrap_or_default()
        .iter()
        .map(|(p, _, c)| (p.as_str(), *c))
        .collect();
    let mut file_count = vec![0i64; n];
    let mut sym_count = vec![0i64; n];
    let mut langs: Vec<BTreeMap<String, i64>> = vec![BTreeMap::new(); n];
    let mut modules: Vec<BTreeMap<String, (i64, i64)>> = vec![BTreeMap::new(); n];
    // Languages from the unit's own code: fixtures and tests in another
    // language (a C# test image in a Go repo) are not what it is written in.
    let mut test_langs: Vec<BTreeMap<String, i64>> = vec![BTreeMap::new(); n];
    for (path, lang) in &indexed {
        let Some(u) = owners.owner(path) else {
            continue;
        };
        let syms = symbols.get(path.as_str()).copied().unwrap_or(0);
        file_count[u] += 1;
        sym_count[u] += syms;
        let target = if ctx.in_scope(path) {
            &mut langs[u]
        } else {
            &mut test_langs[u]
        };
        *target.entry(lang.clone()).or_default() += 1;
        let rel = path.strip_prefix(list[u].root.as_str()).unwrap_or(path);
        if let Some(i) = rel.find('/') {
            let m = modules[u]
                .entry(format!("{}{}", list[u].root, &rel[..=i]))
                .or_default();
            m.0 += 1;
            m.1 += syms;
        }
    }

    // Code can only use another project's code through a declared
    // dependency (direct or transitive) when both have manifests; a link
    // without one is a name-match guess of the resolver.
    let deps = units::resolve_deps(&list);
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (a, b, _) in &deps {
        adj[*a].push(*b);
    }
    let reach: Vec<HashSet<usize>> = (0..n)
        .map(|s| {
            let mut seen = HashSet::new();
            let mut stack = vec![s];
            while let Some(x) = stack.pop() {
                for &y in &adj[x] {
                    if seen.insert(y) {
                        stack.push(y);
                    }
                }
            }
            seen
        })
        .collect();
    let declared = |a: usize, b: usize| {
        list[a].manifests.is_empty() || list[b].manifests.is_empty() || reach[a].contains(&b)
    };

    for (l, t) in langs.iter_mut().zip(test_langs) {
        if l.is_empty() {
            *l = t;
        }
    }
    let mut uses: Vec<UnitUse> = (0..n).map(|_| UnitUse::default()).collect();
    let mut links: BTreeMap<(usize, usize, &'static str), Link> = BTreeMap::new();

    if let Some(rows) = rows {
        let mut routes = RouteCanon::default();
        let mut bridge_inputs: Vec<AggInput> = Vec::new();
        for r in rows {
            let Some(from) = owners.owner(&r.src_path) else {
                continue;
            };
            // An app's own tests say nothing about how it is wired.
            if !list[from].kind.hidden() && !ctx.in_scope(&r.src_path) {
                continue;
            }
            let sym = (r.src_qual.clone(), r.src_path.clone(), r.src_line);
            if is_key_link(&r.kind) {
                let raw =
                    r.tq.as_deref()
                        .map(str::trim)
                        .filter(|t| !t.is_empty())
                        .or(r.tgt_qual.as_deref());
                let Some(raw) = raw else { continue };
                let key = routes.canonical(ctx.db, ctx.graph_version, &r.kind, raw);
                let u = &mut uses[from];
                match r.kind.as_str() {
                    "HTTP_ROUTE" => u.http.add(&key, sym),
                    "RPC_IMPL" => u.rpc.add(&key, sym),
                    "CHANNEL_SUBSCRIBE" => u.bus.add(&key, sym),
                    "CONFIG_SOURCE" => {
                        if r.detail
                            .as_deref()
                            .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok())
                            .is_some_and(|d| d["secret"].is_string())
                        {
                            u.secret_sourced.insert(key.clone());
                        }
                        u.provides.add(&key, sym)
                    }
                    "RPC_ROUTE" => u.declares.add(&key, sym),
                    "CONFIG_READ" if is_secret(&key) => u.secrets.add(&key, sym),
                    "CONFIG_READ" => u.config.add(&key, sym),
                    "CHANNEL_PUBLISH" => u.topics.add(&key, sym),
                    _ => u.calls.add(&key, sym),
                }
                let me = Leaf::Module(from.to_string());
                let ep = Leaf::Endpoint(key);
                let (a, b) = if super::build::receiving(&r.kind) {
                    (ep, me)
                } else {
                    (me, ep)
                };
                bridge_inputs.push(AggInput {
                    from: a,
                    to: b,
                    kind: r.kind.clone(),
                    flags: 0,
                    ev_line: 0,
                    ev: None,
                    ev_file: String::new(),
                    keys: Vec::new(),
                });
                continue;
            }
            // Code: only links into another unit's own files.
            let (Some(tp), Some(tl)) = (&r.tgt_path, &r.tgt_lang) else {
                continue;
            };
            if tl == "external" {
                continue;
            }
            let Some(to) = owners.owner(tp) else { continue };
            if to == from {
                continue;
            }
            let link = links.entry((from, to, "code")).or_default();
            link.weight += 1;
            *link.kinds.entry(r.kind.clone()).or_default() += 1;
            let target = r.tgt_qual.clone().unwrap_or_else(|| tp.clone());
            let t = link.targets.entry(target).or_insert_with(|| {
                (
                    0,
                    r.ev_file.clone(),
                    r.ev_line.unwrap_or(0),
                    r.ev.clone().unwrap_or_default(),
                )
            });
            t.0 += 1;
        }
        for d in derive_leaf_bridges(&bridge_inputs) {
            let (Leaf::Module(a), Leaf::Module(b)) = (&d.from, &d.to) else {
                continue;
            };
            let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) else {
                continue;
            };
            if d.keys.iter().all(|k| is_probe(k)) {
                continue;
            }
            let family: &'static str = match d.kind.as_str() {
                "VIA_HTTP" => "http",
                "VIA_RPC" => "rpc",
                "VIA_BUS" => "bus",
                "VIA_CONFIG" => "config",
                "DECLARES_RPC" => "declares",
                _ => continue,
            };
            // Config is drawn reader -> provider: "reads secret X".
            let (a, b) = if family == "config" { (b, a) } else { (a, b) };
            let link = links.entry((a, b, family)).or_default();
            link.keys.extend(d.keys.iter().cloned());
        }
    }

    // Manifest links.
    for (a, b, ev) in deps {
        let link = links.entry((a, b, "depends")).or_default();
        link.weight += 1;
        link.evidence.push((ev.text, ev.file, ev.line));
    }
    for (a, b, ev) in units::deployments(&list) {
        let link = links.entry((a, b, "deploys")).or_default();
        link.weight += 1;
        link.evidence.push((ev.text, ev.file, ev.line));
    }

    // Keys some unit receives: a call to any other key leaves the repo.
    let served: HashSet<String> = uses
        .iter()
        .flat_map(|u| u.http.keys.keys().chain(u.rpc.keys.keys()).cloned())
        .collect();
    let external_calls: HashSet<String> = uses
        .iter()
        .flat_map(|u| u.calls.keys.keys())
        .filter(|k| !served.contains(*k))
        .cloned()
        .collect();

    let edges: Vec<Value> = links
        .iter_mut()
        .map(|((a, b, family), link)| {
            if !link.keys.is_empty() {
                link.weight = link.keys.len();
                for k in link.keys.iter().take(MAX_EVIDENCE) {
                    let u = &uses[*a];
                    let s = [
                        &u.topics, &u.calls, &u.config, &u.secrets, &u.declares, &u.provides,
                    ]
                    .iter()
                    .find_map(|x| x.first_sym(k))
                    .cloned();
                    let (file, line) = s.map(|s| (s.1, s.2)).unwrap_or_default();
                    link.evidence.push((short_key(k).to_string(), file, line));
                }
            }
            if *family == "code" {
                let mut t: Vec<(&String, &Target)> = link.targets.iter().collect();
                t.sort_by(|x, y| y.1.0.cmp(&x.1.0).then_with(|| x.0.cmp(y.0)));
                for (q, (count, file, line, snippet)) in t.into_iter().take(MAX_EVIDENCE) {
                    let text = if snippet.is_empty() {
                        format!("{q} ×{count}")
                    } else {
                        format!("{q} ×{count} — {}", snippet.trim())
                    };
                    link.evidence.push((text, file.clone(), *line));
                }
            }
            link.evidence.truncate(MAX_EVIDENCE);
            json!({
                "from": a,
                "to": b,
                "family": family,
                "weight": link.weight,
                "label": label(family, link),
                "kinds": link.kinds,
                "declared": *family != "code" || declared(*a, *b),
                "keys": link.keys.iter().take(MAX_KEYS).collect::<Vec<_>>(),
                "evidence": link.evidence.iter().map(|(t, f, l)| json!([t, f, l])).collect::<Vec<_>>(),
            })
        })
        .collect();

    let link_views: Vec<LinkView> = links
        .iter()
        .map(|((a, b, family), link)| LinkView {
            from: *a,
            to: *b,
            family,
            keys: link.keys.iter().cloned().collect(),
            evidence: link.evidence.clone(),
            declared: *family != "code" || declared(*a, *b),
            kinds: link.kinds.clone(),
        })
        .collect();

    // Package groups, and chips for the ones several units share.
    let packages = units::external_packages(&list);
    let groups: Vec<BTreeMap<String, BTreeSet<String>>> = packages
        .iter()
        .map(|pkgs| {
            let mut g: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for p in pkgs {
                g.entry(units::package_group(p))
                    .or_default()
                    .insert(p.clone());
            }
            g
        })
        .collect();
    let mut users: BTreeMap<&String, Vec<usize>> = BTreeMap::new();
    for (i, g) in groups.iter().enumerate() {
        if list[i].kind.hidden() {
            continue;
        }
        for name in g.keys() {
            users.entry(name).or_default().push(i);
        }
    }
    let chips: Vec<Value> = users
        .into_iter()
        .filter(|(_, u)| u.len() >= 2)
        .map(|(name, u)| {
            let members: BTreeSet<&String> = u.iter().flat_map(|&i| &groups[i][name]).collect();
            json!({ "name": name, "users": u, "packages": members })
        })
        .collect();

    // A command or service that only takes messages off a bus is a worker;
    // a command that serves HTTP or RPC is a service.
    let mut list = list;
    for (i, u) in list.iter_mut().enumerate() {
        let x = &uses[i];
        if u.kind == Kind::Cli && (!x.http.keys.is_empty() || !x.rpc.keys.is_empty()) {
            u.kind = Kind::Service;
            u.why = format!(
                "{}, serves {}",
                u.why,
                if x.http.keys.is_empty() {
                    "RPC"
                } else {
                    "HTTP"
                }
            );
            continue;
        }
        if matches!(u.kind, Kind::Cli | Kind::Service)
            && !x.bus.keys.is_empty()
            && x.http.keys.is_empty()
            && x.rpc.keys.is_empty()
        {
            u.kind = Kind::Worker;
            u.why = format!("{}, subscribes to topics", u.why);
        }
    }
    let unit_values: Vec<Value> = list
        .iter()
        .enumerate()
        .map(|(i, u)| {
            unit_json(
                i,
                u,
                &uses[i],
                &langs[i],
                &modules[i],
                &groups[i],
                &external_calls,
                file_count[i],
                sym_count[i],
            )
        })
        .collect();

    let node_unit: Vec<i64> = graph
        .nodes
        .iter()
        .map(|nd| {
            let path = match (&nd.file, nd.kind.as_str()) {
                (Some(f), _) => f.clone(),
                (None, "module") if nd.qual == "./" => String::new(),
                (None, "module") => nd.qual.clone(),
                _ => return -1,
            };
            owners.owner(&path).map(|u| u as i64).unwrap_or(-1)
        })
        .collect();

    let mut out = json!({
        "units": unit_values,
        "edges": edges,
        "chips": chips,
        "node_unit": node_unit,
        "links": rows.is_some(),
    });
    if let (Some(files), Some(rows), Some(max)) = (files, rows, max_symbols) {
        let (zoom, note) = super::zoom::zoom_json(repo_root, ctx, &owners, n, files, rows, max)?;
        out["zoom"] = zoom;
        if let Some(note) = note {
            out["zoom"]["note"] = json!(note);
        }
        // The landing page: built only for the whole-repo app view.
        out["glance"] = super::glance::glance_json(
            ctx,
            &super::glance::Input {
                repo_root,
                units: &list,
                owners: &owners,
                uses: &uses,
                links: &link_views,
                langs: &langs,
                file_count: &file_count,
                sym_count: &sym_count,
                files,
                rows,
            },
        )?;
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn unit_json(
    id: usize,
    u: &Unit,
    usage: &UnitUse,
    langs: &BTreeMap<String, i64>,
    modules: &BTreeMap<String, (i64, i64)>,
    groups: &BTreeMap<String, BTreeSet<String>>,
    external_calls: &HashSet<String>,
    files: i64,
    symbols: i64,
) -> Value {
    let mut lang_list: Vec<(&String, &i64)> = langs.iter().collect();
    // Code first, then by file count.
    lang_list.sort_by(|a, b| {
        let code = |l: &str| !matches!(l, "markdown" | "yaml" | "json" | "bicep");
        code(b.0)
            .cmp(&code(a.0))
            .then_with(|| b.1.cmp(a.1))
            .then_with(|| a.0.cmp(b.0))
    });
    let mut mods: Vec<(&String, &(i64, i64))> = modules.iter().collect();
    mods.sort_by(|a, b| {
        b.1.1
            .cmp(&a.1.1)
            .then_with(|| b.1.0.cmp(&a.1.0))
            .then_with(|| a.0.cmp(b.0))
    });
    let services: BTreeSet<String> = usage.rpc.keys.keys().map(|k| rpc_service(k)).collect();
    json!({
        "id": id,
        "name": u.name,
        "kind": u.kind.as_str(),
        "why": u.why,
        "root": u.display_root(),
        "dir": u.root,
        "file_roots": u.file_roots,
        "hidden": u.kind.hidden(),
        "app": u.kind.is_app(),
        "manifests": u.manifests,
        "dockerfiles": u.dockerfiles,
        "description": u.description,
        "entries": u.entries,
        "files": files,
        "symbols": symbols,
        "langs": lang_list.iter().map(|(l, c)| json!([l, c])).collect::<Vec<_>>(),
        "modules": mods.iter().take(60).map(|(p, (f, s))| json!([p, f, s])).collect::<Vec<_>>(),
        "packages": groups.iter().map(|(g, p)| json!([g, p])).collect::<Vec<_>>(),
        "services": services,
        "exposes": {
            "http": usage.http.to_json(None),
            "rpc": usage.rpc.to_json(None),
            "bus": usage.bus.to_json(None),
            "provides": usage.provides.to_json(None),
            "declares": usage.declares.to_json(None),
        },
        "consumes": {
            "config": usage.config.to_json(None),
            "secrets": usage.secrets.to_json(None),
            "topics": usage.topics.to_json(None),
            "calls": usage.calls.to_json(Some(external_calls)),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_read_plainly() {
        assert!(is_secret("secret://db-conn"));
        assert!(!is_secret("env://DB"));
        assert_eq!(short_key("secret://db-conn"), "db-conn");
        assert_eq!(rpc_service("/pkg.Greeter/SayHello"), "pkg.Greeter");
        let mut l = Link {
            weight: 1,
            ..Link::default()
        };
        l.keys.insert("orders".to_string());
        assert_eq!(label("bus", &l), "publishes to topic orders");
        let mut ev = Link {
            weight: 2,
            ..Link::default()
        };
        ev.keys.insert("channel://OrderPaidEvent".to_string());
        ev.keys.insert("channel://StockLowEvent".to_string());
        assert_eq!(label("bus", &ev), "publishes to 2 events");
        ev.keys.insert("channel://jobs".to_string());
        ev.weight = 3;
        assert_eq!(label("bus", &ev), "publishes to 3 topics");
        l.keys = ["secret://a".to_string()].into();
        assert_eq!(label("config", &l), "reads secret a");
    }
}
