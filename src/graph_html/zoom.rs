//! What the app view's semantic zoom opens into: every unit's files and
//! symbols and the weighted relations between them, embedded in the
//! overview page so a card can open in place down to its classes, methods
//! and functions.
//!
//! The section is columnar so tens of thousands of symbols stay small: every
//! string lives once in `s` and is referenced by index (`-1`: absent).
//!
//! - files: `f_unit`, `f_path`, `f_lang`, `f_syms` (its symbols),
//!   `f_emb` (1 when those symbols are embedded)
//! - symbols: `y_file`, `y_kind`, `y_name`, `y_qual`, `y_line`, `y_parent`
//!   (the enclosing embedded symbol, `-1` at file level), `y_sig`
//! - edges: `e`, flat `[from, to, kind, weight]` quads. An endpoint `>= 0` is
//!   a symbol; `< 0` is file `-1 - end` (an import or call made from a
//!   file's module scope, or into a file whose symbols are not embedded).
//!   Weight is the number of stored edges; for a bridge through shared keys
//!   (`VIA_RPC`, `VIA_HTTP`, `VIA_BUS`, `VIA_CONFIG`, `DECLARES_RPC`) it is
//!   the number of distinct keys. Bridge edges come last, from quad
//!   `bridge_from` on, and `keys[i]` lists the keys of the i-th.
//! - `omitted`: `[[unit, symbols, files]]` left out by `max_symbols`.
//!
//! `max_symbols` keeps the page bounded: whole files are embedded per unit,
//! the budget shared out evenly (a unit smaller than its share keeps all of
//! it), code before tests. Nothing is dropped silently: every file is
//! listed, and the page says how many symbols each unit left out.

use super::apps::{is_key_link, is_probe};
use super::build::{AggInput, Ctx, Leaf, RepoFile, RepoRow, derive_leaf_bridges, receiving};
use super::units::Owners;
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};

/// Kinds that are not drawn as symbols: files and namespaces are the boxes
/// around them, externals belong to no unit.
const SKIP_KINDS: &[&str] = &["external", "module", "namespace", "module_ref"];
/// Edges that are structure, not relations.
const SKIP_EDGES: &[&str] = &["CONTAINS", "MODULE_FILE"];
const MAX_SIG: usize = 160;
/// Keys listed per bridge edge.
const MAX_KEYS: usize = 8;

struct Sym {
    id: i64,
    file: usize,
    kind: String,
    name: String,
    qual: String,
    line: i64,
    sig: Option<String>,
}

#[derive(Default)]
struct Strings {
    index: HashMap<String, i64>,
    list: Vec<String>,
}

impl Strings {
    fn id(&mut self, s: &str) -> i64 {
        if let Some(&i) = self.index.get(s) {
            return i;
        }
        let i = self.list.len() as i64;
        self.list.push(s.to_string());
        self.index.insert(s.to_string(), i);
        i
    }
}

/// A signature on one line, cut to `MAX_SIG` bytes with `…`.
fn one_line(sig: &str) -> String {
    let flat: String = sig.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.len() <= MAX_SIG {
        return flat;
    }
    let mut end = MAX_SIG;
    while !flat.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &flat[..end])
}

/// Per-unit symbol allowances: an even share of `cap`, raised until the
/// budget is spent, so a small unit keeps everything.
fn shares(totals: &[usize], cap: usize) -> Vec<usize> {
    if totals.iter().sum::<usize>() <= cap {
        return totals.to_vec();
    }
    let (mut lo, mut hi) = (0usize, totals.iter().copied().max().unwrap_or(0));
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        let used: usize = totals.iter().map(|&t| t.min(mid)).sum();
        if used <= cap { lo = mid } else { hi = mid - 1 }
    }
    totals.iter().map(|&t| t.min(lo)).collect()
}

/// The `zoom` section, and a note when `max_symbols` left symbols out.
pub(crate) fn zoom_json(
    ctx: &Ctx,
    owners: &Owners,
    units: usize,
    files: &[RepoFile],
    rows: &[RepoRow],
    max_symbols: usize,
) -> Result<(Value, Option<String>)> {
    // Files that some unit owns, in path order.
    let mut file_ix: HashMap<&str, usize> = HashMap::new();
    let mut file_unit: Vec<usize> = Vec::new();
    let mut file_list: Vec<&RepoFile> = Vec::new();
    for f in files {
        if let Some(u) = owners.owner(&f.0) {
            file_ix.insert(f.0.as_str(), file_list.len());
            file_unit.push(u);
            file_list.push(f);
        }
    }

    let conn = ctx.db.read_conn()?;
    let skip = SKIP_KINDS
        .iter()
        .map(|k| format!("'{k}'"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.signature
         FROM symbols s JOIN files f ON s.file_id = f.id
         WHERE s.graph_version = ?1
           AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
           AND s.kind NOT IN ({skip})
         ORDER BY f.path, s.start_byte, s.id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut by_file: Vec<Vec<Sym>> = (0..file_list.len()).map(|_| Vec::new()).collect();
    let mut q = stmt.query([ctx.graph_version])?;
    while let Some(r) = q.next()? {
        let path: String = r.get(1)?;
        let Some(&fi) = file_ix.get(path.as_str()) else {
            continue;
        };
        let sig: Option<String> = r.get(6)?;
        by_file[fi].push(Sym {
            id: r.get(0)?,
            file: fi,
            kind: r.get(2)?,
            name: r.get(3)?,
            qual: r.get(4)?,
            line: r.get(5)?,
            sig: crate::model::public_signature(sig).map(|s| one_line(&s)),
        });
    }
    drop(q);
    drop(stmt);
    // Counted before the cap, so a file left out still says what it holds.
    let f_syms: Vec<usize> = by_file.iter().map(Vec::len).collect();

    // Which files' symbols fit: per unit, code files first, then the rest,
    // whole files only, while the unit's share lasts.
    let mut unit_files: Vec<Vec<usize>> = vec![Vec::new(); units];
    for (fi, &u) in file_unit.iter().enumerate() {
        unit_files[u].push(fi);
    }
    for list in &mut unit_files {
        list.sort_by_key(|&fi| (!ctx.in_scope(&file_list[fi].0), fi));
    }
    let totals: Vec<usize> = unit_files
        .iter()
        .map(|l| l.iter().map(|&fi| by_file[fi].len()).sum())
        .collect();
    let allow = shares(&totals, max_symbols);
    let mut embedded = vec![false; file_list.len()];
    let mut omitted: Vec<(usize, usize, usize)> = Vec::new();
    for (u, list) in unit_files.iter().enumerate() {
        let mut left = allow[u];
        let (mut lost_syms, mut lost_files) = (0, 0);
        for &fi in list {
            let n = by_file[fi].len();
            if n <= left {
                left -= n;
                embedded[fi] = true;
            } else {
                lost_syms += n;
                lost_files += 1;
            }
        }
        if lost_syms > 0 {
            omitted.push((u, lost_syms, lost_files));
        }
    }

    let syms: Vec<Sym> = by_file
        .into_iter()
        .enumerate()
        .filter(|(fi, _)| embedded[*fi])
        .flat_map(|(_, s)| s)
        .collect();
    let sym_ix: HashMap<i64, i64> = syms
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id, i as i64))
        .collect();

    // Parents: the nearest embedded symbol up the CONTAINS chain.
    let mut contains: HashMap<i64, i64> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT source_symbol_id, target_symbol_id FROM edges
         WHERE graph_version = ?1 AND kind = 'CONTAINS'
           AND source_symbol_id IS NOT NULL AND target_symbol_id IS NOT NULL",
    )?;
    let mut q = stmt.query([ctx.graph_version])?;
    while let Some(r) = q.next()? {
        let (p, c): (i64, i64) = (r.get(0)?, r.get(1)?);
        contains.entry(c).or_insert(p);
    }
    let parent_of = |id: i64| -> i64 {
        let mut cur = id;
        for _ in 0..32 {
            match contains.get(&cur) {
                Some(&p) if p != id => {
                    if let Some(&i) = sym_ix.get(&p) {
                        return i;
                    }
                    cur = p;
                }
                _ => break,
            }
        }
        -1
    };

    // Relations between endpoints, counted per kind.
    let file_end = |path: &str| -> Option<i64> { file_ix.get(path).map(|&f| -1 - f as i64) };
    let mut agg: BTreeMap<(i64, i64, String), u64> = BTreeMap::new();
    let mut bridge_inputs: Vec<AggInput> = Vec::new();
    let mut routes = super::build::RouteCanon::default();
    for r in rows {
        if SKIP_EDGES.contains(&r.kind.as_str()) {
            continue;
        }
        let from = match sym_ix.get(&r.src_id) {
            Some(&i) => i,
            None => match file_end(&r.src_path) {
                Some(f) => f,
                None => continue,
            },
        };
        if is_key_link(&r.kind) {
            let raw =
                r.tq.as_deref()
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .or(r.tgt_qual.as_deref());
            let Some(raw) = raw else { continue };
            let key = routes.canonical(ctx.db, ctx.graph_version, &r.kind, raw);
            let me = Leaf::Module(from.to_string());
            let ep = Leaf::Endpoint(key);
            let (a, b) = if receiving(&r.kind) {
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
        if r.tgt_lang.as_deref() == Some("external") {
            continue;
        }
        let to = match r.tgt_id.and_then(|t| sym_ix.get(&t).copied()) {
            Some(i) => i,
            None => match r.tgt_path.as_deref().and_then(file_end) {
                Some(f) => f,
                None => continue,
            },
        };
        if from == to {
            continue;
        }
        *agg.entry((from, to, r.kind.clone())).or_default() += 1;
    }
    let mut bridges: BTreeMap<(i64, i64, String), Vec<String>> = BTreeMap::new();
    for d in derive_leaf_bridges(&bridge_inputs) {
        let (Leaf::Module(a), Leaf::Module(b)) = (&d.from, &d.to) else {
            continue;
        };
        let (Ok(a), Ok(b)) = (a.parse::<i64>(), b.parse::<i64>()) else {
            continue;
        };
        if d.keys.iter().all(|k| is_probe(k)) {
            continue;
        }
        // Config reads as reader -> provider, as on the sheet.
        let (a, b) = if d.kind == "VIA_CONFIG" {
            (b, a)
        } else {
            (a, b)
        };
        let keys = bridges.entry((a, b, d.kind.clone())).or_default();
        for k in &d.keys {
            if !keys.contains(k) {
                keys.push(k.clone());
            }
        }
    }

    let mut s = Strings::default();
    let mut e: Vec<i64> = Vec::with_capacity((agg.len() + bridges.len()) * 4);
    for ((a, b, kind), w) in &agg {
        e.extend([*a, *b, s.id(kind), *w as i64]);
    }
    let mut keys_json: Vec<Value> = Vec::new();
    for ((a, b, kind), keys) in &bridges {
        // Bridge edges follow the plain ones; `keys[i]` is the i-th bridge.
        e.extend([*a, *b, s.id(kind), keys.len() as i64]);
        keys_json.push(json!(keys.iter().take(MAX_KEYS).collect::<Vec<_>>()));
    }

    let f_unit: Vec<usize> = file_unit.clone();
    let f_path: Vec<i64> = file_list.iter().map(|f| s.id(&f.0)).collect();
    let f_lang: Vec<i64> = file_list.iter().map(|f| s.id(&f.1)).collect();
    let f_emb: Vec<u8> = embedded.iter().map(|&b| b as u8).collect();

    let y_file: Vec<usize> = syms.iter().map(|y| y.file).collect();
    let y_kind: Vec<i64> = syms.iter().map(|y| s.id(&y.kind)).collect();
    let y_name: Vec<i64> = syms.iter().map(|y| s.id(&y.name)).collect();
    let y_qual: Vec<i64> = syms.iter().map(|y| s.id(&y.qual)).collect();
    let y_line: Vec<i64> = syms.iter().map(|y| y.line).collect();
    let y_parent: Vec<i64> = syms.iter().map(|y| parent_of(y.id)).collect();
    let y_sig: Vec<i64> = syms
        .iter()
        .map(|y| y.sig.as_deref().map(|g| s.id(g)).unwrap_or(-1))
        .collect();

    let note = if omitted.is_empty() {
        None
    } else {
        let lost: usize = omitted.iter().map(|o| o.1).sum();
        let lost_files: usize = omitted.iter().map(|o| o.2).sum();
        Some(format!(
            "{} in {} not embedded for zooming (--max-symbols {max_symbols}); raise --max-symbols, or see one symbol's neighbourhood with lidx graph --seed <qualname>",
            super::build::count(lost, "symbol", "symbols"),
            super::build::count(lost_files, "file", "files"),
        ))
    };
    Ok((
        json!({
            "s": s.list,
            "f_unit": f_unit,
            "f_path": f_path,
            "f_lang": f_lang,
            "f_syms": f_syms,
            "f_emb": f_emb,
            "y_file": y_file,
            "y_kind": y_kind,
            "y_name": y_name,
            "y_qual": y_qual,
            "y_line": y_line,
            "y_parent": y_parent,
            "y_sig": y_sig,
            "e": e,
            "bridge_from": agg.len(),
            "keys": keys_json,
            "omitted": omitted.iter().map(|(u, n, f)| json!([u, n, f])).collect::<Vec<_>>(),
            "max_symbols": max_symbols,
        }),
        note,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_split_the_budget_evenly_and_keep_small_units_whole() {
        assert_eq!(shares(&[1, 5, 10], 100), vec![1, 5, 10]);
        assert_eq!(shares(&[1, 5, 10], 9), vec![1, 4, 4]);
        assert_eq!(shares(&[6, 6], 7), vec![3, 3]);
        assert_eq!(shares(&[], 5), Vec::<usize>::new());
    }

    #[test]
    fn signatures_fit_on_one_line() {
        assert_eq!(
            one_line("fn a(\n    x: i32,\n) -> i32"),
            "fn a( x: i32, ) -> i32"
        );
        let long = "x".repeat(MAX_SIG + 10);
        assert_eq!(one_line(&long).chars().count(), MAX_SIG + 1);
    }
}
