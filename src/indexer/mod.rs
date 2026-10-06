use crate::db::resolver;
use crate::db::{Db, FileRecord};
use crate::indexer::extract::ExtractedFile;
use crate::metrics;
use crate::model::{ChangedFilesResult, IndexStats};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Check that `repo` exists and is a directory, returning its canonical path.
///
/// This is the single trust-boundary check for user-supplied repo roots. It
/// must run before any database connection is opened or any directory is
/// created, so an invalid path can never materialise `<repo>/.lidx`.
pub fn validate_repo_root(repo: &Path) -> Result<PathBuf> {
    let metadata = std::fs::metadata(repo).with_context(|| {
        format!(
            "repo root does not exist or is inaccessible: {}",
            repo.display()
        )
    })?;
    if !metadata.is_dir() {
        bail!("repo root is not a directory: {}", repo.display());
    }
    std::fs::canonicalize(repo)
        .with_context(|| format!("canonicalize repo root {}", repo.display()))
}

/// The stored record of `file` when a reindex can carry it forward instead
/// of re-extracting it: same hash, no forced re-extraction, not stale.
fn unchanged_record<'a>(
    existing: &'a HashMap<String, FileRecord>,
    file: &scan::ScannedFile,
    force_reextract: bool,
    stale_files: &HashSet<String>,
) -> Option<&'a FileRecord> {
    existing.get(&file.rel_path).filter(|r| {
        r.hash == file.hash && !force_reextract && !stale_files.contains(&file.rel_path)
    })
}

/// Bump whenever extractor output changes (anything under `src/indexer/`), so
/// existing indexes re-extract unchanged files instead of hash-skipping them.
/// Enforced by `tests/extractor_version.rs`.
pub const EXTRACTOR_VERSION: i64 = 21;
const EXTRACTOR_VERSION_KEY: &str = "extractor_version";

pub mod batch;
pub mod bicep;
pub mod channel;
pub mod config;
mod cs_globals;
pub mod csharp;
pub mod differ;
pub mod extract;
pub mod go;
pub mod http;
pub mod javascript;
mod js_stale;
pub mod markdown;
pub mod postgres;
pub mod proto;
mod py_layout;
pub mod python;
pub mod rust;
pub mod scan;
pub mod sql_extractor;
pub mod stable_id;
pub mod string_consts;
pub mod test_detection;
pub mod tree_helpers;
pub mod xref;
pub mod yaml;

#[derive(Debug, Default)]
pub struct SyncStats {
    pub indexed: usize,
    pub deleted: usize,
    pub skipped: usize,
    pub errors: usize,
    pub symbols: usize,
    pub edges: usize,
}

pub struct Indexer {
    repo_root: PathBuf,
    db: Db,
    scan_options: scan::ScanOptions,
    graph_version: i64,
    commit_sha: Option<String>,
    extractors: HashMap<String, Box<dyn extract::LanguageExtractor>>,
    /// C# `global using` entries per project, and project directory per
    /// directory; built lazily, dropped whenever `cs_globals::prepare` runs.
    cs_globals: Option<HashMap<String, Vec<String>>>,
    cs_projects: HashMap<PathBuf, String>,
}

/// `Indexer::index_scanned_file_symbols`'s result: one file's extracted
/// content, its `files.id`, its post-diff symbol rows, (issue #77)
/// `diff.added`'s qualnames — `sync_abs_paths` uses that field to re-check
/// edges elsewhere that may have just become ambiguous -- and (issue #79)
/// whether `diff.deleted` was non-empty, so `sync_abs_paths` can tell
/// `Db::retry_unresolved_references` a symbol went away even when no whole
/// file did (an in-place edit that renames/removes a definition, not just
/// `delete_file`, can unblock a stored `Ambiguous` reference).
struct ScannedFileSymbols {
    extracted: ExtractedFile,
    file_id: i64,
    symbols: Vec<crate::model::Symbol>,
    added: Vec<String>,
    any_deleted: bool,
}

impl Indexer {
    pub fn new(repo_root: PathBuf, db_path: PathBuf) -> Result<Self> {
        Self::new_with_options(repo_root, db_path, scan::ScanOptions::default())
    }

    pub fn new_with_options(
        repo_root: PathBuf,
        db_path: PathBuf,
        scan_options: scan::ScanOptions,
    ) -> Result<Self> {
        let repo_root = validate_repo_root(&repo_root)?;
        let db = Db::new(&db_path)?;
        let graph_version = db.current_graph_version()?;
        let commit_sha = db.graph_version_commit(graph_version)?;

        let mut extractors: HashMap<String, Box<dyn extract::LanguageExtractor>> = HashMap::new();
        extractors.insert(
            "python".into(),
            Box::new(python::PythonExtractor::new()?.with_repo_root(repo_root.clone())),
        );
        extractors.insert(
            "rust".into(),
            Box::new(rust::RustExtractor::new()?.with_repo_root(repo_root.clone())),
        );
        extractors.insert(
            "javascript".into(),
            Box::new(javascript::JavascriptExtractor::new()?),
        );
        extractors.insert(
            "typescript".into(),
            Box::new(javascript::TypescriptExtractor::new()?),
        );
        extractors.insert("tsx".into(), Box::new(javascript::TsxExtractor::new()?));
        extractors.insert("csharp".into(), Box::new(csharp::CSharpExtractor::new()?));
        extractors.insert("go".into(), Box::new(go::GoExtractor::new()?));
        extractors.insert("sql".into(), Box::new(sql_extractor::SqlExtractor::new()?));
        extractors.insert(
            "postgres".into(),
            Box::new(sql_extractor::SqlExtractor::new()?),
        );
        extractors.insert("tsql".into(), Box::new(sql_extractor::SqlExtractor::new()?));
        extractors.insert("proto".into(), Box::new(proto::ProtoExtractor::new()?));
        extractors.insert("yaml".into(), Box::new(yaml::YamlExtractor::new()?));
        extractors.insert("bicep".into(), Box::new(bicep::BicepExtractor::new()?));
        extractors.insert("markdown".into(), Box::new(markdown::MarkdownExtractor));

        Ok(Self {
            repo_root,
            db,
            scan_options,
            graph_version,
            commit_sha,
            extractors,
            cs_globals: None,
            cs_projects: HashMap::new(),
        })
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn db_mut(&mut self) -> &mut Db {
        &mut self.db
    }

    pub fn repo_root(&self) -> &PathBuf {
        &self.repo_root
    }

    /// True when the stored extractor version differs from `EXTRACTOR_VERSION`
    /// (including a never-indexed DB), i.e. a full `reindex` must re-extract.
    pub fn extractor_version_stale(&self) -> Result<bool> {
        Ok(self.db.get_meta_i64(EXTRACTOR_VERSION_KEY)? != Some(EXTRACTOR_VERSION))
    }

    pub fn graph_version(&self) -> i64 {
        self.graph_version
    }

    pub fn commit_sha(&self) -> Option<&str> {
        self.commit_sha.as_deref()
    }

    pub fn changed_files(&mut self, languages: Option<&[String]>) -> Result<ChangedFilesResult> {
        let scanned = scan::scan_repo_with_options(&self.repo_root, self.scan_options)?;
        let scanned: Vec<_> = match languages {
            Some(languages) => scanned
                .into_iter()
                .filter(|file| languages.contains(&file.language))
                .collect(),
            None => scanned,
        };
        let existing = self.db.list_files(self.graph_version)?;
        let mut existing_map: HashMap<String, String> = HashMap::new();
        for record in existing {
            if let Some(languages) = languages
                && !languages.contains(&record.language)
            {
                continue;
            }
            existing_map.insert(record.path, record.hash);
        }

        let mut added = Vec::new();
        let mut modified = Vec::new();
        let mut seen = HashSet::new();
        for file in scanned {
            seen.insert(file.rel_path.clone());
            match existing_map.get(&file.rel_path) {
                None => added.push(file.rel_path),
                Some(hash) if hash != &file.hash => modified.push(file.rel_path),
                _ => {}
            }
        }
        let mut deleted: Vec<String> = existing_map
            .keys()
            .filter(|path| !seen.contains(*path))
            .cloned()
            .collect();

        added.sort();
        modified.sort();
        deleted.sort();
        Ok(ChangedFilesResult {
            added,
            modified,
            deleted,
        })
    }

    pub fn sync_rel_paths(&mut self, rel_paths: &[String]) -> Result<SyncStats> {
        let abs_paths: Vec<PathBuf> = rel_paths
            .iter()
            .map(|rel| self.repo_root.join(rel))
            .collect();
        self.sync_abs_paths(&abs_paths)
    }

    /// Incremental sync of `paths` into the current graph version. Takes the
    /// reindex lock (issue #250) so it cannot interleave with a reindex, and
    /// fails with [`crate::db::ReindexBusy`] when one is running.
    pub fn sync_abs_paths(&mut self, paths: &[PathBuf]) -> Result<SyncStats> {
        let _lock = self.db.try_lock_reindex()?;
        // A reindex in another process may have promoted a new version since
        // this indexer last looked.
        self.adopt_completed_version()?;
        self.sync_abs_paths_locked(paths)
    }

    fn sync_abs_paths_locked(&mut self, paths: &[PathBuf]) -> Result<SyncStats> {
        let mut stats = SyncStats::default();
        let mut touched = false;
        let mut indexed_files = Vec::new();
        // Phase 1: deletions handled inline, but every file that needs
        // (re)indexing only has its symbols extracted and its
        // `visibility` settled here — edge resolution is deferred to
        // phase 2 below, after every file in this batch has been marked.
        // A cross-file guarded-fallback visibility check must never see a
        // later-in-this-batch file's symbol as still unmarked (NULL,
        // meaning "unrestricted") just because that file hasn't been
        // synced yet — same reasoning as `reindex`'s two-pass split
        // (issue #75 follow-up).
        let mut pending: Vec<(
            scan::ScannedFile,
            ExtractedFile,
            i64,
            Vec<crate::model::Symbol>,
        )> = Vec::new();
        // Issue #77: qualnames of every symbol this batch adds —
        // an edge anywhere, even in a file this batch never touches, that
        // is already bound by one of these names must be re-checked after
        // the sync, not left stale, since the addition may have made that
        // name ambiguous. See `Db::unbind_edges_for_qualnames`.
        let mut added_qualnames: HashSet<String> = HashSet::new();
        // Issue #79: whether this batch removed any symbol -- a whole file
        // (`stats.deleted`, below) or just one definition an in-place edit
        // renamed/removed. Either can turn a stored `Ambiguous` reference
        // unique again, which `Db::retry_unresolved_references`'s
        // insertion-only watermark would otherwise never notice.
        let mut any_symbols_deleted = false;
        javascript::clear_export_cache();
        // Hash-unchanged JS/TS files whose chased imports or alias config
        // changed must be re-extracted too. Computed before any deletion so
        // importers' edges still exist.
        let batch_rels: Vec<String> = paths
            .iter()
            .filter_map(|p| crate::util::normalize_rel_path(&self.repo_root, p).ok())
            .collect();
        let js_stale = self.js_stale();
        let mut stale_files = js_stale.stale_js_files(&batch_rels, self.graph_version)?;
        // ... and C# files whose project's `global using`s changed or that
        // call an extension method a changed file declares.
        let graph_version = self.graph_version;
        let live_csharp = |db: &Db| -> Result<Vec<String>> {
            Ok(db
                .list_files(graph_version)?
                .into_iter()
                .map(|f| f.path)
                .filter(|p| cs_globals::is_csharp_path(p))
                .collect())
        };
        // Only files that really change count as "changed" here: a
        // hash-unchanged declaring file is skipped below, so its extension
        // methods must stay in the seed (and its callers un-stale).
        let changed_rels = self.changed_batch_paths(&batch_rels)?;
        let stale_cs = self.stale_csharp_files(&changed_rels, graph_version, live_csharp)?;
        stale_files.extend(stale_cs);
        if batch_rels
            .iter()
            .any(|p| p == "Cargo.toml" || p.ends_with("/Cargo.toml"))
        {
            let stale_rs = self.stale_rust_files(graph_version)?;
            stale_files.extend(stale_rs.into_iter().filter(|p| !batch_rels.contains(p)));
        }
        if batch_rels.iter().any(|p| py_layout::is_layout_marker(p)) {
            let stale_py = self.stale_python_files(graph_version)?;
            stale_files.extend(stale_py.into_iter().filter(|p| !batch_rels.contains(p)));
        }
        self.begin_extraction_run(&changed_rels, graph_version, true)?;
        let mut all_paths: Vec<PathBuf> = paths.to_vec();
        all_paths.extend(stale_files.iter().map(|rel| self.repo_root.join(rel)));
        let stale_callers = self.prescan_files(all_paths.iter().cloned(), graph_version)?;
        all_paths.extend(stale_callers.iter().map(|rel| self.repo_root.join(rel)));
        stale_files.extend(stale_callers);
        for path in &all_paths {
            let rel_path = match crate::util::normalize_rel_path(&self.repo_root, path) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if !path.exists() {
                self.delete_file(&rel_path)?;
                stats.deleted += 1;
                any_symbols_deleted = true;
                touched = true;
                continue;
            }
            let Some(scanned) = scan::scan_path(&self.repo_root, path)? else {
                if !path.exists() {
                    self.delete_file(&rel_path)?;
                    stats.deleted += 1;
                    any_symbols_deleted = true;
                    touched = true;
                }
                continue;
            };
            if let Some(existing) = self.db.get_file_by_path(&scanned.rel_path)?
                && existing.hash == scanned.hash
                && existing.deleted_version.is_none()
                && !stale_files.contains(&scanned.rel_path)
            {
                stats.skipped += 1;
                continue;
            }
            match self.index_scanned_file_symbols(&scanned) {
                Ok(Some(ScannedFileSymbols {
                    extracted,
                    file_id,
                    symbols,
                    added,
                    any_deleted,
                })) => {
                    added_qualnames.extend(added);
                    any_symbols_deleted |= any_deleted;
                    indexed_files.push(scanned.clone());
                    pending.push((scanned, extracted, file_id, symbols));
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!("index error {}: {err}", scanned.rel_path);
                    stats.errors += 1;
                }
            }
        }
        // Phase 2: resolve edges for every file in this batch, now that
        // every file's visibility is settled.
        for (_scanned, extracted, file_id, symbols) in &pending {
            let (symbol_count, edge_count) =
                self.resolve_file_edges(*file_id, extracted, symbols)?;
            stats.indexed += 1;
            stats.symbols += symbol_count;
            stats.edges += edge_count;
            touched = true;
        }

        if !indexed_files.is_empty() {
            let xref_edges = xref::link_cross_language_refs(
                &mut self.db,
                &indexed_files,
                false,
                self.graph_version,
            )?;
            stats.edges += xref_edges;
        }
        javascript::clear_export_cache();
        if batch_rels.iter().any(|p| p.ends_with(".json")) {
            // Keep the reindex fingerprint current so the next reindex
            // doesn't re-extract every JS/TS file for a change sync handled.
            let js_stale = self.js_stale();
            let fingerprint = js_stale.config_fingerprint(&js_stale.js_ts_file_paths()?);
            self.db
                .set_meta_i64(js_stale::CONFIG_FINGERPRINT_KEY, fingerprint)?;
        }
        if touched {
            // Issue #77: an edge outside this batch already bound to a
            // qualname this batch just gave a second (same or differently
            // kinded) symbol must be re-checked, not left pointing at the
            // old candidate — see `added_qualnames` above.
            if !added_qualnames.is_empty() {
                self.db
                    .unbind_edges_for_qualnames(&added_qualnames, self.graph_version)?;
            }

            // Issue #78/#79: reconcile first, so any edge this batch just
            // left with a NULL target and no store row (a forward reference
            // into a file synced earlier in this same batch, or one
            // `unbind_edges_for_qualnames` just cleared) gets an immediate
            // shot at every symbol that exists so far -- not gated by the
            // retry watermark -- before falling to a store row. Then retry
            // stored rows a newly inserted symbol (or, per `any_symbols_deleted`
            // below, a deletion that turned a stored `Ambiguous` row unique
            // again) might satisfy. See `Db::repair_unresolved`.
            self.db.repair_unresolved(
                self.graph_version,
                any_symbols_deleted,
                "incremental sync",
            )?;
            self.db.reconcile_rpc_edges(self.graph_version)?;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            self.db.set_meta_i64("last_indexed", now)?;
        }
        Ok(stats)
    }

    /// Reindex the repository without the empty-scan guard.
    ///
    /// Internal callers (watch, RPC handlers, init) use this: their repo root
    /// was validated at startup, and a walk error on a vanished root is fatal
    /// in the scanner. Only the `lidx reindex` CLI applies the guard, through
    /// [`Indexer::reindex_with_options`].
    pub fn reindex(&mut self) -> Result<IndexStats> {
        self.reindex_impl(true)
    }

    /// Reindex the repository, refusing a destructive empty scan.
    ///
    /// Unless `allow_empty` is true, a scan that finds zero files is refused
    /// with an error naming the previous indexed file count and the scanned
    /// count (0). The refusal happens before any database write, so the index
    /// and graph version are left untouched. Pass `allow_empty = true` to
    /// legitimately index an empty repository or empty an existing index.
    pub fn reindex_with_options(&mut self, allow_empty: bool) -> Result<IndexStats> {
        self.reindex_impl(allow_empty)
    }

    /// Serialises against other reindexes (issue #250), then runs
    /// `reindex_locked`. The new graph version is built while the last
    /// completed one stays current, and is promoted only at the end; on any
    /// error this indexer goes back to the completed version.
    /// Point this indexer at the last completed graph version. A long-lived
    /// indexer's version may be stale (another process can have completed a
    /// reindex since it was constructed) or may name an abandoned `building`
    /// one after a failed run.
    fn adopt_completed_version(&mut self) -> Result<i64> {
        let completed = self.db.current_graph_version()?;
        self.graph_version = completed;
        self.commit_sha = self.db.graph_version_commit(completed)?;
        Ok(completed)
    }

    fn reindex_impl(&mut self, allow_empty: bool) -> Result<IndexStats> {
        let lock = self.db.try_lock_reindex()?;
        let completed = self.adopt_completed_version()?;
        let result = self.reindex_locked(allow_empty, &lock);
        if result.is_err() {
            // Best effort: never let a failure here replace the original error.
            if let Err(err) = self.adopt_completed_version() {
                eprintln!("lidx: could not restore completed graph version {completed}: {err}");
            }
        }
        result
    }

    fn reindex_locked(
        &mut self,
        allow_empty: bool,
        lock: &crate::db::ReindexLock,
    ) -> Result<IndexStats> {
        let started = Instant::now();
        let previous_graph_version = self.graph_version;
        let commit_sha = crate::util::git_head_sha(&self.repo_root);
        let scanned = scan::scan_repo_with_options(&self.repo_root, self.scan_options)?;
        let existing = self.db.list_files(previous_graph_version)?;
        if scanned.is_empty() && !allow_empty {
            bail!(
                "refusing to reindex {}: scan found {} file(s) but the previous index has {}. \
                 This usually means the repo path is wrong, moved or emptied; \
                 pass --allow-empty to proceed",
                self.repo_root.display(),
                scanned.len(),
                existing.len()
            );
        }
        // A `building` version left by a run that died before promoting it is
        // unreachable (never current) but its `files` rows (hashes, deletion
        // marks) were already rewritten, so they no longer describe the
        // completed version: reclaim it and re-extract everything.
        let abandoned = self.db.reclaim_abandoned_graph_versions(lock)?;
        if abandoned > 0 {
            eprintln!(
                "lidx: reclaimed {abandoned} abandoned graph version(s) from an interrupted reindex; re-extracting all files"
            );
        }
        self.graph_version = self.db.allocate_graph_version(commit_sha.as_deref())?;
        self.commit_sha = commit_sha;
        let mut existing_map: HashMap<String, FileRecord> = HashMap::new();
        for record in existing {
            existing_map.insert(record.path.clone(), record);
        }

        // A stale extractor version means unchanged files must be re-extracted.
        let force_reextract = abandoned > 0 || self.extractor_version_stale()?;

        // Hash-unchanged JS/TS files whose chased imports or alias config
        // changed are re-extracted too (see `js_stale`).
        javascript::clear_export_cache();
        let scanned_paths: HashSet<&str> = scanned.iter().map(|f| f.rel_path.as_str()).collect();
        let mut changed_paths: Vec<String> = scanned
            .iter()
            .filter(|f| {
                existing_map
                    .get(&f.rel_path)
                    .is_none_or(|record| record.hash != f.hash)
            })
            .map(|f| f.rel_path.clone())
            .collect();
        changed_paths.extend(
            existing_map
                .keys()
                .filter(|p| !scanned_paths.contains(p.as_str()))
                .cloned(),
        );
        let js_stale = self.js_stale();
        let mut stale_files = js_stale.stale_js_files(&changed_paths, previous_graph_version)?;
        // A tsconfig/jsconfig isn't an indexed file, so its edits show up
        // only as a fingerprint change (this also covers a package base in
        // node_modules, which sync doesn't watch): re-extract every JS/TS file.
        let js_paths: Vec<String> = scanned
            .iter()
            .filter(|f| javascript::is_js_ts_path(&f.rel_path))
            .map(|f| f.rel_path.clone())
            .collect();
        let config_fingerprint = js_stale.config_fingerprint(&js_paths);
        if self.db.get_meta_i64(js_stale::CONFIG_FINGERPRINT_KEY)? != Some(config_fingerprint) {
            stale_files.extend(js_paths);
        }

        // ... and Python files whose package root a layout marker moved.
        stale_files.extend(self.stale_python_files(previous_graph_version)?);
        // ... and Rust files whose crate root a Cargo.toml edit moved.
        stale_files.extend(self.stale_rust_files(previous_graph_version)?);
        // ... and C# files whose project's `global using`s changed.
        let scanned_csharp: Vec<String> = scanned
            .iter()
            .filter(|f| cs_globals::is_csharp_path(&f.rel_path))
            .map(|f| f.rel_path.clone())
            .collect();
        // A stale extractor version re-records every file's directives.
        let cs_changed = if force_reextract {
            scanned_csharp.clone()
        } else {
            changed_paths.clone()
        };
        stale_files.extend(
            self.stale_csharp_files(&cs_changed, previous_graph_version, |_| Ok(scanned_csharp))?,
        );
        // A forced re-extraction visits every file, so it needs no seed.
        self.begin_extraction_run(&changed_paths, previous_graph_version, !force_reextract)?;
        let to_extract: Vec<PathBuf> = scanned
            .iter()
            .filter(|f| unchanged_record(&existing_map, f, force_reextract, &stale_files).is_none())
            .map(|f| f.abs_path.clone())
            .collect();
        stale_files.extend(self.prescan_files(to_extract.into_iter(), previous_graph_version)?);

        let mut seen = HashSet::new();
        let mut stats = IndexStats {
            scanned: scanned.len(),
            indexed: 0,
            skipped: 0,
            deleted: 0,
            symbols: 0,
            edges: 0,
            duration_ms: 0,
            prune_error: None,
        };

        // Phase 4: Use batch writing for reindex
        // Collect file diffs and upsert files first
        let mut batch_writer = batch::BatchWriter::with_defaults();
        let mut file_data: Vec<(scan::ScannedFile, ExtractedFile, differ::SymbolDiff, i64)> =
            Vec::new();
        // Files whose content hash matches `previous_graph_version`: carried forward
        // (symbols + edges copied via SQL) below instead of being re-parsed.
        let mut carry_forward_ids: Vec<i64> = Vec::new();

        for file in &scanned {
            seen.insert(file.rel_path.clone());

            if let Some(existing_record) =
                unchanged_record(&existing_map, file, force_reextract, &stale_files)
            {
                // Unchanged: skip the parse (tree-sitter + symbol extraction is the
                // expensive part) and carry the file's rows forward further down.
                self.db.upsert_file(
                    &file.rel_path,
                    &file.hash,
                    &file.language,
                    file.size,
                    file.modified,
                )?;
                carry_forward_ids.push(existing_record.id);
                stats.skipped += 1;
                continue;
            }

            // Extract symbols
            let source = match crate::util::read_to_string(&file.abs_path) {
                Ok(s) => s,
                Err(err) => {
                    eprintln!("read error {}: {err}", file.rel_path);
                    continue;
                }
            };

            let mut extracted = match self.extract_file(file, &source) {
                Ok(e) => e,
                Err(err) => {
                    eprintln!("extract error {}: {err}", file.rel_path);
                    continue;
                }
            };

            let file_metrics = metrics::compute_file_metrics(&source, &file.language);
            let symbol_metrics =
                metrics::compute_symbol_metrics(&source, &file.language, &extracted.symbols);
            extracted.file_metrics = Some(file_metrics);
            extracted.symbol_metrics = symbol_metrics;

            // Compute diff
            let existing_symbols = self
                .db
                .get_symbols_for_file(&file.rel_path, self.graph_version)?;
            let diff = differ::compute_symbol_diff(existing_symbols, extracted.symbols.clone());
            warn_stable_id_collisions(&file.rel_path, &diff);

            // Upsert file to get file_id
            let file_id = self.db.upsert_file(
                &file.rel_path,
                &file.hash,
                &file.language,
                file.size,
                file.modified,
            )?;

            // Add to batch
            batch_writer.add(batch::FileDiff {
                file_id,
                file_path: file.rel_path.clone(),
                diff: diff.clone(),
                graph_version: self.graph_version,
                commit_sha: self.commit_sha.clone(),
            });

            // Store for edge processing
            file_data.push((file.clone(), extracted, diff, file_id));

            // Flush if batch is ready
            if batch_writer.should_flush() {
                let batch = batch_writer.take();
                self.db.update_files_symbols_batch(&batch)?;
            }
        }

        // Flush remaining batch
        if !batch_writer.is_empty() {
            let batch = batch_writer.take();
            self.db.update_files_symbols_batch(&batch)?;
        }

        // Issue #258: carry unchanged files' *symbols* forward now, before any
        // edge is resolved. Resolution filters candidates on the new graph
        // version, so a re-parsed file's references must see every carried
        // symbol or they resolve differently from a fresh index (lost alias
        // targets, ambiguity collapsing to a bare-name bind). Carried *edges*
        // still wait until after the fresh-file edge loop below.
        let symbols_carried = self.db.carry_forward_symbols(
            &carry_forward_ids,
            previous_graph_version,
            self.graph_version,
        )?;

        // Mark every file's private/unexported symbols in its own pass,
        // before any edge in this reindex is resolved. Must not be
        // interleaved with the edge loop below: a cross-file candidate's
        // `visibility` has to be settled repo-wide first, or a file
        // processed early would see a later file's private symbols as
        // still-NULL (unrestricted) and bind to them (issue #75 follow-up).
        for (_file, extracted, _diff, file_id) in &file_data {
            self.db.set_private_symbols(
                *file_id,
                self.graph_version,
                &extracted.private_qualnames,
                &extracted.static_member_qualnames,
                &extracted.override_symbols,
            )?;
        }

        // Issue #79: whether this reindex removed any symbol -- a whole
        // file (`stats.deleted`, set below by the not-`seen` loop) or just
        // one definition a re-parsed file's diff dropped. Either can turn a
        // stored `Ambiguous` reference unique again, which
        // `Db::retry_unresolved_references`'s insertion-only watermark
        // would otherwise never notice -- see `sync_abs_paths`'s matching
        // flag.
        let mut any_symbols_deleted = false;

        // Now process edges for all files
        for (file, extracted, diff, file_id) in file_data {
            any_symbols_deleted |= !diff.deleted.is_empty();
            // Delete existing edges
            self.db.delete_edges_for_file(file_id, self.graph_version)?;

            // Get symbols for edge resolution
            let symbols = self
                .db
                .get_symbols_for_file(&file.rel_path, self.graph_version)?;
            let symbol_map = resolver::build_exact_symbol_map(&symbols);

            // Insert edges
            let edges_count = self.db.insert_edges(
                file_id,
                &extracted.edges,
                &symbol_map,
                self.graph_version,
                self.commit_sha.as_deref(),
            )?;

            // Update metrics
            if let Some(metrics) = extracted.file_metrics.as_ref() {
                self.db.upsert_file_metrics(file_id, metrics)?;
            }
            self.db
                .insert_symbol_metrics(file_id, &extracted.symbol_metrics, &symbols)?;

            stats.indexed += 1;
            stats.symbols += diff.added.len() + diff.modified.len() + diff.unchanged.len();
            stats.edges += edges_count;
        }

        // Carry forward unchanged files' edges (and metrics, stubs, stored
        // unresolved references) into the new graph version. Must run after the
        // fresh-file edge loop above, so cross-file edge targets that land in a
        // re-parsed file already have their new-version symbol row. Their
        // symbols were carried earlier (see above), before resolution.
        if !carry_forward_ids.is_empty() {
            let files = symbols_carried.file_count();
            let symbols = symbols_carried.symbols;
            let refs = self.db.carry_forward_references(symbols_carried)?;
            eprintln!(
                "lidx: carried forward {files} unchanged file(s): {} symbol(s), {} edge(s)",
                symbols + refs.stubs,
                refs.edges
            );
        }

        for path in existing_map.keys() {
            if !seen.contains(path) {
                self.db.mark_file_deleted(path, self.graph_version)?;
                stats.deleted += 1;
            }
        }

        let xref_edges =
            xref::link_cross_language_refs(&mut self.db, &scanned, true, self.graph_version)?;
        stats.edges += xref_edges;

        // Repair pass: re-resolve NULL edge targets by qualname, same as the
        // incremental (sync_abs_paths) path already does. Runs after both the
        // fresh-file edge loop and carry_forward_references (and after xref, so
        // XREF/ROUTE edges get the same treatment) so every current-version
        // symbol this reindex will produce already exists to resolve against;
        // runs before prune_and_maybe_vacuum so nothing is wasted repairing
        // rows about to be deleted.
        //
        // Gate: always run when this reindex actually indexed or deleted a file (cheapest
        // check, and those runs already pay far more than the repair pass costs). On a
        // purely-carried-forward run (nothing indexed or deleted), fall back to a COUNT of
        // this version's NULL-target edges -- issue #79 means that's a Bridge Edge kind
        // row exclusively (every other kind's unresolved reference lives only in the
        // `unresolved_references` store, which carry-forward always copies as-is, so it
        // can't develop this kind of hole; see `Db::carry_forward_references`). That COUNT is
        // what distinguishes a truly idle warm reindex (nothing to do, stay fast) from one
        // carrying forward a degraded Bridge Edge kind: carry_forward_references re-links every
        // edge by stable_id into the new version and leaves target_symbol_id NULL wherever
        // that lookup misses (a deleted/renamed target, or a target manually NULLed out by
        // outside SQL), so a degraded index's holes are visible in the *new* graph_version's
        // edge rows even when zero files changed. Without this fallback those NULLs — and
        // the stale target_qualname strings that ride along with them, e.g. after a callee
        // moves modules — propagate forward untouched on every subsequent reindex, which is
        // exactly the self-healing gap this exists to close.
        //
        // The COUNT alone isn't enough to gate on, though: real codebases always have edges
        // into external/stdlib symbols (`std::fs::remove_dir_all`, `serde_json::from_str`, a
        // JS `console.log`) that have a target_qualname but no matching local symbol, so they
        // are — correctly — never resolved and never will be. Those sit in the COUNT on every
        // single run, so a bare "count > 0" would make repair run on every warm reindex of any
        // real repo, not just a degraded one (measured: +~1.2s on this repo, every time —
        // exactly the regression this function must not cause). Instead compare against the
        // floor recorded the last time repair actually ran (`unresolved_edge_floor` meta,
        // absent = 0, i.e. conservative on a never-repaired-under-this-binary db): only a
        // count *above* that floor — something that used to resolve and no longer does — is
        // new repair work.
        let unresolved_edge_count = |db: &Db, graph_version: i64| -> Result<i64> {
            Ok(db.read_conn()?.query_row(
                "SELECT COUNT(*) FROM edges
                 WHERE graph_version = ?
                   AND target_symbol_id IS NULL
                   AND target_qualname IS NOT NULL",
                rusqlite::params![graph_version],
                |row| row.get(0),
            )?)
        };
        let needs_repair = if stats.indexed > 0 || stats.deleted > 0 {
            true
        } else {
            let unresolved = unresolved_edge_count(&self.db, self.graph_version)?;
            let floor = self.db.get_meta_i64("unresolved_edge_floor")?.unwrap_or(0);
            unresolved > floor
        };
        if needs_repair {
            // Issue #78/#79: reconcile first -- catches an edge that went
            // NULL only after it was first resolved (a deleted/renamed
            // target, or `unbind_edges_for_qualnames`), or a
            // `carry_forward_references` edge whose store row it couldn't carry
            // forward (an endpoint with no `stable_id` match) -- so it gets
            // a shot at every symbol that exists so far before falling to a
            // store row. Then targeted, store-driven retry (see the
            // matching call in `sync_abs_paths`). `stats.deleted` (whole
            // files) is now final, so fold it in alongside
            // `any_symbols_deleted` (definitions a re-parsed file's diff
            // dropped in place) -- see `Db::repair_unresolved`.
            self.db.repair_unresolved(
                self.graph_version,
                any_symbols_deleted || stats.deleted > 0,
                "reindex",
            )?;
            self.db.reconcile_rpc_edges(self.graph_version)?;

            let remaining = unresolved_edge_count(&self.db, self.graph_version)?;
            self.db.set_meta_i64("unresolved_edge_floor", remaining)?;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.db
            .set_meta_i64(js_stale::CONFIG_FINGERPRINT_KEY, config_fingerprint)?;
        self.db.set_meta_i64("last_indexed", now)?;
        self.db
            .set_meta_i64(EXTRACTOR_VERSION_KEY, EXTRACTOR_VERSION)?;

        // The version is fully populated: only now does it become current.
        self.db.promote_graph_version(self.graph_version)?;

        // Reclaim rows from graph versions this reindex just aged out. Safe to
        // run now: this reindex's own carry-forward already read everything it
        // needed from `previous_graph_version` above.
        match self.db.prune_and_maybe_vacuum() {
            Ok((symbols_pruned, edges_pruned, versions_pruned, vacuumed)) => {
                if versions_pruned > 0 {
                    eprintln!(
                        "lidx: pruned {versions_pruned} old graph version(s): {symbols_pruned} symbol row(s), {edges_pruned} edge row(s){}",
                        if vacuumed {
                            ", reclaimed space via VACUUM"
                        } else {
                            ""
                        }
                    );
                }
            }
            Err(err) => {
                eprintln!("Warning: graph version prune failed: {err}");
                stats.prune_error = Some(err.to_string());
            }
        }

        stats.duration_ms = started.elapsed().as_millis() as u64;
        Ok(stats)
    }

    /// Phase 1 of syncing one file: extract, diff, write its symbols, and
    /// settle its `visibility` marks. Deliberately stops short of edge
    /// resolution (`resolve_file_edges`) — see `sync_abs_paths`'s doc for
    /// why the two are split across a whole batch rather than done
    /// per-file. `Ok(None)` means the file was skipped (too large), not
    /// an error.
    fn index_scanned_file_symbols(
        &mut self,
        file: &scan::ScannedFile,
    ) -> Result<Option<ScannedFileSymbols>> {
        // Phase 6: Check file size before reading (skip very large files)
        const MAX_FILE_SIZE_MB: u64 = 10;
        let metadata = std::fs::metadata(&file.abs_path)?;
        if metadata.len() > MAX_FILE_SIZE_MB * 1024 * 1024 {
            eprintln!(
                "lidx: Skipping large file ({}MB): {}",
                metadata.len() / (1024 * 1024),
                file.rel_path
            );
            return Ok(None);
        }

        let source = crate::util::read_to_string(&file.abs_path)?;
        let mut extracted = self.extract_file(file, &source)?;
        let file_metrics = metrics::compute_file_metrics(&source, &file.language);
        let symbol_metrics =
            metrics::compute_symbol_metrics(&source, &file.language, &extracted.symbols);
        extracted.file_metrics = Some(file_metrics);
        extracted.symbol_metrics = symbol_metrics;

        // Phase 2: Compute symbol diff for incremental updates
        // Fetch existing symbols from database
        let existing_symbols = self
            .db
            .get_symbols_for_file(&file.rel_path, self.graph_version)?;

        // Compute diff between old and new symbols
        let diff = differ::compute_symbol_diff(existing_symbols, extracted.symbols.clone());
        warn_stable_id_collisions(&file.rel_path, &diff);

        // Phase 3: Log diff statistics
        if !diff.added.is_empty() || !diff.modified.is_empty() || !diff.deleted.is_empty() {
            eprintln!(
                "lidx: symbol diff for {}: +{} ~{} -{} ={} (total: {})",
                file.rel_path,
                diff.added.len(),
                diff.modified.len(),
                diff.deleted.len(),
                diff.unchanged.len(),
                diff.added.len() + diff.modified.len() + diff.deleted.len() + diff.unchanged.len()
            );
        }

        let file_id = self.db.upsert_file(
            &file.rel_path,
            &file.hash,
            &file.language,
            file.size,
            file.modified,
        )?;

        // Issue #77: qualnames this sync is about to add, captured before
        // `update_file_symbols` consumes `diff` — see `sync_abs_paths`.
        let added_qualnames: Vec<String> = diff.added.iter().map(|s| s.qualname.clone()).collect();
        // Issue #79: likewise captured before `diff` moves, for
        // `retry_unresolved_references`'s deletion-driven ambiguity retry.
        let any_deleted = !diff.deleted.is_empty();

        // Phase 3: Use incremental updates for symbols
        let symbols = self.db.update_file_symbols(
            file_id,
            &file.rel_path,
            diff,
            self.graph_version,
            self.commit_sha.as_deref(),
        )?;

        // Mark this file's private/unexported symbols. Must happen for
        // every file in the batch before any file's edges are resolved —
        // see `sync_abs_paths`.
        self.db.set_private_symbols(
            file_id,
            self.graph_version,
            &extracted.private_qualnames,
            &extracted.static_member_qualnames,
            &extracted.override_symbols,
        )?;

        Ok(Some(ScannedFileSymbols {
            extracted,
            file_id,
            symbols,
            any_deleted,
            added: added_qualnames,
        }))
    }

    /// Delete `rel_path`'s stored file (symbols, edges, metrics, and its
    /// `deleted_version` mark) if it's currently indexed. A no-op when the
    /// path isn't indexed at all.
    fn delete_file(&mut self, rel_path: &str) -> Result<()> {
        let Some(existing) = self.db.get_file_by_path(rel_path)? else {
            return Ok(());
        };
        self.db
            .delete_symbols_edges_for_file(existing.id, self.graph_version)?;
        self.db.mark_file_deleted(rel_path, self.graph_version)?;
        Ok(())
    }

    /// Phase 2 of syncing one file: resolve its edges and write its
    /// metrics, against `symbols` (this file's own, from
    /// `index_scanned_file_symbols`) — every other file's `visibility` in
    /// this batch must already be settled by the time this runs.
    fn resolve_file_edges(
        &mut self,
        file_id: i64,
        extracted: &ExtractedFile,
        symbols: &[crate::model::Symbol],
    ) -> Result<(usize, usize)> {
        // For edges, still use delete-all-insert for now (can optimize in future)
        // Delete existing edges for this file
        self.db.delete_edges_for_file(file_id, self.graph_version)?;
        let symbol_map = resolver::build_exact_symbol_map(symbols);
        let edges_count = self.db.insert_edges(
            file_id,
            &extracted.edges,
            &symbol_map,
            self.graph_version,
            self.commit_sha.as_deref(),
        )?;
        if let Some(metrics) = extracted.file_metrics.as_ref() {
            self.db.upsert_file_metrics(file_id, metrics)?;
        }
        self.db
            .insert_symbol_metrics(file_id, &extracted.symbol_metrics, symbols)?;

        Ok((symbols.len(), edges_count))
    }

    /// The subset of `rels` a sync will actually re-index: deleted files and
    /// files whose stored hash is missing or differs from the disk content
    /// (the same notion of "changed" `reindex` uses).
    fn changed_batch_paths(&self, rels: &[String]) -> Result<Vec<String>> {
        let mut changed = Vec::new();
        for rel in rels {
            let path = self.repo_root.join(rel);
            let unchanged = if path.exists() {
                match (
                    scan::scan_path(&self.repo_root, &path)?,
                    self.db.get_file_by_path(rel)?,
                ) {
                    (Some(scanned), Some(existing)) => {
                        existing.hash == scanned.hash && existing.deleted_version.is_none()
                    }
                    _ => false,
                }
            } else {
                false
            };
            if !unchanged {
                changed.push(rel.clone());
            }
        }
        Ok(changed)
    }

    /// Refresh the recorded C# `global using`s for `changed` paths and return
    /// the hash-unchanged C# files of every project they affect (see
    /// `cs_globals`). `all_csharp` lists every live C# path.
    fn stale_csharp_files(
        &mut self,
        changed: &[String],
        graph_version: i64,
        all_csharp: impl FnOnce(&Db) -> Result<Vec<String>>,
    ) -> Result<HashSet<String>> {
        self.cs_globals = None;
        self.cs_projects.clear();
        let db = &self.db;
        let mut stale = self.cs_globals_db().prepare(changed, || all_csharp(db))?;
        // ... and C# callers of an extension method a changed file declares.
        stale.extend(
            self.cs_globals_db()
                .stale_extension_callers(changed, graph_version)?,
        );
        Ok(stale)
    }

    /// Python files whose module name under the current package layout
    /// differs from the one `graph_version` stores: a root marker
    /// (`__init__.py`, `pyproject.toml`, `setup.py`, `setup.cfg`) was added,
    /// removed or edited since they were extracted, so a hash skip would keep
    /// a stale qualname. Only files still on disk.
    fn stale_python_files(&mut self, graph_version: i64) -> Result<HashSet<String>> {
        let Some(extractor) = self.extractors.get_mut("python") else {
            return Ok(HashSet::new());
        };
        extractor.begin_run();
        let mut stale = HashSet::new();
        for (path, stored) in self.db.python_module_names(graph_version)? {
            if self.repo_root.join(&path).is_file()
                && extractor.module_name_from_rel_path(&path) != stored
            {
                stale.insert(path);
            }
        }
        Ok(stale)
    }

    /// Rust files whose module name or crate-root signature under the
    /// current Cargo manifests differs from what `graph_version` stores
    /// (a `Cargo.toml` was added, removed or edited since extraction), so a
    /// hash skip would keep a stale qualname. Only files still on disk.
    fn stale_rust_files(&mut self, graph_version: i64) -> Result<HashSet<String>> {
        let Some(extractor) = self.extractors.get_mut("rust") else {
            return Ok(HashSet::new());
        };
        extractor.begin_run();
        let mut stored: std::collections::HashMap<String, Vec<(String, Option<String>)>> =
            std::collections::HashMap::new();
        for (path, qualname, signature) in self.db.rust_module_symbols(graph_version)? {
            stored.entry(path).or_default().push((qualname, signature));
        }
        let mut stale = HashSet::new();
        for (path, modules) in stored {
            if !self.repo_root.join(&path).is_file() {
                continue;
            }
            let module = extractor.module_name_from_rel_path(&path);
            let Some((_, signature)) = modules.iter().find(|(q, _)| *q == module) else {
                stale.insert(path);
                continue;
            };
            if module == "crate" && *signature != extractor.root_module_signature(&path) {
                stale.insert(path);
            }
        }
        Ok(stale)
    }

    fn cs_globals_db(&self) -> cs_globals::CsGlobals<'_> {
        cs_globals::CsGlobals {
            db: &self.db,
            repo_root: &self.repo_root,
        }
    }

    fn js_stale(&self) -> js_stale::JsStale<'_> {
        js_stale::JsStale {
            db: &self.db,
            repo_root: &self.repo_root,
        }
    }

    /// Reset every extractor's per-run cross-file state (see
    /// `LanguageExtractor::begin_run`), then, when `reseed`, re-register the
    /// extension methods `graph_version` stores for the files outside
    /// `changed` (the extractor re-registers `changed` ones itself), so the
    /// registry covers the whole repository whichever files are extracted.
    fn begin_extraction_run(
        &mut self,
        changed: &[String],
        graph_version: i64,
        reseed: bool,
    ) -> Result<()> {
        let methods = if reseed {
            self.cs_globals_db()
                .extension_methods(changed, graph_version)?
        } else {
            Vec::new()
        };
        for extractor in self.extractors.values_mut() {
            extractor.begin_run();
            extractor.seed_extension_methods(&methods);
        }
        Ok(())
    }

    /// Let each file's own extractor register cross-file declarations
    /// before any file is extracted (see `LanguageExtractor::prescan`), and
    /// re-extract the hash-unchanged C# files whose stored unresolved calls
    /// name a declaration found: they could not resolve it when last
    /// indexed. Returns those callers' repo-relative paths.
    fn prescan_files(
        &mut self,
        paths: impl Iterator<Item = PathBuf>,
        graph_version: i64,
    ) -> Result<HashSet<String>> {
        let mut declared: HashSet<String> = HashSet::new();
        for path in paths {
            let Ok(rel) = crate::util::normalize_rel_path(&self.repo_root, &path) else {
                continue;
            };
            let Some(language) = scan::language_for_path(&path) else {
                continue;
            };
            let Ok(source) = crate::util::read_to_string(&path) else {
                continue;
            };
            if let Some(extractor) = self.extractors.get_mut(language) {
                let module_name = extractor.module_name_from_rel_path(&rel);
                declared.extend(extractor.prescan(&source, &module_name));
            }
        }
        self.cs_globals_db()
            .stale_unresolved_extension_callers(&declared, graph_version)
    }

    fn extract_file(&mut self, file: &scan::ScannedFile, source: &str) -> Result<ExtractedFile> {
        let extractor = self
            .extractors
            .get_mut(file.language.as_str())
            .ok_or_else(|| anyhow!("skip {}: unknown language {}", file.rel_path, file.language))?;
        let module_name = extractor.module_name_from_rel_path(&file.rel_path);
        if file.language == "csharp" {
            if self.cs_globals.is_none() {
                self.cs_globals = Some(self.cs_globals_db().by_project()?);
            }
            let project =
                cs_globals::project_dir(&self.repo_root, &file.rel_path, &mut self.cs_projects);
            let globals = self
                .cs_globals
                .as_ref()
                .and_then(|m| m.get(&project))
                .cloned()
                .unwrap_or_default();
            // (re-borrowed: `self.cs_globals` above needed `&mut self`)
            let extractor = self.extractors.get_mut("csharp").unwrap();
            extractor.set_project_globals(&globals);
        }
        let extractor = self
            .extractors
            .get_mut(file.language.as_str())
            .ok_or_else(|| anyhow!("skip {}: unknown language {}", file.rel_path, file.language))?;
        let mut extracted = extractor
            .extract(source, &module_name)
            .map_err(|err| anyhow!("extract error {} ({module_name}): {err}", file.rel_path))?;
        // Re-borrow immutably for resolve_imports (extract's &mut borrow is released)
        let extractor = self.extractors.get(file.language.as_str()).unwrap();
        extractor.resolve_imports(
            &self.repo_root,
            &file.rel_path,
            &module_name,
            &mut extracted.edges,
        );
        if let Some(surface) = extracted.export_surface {
            self.db
                .set_meta_i64(&js_stale::export_surface_key(&file.rel_path), surface)?;
        }
        Ok(extracted)
    }
}

/// A stable-id collision is never resolved by dropping a symbol (issue #212):
/// the differ tells the twins apart by ordinal. Say so, since it means the
/// identity scheme cannot yet distinguish these declarations by themselves.
fn warn_stable_id_collisions(rel_path: &str, diff: &differ::SymbolDiff) {
    for collision in &diff.collisions {
        eprintln!(
            "lidx: warning: {} declarations of {} {} in {rel_path} share one stable id; \
             kept all, disambiguated by declaration order",
            collision.count, collision.kind, collision.qualname
        );
    }
}
