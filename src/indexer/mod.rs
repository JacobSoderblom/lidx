use crate::db::Db;
use crate::indexer::extract::ExtractedFile;
use crate::metrics;
use crate::model::{ChangedFilesResult, IndexStats};
use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

pub mod batch;
pub mod channel;
pub mod csharp;
pub mod differ;
pub mod extract;
pub mod go;
pub mod http;
pub mod javascript;
pub mod lua;
pub mod markdown;
pub mod postgres;
pub mod proto;
pub mod python;
pub mod rust;
pub mod scan;
pub mod sql;
pub mod stable_id;
pub mod test_detection;
pub mod xref;

#[derive(Debug, Default)]
pub(crate) struct SyncStats {
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
        let repo_root = std::fs::canonicalize(&repo_root).unwrap_or(repo_root);
        let db = Db::new(&db_path)?;
        let graph_version = db.current_graph_version()?;
        let commit_sha = db.graph_version_commit(graph_version)?;

        let mut extractors: HashMap<String, Box<dyn extract::LanguageExtractor>> = HashMap::new();
        extractors.insert("python".into(), Box::new(python::PythonExtractor::new()?));
        extractors.insert("rust".into(), Box::new(rust::RustExtractor::new()?));
        extractors.insert("javascript".into(), Box::new(javascript::JavascriptExtractor::new()?));
        extractors.insert("typescript".into(), Box::new(javascript::TypescriptExtractor::new()?));
        extractors.insert("tsx".into(), Box::new(javascript::TsxExtractor::new()?));
        extractors.insert("csharp".into(), Box::new(csharp::CSharpExtractor::new()?));
        extractors.insert("go".into(), Box::new(go::GoExtractor::new()?));
        extractors.insert("lua".into(), Box::new(lua::LuaExtractor::new()?));
        extractors.insert("sql".into(), Box::new(sql::SqlExtractor::new()?));
        extractors.insert("postgres".into(), Box::new(postgres::PostgresExtractor::new()?));
        extractors.insert("tsql".into(), Box::new(sql::SqlExtractor::new()?));
        extractors.insert("markdown".into(), Box::new(markdown::MarkdownExtractor::new()?));
        extractors.insert("proto".into(), Box::new(proto::ProtoExtractor::new()?));

        Ok(Self {
            repo_root,
            db,
            scan_options,
            graph_version,
            commit_sha,
            extractors,
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
            if let Some(languages) = languages {
                if !languages.contains(&record.language) {
                    continue;
                }
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

    pub(crate) fn sync_rel_paths(&mut self, rel_paths: &[String]) -> Result<SyncStats> {
        let abs_paths: Vec<PathBuf> = rel_paths
            .iter()
            .map(|rel| self.repo_root.join(rel))
            .collect();
        self.sync_abs_paths(&abs_paths)
    }

    pub(crate) fn sync_abs_paths(&mut self, paths: &[PathBuf]) -> Result<SyncStats> {
        let mut stats = SyncStats::default();
        let mut touched = false;
        let mut indexed_files = Vec::new();
        for path in paths {
            let rel_path = match crate::util::normalize_rel_path(&self.repo_root, path) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if !path.exists() {
                if let Some(existing) = self.db.get_file_by_path(&rel_path)? {
                    self.db
                        .delete_symbols_edges_for_file(existing.id, self.graph_version)?;
                    self.db.mark_file_deleted(&rel_path, self.graph_version)?;
                }
                stats.deleted += 1;
                touched = true;
                continue;
            }
            let Some(scanned) = scan::scan_path(&self.repo_root, path)? else {
                if !path.exists() {
                    if let Some(existing) = self.db.get_file_by_path(&rel_path)? {
                        self.db
                            .delete_symbols_edges_for_file(existing.id, self.graph_version)?;
                        self.db.mark_file_deleted(&rel_path, self.graph_version)?;
                    }
                    stats.deleted += 1;
                    touched = true;
                }
                continue;
            };
            if let Some(existing) = self.db.get_file_by_path(&scanned.rel_path)? {
                if existing.hash == scanned.hash {
                    stats.skipped += 1;
                    continue;
                }
            }
            match self.index_scanned_file(&scanned) {
                Ok((symbols, edges)) => {
                    stats.indexed += 1;
                    stats.symbols += symbols;
                    stats.edges += edges;
                    touched = true;
                    indexed_files.push(scanned.clone());
                }
                Err(err) => {
                    eprintln!("index error {}: {err}", scanned.rel_path);
                    stats.errors += 1;
                }
            }
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
        if touched {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            self.db.set_meta_i64("last_indexed", now)?;
        }
        Ok(stats)
    }

    pub fn reindex(&mut self) -> Result<IndexStats> {
        let started = Instant::now();
        let previous_graph_version = self.graph_version;
        let commit_sha = crate::util::git_head_sha(&self.repo_root);
        self.graph_version = self.db.create_graph_version(commit_sha.as_deref())?;
        self.commit_sha = commit_sha;
        let scanned = scan::scan_repo_with_options(&self.repo_root, self.scan_options)?;
        let existing = self.db.list_files(previous_graph_version)?;
        let mut existing_map: HashMap<String, String> = HashMap::new();
        for record in existing {
            existing_map.insert(record.path, record.hash);
        }

        let mut seen = HashSet::new();
        let mut stats = IndexStats {
            scanned: scanned.len(),
            indexed: 0,
            skipped: 0,
            deleted: 0,
            symbols: 0,
            edges: 0,
            duration_ms: 0,
        };

        // Phase 4: Use batch writing for reindex
        // Collect file diffs and upsert files first
        let mut batch_writer = batch::BatchWriter::with_defaults();
        let mut file_data: Vec<(scan::ScannedFile, ExtractedFile, differ::SymbolDiff, i64)> =
            Vec::new();

        for file in &scanned {
            seen.insert(file.rel_path.clone());

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

        // Now process edges for all files
        for (file, extracted, diff, file_id) in file_data {
            // Delete existing edges
            self.db.delete_edges_for_file(file_id, self.graph_version)?;

            // Get symbols for edge resolution
            let symbols = self
                .db
                .get_symbols_for_file(&file.rel_path, self.graph_version)?;
            let mut symbol_map = HashMap::new();
            for symbol in &symbols {
                symbol_map.insert(symbol.qualname.clone(), symbol.id);
            }

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
                .insert_symbol_metrics(file_id, &extracted.symbol_metrics, &symbol_map)?;

            stats.indexed += 1;
            stats.symbols += diff.added.len() + diff.modified.len() + diff.unchanged.len();
            stats.edges += edges_count;
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

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.db.set_meta_i64("last_indexed", now)?;
        stats.duration_ms = started.elapsed().as_millis() as u64;
        Ok(stats)
    }




    fn index_scanned_file(&mut self, file: &scan::ScannedFile) -> Result<(usize, usize)> {
        // Phase 6: Check file size before reading (skip very large files)
        const MAX_FILE_SIZE_MB: u64 = 10;
        let metadata = std::fs::metadata(&file.abs_path)?;
        if metadata.len() > MAX_FILE_SIZE_MB * 1024 * 1024 {
            eprintln!(
                "lidx: Skipping large file ({}MB): {}",
                metadata.len() / (1024 * 1024),
                file.rel_path
            );
            return Ok((0, 0));
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

        // Phase 3: Use incremental updates for symbols
        let (symbol_count, edge_count) = self.index_file(file, extracted, diff)?;

        Ok((symbol_count, edge_count))
    }

    fn extract_file(&mut self, file: &scan::ScannedFile, source: &str) -> Result<ExtractedFile> {
        let extractor = self.extractors.get_mut(file.language.as_str())
            .ok_or_else(|| anyhow!("skip {}: unknown language {}", file.rel_path, file.language))?;
        let module_name = extractor.module_name_from_rel_path(&file.rel_path);
        let mut extracted = extractor.extract(source, &module_name)
            .map_err(|err| anyhow!("extract error {} ({module_name}): {err}", file.rel_path))?;
        // Re-borrow immutably for resolve_imports (extract's &mut borrow is released)
        let extractor = self.extractors.get(file.language.as_str()).unwrap();
        extractor.resolve_imports(&self.repo_root, &file.rel_path, &module_name, &mut extracted.edges);
        Ok(extracted)
    }

    fn index_file(
        &mut self,
        file: &scan::ScannedFile,
        extracted: ExtractedFile,
        diff: differ::SymbolDiff,
    ) -> Result<(usize, usize)> {
        let file_id = self.db.upsert_file(
            &file.rel_path,
            &file.hash,
            &file.language,
            file.size,
            file.modified,
        )?;

        // Phase 3: Use incremental symbol updates instead of delete-all-insert
        let symbols = self.db.update_file_symbols(
            file_id,
            &file.rel_path,
            diff,
            self.graph_version,
            self.commit_sha.as_deref(),
        )?;

        // For edges, still use delete-all-insert for now (can optimize in future)
        // Delete existing edges for this file
        self.db.delete_edges_for_file(file_id, self.graph_version)?;
        let mut symbol_map = HashMap::new();
        for symbol in &symbols {
            symbol_map.insert(symbol.qualname.clone(), symbol.id);
        }
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
            .insert_symbol_metrics(file_id, &extracted.symbol_metrics, &symbol_map)?;

        Ok((symbols.len(), edges_count))
    }

}
