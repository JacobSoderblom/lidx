use crate::indexer::{Indexer, SyncStats, scan};
use anyhow::{Context, Result};
use clap::ValueEnum;
use ignore::{
    Match as IgnoreMatch,
    gitignore::{Gitignore, GitignoreBuilder},
};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_DEBOUNCE_MS: u64 = 300;
const DEFAULT_URGENT_DEBOUNCE_MS: u64 = 50;
const DEFAULT_FALLBACK_SCAN_SECS: u64 = 300;
const DEFAULT_BATCH_REINDEX: usize = 1000;
const DEFAULT_BATCH_THRESHOLD: usize = 10;
const DEFAULT_URGENT_WINDOW_SECS: u64 = 60;

#[derive(ValueEnum, Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchMode {
    Off,
    Auto,
    On,
}

#[derive(Clone, Copy, Debug)]
pub struct WatchConfig {
    pub mode: WatchMode,
    pub debounce: Duration,
    pub urgent_debounce: Duration,
    pub fallback_scan: Duration,
    pub max_batch: usize,
    pub batch_threshold: usize,
    pub urgent_window: Duration,
    pub bootstrap: bool,
    pub scan_options: scan::ScanOptions,
}

impl WatchConfig {
    pub fn new(
        mode: WatchMode,
        debounce_ms: u64,
        fallback_scan_secs: u64,
        max_batch: usize,
        no_ignore: bool,
    ) -> Self {
        let urgent_debounce_ms = std::env::var("LIDX_URGENT_DEBOUNCE_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_URGENT_DEBOUNCE_MS);

        let batch_threshold = std::env::var("LIDX_BATCH_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_BATCH_THRESHOLD);

        let urgent_window_secs = std::env::var("LIDX_URGENT_WINDOW_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_URGENT_WINDOW_SECS);

        Self {
            mode,
            debounce: Duration::from_millis(debounce_ms.max(1)),
            urgent_debounce: Duration::from_millis(urgent_debounce_ms.max(1)),
            fallback_scan: Duration::from_secs(fallback_scan_secs.max(1)),
            max_batch: max_batch.max(1),
            batch_threshold: batch_threshold.max(1),
            urgent_window: Duration::from_secs(urgent_window_secs.max(1)),
            bootstrap: true,
            scan_options: scan::ScanOptions::new(no_ignore),
        }
    }
}

impl Default for WatchConfig {
    fn default() -> Self {
        let urgent_debounce_ms = std::env::var("LIDX_URGENT_DEBOUNCE_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_URGENT_DEBOUNCE_MS);

        let batch_threshold = std::env::var("LIDX_BATCH_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_BATCH_THRESHOLD);

        let urgent_window_secs = std::env::var("LIDX_URGENT_WINDOW_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_URGENT_WINDOW_SECS);

        Self {
            mode: WatchMode::Auto,
            debounce: Duration::from_millis(DEFAULT_DEBOUNCE_MS),
            urgent_debounce: Duration::from_millis(urgent_debounce_ms),
            fallback_scan: Duration::from_secs(DEFAULT_FALLBACK_SCAN_SECS),
            max_batch: DEFAULT_BATCH_REINDEX,
            batch_threshold,
            urgent_window: Duration::from_secs(urgent_window_secs),
            bootstrap: true,
            scan_options: scan::ScanOptions::default(),
        }
    }
}

pub struct WatchHandle {
    stop: Sender<()>,
    thread: Option<thread::JoinHandle<()>>,
}

impl WatchHandle {
    pub fn stop(mut self) {
        let _ = self.stop.send(());
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        let _ = self.stop.send(());
    }
}

/// Priority queue for file changes
///
/// Files are classified as "urgent" if they were recently edited (within urgent_window).
/// This allows for low-latency processing of actively-edited files while batching
/// background changes.
struct PrioritizedFileQueue {
    /// Recently edited files (high priority)
    urgent: VecDeque<PathBuf>,
    /// Other file changes (normal priority)
    normal: VecDeque<PathBuf>,
    /// Track when files were last edited
    recently_edited: HashMap<PathBuf, Instant>,
    /// Time window for considering files "urgent"
    urgent_window: Duration,
}

impl PrioritizedFileQueue {
    fn new(urgent_window: Duration) -> Self {
        Self {
            urgent: VecDeque::new(),
            normal: VecDeque::new(),
            recently_edited: HashMap::new(),
            urgent_window,
        }
    }

    /// Add a file to the appropriate queue based on edit history
    fn enqueue(&mut self, path: PathBuf) {
        // Check if this file was edited recently
        let is_urgent = if let Some(last_edit) = self.recently_edited.get(&path) {
            last_edit.elapsed() < self.urgent_window
        } else {
            false
        };

        // Update edit timestamp
        self.recently_edited.insert(path.clone(), Instant::now());

        // Add to appropriate queue
        if is_urgent {
            self.urgent.push_back(path);
        } else {
            self.normal.push_back(path);
        }
    }

    /// Remove and return the highest priority file
    /// Urgent files are always dequeued before normal files
    fn dequeue(&mut self) -> Option<PathBuf> {
        self.urgent.pop_front().or_else(|| self.normal.pop_front())
    }

    /// Get total number of pending files across both queues
    fn len(&self) -> usize {
        self.urgent.len() + self.normal.len()
    }

    /// Check if both queues are empty
    fn is_empty(&self) -> bool {
        self.urgent.is_empty() && self.normal.is_empty()
    }

    /// Clear all queues
    fn clear(&mut self) {
        self.urgent.clear();
        self.normal.clear();
    }

    /// Get number of urgent files
    fn urgent_count(&self) -> usize {
        self.urgent.len()
    }

    /// Clean up old entries from recently_edited map to prevent unbounded growth
    fn cleanup_old_entries(&mut self, threshold: Duration) {
        self.recently_edited
            .retain(|_, last_edit| last_edit.elapsed() < threshold);
    }
}

/// Compute adaptive debounce duration based on queue state and configuration
///
/// Strategy:
/// - Single urgent file: Use urgent_debounce (50ms) for fast response
/// - Small batch (< threshold): Use normal_debounce (300ms)
/// - Large batch (>= threshold): Use normal_debounce (300ms)
fn compute_debounce(queue: &PrioritizedFileQueue, config: &WatchConfig) -> Duration {
    let total_count = queue.len();
    let urgent_count = queue.urgent_count();

    // Single urgent file → fast response
    if total_count == 1 && urgent_count == 1 {
        config.urgent_debounce
    }
    // Multiple urgent files but still small → fast response
    else if total_count > 0 && total_count < config.batch_threshold && urgent_count > 0 {
        config.urgent_debounce
    }
    // Large batch or all normal files → normal debounce
    else {
        config.debounce
    }
}

pub fn start(
    repo_root: PathBuf,
    db_path: PathBuf,
    config: WatchConfig,
) -> Result<Option<WatchHandle>> {
    if config.mode == WatchMode::Off {
        return Ok(None);
    }
    let (ready_tx, ready_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        if let Err(err) = run_loop(repo_root, db_path, config, stop_rx, ready_tx) {
            eprintln!("watch error: {err}");
        }
    });
    match ready_rx.recv_timeout(Duration::from_secs(2)) {
        Ok(Ok(())) => Ok(Some(WatchHandle {
            stop: stop_tx,
            thread: Some(handle),
        })),
        Ok(Err(err)) => Err(err),
        Err(_) => Ok(Some(WatchHandle {
            stop: stop_tx,
            thread: Some(handle),
        })),
    }
}

fn run_loop(
    repo_root: PathBuf,
    db_path: PathBuf,
    config: WatchConfig,
    stop_rx: Receiver<()>,
    ready: Sender<Result<()>>,
) -> Result<()> {
    let repo_root = std::fs::canonicalize(&repo_root).unwrap_or(repo_root);
    let mut indexer = Indexer::new_with_options(repo_root.clone(), db_path, config.scan_options)
        .with_context(|| format!("watch open indexer {}", repo_root.display()))?;
    if config.bootstrap {
        if indexer.db().get_meta_i64("last_indexed")?.is_none() {
            if let Err(err) = indexer.reindex() {
                eprintln!("watch bootstrap reindex failed: {err}");
            }
        }
    }

    let mut filter = PathFilter::new(&repo_root, config.scan_options.no_ignore);
    let (mut watcher, mut event_rx) = match try_start_watcher(&repo_root) {
        Ok((watcher, rx)) => {
            let _ = ready.send(Ok(()));
            (Some(watcher), Some(rx))
        }
        Err(err) => {
            if config.mode == WatchMode::On {
                let _ = ready.send(Err(err));
                return Ok(());
            }
            eprintln!("watch disabled, falling back to scan: {err}");
            let _ = ready.send(Ok(()));
            (None, None)
        }
    };

    let mut pending = PrioritizedFileQueue::new(config.urgent_window);
    let mut last_event = Instant::now();
    let mut last_fallback = Instant::now();
    let mut last_cleanup = Instant::now();
    let mut force_reindex = false;

    loop {
        if stop_requested(&stop_rx) {
            return Ok(());
        }

        // Periodic cleanup of old entries (every 5 minutes)
        if last_cleanup.elapsed() >= Duration::from_secs(300) {
            pending.cleanup_old_entries(config.urgent_window * 2);
            last_cleanup = Instant::now();
        }

        if watcher.is_some() {
            let Some(rx) = event_rx.as_ref() else {
                watcher = None;
                event_rx = None;
                continue;
            };

            // Compute adaptive debounce based on queue state
            let debounce = if pending.is_empty() {
                Duration::from_millis(200)
            } else {
                compute_debounce(&pending, &config)
            };

            match rx.recv_timeout(debounce) {
                Ok(Ok(event)) => {
                    if event.need_rescan() {
                        force_reindex = true;
                        pending.clear();
                        last_event = Instant::now();
                        continue;
                    }
                    if is_noise_event(&event) {
                        continue;
                    }
                    for path in &event.paths {
                        if filter.is_ignored(path) {
                            continue;
                        }
                        pending.enqueue(path.to_path_buf());
                    }
                    if pending.len() >= config.max_batch {
                        force_reindex = true;
                        pending.clear();
                    }
                    last_event = Instant::now();
                }
                Ok(Err(err)) => {
                    if should_fallback(&err, config.mode) {
                        eprintln!("watch fallback to scan: {err}");
                        watcher = None;
                        event_rx = None;
                        if let Err(err) = fallback_scan(&mut indexer, config.max_batch) {
                            eprintln!("watch fallback scan failed: {err}");
                        }
                        last_fallback = Instant::now();
                    } else {
                        eprintln!("watch error: {err}");
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if config.mode == WatchMode::Auto {
                        watcher = None;
                        event_rx = None;
                        if let Err(err) = fallback_scan(&mut indexer, config.max_batch) {
                            eprintln!("watch fallback scan failed: {err}");
                        }
                        last_fallback = Instant::now();
                    }
                }
            }

            // Compute adaptive debounce and check if ready to flush
            let current_debounce = compute_debounce(&pending, &config);
            let should_flush = !pending.is_empty() && last_event.elapsed() >= current_debounce;

            if force_reindex || should_flush {
                if force_reindex {
                    if let Err(err) = indexer.reindex() {
                        eprintln!("watch reindex failed: {err}");
                    }
                } else {
                    // Drain queue in priority order
                    let mut paths = Vec::new();
                    while let Some(path) = pending.dequeue() {
                        paths.push(path);
                    }
                    if let Err(err) = apply_paths(&mut indexer, paths) {
                        eprintln!("watch apply failed: {err}");
                    }
                }
                force_reindex = false;
                pending.clear();
            }
        } else if last_fallback.elapsed() >= config.fallback_scan {
            if let Err(err) = fallback_scan(&mut indexer, config.max_batch) {
                eprintln!("watch fallback scan failed: {err}");
            }
            last_fallback = Instant::now();
        } else {
            thread::sleep(Duration::from_millis(200));
        }
    }
}

fn stop_requested(stop_rx: &Receiver<()>) -> bool {
    match stop_rx.try_recv() {
        Ok(()) => true,
        Err(TryRecvError::Disconnected) => true,
        Err(TryRecvError::Empty) => false,
    }
}

fn try_start_watcher(
    repo_root: &Path,
) -> Result<(RecommendedWatcher, Receiver<notify::Result<Event>>)> {
    let (event_tx, event_rx) = mpsc::channel();
    let handler = move |res| {
        let _ = event_tx.send(res);
    };
    let mut watcher = notify::recommended_watcher(handler)?;
    watcher.watch(repo_root, RecursiveMode::Recursive)?;
    Ok((watcher, event_rx))
}

fn fallback_scan(indexer: &mut Indexer, max_batch: usize) -> Result<()> {
    let changed = indexer.changed_files(None)?;
    let mut rel_paths = Vec::new();
    rel_paths.extend(changed.added);
    rel_paths.extend(changed.modified);
    rel_paths.extend(changed.deleted);
    if rel_paths.is_empty() {
        return Ok(());
    }
    if rel_paths.len() >= max_batch {
        indexer.reindex()?;
        return Ok(());
    }
    let stats = indexer.sync_rel_paths(&rel_paths)?;
    report_errors(&stats, "watch fallback");
    Ok(())
}

fn apply_paths(indexer: &mut Indexer, paths: Vec<PathBuf>) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let stats = indexer.sync_abs_paths(&paths)?;
    report_errors(&stats, "watch apply");
    Ok(())
}

fn report_errors(stats: &SyncStats, context: &str) {
    if stats.errors > 0 {
        eprintln!("{context}: {} errors", stats.errors);
    }
}

fn is_noise_event(event: &Event) -> bool {
    matches!(event.kind, EventKind::Access(_))
}

fn should_fallback(err: &notify::Error, mode: WatchMode) -> bool {
    if mode != WatchMode::Auto {
        return false;
    }
    matches!(
        &err.kind,
        notify::ErrorKind::MaxFilesWatch
            | notify::ErrorKind::WatchNotFound
            | notify::ErrorKind::PathNotFound
    )
}

struct PathFilter {
    repo_root: PathBuf,
    no_ignore: bool,
    gitignores: HashMap<PathBuf, Gitignore>,
    ignores: HashMap<PathBuf, Gitignore>,
    git_exclude: Gitignore,
    global_ignore: Gitignore,
}

impl PathFilter {
    fn new(repo_root: &Path, no_ignore: bool) -> Self {
        let git_exclude = if no_ignore {
            Gitignore::empty()
        } else {
            build_gitignore(repo_root, repo_root.join(".git/info/exclude"))
        };
        let (global_ignore, err) = if no_ignore {
            (Gitignore::empty(), None)
        } else {
            GitignoreBuilder::new(repo_root).build_global()
        };
        if let Some(err) = err {
            eprintln!("watch: global ignore error: {err}");
        }
        Self {
            repo_root: repo_root.to_path_buf(),
            no_ignore,
            gitignores: HashMap::new(),
            ignores: HashMap::new(),
            git_exclude,
            global_ignore,
        }
    }

    fn is_ignored(&mut self, path: &Path) -> bool {
        let rel = match path.strip_prefix(&self.repo_root) {
            Ok(value) => value,
            Err(_) => return true,
        };
        let mut components = rel.components();
        let Some(first) = components.next() else {
            return false;
        };
        if first.as_os_str() == ".git" || first.as_os_str() == ".lidx" {
            return true;
        }
        if self.no_ignore {
            return false;
        }
        let is_dir = path.is_dir();
        let mut decision = None;
        apply_match(
            &mut decision,
            self.global_ignore.matched_path_or_any_parents(path, is_dir),
        );
        apply_match(
            &mut decision,
            self.git_exclude.matched_path_or_any_parents(path, is_dir),
        );
        let ancestors = self.ancestor_dirs(path);
        for dir in &ancestors {
            let matcher = self.gitignore_for(dir);
            apply_match(
                &mut decision,
                matcher.matched_path_or_any_parents(path, is_dir),
            );
        }
        for dir in &ancestors {
            let matcher = self.ignore_for(dir);
            apply_match(
                &mut decision,
                matcher.matched_path_or_any_parents(path, is_dir),
            );
        }
        decision.unwrap_or(false)
    }

    fn ancestor_dirs(&self, path: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Some(mut current) = path.parent() else {
            return out;
        };
        loop {
            if !current.starts_with(&self.repo_root) {
                break;
            }
            out.push(current.to_path_buf());
            if current == self.repo_root {
                break;
            }
            let Some(parent) = current.parent() else {
                break;
            };
            current = parent;
        }
        out.reverse();
        out
    }

    fn gitignore_for(&mut self, dir: &Path) -> &Gitignore {
        load_ignore_file(&mut self.gitignores, dir, ".gitignore")
    }

    fn ignore_for(&mut self, dir: &Path) -> &Gitignore {
        load_ignore_file(&mut self.ignores, dir, ".ignore")
    }
}

fn apply_match<T>(decision: &mut Option<bool>, matched: IgnoreMatch<T>) {
    match matched {
        IgnoreMatch::Ignore(_) => *decision = Some(true),
        IgnoreMatch::Whitelist(_) => *decision = Some(false),
        IgnoreMatch::None => {}
    }
}

fn load_ignore_file<'a>(
    cache: &'a mut HashMap<PathBuf, Gitignore>,
    dir: &Path,
    filename: &str,
) -> &'a Gitignore {
    use std::collections::hash_map::Entry;
    match cache.entry(dir.to_path_buf()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let path = dir.join(filename);
            let matcher = if path.is_file() {
                let (ignore, err) = Gitignore::new(&path);
                if let Some(err) = err {
                    eprintln!("watch: ignore parse error: {err}");
                }
                ignore
            } else {
                Gitignore::empty()
            };
            entry.insert(matcher)
        }
    }
}

fn build_gitignore(root: &Path, path: PathBuf) -> Gitignore {
    if !path.is_file() {
        return Gitignore::empty();
    }
    let mut builder = GitignoreBuilder::new(root);
    if let Some(err) = builder.add(path) {
        eprintln!("watch: ignore parse error: {err}");
    }
    builder.build().unwrap_or_else(|err| {
        eprintln!("watch: ignore build error: {err}");
        Gitignore::empty()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_queue_new_file_goes_to_normal() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let path = PathBuf::from("/test/file.rs");

        queue.enqueue(path.clone());

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.urgent_count(), 0);
        assert_eq!(queue.dequeue(), Some(path));
    }

    #[test]
    fn test_priority_queue_recently_edited_goes_to_urgent() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let path = PathBuf::from("/test/file.rs");

        // First edit → normal queue
        queue.enqueue(path.clone());
        assert_eq!(queue.urgent_count(), 0);
        queue.dequeue(); // Remove it

        // Second edit within window → urgent queue
        queue.enqueue(path.clone());
        assert_eq!(queue.urgent_count(), 1);
        assert_eq!(queue.dequeue(), Some(path));
    }

    #[test]
    fn test_priority_queue_urgent_dequeued_first() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let urgent_file = PathBuf::from("/test/urgent.rs");
        let normal_file = PathBuf::from("/test/normal.rs");

        // Create urgent file by editing twice
        queue.enqueue(urgent_file.clone());
        queue.dequeue();
        queue.enqueue(urgent_file.clone());

        // Add normal file
        queue.enqueue(normal_file.clone());

        assert_eq!(queue.len(), 2);
        assert_eq!(queue.urgent_count(), 1);

        // Urgent file should come first
        assert_eq!(queue.dequeue(), Some(urgent_file));
        assert_eq!(queue.dequeue(), Some(normal_file));
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn test_priority_queue_clear() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        queue.enqueue(PathBuf::from("/test/file1.rs"));
        queue.enqueue(PathBuf::from("/test/file2.rs"));

        assert_eq!(queue.len(), 2);
        queue.clear();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_cleanup_old_entries() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_millis(100));
        let path = PathBuf::from("/test/file.rs");

        queue.enqueue(path.clone());
        assert_eq!(queue.recently_edited.len(), 1);

        // Wait for entry to become old
        std::thread::sleep(Duration::from_millis(150));

        // Cleanup with threshold smaller than elapsed time
        queue.cleanup_old_entries(Duration::from_millis(100));
        assert_eq!(queue.recently_edited.len(), 0);
    }

    #[test]
    fn test_compute_debounce_single_urgent_file() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let config = WatchConfig::default();
        let path = PathBuf::from("/test/file.rs");

        // Create urgent file
        queue.enqueue(path.clone());
        queue.dequeue();
        queue.enqueue(path);

        // Single urgent file → urgent debounce
        let debounce = compute_debounce(&queue, &config);
        assert_eq!(debounce, config.urgent_debounce);
    }

    #[test]
    fn test_compute_debounce_small_batch_with_urgent() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let mut config = WatchConfig::default();
        config.batch_threshold = 10;

        // Create 3 urgent files
        for i in 0..3 {
            let path = PathBuf::from(format!("/test/file{}.rs", i));
            queue.enqueue(path.clone());
            queue.dequeue();
            queue.enqueue(path);
        }

        // Small batch with urgent files → urgent debounce
        let debounce = compute_debounce(&queue, &config);
        assert_eq!(debounce, config.urgent_debounce);
    }

    #[test]
    fn test_compute_debounce_large_batch() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let mut config = WatchConfig::default();
        config.batch_threshold = 10;

        // Add 15 files (exceeds threshold)
        for i in 0..15 {
            queue.enqueue(PathBuf::from(format!("/test/file{}.rs", i)));
        }

        // Large batch → normal debounce
        let debounce = compute_debounce(&queue, &config);
        assert_eq!(debounce, config.debounce);
    }

    #[test]
    fn test_compute_debounce_normal_files() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let config = WatchConfig::default();

        // Add 3 normal files (first edit each)
        for i in 0..3 {
            queue.enqueue(PathBuf::from(format!("/test/file{}.rs", i)));
        }

        // All normal files → normal debounce
        let debounce = compute_debounce(&queue, &config);
        assert_eq!(debounce, config.debounce);
    }

    #[test]
    fn test_watch_config_defaults() {
        let config = WatchConfig::default();
        assert_eq!(config.mode, WatchMode::Auto);
        assert_eq!(config.debounce, Duration::from_millis(DEFAULT_DEBOUNCE_MS));
        assert_eq!(
            config.urgent_debounce,
            Duration::from_millis(DEFAULT_URGENT_DEBOUNCE_MS)
        );
        assert!(config.batch_threshold >= 1);
        assert!(config.urgent_window >= Duration::from_secs(1));
    }

    #[test]
    fn test_priority_queue_multiple_edits_same_file() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_secs(60));
        let path = PathBuf::from("/test/file.rs");

        // Edit 1 → normal
        queue.enqueue(path.clone());
        assert_eq!(queue.urgent_count(), 0);
        queue.dequeue();

        // Edit 2 → urgent
        queue.enqueue(path.clone());
        assert_eq!(queue.urgent_count(), 1);
        queue.dequeue();

        // Edit 3 → still urgent
        queue.enqueue(path.clone());
        assert_eq!(queue.urgent_count(), 1);
    }

    #[test]
    fn test_priority_queue_urgent_window_expiry() {
        let mut queue = PrioritizedFileQueue::new(Duration::from_millis(50));
        let path = PathBuf::from("/test/file.rs");

        // First edit
        queue.enqueue(path.clone());
        queue.dequeue();

        // Wait for window to expire
        std::thread::sleep(Duration::from_millis(100));

        // Second edit after expiry → normal queue (not urgent)
        queue.enqueue(path.clone());
        assert_eq!(queue.urgent_count(), 0);
    }
}
