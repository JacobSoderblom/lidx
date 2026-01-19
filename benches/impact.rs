use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lidx::db::Db;
use lidx::impact::{analyze_impact, TraversalDirection};
use lidx::indexer::Indexer;
use std::collections::HashSet;
use std::path::PathBuf;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn setup_test_repo() -> (PathBuf, PathBuf) {
    let src = fixture_path("py_mvp");
    let repo_root = std::env::temp_dir().join(format!(
        "lidx-bench-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // Copy fixture to temp location
    std::fs::create_dir_all(&repo_root).unwrap();
    copy_dir(&src, &repo_root);

    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");

    // Index the repo
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    let result = indexer.reindex().unwrap();
    eprintln!("Indexed {} files with {} symbols", result.indexed, result.symbols);

    (repo_root, db_path)
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

fn cleanup_repo(repo_root: &PathBuf) {
    let _ = std::fs::remove_dir_all(repo_root);
}

fn find_greeter_symbol(db: &Db) -> lidx::model::Symbol {
    // Get the actual graph_version from the database
    let graph_versions = db.list_graph_versions(1, 0).unwrap();
    let graph_version = graph_versions.first().map(|v| v.id).unwrap_or(1);
    eprintln!("Latest graph version: {}", graph_version);

    db.get_symbol_by_qualname("pkg.core.Greeter", graph_version)
        .ok()
        .flatten()
        .or_else(|| {
            // Try finding with find_symbols which is more flexible
            let symbols = db.find_symbols("Greeter", 10, None, graph_version).ok()?;
            eprintln!("Found {} symbols matching 'Greeter' at version {}", symbols.len(), graph_version);
            symbols.into_iter().next()
        })
        .or_else(|| {
            // Try getting any symbol as a fallback
            let symbols = db.find_symbols("", 10, None, graph_version).ok()?;
            eprintln!("Found {} total symbols at version {}", symbols.len(), graph_version);
            if !symbols.is_empty() {
                eprintln!("Using fallback symbol: {} ({})", symbols[0].name, symbols[0].qualname);
            }
            symbols.into_iter().next()
        })
        .expect("Could not find any suitable symbol to benchmark")
}

/// Benchmark current analyze_impact baseline performance
fn bench_analyze_impact_baseline(c: &mut Criterion) {
    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    c.bench_function("impact_baseline_depth3", |b| {
        b.iter(|| {
            let result = analyze_impact(
                black_box(&db),
                black_box(&[seed_id]),
                black_box(3),
                black_box(TraversalDirection::Both),
                black_box(&HashSet::new()),
                black_box(true),
                black_box(false),
                black_box(10000),
                black_box(None),
                black_box(-1),
            );
            black_box(result)
        })
    });

    cleanup_repo(&repo_root);
}

/// Benchmark direct impact layer (BFS traversal only)
fn bench_direct_layer(c: &mut Criterion) {
    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    c.bench_function("direct_layer_depth3", |b| {
        b.iter(|| {
            // This is the same as baseline for now, but will be separated in Phase 1
            let result = analyze_impact(
                black_box(&db),
                black_box(&[seed_id]),
                black_box(3),
                black_box(TraversalDirection::Both),
                black_box(&HashSet::new()),
                black_box(true),
                black_box(false),
                black_box(10000),
                black_box(None),
                black_box(-1),
            );
            black_box(result)
        })
    });

    cleanup_repo(&repo_root);
}

/// Benchmark with different max depths
fn bench_varying_depth(c: &mut Criterion) {
    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    let mut group = c.benchmark_group("impact_varying_depth");

    for depth in [1, 2, 3, 5].iter() {
        group.bench_with_input(format!("depth_{}", depth), depth, |b, &depth| {
            b.iter(|| {
                let result = analyze_impact(
                    black_box(&db),
                    black_box(&[seed_id]),
                    black_box(depth),
                    black_box(TraversalDirection::Both),
                    black_box(&HashSet::new()),
                    black_box(true),
                    black_box(false),
                    black_box(10000),
                    black_box(None),
                    black_box(-1),
                );
                black_box(result)
            })
        });
    }

    group.finish();
    cleanup_repo(&repo_root);
}

/// Benchmark upstream vs downstream vs both directions
fn bench_directions(c: &mut Criterion) {
    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    let mut group = c.benchmark_group("impact_directions");

    group.bench_function("upstream", |b| {
        b.iter(|| {
            let result = analyze_impact(
                black_box(&db),
                black_box(&[seed_id]),
                black_box(3),
                black_box(TraversalDirection::Upstream),
                black_box(&HashSet::new()),
                black_box(true),
                black_box(false),
                black_box(10000),
                black_box(None),
                black_box(-1),
            );
            black_box(result)
        })
    });

    group.bench_function("downstream", |b| {
        b.iter(|| {
            let result = analyze_impact(
                black_box(&db),
                black_box(&[seed_id]),
                black_box(3),
                black_box(TraversalDirection::Downstream),
                black_box(&HashSet::new()),
                black_box(true),
                black_box(false),
                black_box(10000),
                black_box(None),
                black_box(-1),
            );
            black_box(result)
        })
    });

    group.bench_function("both", |b| {
        b.iter(|| {
            let result = analyze_impact(
                black_box(&db),
                black_box(&[seed_id]),
                black_box(3),
                black_box(TraversalDirection::Both),
                black_box(&HashSet::new()),
                black_box(true),
                black_box(false),
                black_box(10000),
                black_box(None),
                black_box(-1),
            );
            black_box(result)
        })
    });

    group.finish();
    cleanup_repo(&repo_root);
}

/// Benchmark with path inclusion (more expensive)
fn bench_with_paths(c: &mut Criterion) {
    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    let mut group = c.benchmark_group("impact_with_paths");

    group.bench_function("without_paths", |b| {
        b.iter(|| {
            let result = analyze_impact(
                black_box(&db),
                black_box(&[seed_id]),
                black_box(3),
                black_box(TraversalDirection::Both),
                black_box(&HashSet::new()),
                black_box(true),
                black_box(false), // include_paths = false
                black_box(10000),
                black_box(None),
                black_box(-1),
            );
            black_box(result)
        })
    });

    group.bench_function("with_paths", |b| {
        b.iter(|| {
            let result = analyze_impact(
                black_box(&db),
                black_box(&[seed_id]),
                black_box(3),
                black_box(TraversalDirection::Both),
                black_box(&HashSet::new()),
                black_box(true),
                black_box(true), // include_paths = true
                black_box(10000),
                black_box(None),
                black_box(-1),
            );
            black_box(result)
        })
    });

    group.finish();
    cleanup_repo(&repo_root);
}

/// Benchmark multi-layer analysis with all layers enabled (sequential)
fn bench_multi_layer_sequential(c: &mut Criterion) {
    use lidx::impact::config::MultiLayerConfig;
    use lidx::impact::orchestrator::MultiLayerOrchestrator;

    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    // Config with all layers enabled
    let config = MultiLayerConfig::all_layers();

    c.bench_function("multi_layer_all_sequential", |b| {
        b.iter(|| {
            let orchestrator = MultiLayerOrchestrator::new(black_box(&db), black_box(config.clone()));
            let result = orchestrator.analyze(black_box(&[seed_id]), black_box(-1));
            black_box(result)
        })
    });

    cleanup_repo(&repo_root);
}

/// Benchmark multi-layer analysis with all layers enabled (parallel)
fn bench_multi_layer_parallel(c: &mut Criterion) {
    use lidx::impact::config::MultiLayerConfig;
    use lidx::impact::orchestrator::MultiLayerOrchestrator;

    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    // Config with all layers enabled
    let config = MultiLayerConfig::all_layers();

    c.bench_function("multi_layer_all_parallel", |b| {
        b.iter(|| {
            let orchestrator = MultiLayerOrchestrator::new(black_box(&db), black_box(config.clone()));
            let result = orchestrator.analyze_parallel(black_box(&[seed_id]), black_box(-1));
            black_box(result)
        })
    });

    cleanup_repo(&repo_root);
}

/// Benchmark multi-layer analysis with only direct layer (for comparison)
fn bench_multi_layer_direct_only(c: &mut Criterion) {
    use lidx::impact::config::MultiLayerConfig;
    use lidx::impact::orchestrator::MultiLayerOrchestrator;

    let (repo_root, db_path) = setup_test_repo();
    let db = Db::new(&db_path).unwrap();

    let symbol = find_greeter_symbol(&db);
    let seed_id = symbol.id;

    // Config with only direct layer
    let config = MultiLayerConfig::direct_only();

    c.bench_function("multi_layer_direct_only", |b| {
        b.iter(|| {
            let orchestrator = MultiLayerOrchestrator::new(black_box(&db), black_box(config.clone()));
            let result = orchestrator.analyze(black_box(&[seed_id]), black_box(-1));
            black_box(result)
        })
    });

    cleanup_repo(&repo_root);
}

/// Placeholder: Test impact layer benchmark
fn bench_test_layer(_c: &mut Criterion) {
    // Note: Test layer requires TESTS edges in the database
    // For now, this is covered by multi-layer benchmarks above
}

/// Placeholder: Historical impact layer benchmark
fn bench_historical_layer(_c: &mut Criterion) {
    // Note: Historical layer requires CO_CHANGES edges in the database
    // For now, this is covered by multi-layer benchmarks above
}

/// Placeholder: Semantic impact layer benchmark
fn bench_semantic_layer(_c: &mut Criterion) {
    // Note: Semantic layer requires embeddings in the database
    // For now, this is covered by multi-layer benchmarks above
}

criterion_group!(
    benches,
    bench_analyze_impact_baseline,
    bench_direct_layer,
    bench_varying_depth,
    bench_directions,
    bench_with_paths,
    // Multi-layer benchmarks (Phase 5)
    bench_multi_layer_sequential,
    bench_multi_layer_parallel,
    bench_multi_layer_direct_only,
    // Placeholder benchmarks (no-op for now)
    bench_test_layer,
    bench_historical_layer,
    bench_semantic_layer,
);

criterion_main!(benches);
