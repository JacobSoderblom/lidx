//! Issue #185: timing of the dispatch query on a large synthetic C# index.
//! Slow; run with `cargo test --release --test dispatch_perf -- --ignored --nocapture`.
mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use std::time::Instant;

const CLASSES: usize = 20_000;
const INTERFACES: usize = 5_000;
const FILES: usize = 200;

fn generate(root: &std::path::Path) {
    let mut ifaces = String::new();
    for i in 0..INTERFACES {
        ifaces.push_str(&format!(
            "namespace Ns{}\n{{\n    public interface IThing{i}<T>\n    {{\n        void Run();\n        void Stop();\n        int P {{ get; }}\n    }}\n}}\n",
            i % 50
        ));
    }
    std::fs::write(root.join("Interfaces.cs"), ifaces).unwrap();
    let per_file = CLASSES / FILES;
    for f in 0..FILES {
        let mut src = String::from("using System;\n");
        for n in f * per_file..(f + 1) * per_file {
            let i = n % INTERFACES;
            let ns = i % 50;
            src.push_str(&format!(
                "namespace App{f}\n{{\n    public class Impl{n} : Ns{ns}.IThing{i}<int>\n    {{\n        void Ns{ns}.IThing{i}<int>.Run() {{ }}\n        public void Stop() {{ }}\n        public int P => 1;\n        public void Extra() {{ }}\n    }}\n    public class Caller{n}\n    {{\n        private readonly Ns{ns}.IThing{i}<int> _t;\n        public void Go() {{ _t.Run(); _t.Stop(); }}\n    }}\n}}\n"
            ));
        }
        std::fs::write(root.join(format!("Impls{f}.cs")), src).unwrap();
    }
}

#[test]
#[ignore = "slow: indexes ~20k classes"]
fn dispatch_queries_scale() {
    let (_tmp, repo, db) = common::setup_repo("cs_dispatch_twin");
    generate(&repo);
    let t = Instant::now();
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    eprintln!("reindex: {:?}", t.elapsed());
    let gv = indexer.db().current_graph_version().unwrap();

    let ids: Vec<i64> = (1..=50).collect();
    let t = Instant::now();
    let pairs = indexer.db().dispatch_pairs(&ids, gv).unwrap();
    eprintln!(
        "dispatch_pairs(50 ids): {:?} ({} pairs)",
        t.elapsed(),
        pairs.len()
    );
    let t = Instant::now();
    let _ = indexer.db().dispatch_pairs(&[ids[0]], gv).unwrap();
    eprintln!("dispatch_pairs(1 id): {:?}", t.elapsed());
    drop(indexer);

    let t = Instant::now();
    let raw = rpc::call(
        repo.clone(),
        db.clone(),
        "dead_symbols".to_string(),
        &serde_json::json!({"limit": 100}).to_string(),
        "1",
    )
    .unwrap();
    eprintln!(
        "dead_symbols(limit 100): {:?} ({} bytes)",
        t.elapsed(),
        raw.len()
    );
}
