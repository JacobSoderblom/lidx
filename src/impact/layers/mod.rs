//! Impact analysis layers
//!
//! This module contains the implementation of each impact analysis layer:
//! - Layer 1: Direct impact (BFS graph traversal)
//! - Layer 2: Test impact (test-to-source relationships)
//! - Layer 3: Historical impact (co-change patterns)

pub mod direct;
pub mod historical;
pub mod test;

pub use direct::analyze_direct_impact;
pub use historical::HistoricalImpactLayer;
pub use test::TestImpactLayer;
