//! Confidence scoring and fusion for impact analysis
//!
//! This module implements confidence propagation, decay, and fusion algorithms
//! for combining evidence from multiple sources.

use crate::impact::types::{ConfidenceScore, ImpactSource};

/// Distance-based confidence decay factor
/// Confidence decays as: base_confidence * (DECAY_FACTOR ^ distance)
pub const DECAY_FACTOR: f32 = 0.9;

/// Maximum confidence for multi-source symbols (dampening)
/// When 3+ sources agree, cap combined confidence to prevent overconfidence
pub const MAX_MULTI_SOURCE_CONFIDENCE: f32 = 0.95;

/// Apply distance-based decay to confidence score
///
/// # Examples
///
/// ```
/// use lidx::impact::confidence::apply_distance_decay;
///
/// let base = 0.95;
/// assert_eq!(apply_distance_decay(base, 0), 0.95);
/// assert!((apply_distance_decay(base, 1) - 0.855).abs() < 0.001);
/// assert!((apply_distance_decay(base, 2) - 0.7695).abs() < 0.001);
/// ```
pub fn apply_distance_decay(base_confidence: ConfidenceScore, distance: usize) -> ConfidenceScore {
    if distance == 0 {
        return base_confidence;
    }
    base_confidence * DECAY_FACTOR.powi(distance as i32)
}

/// Fuse multiple confidence scores using Noisy-OR algorithm
///
/// Noisy-OR assumes independent evidence sources and computes:
/// combined = 1 - PRODUCT(1 - c_i)
///
/// This means:
/// - Single strong signal (0.9) → 0.9
/// - Multiple weak signals (0.5, 0.5, 0.5) → 0.875
/// - One certain signal (1.0) → 1.0 (dominates)
/// - One impossible signal (0.0) → doesn't contribute
///
/// # Examples
///
/// ```
/// use lidx::impact::confidence::fuse_confidence_noisy_or;
///
/// // Single source
/// assert_eq!(fuse_confidence_noisy_or(&[0.9]), 0.9);
///
/// // Two sources
/// assert_eq!(fuse_confidence_noisy_or(&[0.5, 0.5]), 0.75);
///
/// // Three sources reinforce
/// let result = fuse_confidence_noisy_or(&[0.5, 0.5, 0.5]);
/// assert!((result - 0.875).abs() < 0.001);
/// ```
pub fn fuse_confidence_noisy_or(scores: &[ConfidenceScore]) -> ConfidenceScore {
    if scores.is_empty() {
        return 0.0;
    }

    let product: f32 = scores.iter().map(|&c| 1.0 - c).product();
    1.0 - product
}

/// Fuse confidence scores with dampening for multi-source symbols
///
/// Applies Noisy-OR fusion, but caps the result at MAX_MULTI_SOURCE_CONFIDENCE
/// when 3+ sources agree, to prevent overconfidence from correlated signals.
///
/// # Examples
///
/// ```
/// use lidx::impact::confidence::fuse_confidence_with_dampening;
///
/// // Two sources: no dampening
/// assert_eq!(fuse_confidence_with_dampening(&[0.9, 0.9]), 0.99);
///
/// // Three sources: dampened to 0.95
/// let result = fuse_confidence_with_dampening(&[0.9, 0.9, 0.9]);
/// assert_eq!(result, 0.95);
/// ```
pub fn fuse_confidence_with_dampening(scores: &[ConfidenceScore]) -> ConfidenceScore {
    let raw = fuse_confidence_noisy_or(scores);

    if scores.len() >= 3 {
        raw.min(MAX_MULTI_SOURCE_CONFIDENCE)
    } else {
        raw
    }
}

/// Extract confidence score from impact source
pub fn confidence_from_source(source: &ImpactSource) -> ConfidenceScore {
    match source {
        ImpactSource::DirectEdge { distance, .. } => {
            // Direct edges have high base confidence
            let base = 0.95;
            apply_distance_decay(base, *distance)
        }
        ImpactSource::TestLink { strategy, .. } => {
            // Test link confidence depends on strategy
            match strategy.as_str() {
                "call" => 0.95,
                "import" => 0.7,
                "naming" => 0.6,
                "proximity" => 0.4,
                _ => 0.5, // Unknown strategy
            }
        }
        ImpactSource::CoChange { frequency, .. } => {
            // Co-change confidence is the frequency itself
            *frequency
        }
    }
}

/// Fuse evidence from multiple sources for a single symbol
pub fn fuse_evidence(sources: &[ImpactSource]) -> ConfidenceScore {
    let confidences: Vec<ConfidenceScore> = sources
        .iter()
        .map(confidence_from_source)
        .collect();

    fuse_confidence_with_dampening(&confidences)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_decay_zero_distance() {
        assert_eq!(apply_distance_decay(0.95, 0), 0.95);
    }

    #[test]
    fn distance_decay_one_hop() {
        let result = apply_distance_decay(0.95, 1);
        assert!((result - 0.855).abs() < 0.001);
    }

    #[test]
    fn distance_decay_two_hops() {
        let result = apply_distance_decay(0.95, 2);
        assert!((result - 0.7695).abs() < 0.001);
    }

    #[test]
    fn noisy_or_empty() {
        assert_eq!(fuse_confidence_noisy_or(&[]), 0.0);
    }

    #[test]
    fn noisy_or_single() {
        assert_eq!(fuse_confidence_noisy_or(&[0.9]), 0.9);
    }

    #[test]
    fn noisy_or_two_sources() {
        let result = fuse_confidence_noisy_or(&[0.5, 0.5]);
        assert!((result - 0.75).abs() < 0.001);
    }

    #[test]
    fn noisy_or_three_sources() {
        let result = fuse_confidence_noisy_or(&[0.5, 0.5, 0.5]);
        assert!((result - 0.875).abs() < 0.001);
    }

    #[test]
    fn noisy_or_with_zero() {
        // Zero confidence doesn't contribute
        let result = fuse_confidence_noisy_or(&[0.9, 0.0]);
        assert!((result - 0.9).abs() < 0.001);
    }

    #[test]
    fn noisy_or_with_one() {
        // One confidence dominates
        let result = fuse_confidence_noisy_or(&[1.0, 0.5]);
        assert_eq!(result, 1.0);
    }

    #[test]
    fn dampening_two_sources() {
        // No dampening for 2 sources
        let result = fuse_confidence_with_dampening(&[0.9, 0.9]);
        assert!((result - 0.99).abs() < 0.001);
    }

    #[test]
    fn dampening_three_sources() {
        // Dampened to 0.95 for 3+ sources
        let result = fuse_confidence_with_dampening(&[0.9, 0.9, 0.9]);
        assert_eq!(result, 0.95);
    }

    #[test]
    fn confidence_from_direct_edge() {
        let source = ImpactSource::DirectEdge {
            edge_kind: "CALL".to_string(),
            distance: 1,
        };
        let conf = confidence_from_source(&source);
        assert!((conf - 0.855).abs() < 0.001);
    }

    #[test]
    fn confidence_from_test_link() {
        let source = ImpactSource::TestLink {
            strategy: "call".to_string(),
            test_type: "unit".to_string(),
        };
        assert_eq!(confidence_from_source(&source), 0.95);

        let source2 = ImpactSource::TestLink {
            strategy: "import".to_string(),
            test_type: "integration".to_string(),
        };
        assert_eq!(confidence_from_source(&source2), 0.7);
    }

    #[test]
    fn confidence_from_cochange() {
        let source = ImpactSource::CoChange {
            frequency: 0.45,
            co_change_count: 15,
            last_cochange: None,
        };
        assert_eq!(confidence_from_source(&source), 0.45);
    }

    #[test]
    fn fuse_mixed_evidence() {
        let sources = vec![
            ImpactSource::DirectEdge {
                edge_kind: "CALL".to_string(),
                distance: 1,
            },
            ImpactSource::TestLink {
                strategy: "call".to_string(),
                test_type: "unit".to_string(),
            },
        ];
        let result = fuse_evidence(&sources);
        // Should combine 0.855 and 0.95
        assert!(result > 0.95); // Noisy-OR increases confidence
    }
}
