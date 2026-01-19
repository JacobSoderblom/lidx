//! Configuration for impact analysis
//!
//! This module provides configuration types and builders for controlling
//! multi-layer impact analysis behavior.

pub use crate::impact::types::{
    DirectConfig, HistoricalConfig, MultiLayerConfig, TestConfig,
};

impl MultiLayerConfig {
    /// Create a builder for fluent configuration
    pub fn builder() -> MultiLayerConfigBuilder {
        MultiLayerConfigBuilder::new()
    }

    /// Create config with only direct layer enabled (v1 compatibility)
    pub fn direct_only() -> Self {
        Self {
            direct: DirectConfig {
                enabled: true,
                ..Default::default()
            },
            test: TestConfig {
                enabled: false,
                ..Default::default()
            },
            historical: HistoricalConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Create config with all layers enabled
    pub fn all_layers() -> Self {
        Self {
            direct: DirectConfig {
                enabled: true,
                ..Default::default()
            },
            test: TestConfig {
                enabled: true,
                ..Default::default()
            },
            historical: HistoricalConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

/// Builder for MultiLayerConfig with fluent API
#[derive(Default)]
pub struct MultiLayerConfigBuilder {
    direct: DirectConfig,
    test: TestConfig,
    historical: HistoricalConfig,
    include_paths: bool,
    min_confidence: f32,
    limit: usize,
}

impl MultiLayerConfigBuilder {
    pub fn new() -> Self {
        Self {
            direct: DirectConfig::default(),
            test: TestConfig::default(),
            historical: HistoricalConfig::default(),
            include_paths: false,
            min_confidence: 0.0,
            limit: 10000,
        }
    }

    pub fn max_depth(mut self, depth: usize) -> Self {
        self.direct.max_depth = depth;
        self
    }

    pub fn direction(mut self, direction: String) -> Self {
        self.direct.direction = direction;
        self
    }

    pub fn include_tests(mut self, include: bool) -> Self {
        self.direct.include_tests = include;
        self
    }

    pub fn include_paths(mut self, include: bool) -> Self {
        self.include_paths = include;
        self
    }

    pub fn min_confidence(mut self, min: f32) -> Self {
        self.min_confidence = min;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn enable_test_layer(mut self, enabled: bool) -> Self {
        self.test.enabled = enabled;
        self
    }

    pub fn enable_historical_layer(mut self, enabled: bool) -> Self {
        self.historical.enabled = enabled;
        self
    }

    pub fn direct_config(mut self, config: DirectConfig) -> Self {
        self.direct = config;
        self
    }

    pub fn test_config(mut self, config: TestConfig) -> Self {
        self.test = config;
        self
    }

    pub fn historical_config(mut self, config: HistoricalConfig) -> Self {
        self.historical = config;
        self
    }

    pub fn build(self) -> MultiLayerConfig {
        MultiLayerConfig {
            direct: self.direct,
            test: self.test,
            historical: self.historical,
            include_paths: self.include_paths,
            min_confidence: self.min_confidence,
            limit: self.limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_works() {
        let config = MultiLayerConfig::builder()
            .max_depth(5)
            .direction("upstream".to_string())
            .include_paths(true)
            .min_confidence(0.7)
            .limit(5000)
            .enable_test_layer(true)
            .build();

        assert_eq!(config.direct.max_depth, 5);
        assert_eq!(config.direct.direction, "upstream");
        assert!(config.include_paths);
        assert_eq!(config.min_confidence, 0.7);
        assert_eq!(config.limit, 5000);
        assert!(config.test.enabled);
        // Historical is now enabled by default (Phase 3 complete)
        assert!(config.historical.enabled);
    }

    #[test]
    fn direct_only_config() {
        let config = MultiLayerConfig::direct_only();
        assert!(config.direct.enabled);
        assert!(!config.test.enabled);
        assert!(!config.historical.enabled);
    }

    #[test]
    fn all_layers_config() {
        let config = MultiLayerConfig::all_layers();
        assert!(config.direct.enabled);
        assert!(config.test.enabled);
        assert!(config.historical.enabled);
    }
}
