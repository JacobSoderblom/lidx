// Configuration module for lidx
// Reads from environment variables with sensible defaults

use std::env;
use std::sync::OnceLock;

/// Global configuration instance
static CONFIG: OnceLock<Config> = OnceLock::new();

/// Application configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Search timeout in seconds (LIDX_SEARCH_TIMEOUT_SECS)
    pub search_timeout_secs: u32,

    /// Maximum pattern length in bytes (LIDX_PATTERN_MAX_LENGTH)
    pub pattern_max_length: usize,

    /// Database connection pool size (LIDX_POOL_SIZE)
    pub pool_size: u32,

    /// Database connection pool minimum idle connections (LIDX_POOL_MIN_IDLE)
    pub pool_min_idle: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            search_timeout_secs: 30,
            pattern_max_length: 10_000,
            pool_size: 10,
            pool_min_idle: 2,
        }
    }
}

impl Config {
    /// Load configuration from environment variables
    fn from_env() -> Self {
        let mut config = Config::default();

        if let Ok(val) = env::var("LIDX_SEARCH_TIMEOUT_SECS") {
            if let Ok(parsed) = val.parse() {
                config.search_timeout_secs = parsed;
            } else {
                eprintln!(
                    "lidx: Warning: Invalid LIDX_SEARCH_TIMEOUT_SECS value: {}, using default: {}",
                    val, config.search_timeout_secs
                );
            }
        }

        if let Ok(val) = env::var("LIDX_PATTERN_MAX_LENGTH") {
            if let Ok(parsed) = val.parse() {
                config.pattern_max_length = parsed;
            } else {
                eprintln!(
                    "lidx: Warning: Invalid LIDX_PATTERN_MAX_LENGTH value: {}, using default: {}",
                    val, config.pattern_max_length
                );
            }
        }

        if let Ok(val) = env::var("LIDX_POOL_SIZE") {
            if let Ok(parsed) = val.parse() {
                config.pool_size = parsed;
            } else {
                eprintln!(
                    "lidx: Warning: Invalid LIDX_POOL_SIZE value: {}, using default: {}",
                    val, config.pool_size
                );
            }
        }

        if let Ok(val) = env::var("LIDX_POOL_MIN_IDLE") {
            if let Ok(parsed) = val.parse() {
                config.pool_min_idle = parsed;
            } else {
                eprintln!(
                    "lidx: Warning: Invalid LIDX_POOL_MIN_IDLE value: {}, using default: {}",
                    val, config.pool_min_idle
                );
            }
        }

        config
    }

    /// Get the global configuration instance
    pub fn get() -> &'static Config {
        CONFIG.get_or_init(Config::from_env)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.search_timeout_secs, 30);
        assert_eq!(config.pattern_max_length, 10_000);
        assert_eq!(config.pool_size, 10);
        assert_eq!(config.pool_min_idle, 2);
    }
}
