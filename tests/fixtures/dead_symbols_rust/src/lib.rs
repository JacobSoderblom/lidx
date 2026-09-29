use std::sync::OnceLock;

pub struct Config {
    pub name: String,
}

impl Config {
    pub fn from_env() -> Self {
        Config {
            name: String::new(),
        }
    }
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn shared() -> &'static Config {
    CONFIG.get_or_init(Config::from_env)
}

pub struct CrossRef {
    pub target: String,
}

pub struct FileContext {
    pub refs: Vec<CrossRef>,
}

pub fn build() -> FileContext {
    FileContext { refs: Vec::new() }
}

pub trait Customizer {
    fn on_acquire(&self);
}

pub struct PoolCustomizer;

impl Customizer for PoolCustomizer {
    fn on_acquire(&self) {}
}

pub fn truly_dead_function() {}

pub struct TrulyDeadStruct;

#[cfg(test)]
mod tests {
    #[test]
    fn test_default_config() {}

    #[tokio::test]
    async fn test_async_thing() {}
}
