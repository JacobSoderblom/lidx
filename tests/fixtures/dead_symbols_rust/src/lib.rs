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

impl PoolCustomizer {
    pub fn inherent_dead_method(&self) {}
}

impl std::fmt::Display for PoolCustomizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pool")
    }
}

pub fn truly_dead_function() {}

pub struct TrulyDeadStruct;

// --- cfg twins: one qualname under two `#[cfg]`s, called once ---------------

#[cfg(unix)]
fn platform_name() -> &'static str {
    "unix"
}

#[cfg(not(unix))]
fn platform_name() -> &'static str {
    "other"
}

#[cfg(unix)]
fn dead_twin() {}

#[cfg(not(unix))]
fn dead_twin() {}

pub fn describe_platform() -> &'static str {
    platform_name()
}

// --- `cfg_if!` branches: items at module level, statements in a body --------

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        fn cfg_if_helper() -> u8 { 1 }
    } else {
        fn cfg_if_helper() -> u8 { 2 }
    }
}

fn in_body_helper() {}

pub fn run_cfg_if() -> u8 {
    cfg_if::cfg_if! {
        if #[cfg(feature = "extra")] {
            in_body_helper();
        }
    }
    cfg_if_helper()
}

// --- `macro_rules!` transcribers ---------------------------------------------

fn macro_called_from_body() -> bool {
    true
}

fn sort_key(a: &u8, b: &u8) -> std::cmp::Ordering {
    a.cmp(b)
}

#[macro_export]
macro_rules! log_it {
    ($($t:tt)*) => {
        if $crate::macro_called_from_body() {
            let mut v = vec![$($t)*];
            v.sort_by(sort_key);
        }
    };
}

// --- attribute arguments ------------------------------------------------------

fn default_name() -> String {
    String::new()
}

fn parse_size(s: &str) -> Result<u64, String> {
    s.parse().map_err(|_| String::new())
}

fn describe() -> String {
    String::new()
}

fn mk_input(n: u8) -> u8 {
    n
}

mod completion {
    pub struct Completer;

    impl Completer {
        pub(crate) fn complete_thing(_current: &str) -> Vec<String> {
            Vec::new()
        }
    }
}

#[derive(Debug)]
struct Settings {
    #[serde(default = "default_name")]
    name: String,
    #[arg(long, hide = true, value_parser = parse_size)]
    size: u64,
    #[arg(add = ArgValueCompleter::new(completion::Completer::complete_thing))]
    thing: String,
}

#[derive(Debug)]
enum Failure {
    #[error("unsupported: {}", describe())]
    Unsupported,
}

#[cfg(test)]
mod attribute_tests {
    use super::mk_input;

    #[test_case(mk_input(1); "one")]
    fn runs_case(_n: u8) {}
}

// --- a function-local unit struct passed as a value --------------------------

fn visit_thing(deserializer: &dyn Fn(V) -> u8) -> u8 {
    struct V;
    deserializer(V)
}

// --- calls in `const` / `static` initialisers --------------------------------

struct Alias(&'static str);

const fn alias(name: &'static str) -> Alias {
    Alias(name)
}

const ALIASES: &[Alias] = &[alias("a"), alias("b")];

struct Inner;

impl Inner {
    fn new() -> Inner {
        Inner
    }

    fn to_long(&self) -> String {
        String::new()
    }
}

static LONG: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let inner = Inner::new();
    inner.to_long()
});

// --- a `use .. as` inside a fn body -----------------------------------------

mod sub {
    pub struct Thing;

    impl Thing {
        pub(crate) fn make() -> Thing {
            Thing
        }
    }
}

pub fn uses_local_use() -> sub::Thing {
    use crate::sub::Thing as T;
    T::make()
}

// --- nested fn items ----------------------------------------------------------

pub fn outer() -> u8 {
    fn inner() -> u8 {
        1
    }
    let inner_value = inner;
    inner_value()
}

pub fn outer_with_unused_nested() {
    fn never_used() {}
}

// --- a fn named like a primitive, used as a value --------------------------

fn bool(_: &str) -> bool {
    true
}

fn apply_primitive_named(f: fn(&str) -> bool) -> bool {
    f("x")
}

pub fn uses_primitive_named_fn() -> bool {
    apply_primitive_named(bool)
}

// --- a `pub` method of a `pub` type implemented in another module ------------

mod shared_types {
    pub struct Shared;
}

mod shared_impls {
    use crate::shared_types::Shared;

    impl Shared {
        pub fn never_called_pub_method(&self) {}
    }
}

// --- a default method of a trait implemented for a std type ----------------

trait RangeLabel {
    fn label_never_called_here(&self) -> usize {
        1
    }
}

impl RangeLabel for std::ops::RangeInclusive<usize> {}

// --- the `high` tier: a name spelled nowhere else ------------------------------

fn spelled_nowhere() {}

fn spelled_in_string() {}

pub const MESSAGE: &str = "spelled_in_string";

#[cfg(test)]
mod tests {
    #[test]
    fn test_default_config() {}

    #[tokio::test]
    async fn test_async_thing() {}
}
