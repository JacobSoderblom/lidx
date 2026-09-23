/// Public item, called from `caller` across modules.
pub fn format_greeting(name: &str) -> &'static str {
    decorate(name)
}

/// Private item: only `helper` may call it.
fn decorate(_name: &str) -> &'static str {
    "Hi"
}
