pub struct Greeter;

impl Greeter {
    /// Method call on self.
    pub fn greet(&self, name: &str) -> &'static str {
        self.format(name)
    }

    fn format(&self, _name: &str) -> &'static str {
        "Hello"
    }
}

/// Decoy: a second, unrelated `greet` and `format`.
pub struct LoudGreeter;

impl LoudGreeter {
    pub fn greet(&self, _name: &str) -> &'static str {
        "HELLO"
    }

    fn format(&self, _name: &str) -> &'static str {
        "LOUD"
    }
}
