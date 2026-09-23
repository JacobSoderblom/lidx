pub trait Speak {
    /// Default method `Dog` inherits without overriding.
    fn speak(&self) -> &'static str {
        "..."
    }
}

pub struct Dog;

impl Speak for Dog {}

/// Decoy: an unrelated type with its own inherent `speak`.
pub struct Cat;

impl Cat {
    pub fn speak(&self) -> &'static str {
        "Meow"
    }
}
