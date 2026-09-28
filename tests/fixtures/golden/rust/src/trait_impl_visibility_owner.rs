/// The only `Conf` in this fixture. `Default::default` carries no `pub`
/// (Rust doesn't attach one to a trait impl method) but is exactly as
/// visible as the `Default` trait itself -- must never be recorded
/// private (issue #75 follow-up, finding B).
pub struct Conf;

impl Default for Conf {
    fn default() -> Self {
        Conf
    }
}
