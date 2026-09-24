/// Private item: only `visibility_private_owner` may call it directly by
/// name. #75 fixture (paired with `visibility_private_prober`): a
/// cross-module bare call to this must stay `UNRESOLVED`, not bind
/// through the name fallback.
fn secret_helper() -> &'static str {
    "secret"
}

pub fn call_locally() -> &'static str {
    secret_helper()
}
