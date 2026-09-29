/// Issue #189 review: receiver types read off declarations in another file
/// (`receiver_infer.rs`), finished by the resolver. `Engine` and `Cache`
/// both declare `resolve`, so only the deferred type can pick the target.
use crate::receiver_infer::{Cache, Engine, Pair, Slot, make_cache};

/// `Ok(e)` of another file's `Result`-returning constructor.
pub fn remote_ok_of_ctor() -> u32 {
    if let Ok(e) = Engine::from_parts() {
        return e.resolve();
    }
    0
}

/// `Some(c)` of another file's free fn.
pub fn remote_some_of_free_fn() -> u32 {
    if let Some(c) = make_cache() {
        return c.resolve();
    }
    0
}

/// Struct pattern of another file's struct.
pub fn remote_struct_pattern(p: Pair) -> u32 {
    let Pair { engine, cache } = p;
    engine.resolve() + cache.resolve()
}

/// Enum variant patterns of another file's enum.
pub fn remote_variants(s: Slot) -> u32 {
    match s {
        Slot::Full(e) => e.resolve(),
        Slot::Named { cache } => cache.resolve(),
        Slot::Empty => 0,
    }
}

/// Field of another file's struct.
pub fn remote_field(p: Pair) -> u32 {
    let e = p.engine;
    e.resolve()
}

/// A method call on a known type, typed by its declaration in another file.
pub fn remote_method(e: Engine) -> u32 {
    let c = e.duplicate();
    c.resolve()
}

/// Untouched by any of the above: stays unresolved.
pub fn remote_unknown(c: impl Sized) -> u32 {
    c.resolve()
}
