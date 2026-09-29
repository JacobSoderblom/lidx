/// Issue #189 review: receiver types read off declarations in another file
/// (`receiver_infer.rs`), finished by the resolver. `Engine` and `Cache`
/// both declare `resolve`, so only the deferred type can pick the target.
use crate::receiver_infer::{Cache, Engine, Pair, Slot, make_boxed, make_cache, make_engine_async, make_generic, make_test_engine, Wrapper};

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

/// Another file's `async fn`, awaited: the output type is known.
pub async fn remote_async_awaited() -> u32 {
    let e = make_engine_async().await;
    e.resolve()
}

/// Another file's `async fn`, not awaited: a future, so untracked.
pub async fn remote_async_not_awaited() -> u32 {
    let f = make_engine_async();
    f.resolve()
}

/// Generic return types stay untracked across files too.
pub fn remote_generic_fn_return() -> u32 {
    let e = make_generic();
    e.resolve()
}

pub fn remote_generic_method_return(w: Wrapper<u8>) -> u32 {
    let i = w.get();
    i.resolve()
}

/// `Box<Engine>` return: receivers auto-deref.
pub fn remote_boxed_return() -> u32 {
    let e = make_boxed();
    e.resolve()
}

/// `#[test] async fn`, awaited.
pub async fn remote_test_async_awaited() -> u32 {
    let e = make_test_engine().await;
    e.resolve()
}
