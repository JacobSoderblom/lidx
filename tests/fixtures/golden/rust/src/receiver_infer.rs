/// Issue #108: receiver-type inference for locals and typed params. `Engine`
/// and `Cache` both declare `resolve`, so a bare-name fallback is ambiguous;
/// the receiver's locally-known type must pick the target.
pub struct Engine {}

impl Engine {
    pub fn new() -> Self {
        Engine {}
    }

    pub fn from_parts() -> Result<Self, ()> {
        Ok(Engine {})
    }

    pub fn resolve(&self) -> u32 {
        1
    }
}

pub struct Cache;

impl Cache {
    pub fn resolve(&self) -> u32 {
        2
    }
}

/// `let e = Engine::new(..)` local.
pub fn via_ctor_local() -> u32 {
    let e = Engine::new();
    e.resolve()
}

/// `let e = Engine { .. }` local.
pub fn via_struct_literal() -> u32 {
    let e = Engine {};
    e.resolve()
}

/// `let e: Engine = ..` annotated local.
pub fn via_annotated_local() -> u32 {
    let e: Engine = make();
    e.resolve()
}

/// `e: &Engine` param.
pub fn via_ref_param(e: &Engine) -> u32 {
    e.resolve()
}

/// `e: &mut Engine<'_>` param.
pub fn via_mut_generic_param(e: &mut Engine<'_>) -> u32 {
    e.resolve()
}

/// Untyped receiver: nothing locally knowable, must stay ambiguous.
pub fn via_unknown(e: impl Sized) -> u32 {
    e.resolve()
}

fn make() -> Engine {
    Engine {}
}

/// `let mut e = Engine::new()` is a `mut_pattern`.
pub fn via_mut_local() -> u32 {
    let mut e = Engine::new();
    e.resolve()
}

/// `.unwrap()` peeled off the initializer.
pub fn via_unwrap() -> u32 {
    let e = Engine::from_parts().unwrap();
    e.resolve()
}

/// `?` peeled off the initializer.
pub fn via_try() -> Option<u32> {
    let e = Engine::from_parts()?;
    Some(e.resolve())
}

/// A generic param `T` is not a receiver type: nothing locally knowable, so
/// the ambiguous bare name stays unresolved (as before inference existed).
pub fn via_generic<T: Sized>(e: T) -> u32 {
    e.resolve()
}

/// A tuple pattern rebinds `e`, poisoning the earlier `Engine::new()` type.
pub fn via_shadowed_by_tuple() -> u32 {
    let e = Engine::new();
    let (e, _) = (Cache, 0);
    e.resolve()
}

/// Issue #189: pattern bindings. `Engine::resolve` and `Cache::resolve`
/// collide on the bare name; the scrutinee's known type decides.
pub struct Pair {
    pub engine: Engine,
    pub cache: Cache,
}

pub enum Slot {
    Full(Engine),
    Named { cache: Cache },
    Empty,
}

/// `if let Some(x) = opt` with `opt: Option<Engine>`.
pub fn via_if_let(opt: Option<Engine>) -> u32 {
    if let Some(x) = opt {
        return x.resolve();
    }
    0
}

/// `while let Some(x) = it.next()` with a known iterator item type.
pub fn via_while_let(mut it: impl Iterator<Item = Engine>) -> u32 {
    let mut n = 0;
    while let Some(x) = it.next() {
        n += x.resolve();
    }
    n
}

/// `while let Some(x) = stack.pop()` over `Vec<Cache>`.
pub fn via_while_let_pop(mut stack: Vec<Cache>) -> u32 {
    let mut n = 0;
    while let Some(x) = stack.pop() {
        n += x.resolve();
    }
    n
}

/// `match` arm bindings over `Option<Engine>`.
pub fn via_match_option(opt: Option<Engine>) -> u32 {
    match opt {
        Some(x) => x.resolve(),
        None => 0,
    }
}

/// `Ok(x)` / `Err(e)` of `Result<Engine, Cache>`.
pub fn via_match_result(r: Result<Engine, Cache>) -> u32 {
    match r {
        Ok(x) => x.resolve(),
        Err(e) => e.resolve(),
    }
}

/// Tuple pattern of a known tuple type.
pub fn via_tuple_pattern(t: (Engine, Cache)) -> u32 {
    let (a, b) = t;
    a.resolve() + b.resolve()
}

/// Struct pattern of a same-file struct.
pub fn via_struct_pattern(p: Pair) -> u32 {
    let Pair { engine, cache } = p;
    engine.resolve() + cache.resolve()
}

/// Enum variant patterns of a same-file enum.
pub fn via_enum_variants(s: Slot) -> u32 {
    match s {
        Slot::Full(e) => e.resolve(),
        Slot::Named { cache } => cache.resolve(),
        Slot::Empty => 0,
    }
}

/// Unknown scrutinee type: every binding is poisoned, never guessed.
pub fn via_unknown_scrutinee(opt: impl Sized) -> u32 {
    if let Some(x) = opt {
        return x.resolve();
    }
    0
}

/// Arm bindings do not leak: the same name is `Engine` in one arm and
/// `Cache` in the other, and each arm resolves on its own type.
pub fn via_arm_scoping(r: Result<Engine, Cache>) -> u32 {
    match r {
        Ok(v) => v.resolve(),
        Err(v) => v.resolve(),
    }
}

/// An `if let` binding does not leak past its block: after it, `x` is the
/// outer `Cache` again.
pub fn via_no_leak(opt: Option<Engine>, x: Cache) -> u32 {
    if let Some(x) = opt {
        x.resolve();
    }
    x.resolve()
}

/// A pattern shape that does not fit the scrutinee type is poisoned.
pub fn via_mismatched_pattern(opt: Option<Engine>) -> u32 {
    match opt {
        Ok(x) => x.resolve(),
        _ => 0,
    }
}
