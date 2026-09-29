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
