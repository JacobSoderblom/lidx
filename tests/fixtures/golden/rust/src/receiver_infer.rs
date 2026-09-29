/// Issue #108: receiver-type inference for locals and typed params. `Engine`
/// and `Cache` both declare `resolve`, so a bare-name fallback is ambiguous;
/// the receiver's locally-known type must pick the target.
pub struct Engine {}

impl Engine {
    pub fn new() -> Self {
        Engine {}
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
