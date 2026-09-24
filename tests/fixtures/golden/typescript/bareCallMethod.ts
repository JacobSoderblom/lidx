class Utility {
  // The only `recalc` in this fixture -- a method-kind symbol. Before
  // #75, a receiver-less call to the same name would fuzzy-bind to this
  // via the bare-name fallback; #75 bars a bare call from binding to any
  // method-kind candidate.
  recalc(): string {
    return "widget";
  }
}

// Bare call, no receiver: recalc is not declared or imported into this
// module. Must stay UNRESOLVED.
export function bareCaller(): string {
  return recalc();
}
