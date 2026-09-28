function helperOne(): string {
  return "one";
}

// CommonJS-style export: `helperOne` is never directly wrapped in an
// `export` statement, but is reachable from another file via
// `module.exports`. Issue #75 follow-up, finding E: a top-level function
// must not be recorded private just because it isn't `export`ed inline —
// "not exported" and "unreachable from another file" aren't the same
// thing (CommonJS, a separate `export { name }`, re-exports, ...).
module.exports = { helperOne };
