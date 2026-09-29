const { helperOne } = require("./commonjsExportOwner");

// Bare call: `require` isn't recognized as an import binding (only ES
// `import` statements are), so this reaches the bare-name fallback with
// no import candidate at all -- the same shape the old (wrongly private)
// bug hit.
export function useHelperOne(): string {
  return helperOne();
}
