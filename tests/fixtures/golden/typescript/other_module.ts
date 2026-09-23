// Decoys: same names as `caller.localUtil`, `helper.formatGreeting` and
// `helper.decorate`.
export function localUtil(): string {
  return "other";
}

export function formatGreeting(name: string): string {
  return "Other";
}

function decorate(name: string): string {
  return "other";
}
