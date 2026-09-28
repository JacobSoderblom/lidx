export function formatGreeting(name: string): string {
  return decorate(name);
}

// Not exported: only callable within this module.
function decorate(name: string): string {
  return "Hi";
}
