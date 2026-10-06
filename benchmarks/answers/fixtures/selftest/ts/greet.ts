export function greet(name: string): string {
  return "hi " + decorate(name);
}

export function decorate(s: string): string {
  return "<" + s + ">";
}

export function neverUsed(s: string): string {
  return s;
}
