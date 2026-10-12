// No import: `Registry.lookup` is matched by its two-segment tail.
export function useRegistry(): number {
  return Registry.lookup();
}
