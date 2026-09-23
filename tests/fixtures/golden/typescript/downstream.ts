import { entry } from "./caller";

// Cross-file incoming call into `caller.entry`.
export function useEntry(): string {
  return entry();
}
