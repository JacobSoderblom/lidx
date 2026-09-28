import axios from "axios";
import { run } from "./ambiguous_a";
import { Dog, Speaker } from "./animals";
import { Greeter } from "./greeter";
import { formatGreeting } from "./helper";
import * as helperNs from "./helper";

function localUtil(): string {
  return "local";
}

// Plain call: a bare same-module function call.
export function entry(): string {
  return localUtil();
}

// Named import call: `formatGreeting` is bound by the import only.
export function callImportedHelper(name: string): string {
  return formatGreeting(name);
}

// Namespace import call: `helperNs.formatGreeting`.
export function callNamespaceImport(name: string): string {
  return helperNs.formatGreeting(name);
}

// Receiver-typed call: `g: Greeter` pins `.greet`.
export function callReceiverTyped(g: Greeter): string {
  return g.greet("world");
}

// Inherited method call: `Dog` has no `speak` of its own.
export function callInherited(d: Dog): string {
  return d.speak();
}

// Interface call: dispatch through `Speaker`.
export function callInterface(s: Speaker): string {
  return s.say();
}

// Ambiguous name: `run` exists in two modules; the import picks
// `ambiguous_a`.
export function callAmbiguous(): void {
  run();
}

// External call: `axios` is never indexed.
export function callExternal(): void {
  axios.get("https://example.com");
}
