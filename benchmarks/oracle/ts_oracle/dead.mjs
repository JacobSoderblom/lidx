// tsc-based dead-code oracle.
//
// Usage: node dead.mjs --repo DIR [--tsconfig REL] --subdir REL[,REL] > out.json
//
// Declarations (functions, methods, types, module-level consts) of the production files in --subdir, and two verdicts each:
//
//   unref  "dead" when the compiler says nothing uses it, "live" otherwise:
//            - module-level declarations that are not exported (and private class members): the compiler's own
//              `noUnusedLocals` diagnostics 6133 / 6196, which the oracle switches on and reads;
//            - exported declarations and public/protected members: LanguageService `findReferences`, ignoring the definition,
//              references inside the declaration's own text, imports and the declaring file's own export list;
//          declarations reachable through re-exports from the package entry (package.json main/module/types/exports/bin mapped to
//          the sources) are public API (`api`): other code may use them, so they get no verdict.
//   reach  functions and methods only: "live" when a root reaches them through references (calls, `new`, callbacks and any other
//          mention the checker resolves), "dead" otherwise. Roots: the public API, and the module-level code of every file the entry
//          files import transitively. A method also counts as reached when a member it overrides or implements is reached.
//
// Only the compiler API reads the sources; nothing from the repository is executed or installed. Test files are not part of the
// program (the same file set as oracle.mjs), so a declaration used only by tests is dead here.
import ts from "typescript";
import fs from "node:fs";
import path from "node:path";

const args = process.argv.slice(2);
const opt = (n, d) => { const i = args.indexOf(n); return i >= 0 ? args[i + 1] : d; };
const repo = fs.realpathSync(path.resolve(opt("--repo")));
const tsconfigRel = opt("--tsconfig", "tsconfig.json");
const subdirs = (opt("--subdir", "") || "").split(",").filter(Boolean).map((d) => d.replace(/\/+$/, ""));
const inScope = (r) => subdirs.length === 0 || subdirs.some((d) => r === d || r.startsWith(d + "/"));
const SKIP_DIRS = new Set(["node_modules", ".git", "dist", "build", "coverage", ".next", "out", "lib-cjs"]);
const isTestPath = (p) => /(^|\/)(__tests__|tests?|test-utils|e2e|runtime-tests)\//.test(p) || /\.(test|spec|bench)\.[cm]?[tj]sx?$/.test(p);
const t0 = Date.now();

function walk(dir, acc) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    if (ent.name.startsWith(".") && ent.name !== ".") continue;
    const full = path.join(dir, ent.name);
    if (ent.isDirectory()) { if (!SKIP_DIRS.has(ent.name)) walk(full, acc); }
    else if (/\.[cm]?tsx?$/.test(ent.name) && !/\.d\.[cm]?ts$/.test(ent.name)) acc.push(full);
  }
  return acc;
}
const rel = (f) => path.relative(repo, f).split(path.sep).join("/");

let options = { allowJs: false, noEmit: true, skipLibCheck: true, target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext, moduleResolution: ts.ModuleResolutionKind.Bundler, jsx: ts.JsxEmit.ReactJSX, strict: true };
const cfgPath = path.join(repo, tsconfigRel);
if (fs.existsSync(cfgPath)) {
  const cfg = ts.readConfigFile(cfgPath, ts.sys.readFile);
  const parsed = ts.parseJsonConfigFileContent(cfg.config, ts.sys, path.dirname(cfgPath), {}, cfgPath);
  options = { ...options, ...parsed.options, noEmit: true, skipLibCheck: true, composite: false, incremental: false, declaration: false, declarationMap: false, rootDir: undefined, outDir: undefined, noEmitOnError: false };
  delete options.rootDirs; delete options.tsBuildInfoFile;
}
// the compiler's unused-code diagnostics are the oracle for non-exported declarations
options = { ...options, noUnusedLocals: true, noUnusedParameters: false };

const files = walk(repo, []).filter((f) => !isTestPath(rel(f)) && inScope(rel(f)));
const host = {
  getScriptFileNames: () => files,
  getScriptVersion: () => "1",
  getScriptSnapshot: (f) => (ts.sys.fileExists(f) ? ts.ScriptSnapshot.fromString(ts.sys.readFile(f) ?? "") : undefined),
  getCurrentDirectory: () => repo,
  getCompilationSettings: () => options,
  getDefaultLibFileName: (o) => ts.getDefaultLibFilePath(o),
  fileExists: ts.sys.fileExists, readFile: ts.sys.readFile, readDirectory: ts.sys.readDirectory,
  directoryExists: ts.sys.directoryExists, getDirectories: ts.sys.getDirectories, realpath: ts.sys.realpath,
};
const ls = ts.createLanguageService(host, ts.createDocumentRegistry());
const program = ls.getProgram();
const checker = program.getTypeChecker();
const srcFiles = program.getSourceFiles().filter((sf) => !sf.isDeclarationFile && inScope(rel(sf.fileName)) && !isTestPath(rel(sf.fileName)) && !rel(sf.fileName).startsWith(".."));
const fileSet = new Set(srcFiles.map((sf) => rel(sf.fileName)));
const lineOf = (sf, pos) => sf.getLineAndCharacterOfPosition(pos).line + 1;
const keyOf = (file, line) => `${file}:${line}`;

// ---------------------------------------------------------------- package entry -> source files
function packageRoot() {
  let d = path.join(repo, subdirs[0] ?? "");
  while (d.startsWith(repo)) {
    if (fs.existsSync(path.join(d, "package.json"))) return d;
    if (d === repo) break;
    d = path.dirname(d);
  }
  return repo;
}
const pkgRoot = packageRoot();
const pkg = fs.existsSync(path.join(pkgRoot, "package.json")) ? JSON.parse(fs.readFileSync(path.join(pkgRoot, "package.json"), "utf8")) : {};
function leafStrings(v, acc) {
  if (typeof v === "string") acc.push(v);
  else if (Array.isArray(v)) v.forEach((x) => leafStrings(x, acc));
  else if (v && typeof v === "object") Object.values(v).forEach((x) => leafStrings(x, acc));
  return acc;
}
const targets = new Set();
for (const k of ["main", "module", "types", "typings", "browser", "bin"]) leafStrings(pkg[k], []).forEach((s) => targets.add(s));
leafStrings(pkg.exports, []).forEach((s) => targets.add(s));
const BUILD_DIRS = new Set(["dist", "distribution", "lib", "build", "esm", "cjs", "es", "out", "types", "module", "mjs", "cts", "mts"]);
const EXTS = /(\.d)?\.([cm]?[jt]sx?)$/;
const entryMapped = new Map(); // target -> source file | null
const scopeRel = subdirs.map((d) => path.relative(pkgRoot, path.join(repo, d)).split(path.sep).join("/"));
function mapTarget(t) {
  const pattern = t.includes("*");
  let p = t.replace(/^\.\//, "");
  if (/\.(json|node|css)$/.test(p)) return { kind: "non_source" };
  const stem = p.replace(EXTS, "");
  const segs = stem.split("/");
  const variants = [];
  for (let i = 0; i <= segs.length - 1; i++) {
    if (i > 0 && !BUILD_DIRS.has(segs[i - 1])) break;
    variants.push(segs.slice(i).join("/"));
  }
  const bases = ["", ...scopeRel.map((s) => s + "/")];
  const found = new Set();
  for (const v of variants) for (const b of bases) {
    if (pattern) {
      const re = new RegExp("^" + path.relative(repo, path.join(pkgRoot, b + v)).split(path.sep).join("/").replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, "[^/]+") + "(\\.tsx?|/index\\.tsx?)$");
      for (const f of fileSet) if (re.test(f)) found.add(f);
      continue;
    }
    for (const ext of [".ts", ".tsx", ".mts", ".cts", "/index.ts", "/index.tsx"]) {
      const cand = path.relative(repo, path.join(pkgRoot, b + v + ext)).split(path.sep).join("/");
      if (fileSet.has(cand)) { found.add(cand); break; }
    }
    if (found.size && !pattern) break;
  }
  return found.size ? { kind: "source", files: [...found].sort() } : { kind: "unmapped" };
}
const entryFiles = new Set();
const entryReport = [];
for (const t of targets) {
  const m = mapTarget(t);
  entryReport.push({ target: t, ...m });
  if (m.kind === "source") m.files.forEach((f) => entryFiles.add(f));
}

// ---------------------------------------------------------------- declarations
const modFlags = (n) => ts.getCombinedModifierFlags(n);
const strip = (x) => { while (x && (ts.isParenthesizedExpression(x) || ts.isAsExpression(x) || ts.isNonNullExpression(x) || ts.isSatisfiesExpression?.(x))) x = x.expression; return x; };
const isFnInit = (x) => { x = strip(x); return !!x && (ts.isArrowFunction(x) || ts.isFunctionExpression(x)); };
const decls = []; // {node, nameNode, file, line, name, kind, subkind, exported, private, sf, cls}
const byNode = new Map(); // declaration node -> decl
const bySymbol = new Map();

function addDecl(node, nameNode, kind, subkind, sf, extra = {}) {
  const d = { node, nameNode, file: rel(sf.fileName), line: lineOf(sf, nameNode.getStart(sf)), name: nameNode.kind === ts.SyntaxKind.DefaultKeyword ? "default" : nameNode.getText(sf), kind, subkind, sf, exported: false, api: false, ...extra };
  d.key = keyOf(d.file, d.line);
  decls.push(d);
  byNode.set(node, d);
  return d;
}
for (const sf of srcFiles) {
  // names the file exports through a local list (`export { a, b as c }`, `export default a`, `export = a`)
  const listExported = new Set();
  for (const st of sf.statements) {
    if (ts.isExportDeclaration(st) && !st.moduleSpecifier && st.exportClause && ts.isNamedExports(st.exportClause)) {
      for (const el of st.exportClause.elements) { const s = checker.getExportSpecifierLocalTargetSymbol(el); if (s) listExported.add(s); }
    } else if (ts.isExportAssignment(st) && ts.isIdentifier(st.expression)) {
      const s = checker.getSymbolAtLocation(st.expression); if (s) listExported.add(s);
    }
  }
  const isExp = (node, nameNode) => {
    if (modFlags(node) & (ts.ModifierFlags.Export | ts.ModifierFlags.ExportDefault)) return true;
    const s = nameNode && nameNode.kind !== ts.SyntaxKind.DefaultKeyword ? checker.getSymbolAtLocation(nameNode) : undefined;
    return !!s && listExported.has(s);
  };
  for (const st of sf.statements) {
    if (ts.isFunctionDeclaration(st) && st.body) {
      const nm = st.name ?? st.modifiers?.find((x) => x.kind === ts.SyntaxKind.DefaultKeyword);
      if (!nm) continue;
      const d = addDecl(st, nm, "function", "function", sf);
      d.exported = isExp(st, st.name);
    } else if (ts.isVariableStatement(st)) {
      for (const vd of st.declarationList.declarations) {
        if (!ts.isIdentifier(vd.name)) continue;
        const fn = vd.initializer && isFnInit(vd.initializer);
        const d = addDecl(vd, vd.name, fn ? "function" : "const", fn ? "arrow" : "const", sf);
        d.exported = !!(modFlags(st) & ts.ModifierFlags.Export) || (checker.getSymbolAtLocation(vd.name) && listExported.has(checker.getSymbolAtLocation(vd.name)));
      }
    } else if (ts.isClassDeclaration(st)) {
      const nm = st.name ?? st.modifiers?.find((x) => x.kind === ts.SyntaxKind.DefaultKeyword);
      if (!nm) continue;
      const cd = addDecl(st, nm, "type", "class", sf);
      cd.exported = isExp(st, st.name);
      for (const m of st.members) {
        if (!ts.isMethodDeclaration(m) || !m.body || !m.name) continue;
        if (ts.isComputedPropertyName(m.name)) continue;
        const f = ts.getCombinedModifierFlags(m);
        const priv = !!(f & ts.ModifierFlags.Private) || ts.isPrivateIdentifier(m.name);
        const d = addDecl(m, m.name, "method", f & ts.ModifierFlags.Static ? "static" : "instance", sf, { cls: cd, private: priv, protected: !!(f & ts.ModifierFlags.Protected) });
        d.exported = cd.exported && !priv;
      }
    } else if (ts.isInterfaceDeclaration(st)) {
      const d = addDecl(st, st.name, "type", "interface", sf); d.exported = isExp(st, st.name);
    } else if (ts.isTypeAliasDeclaration(st)) {
      const d = addDecl(st, st.name, "type", "alias", sf); d.exported = isExp(st, st.name);
    } else if (ts.isEnumDeclaration(st)) {
      const d = addDecl(st, st.name, "type", "enum", sf); d.exported = isExp(st, st.name);
    }
  }
}
// symbol -> decl, for resolving identifiers
for (const d of decls) {
  const s = checker.getSymbolAtLocation(d.nameNode.kind === ts.SyntaxKind.DefaultKeyword ? d.node : d.nameNode) ?? d.node.symbol;
  if (s) { bySymbol.set(s, d); d.symbol = s; }
}
const declOfNode = (n) => byNode.get(n);

// ---------------------------------------------------------------- public API: what the entry files export
const apiNodes = new Set();
const seenModules = new Set();
function addExportsOf(modSym, depth) {
  if (!modSym || seenModules.has(modSym) || depth > 8) return;
  seenModules.add(modSym);
  let exps = [];
  try { exps = checker.getExportsOfModule(modSym); } catch { return; }
  for (const e of exps) {
    let s = e;
    if (s.flags & ts.SymbolFlags.Alias) { try { s = checker.getAliasedSymbol(s); } catch { continue; } }
    for (const dn of s.declarations ?? []) {
      if (ts.isSourceFile(dn)) { const ms = checker.getSymbolAtLocation(dn); if (ms) addExportsOf(ms, depth + 1); continue; }
      apiNodes.add(dn);
      if (ts.isVariableDeclaration(dn) || ts.isFunctionDeclaration(dn) || ts.isClassDeclaration(dn) || ts.isInterfaceDeclaration(dn) || ts.isTypeAliasDeclaration(dn) || ts.isEnumDeclaration(dn)) { /* a declaration */ }
      if (ts.isModuleDeclaration(dn)) { const ms = checker.getSymbolAtLocation(dn.name); if (ms) addExportsOf(ms, depth + 1); }
    }
    // `export * as ns from "./x"`: the namespace's exports are reachable through `ns.`
    if (s.flags & ts.SymbolFlags.ValueModule) addExportsOf(s, depth + 1);
  }
}
for (const f of entryFiles) {
  const sf = program.getSourceFile(path.join(repo, f));
  const ms = sf && checker.getSymbolAtLocation(sf);
  if (ms) addExportsOf(ms, 0);
}
for (const d of decls) {
  if (apiNodes.has(d.node)) d.api = true;
  if (d.kind === "method" && d.cls && apiNodes.has(d.cls.node) && !d.private) d.api = true; // members of an exported class
  if (d.kind === "method" && d.cls && d.cls.api && !d.private) d.api = true;
}

// ---------------------------------------------------------------- unreferenced
const diag = new Map(); // key -> code
for (const sf of srcFiles) {
  for (const dg of program.getSemanticDiagnostics(sf)) {
    if ((dg.code === 6133 || dg.code === 6196) && dg.start !== undefined) diag.set(keyOf(rel(sf.fileName), lineOf(sf, dg.start)), dg.code);
  }
}
const findNode = (sf, pos) => {
  let n = sf;
  for (;;) {
    const c = ts.forEachChild(n, (ch) => (ch.getStart(sf) <= pos && pos < ch.end ? ch : undefined));
    if (!c) return n;
    n = c;
  }
};
const inside = (node, pred) => { for (let p = node; p; p = p.parent) if (pred(p)) return true; return false; };
let frCalls = 0;
function usedByFindReferences(d) {
  frCalls++;
  const sf = d.sf;
  const pos = d.nameNode.getStart(sf);
  let refs;
  try { refs = ls.findReferences(sf.fileName, pos); } catch { return null; }
  if (!refs) return null;
  const ownStart = d.node.getStart(sf), ownEnd = d.node.end;
  let n = 0;
  for (const rs of refs) for (const r of rs.references) {
    if (r.isDefinition) continue;
    const rsf = program.getSourceFile(r.fileName);
    if (!rsf) continue;
    const here = rsf === sf;
    if (here && r.textSpan.start >= ownStart && r.textSpan.start < ownEnd) continue; // inside the declaration itself
    const node = findNode(rsf, r.textSpan.start);
    if (inside(node, (p) => ts.isImportDeclaration(p) || ts.isImportEqualsDeclaration(p))) continue;
    if (here && inside(node, (p) => (ts.isExportDeclaration(p) && !p.moduleSpecifier) || ts.isExportAssignment(p))) continue; // the declaring file's own export list
    n++;
  }
  return n;
}
for (const d of decls) {
  if (d.api) { d.unref = ""; d.unref_how = "public_api"; continue; }
  const code = diag.get(d.key);
  if (!d.exported && d.kind !== "method") {
    d.unref = code ? "dead" : "live"; d.unref_how = "noUnusedLocals";
  } else if (d.kind === "method" && d.private) {
    d.unref = code ? "dead" : "live"; d.unref_how = "noUnusedLocals";
  } else {
    const n = usedByFindReferences(d);
    d.unref = n === null ? "" : n === 0 ? "dead" : "live"; d.unref_how = "findReferences"; d.refs = n;
  }
}

// ---------------------------------------------------------------- reference graph and reachability
const moduleKey = (file) => keyOf(file, 0);
const edges = new Map(); // owner key -> Set(target key)
const addEdge = (from, to) => { if (from === to) return; let s = edges.get(from); if (!s) edges.set(from, (s = new Set())); s.add(to); };
const declByKey = new Map(decls.map((d) => [d.key, d]));
const ownerTracked = (n) => {
  const d = byNode.get(n);
  return d && (d.kind === "function" || d.kind === "const" || (d.kind === "method") || d.kind === "type") ? d : undefined;
};
const isDeclName = (id) => {
  const p = id.parent;
  if (!p) return false;
  if ((ts.isFunctionDeclaration(p) || ts.isClassDeclaration(p) || ts.isMethodDeclaration(p) || ts.isVariableDeclaration(p) || ts.isInterfaceDeclaration(p) ||
       ts.isTypeAliasDeclaration(p) || ts.isEnumDeclaration(p) || ts.isParameter(p) || ts.isPropertyDeclaration(p) || ts.isPropertySignature(p) ||
       ts.isMethodSignature(p) || ts.isBindingElement(p) || ts.isTypeParameterDeclaration(p) || ts.isEnumMember(p) || ts.isGetAccessorDeclaration(p) ||
       ts.isSetAccessorDeclaration(p) || ts.isImportSpecifier(p) || ts.isImportClause(p) || ts.isNamespaceImport(p) || ts.isImportEqualsDeclaration(p)) && p.name === id) return true;
  if (ts.isPropertyAssignment(p) && p.name === id) return true;
  return false;
};
// the graph key of a member declaration that is not a tracked declaration (an interface or abstract member, a property): a call
// through a base type reaches it, and an override is reached through it
const memberKey = (dn) => {
  const nm = dn.name;
  if (!nm || ts.isComputedPropertyName(nm)) return undefined;
  const sf = dn.getSourceFile();
  if (sf.isDeclarationFile || !fileSet.has(rel(sf.fileName))) return undefined;
  return keyOf(rel(sf.fileName), lineOf(sf, nm.getStart(sf)));
};
function targetsOf(id) {
  let sym;
  try {
    sym = ts.isShorthandPropertyAssignment(id.parent) && id.parent.name === id ? checker.getShorthandAssignmentValueSymbol(id.parent) : checker.getSymbolAtLocation(id);
  } catch { return []; }
  if (!sym) return [];
  if (sym.flags & ts.SymbolFlags.Alias) { try { sym = checker.getAliasedSymbol(sym); } catch { return []; } }
  const out = new Set();
  const direct = bySymbol.get(sym);
  if (direct) out.add(direct.key);
  for (const dn of sym.declarations ?? []) {
    const d = byNode.get(dn);
    if (d) out.add(d.key);
    else if (ts.isMethodSignature(dn) || ts.isPropertySignature(dn) || ts.isMethodDeclaration(dn) || ts.isPropertyDeclaration(dn) || ts.isGetAccessorDeclaration(dn) || ts.isSetAccessorDeclaration(dn)) {
      const k = memberKey(dn);
      if (k) out.add(k);
    }
  }
  return [...out];
}
for (const sf of srcFiles) {
  const file = rel(sf.fileName);
  const visit = (node, owner) => {
    if (ts.isImportDeclaration(node) || ts.isImportEqualsDeclaration(node)) return;
    const d = ownerTracked(node);
    const here = d ? d.key : owner;
    if ((ts.isIdentifier(node) || ts.isPrivateIdentifier(node)) && !isDeclName(node)) {
      for (const t of targetsOf(node)) addEdge(here, t);
    }
    ts.forEachChild(node, (c) => visit(c, here));
  };
  ts.forEachChild(sf, (st) => visit(st, moduleKey(file)));
}
// a member overriding or implementing a base member is reached when the base member is
function baseMembers(d) {
  const out = new Set();
  const cls = d.cls?.node;
  if (!cls) return out;
  const bases = [];
  for (const hc of cls.heritageClauses ?? []) for (const t of hc.types) bases.push(t);
  for (const t of bases) {
    let ty; try { ty = checker.getTypeAtLocation(t.expression); } catch { continue; }
    // the instance side of the base class: its declared type
    let inst = ty;
    try { const decl = ty.symbol?.declarations?.find((x) => ts.isClassDeclaration(x) || ts.isInterfaceDeclaration(x)); if (decl) inst = checker.getDeclaredTypeOfSymbol(ty.symbol); } catch { /* keep */ }
    const p = d.subkind === "static" ? undefined : checker.getPropertyOfType(inst, d.name);
    for (const pd of p?.declarations ?? []) { const bd = byNode.get(pd); if (bd) out.add(bd.key); else out.add(keyOf(rel(pd.getSourceFile().fileName), lineOf(pd.getSourceFile(), pd.name ? pd.name.getStart() : pd.getStart()))); }
  }
  return out;
}
for (const d of decls) {
  if (d.kind !== "method") continue;
  d.overrides = [...baseMembers(d)];
  for (const b of d.overrides) addEdge(b, d.key);
}
// import graph between source files
const imports = new Map(); // file -> Set(file)
const importsTypeOnly = new Map();
for (const sf of srcFiles) {
  const file = rel(sf.fileName);
  const add = (spec, typeOnly) => {
    let r;
    try { r = ts.resolveModuleName(spec, sf.fileName, options, ts.sys).resolvedModule; } catch { return; }
    if (!r) return;
    const t = rel(fs.realpathSync(r.resolvedFileName));
    if (!fileSet.has(t) || t === file) return;
    (typeOnly ? importsTypeOnly : imports).set(file, ((typeOnly ? importsTypeOnly : imports).get(file) ?? new Set()).add(t));
  };
  const visit = (n) => {
    if ((ts.isImportDeclaration(n) || ts.isExportDeclaration(n)) && n.moduleSpecifier && ts.isStringLiteral(n.moduleSpecifier)) {
      const typeOnly = ts.isImportDeclaration(n) ? !!n.importClause?.isTypeOnly : !!n.isTypeOnly;
      add(n.moduleSpecifier.text, typeOnly);
    } else if (ts.isCallExpression(n) && n.expression.kind === ts.SyntaxKind.ImportKeyword && n.arguments[0] && ts.isStringLiteralLike(n.arguments[0])) {
      add(n.arguments[0].text, false);
    } else if (ts.isImportEqualsDeclaration(n) && ts.isExternalModuleReference(n.moduleReference) && ts.isStringLiteralLike(n.moduleReference.expression)) {
      add(n.moduleReference.expression.text, false);
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
}
const importerCount = new Map(), importerCountTypeOnly = new Map();
for (const [f, ts_] of imports) for (const t of ts_) importerCount.set(t, (importerCount.get(t) ?? 0) + 1);
for (const [f, ts_] of importsTypeOnly) for (const t of ts_) importerCountTypeOnly.set(t, (importerCountTypeOnly.get(t) ?? 0) + 1);
// files reachable from the entry files through imports (their module-level code runs)
const liveFiles = new Set();
const stack = [...entryFiles];
while (stack.length) {
  const f = stack.pop();
  if (liveFiles.has(f)) continue;
  liveFiles.add(f);
  for (const t of imports.get(f) ?? []) stack.push(t);
  for (const t of importsTypeOnly.get(f) ?? []) stack.push(t); // a type-only import still pulls the file into the type program; its module code does not run, but keep the file "reached" for the file-level answer
}
const roots = new Set();
for (const d of decls) if (d.api) roots.add(d.key);
for (const f of liveFiles) roots.add(moduleKey(f));
const reached = new Set();
const work = [...roots];
while (work.length) {
  const k = work.pop();
  if (reached.has(k)) continue;
  reached.add(k);
  for (const t of edges.get(k) ?? []) work.push(t);
}
for (const d of decls) {
  if (d.kind === "function" || d.kind === "method") d.reach = reached.has(d.key) ? "live" : "dead";
}

// ---------------------------------------------------------------- output
const out = {
  repo, tsconfig: tsconfigRel, subdirs, ts_version: ts.version, node_version: process.version,
  package_root: rel(pkgRoot) || ".", package_name: pkg.name ?? null,
  entry_files: [...entryFiles].sort(), entry_targets: entryReport,
  library: entryFiles.size > 0, // a package entry exists: its exports are public API
  files: srcFiles.map((sf) => rel(sf.fileName)).sort(),
  decls: decls.map((d) => ({
    file: d.file, line: d.line, name: d.name, kind: d.kind, subkind: d.subkind, exported: !!d.exported, api: !!d.api,
    ...(d.private ? { private: true } : {}), ...(d.cls ? { recv: d.cls.name } : {}),
    unref: d.unref ?? "", unref_how: d.unref_how, ...(d.reach ? { reach: d.reach } : {}), ...(d.refs !== undefined ? { refs: d.refs } : {}),
    ...(d.overrides && d.overrides.length ? { overrides: d.overrides } : {}),
  })).sort((a, b) => a.file.localeCompare(b.file) || a.line - b.line),
  file_info: srcFiles.map((sf) => {
    const f = rel(sf.fileName);
    const fn = decls.filter((d) => d.file === f && (d.kind === "function" || d.kind === "method"));
    return { file: f, importers: importerCount.get(f) ?? 0, importers_type_only: importerCountTypeOnly.get(f) ?? 0, entry: entryFiles.has(f),
      funcs: fn.length, funcs_reachable: fn.filter((d) => d.reach === "live").length, module_reached: liveFiles.has(f) };
  }).sort((a, b) => a.file.localeCompare(b.file)),
  stats: { seconds: Math.round((Date.now() - t0) / 100) / 10, find_references_calls: frCalls, diagnostics_6133_6196: diag.size, api_declarations: apiNodes.size, reference_edges: [...edges.values()].reduce((a, s) => a + s.size, 0) },
};
process.stdout.write(JSON.stringify(out, null, 1));
