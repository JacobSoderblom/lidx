// tsc-based call-graph oracle.
//
// Usage: node oracle.mjs --repo DIR [--tsconfig REL] [--tests] > out.json
//
// Walks every CallExpression / NewExpression in the repository's TypeScript
// sources, resolves the callee with the type checker and emits
// (enclosing declaration, callee declaration) pairs keyed by the line of the
// declaration's name identifier. Only the compiler API is used: nothing from the
// analysed repository is executed or installed.
import ts from "typescript";
import fs from "node:fs";
import path from "node:path";

const args = process.argv.slice(2);
const opt = (n, d) => { const i = args.indexOf(n); return i >= 0 ? args[i + 1] : d; };
const repo = fs.realpathSync(path.resolve(opt("--repo")));
const tsconfigRel = opt("--tsconfig", "tsconfig.json");
const withTests = args.includes("--tests");

const SKIP_DIRS = new Set(["node_modules", ".git", "dist", "build", "coverage", ".next", "out", "lib-cjs"]);
const isTestPath = (p) => /(^|\/)(__tests__|tests?|test-utils|e2e|runtime-tests)\//.test(p) || /\.(test|spec|bench)\.[cm]?[tj]sx?$/.test(p);

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
const inRepo = (f) => { const r = path.relative(repo, f); return !r.startsWith("..") && !path.isAbsolute(r) && !r.split(path.sep).includes("node_modules"); };

// compiler options from the repository's own tsconfig (extends resolved)
let options = { allowJs: false, noEmit: true, skipLibCheck: true, target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext, moduleResolution: ts.ModuleResolutionKind.Bundler, jsx: ts.JsxEmit.ReactJSX, strict: true };
const cfgPath = path.join(repo, tsconfigRel);
if (fs.existsSync(cfgPath)) {
  const cfg = ts.readConfigFile(cfgPath, ts.sys.readFile);
  const parsed = ts.parseJsonConfigFileContent(cfg.config, ts.sys, path.dirname(cfgPath), {}, cfgPath);
  options = { ...options, ...parsed.options, noEmit: true, skipLibCheck: true, composite: false, incremental: false, declaration: false, declarationMap: false, rootDir: undefined, outDir: undefined, noEmitOnError: false };
  delete options.rootDirs; delete options.tsBuildInfoFile;
}

let files = walk(repo, []).filter((f) => withTests || !isTestPath(rel(f)));
const program = ts.createProgram({ rootNames: files, options });
const checker = program.getTypeChecker();

const out = { repo, tests: withTests, tsconfig: tsconfigRel, ts_version: ts.version, files: [], callers: [], edges: [], dropped: {} };
const drop = (k) => { out.dropped[k] = (out.dropped[k] || 0) + 1; };
const seenEdges = new Map();
const unresolvedSites = new Set();
const extSites = new Set();
const callerSeen = new Set();

const lineOf = (sf, pos) => sf.getLineAndCharacterOfPosition(pos).line + 1;

const isFnLike = (n) => ts.isFunctionDeclaration(n) || ts.isMethodDeclaration(n) || ts.isConstructorDeclaration(n) ||
  ts.isGetAccessorDeclaration(n) || ts.isSetAccessorDeclaration(n) || ts.isFunctionExpression(n) || ts.isArrowFunction(n) ||
  ts.isMethodSignature(n) || ts.isFunctionTypeNode(n) || ts.isCallSignatureDeclaration(n) || ts.isConstructSignatureDeclaration(n);

// name token (position) of a function-like declaration; undefined when anonymous
function declName(n) {
  if (ts.isConstructorDeclaration(n)) {
    const kw = n.getChildren().find((c) => c.kind === ts.SyntaxKind.ConstructorKeyword);
    return kw ? { node: kw, name: "constructor" } : undefined;
  }
  if (n.name) return { node: n.name, name: n.name.getText() };
  if (ts.isArrowFunction(n) || ts.isFunctionExpression(n)) {
    const p = n.parent;
    if (ts.isVariableDeclaration(p) && ts.isIdentifier(p.name)) return { node: p.name, name: p.name.text };
    if ((ts.isPropertyAssignment(p) || ts.isPropertyDeclaration(p)) && p.name) return { node: p.name, name: p.name.getText() };
    if (ts.isBinaryExpression(p) && p.operatorToken.kind === ts.SyntaxKind.EqualsToken) {
      const l = p.left;
      if (ts.isPropertyAccessExpression(l)) return { node: l.name, name: l.name.text };
      if (ts.isIdentifier(l)) return { node: l, name: l.text };
    }
  }
  return undefined;
}
const mkDecl = (n) => {
  const d = declName(n);
  if (!d) return undefined;
  const sf = n.getSourceFile();
  return { file: rel(sf.fileName), line: lineOf(sf, d.node.getStart(sf)), name: d.name };
};
const keyOf = (d) => `${d.file}:${d.line}`;

// nearest named enclosing function-like and the outermost named one
function enclosing(node) {
  let nearest, outer;
  for (let p = node.parent; p; p = p.parent) {
    if (isFnLike(p)) {
      const d = mkDecl(p);
      if (d) { nearest ??= d; outer = d; }
    } else if (ts.isClassStaticBlockDeclaration?.(p)) { /* static block: attribute to class */ }
    else if (ts.isPropertyDeclaration(p) && p.initializer && !isFnLike(p.initializer)) {
      // field initialiser: attribute to the field
      const d = p.name ? { file: rel(p.getSourceFile().fileName), line: lineOf(p.getSourceFile(), p.name.getStart()), name: p.name.getText() } : undefined;
      if (d) { nearest ??= d; outer = d; }
    }
  }
  return { nearest, outer };
}

function calleeDecls(call) {
  let sig;
  try { sig = checker.getResolvedSignature(call); } catch { return { err: "signature_throw" }; }
  let sym;
  const target = call.expression;
  try { sym = checker.getSymbolAtLocation(ts.isPropertyAccessExpression(target) ? target.name : target); } catch { /* ignore */ }
  if (sym && sym.flags & ts.SymbolFlags.Alias) { try { sym = checker.getAliasedSymbol(sym); } catch { /* ignore */ } }
  let decl = sig && sig.declaration;
  const alts = [];
  if (sym && sym.declarations) for (const d of sym.declarations) alts.push(d);
  if (!decl || !isFnLike(decl)) {
    // `new C()` with no explicit constructor, calls on `any`, etc.
    if (ts.isNewExpression(call) && sym && sym.declarations) {
      const cls = sym.declarations.find((d) => ts.isClassDeclaration(d) || ts.isClassExpression(d));
      if (cls) return { decl: cls, alts, kind: "class" };
    }
    if (sym && sym.declarations && sym.declarations.length) {
      const d = sym.declarations.find(isFnLike) ?? sym.declarations[0];
      if (ts.isVariableDeclaration(d) && d.initializer && isFnLike(d.initializer)) return { decl: d.initializer, alts };
      if (isFnLike(d)) return { decl: d, alts };
    }
    return { err: "unresolved" };
  }
  return { decl, alts };
}

function declLoc(d) {
  if (ts.isClassDeclaration(d) || ts.isClassExpression(d)) {
    if (!d.name) return undefined;
    const sf = d.getSourceFile();
    return { file: rel(sf.fileName), line: lineOf(sf, d.name.getStart(sf)), name: d.name.text };
  }
  return mkDecl(d);
}

for (const sf of program.getSourceFiles()) {
  if (!inRepo(sf.fileName) || sf.isDeclarationFile) continue;
  if (!withTests && isTestPath(rel(sf.fileName))) continue;
  out.files.push(rel(sf.fileName));
  // every named function-like declaration is a judgeable caller
  const visit = (node) => {
    if (isFnLike(node) && !ts.isMethodSignature(node) && !ts.isFunctionTypeNode(node) && (node.body || ts.isArrowFunction(node))) {
      const d = mkDecl(node);
      if (d) callerSeen.add(JSON.stringify(d));
    }
    if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
      if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) { drop("dynamic_import"); }
      else {
        const { nearest, outer } = enclosing(node);
        const r = calleeDecls(node);
        if (r.err) { drop("callee_" + r.err); unresolvedSites.add(`${rel(sf.fileName)}:${lineOf(sf, node.getStart(sf))}`); if (process.env.ORACLE_DEBUG) console.error("UNRESOLVED", rel(sf.fileName) + ":" + lineOf(sf, node.getStart(sf)), node.getText().slice(0, 80).replace(/\n/g, " ")); }
        else {
          let target = r.decl;
          const tf = target.getSourceFile();
          if (!inRepo(tf.fileName) || tf.isDeclarationFile) { drop("callee_external"); extSites.add(`${rel(sf.fileName)}:${lineOf(sf, node.getStart(sf))}`); }
          else {
            // overload signatures: judge against the implementation declaration
            let impl = target;
            if (r.alts.length > 1 && !target.body) { const b = r.alts.find((a) => isFnLike(a) && a.body); if (b) impl = b; }
            const callee = declLoc(impl);
            const calleeAlts = [...new Set(r.alts.map(declLoc).filter(Boolean).concat(declLoc(target) ? [declLoc(target)] : []).map((d) => keyOf(d)))];
            if (!callee) drop("callee_anonymous");
            else {
              const nearest2 = nearest ?? { file: rel(sf.fileName), line: 0, name: "<module>" };
              callerSeen.add(JSON.stringify(nearest2));
              const k = `${keyOf(nearest2)}>${keyOf(callee)}`;
              const site = `${rel(sf.fileName)}:${lineOf(sf, node.getStart(sf))}`;
              if (!seenEdges.has(k)) {
                const e = { caller: nearest2, callee, sites: [site] };
                seenEdges.set(k, e);
                if (outer && keyOf(outer) !== keyOf(nearest2)) e.caller_alts = [keyOf(outer)];
                const ca = calleeAlts.filter((x) => x !== keyOf(callee));
                if (ca.length) e.callee_alts = ca;
                out.edges.push(e);
              } else if (seenEdges.get(k).sites.length < 20) seenEdges.get(k).sites.push(site);
            }
          }
        }
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
}
out.unresolved_sites = [...unresolvedSites].sort();
out.ext_sites = [...extSites].sort();
out.callers = [...callerSeen].map((s) => JSON.parse(s)).sort((a, b) => a.file.localeCompare(b.file) || a.line - b.line);
out.edges.sort((a, b) => a.caller.file.localeCompare(b.caller.file) || a.caller.line - b.caller.line || a.callee.file.localeCompare(b.callee.file) || a.callee.line - b.callee.line);
const diag = ts.getPreEmitDiagnostics(program).filter((d) => d.code === 2307).length;
out.unresolved_module_diagnostics = diag;
if (process.env.ORACLE_DEBUG) { const c = {}; for (const d of ts.getPreEmitDiagnostics(program).filter((d) => d.code === 2307)) { const m = /module '(.*?)'/.exec(ts.flattenDiagnosticMessageText(d.messageText, " ")); const k = m ? m[1] : "?"; c[k] = (c[k] || 0) + 1; } console.error("MODDIAG", JSON.stringify(Object.entries(c).sort((a, b) => b[1] - a[1]).slice(0, 25))); }
process.stdout.write(JSON.stringify(out, null, 1));
