// Value-reference oracle for the TypeScript checker (issue #452): a function, method, class or function-valued const/property
// used as a value (callback argument, object-literal entry, array element, assignment, `this.m` bound later), found as an
// identifier whose checker symbol is function-like and whose position is not one of the exclusions below. Nothing is derived
// from lidx; keys are `file:line` of the declaration's name identifier, like the call edges.
//
// Excluded positions, each a named bucket in `refs_dropped`:
//   callee            callee of a call / `new` / tagged template / decorator / JSX tag (a call, graded as one); `f.call(..)` and
//                     `f.apply(..)` count as calls too
//   declaration_name  the name of a declaration (also the key of `{ key: value }`)
//   type_position     inside a type node: `typeof f`, annotations, type arguments, `implements X`, `extends X` (a hierarchy edge)
//   import_export     an import or export specifier / clause / declaration (`export { f }`); `export default f` is a reference (ctx export_default)
//   member_access_base the object of a property access that is a class (`Foo.create`: static access, not the class as a value) or
//                     a function used as an object (`f.displayName`); `f.bind(..)` is kept (it binds the function as a value)
//   assignment_target the left side of an assignment (`f = ..`, `this.m = ..`): a write, not a use of the function it holds
//   external_target   the symbol is declared outside the analysed program or in a .d.ts
//   abstract_target   an interface / type-literal member or abstract method: no body to point at
import ts from "typescript";

export function computeRefs({ program, checker, rel, inRepo, inScope, isTestPath, withTests, enclosing, declLoc, keyOf, lineOf, isFnLike }) {
  const out = { refs: [], dropped: {} };
  const drop = (k) => { out.dropped[k] = (out.dropped[k] || 0) + 1; };
  const byKey = new Map();
  const called = new Set();
  const analysed = (sf) => inRepo(sf.fileName) && !sf.isDeclarationFile && (withTests || !isTestPath(rel(sf.fileName))) && inScope(rel(sf.fileName));

  const strip = (x) => { while (x && (ts.isParenthesizedExpression(x) || ts.isAsExpression(x) || ts.isNonNullExpression(x) || ts.isSatisfiesExpression?.(x) || ts.isTypeAssertionExpression(x))) x = x.expression; return x; };
  const isFnInit = (d) => d && d.initializer && isFnLike(strip(d.initializer)) && !ts.isMethodSignature(strip(d.initializer));

  // the value position of an identifier: the identifier, or the property access it is the name of, or the wrapper around either
  const valueExpr = (id) => {
    let e = ts.isPropertyAccessExpression(id.parent) && id.parent.name === id ? id.parent : id;
    while (e.parent && (ts.isParenthesizedExpression(e.parent) || ts.isNonNullExpression(e.parent) || ts.isAsExpression(e.parent) || ts.isSatisfiesExpression?.(e.parent) || ts.isTypeAssertionExpression(e.parent)) && e.parent.expression === e) e = e.parent;
    return e;
  };
  const isCalleeExpr = (e) => {
    const p = e.parent;
    if (!p) return false;
    if (ts.isCallExpression(p) || ts.isNewExpression(p)) return p.expression === e;
    if (ts.isTaggedTemplateExpression(p)) return p.tag === e;
    if (ts.isDecorator(p)) return p.expression === e;
    if (ts.isJsxOpeningElement(p) || ts.isJsxSelfClosingElement(p) || ts.isJsxClosingElement(p)) return p.tagName === e;
    return false;
  };
  const inTypeOrImport = (id) => {
    let n = id;
    for (let p = id.parent; p; p = p.parent) {
      if (ts.isImportDeclaration(p) || ts.isExportDeclaration(p) || ts.isImportEqualsDeclaration(p) || ts.isImportClause(p) || ts.isExportSpecifier(p) || ts.isImportSpecifier(p) || ts.isNamespaceImport(p) || ts.isNamespaceExport?.(p)) return "import_export";
      if (ts.isExpressionWithTypeArguments(p)) { if (p.expression === n) return "type_position"; return null; }
      if (ts.isTypeNode(p)) return "type_position";
      if (ts.isQualifiedName(p) || ts.isPropertyAccessExpression(p)) { n = p; continue; }
      return null;
    }
    return null;
  };

  // the declaration a function-like symbol is keyed at, or a drop reason
  const targetOf = (sym0) => {
    let sym = sym0;
    if (sym.flags & ts.SymbolFlags.Alias) { try { sym = checker.getAliasedSymbol(sym); } catch { return { why: "unresolved" }; } }
    const decls = sym.declarations ?? [];
    if (!decls.length) return { why: "unresolved" };
    const vd = sym.valueDeclaration;
    let decl;
    if (sym.flags & ts.SymbolFlags.Class) decl = decls.find((d) => ts.isClassDeclaration(d) || ts.isClassExpression(d));
    else if (sym.flags & (ts.SymbolFlags.Function | ts.SymbolFlags.Method)) {
      const fns = decls.filter(isFnLike);
      decl = fns.find((d) => d.body) ?? fns[0];
      if (decl && (ts.isMethodSignature(decl) || (!decl.body && ts.isMethodDeclaration(decl) && !ts.isClassLike(decl.parent)) || (!decl.body && (ts.getCombinedModifierFlags(decl) & ts.ModifierFlags.Abstract) !== 0) || (!decl.body && ts.isMethodDeclaration(decl) && ts.isClassLike(decl.parent) && !fns.some((d) => d.body)))) {
        const sf = decl.getSourceFile();
        if (!inRepo(sf.fileName) || sf.isDeclarationFile) return { why: "external_target" };
        return { why: "abstract_target" };
      }
    } else if (vd && (ts.isVariableDeclaration(vd) || ts.isPropertyDeclaration(vd) || ts.isPropertyAssignment(vd)) && isFnInit(vd)) decl = strip(vd.initializer);
    else return { why: "not_function_like" };
    if (!decl) return { why: "not_function_like" };
    const sf = decl.getSourceFile();
    if (!inRepo(sf.fileName) || sf.isDeclarationFile) return { why: "external_target" };
    if (!inScope(rel(sf.fileName))) return { why: "external_target" };
    const loc = declLoc(decl);
    if (!loc) return { why: "anonymous" };
    // what the declaration is: lets the matcher tell a function-valued const (a `USES` row to it is a function-target use) from a class
    return { loc, kind: ts.isClassLike(decl) ? "class" : (ts.isFunctionDeclaration(decl) ? "function" : (ts.isMethodDeclaration(decl) || ts.isGetAccessorDeclaration(decl) || ts.isSetAccessorDeclaration(decl)) ? "method" : "variable") };
  };

  const ctxOf = (e) => {
    const p = e.parent;
    if (!p) return "other";
    if (ts.isCallExpression(p) || ts.isNewExpression(p)) return "call_argument";
    if (ts.isPropertyAssignment(p) && p.initializer === e) return "object_property";
    if (ts.isShorthandPropertyAssignment(p)) return "object_property";
    if (ts.isArrayLiteralExpression(p)) return "array_element";
    if (ts.isVariableDeclaration(p) && p.initializer === e) return "var_init";
    if (ts.isPropertyDeclaration(p) && p.initializer === e) return "class_field";
    if (ts.isBinaryExpression(p) && p.operatorToken.kind === ts.SyntaxKind.EqualsToken && p.right === e) return "assignment";
    if (ts.isReturnStatement(p) || (ts.isArrowFunction(p) && p.body === e)) return "return";
    if (ts.isJsxExpression(p)) return "jsx_attribute";
    if (ts.isSpreadElement(p)) return "spread";
    if (ts.isExportAssignment(p)) return "export_default";
    return "other";
  };

  for (const sf of program.getSourceFiles()) {
    if (!analysed(sf)) continue;
    const visit = (node) => {
      if (ts.isIdentifier(node)) classify(node, sf);
      ts.forEachChild(node, visit);
    };
    visit(sf);
  }

  function classify(id, sf) {
    const p = id.parent;
    if (!p) return;
    let sym;
    let form = "ident";
    let viaCall = false;
    if (ts.isShorthandPropertyAssignment(p) && p.name === id) {
      try { sym = checker.getShorthandAssignmentValueSymbol(p); } catch { /* ignore */ }
      form = "shorthand";
    } else {
      if (p.name === id && !ts.isPropertyAccessExpression(p)) return; // declaration_name: not counted, there are far too many
      if (ts.isBindingElement(p) && p.propertyName === id) return;
      if ((ts.isImportSpecifier(p) || ts.isExportSpecifier(p)) ) return;
      const ve = valueExpr(id);
      const vp = ve.parent;
      if (vp && ts.isPropertyAccessExpression(vp) && vp.expression === ve && !inTypeOrImport(id)) {
        // the object of a property access: `Foo.create`, `fn.displayName`, `this.m.bind(this)`, `fn.call(..)`
        let s0;
        try { s0 = checker.getSymbolAtLocation(id); } catch { /* ignore */ }
        if (s0 && s0.flags & ts.SymbolFlags.Alias) { try { s0 = checker.getAliasedSymbol(s0); } catch { s0 = undefined; } }
        if (!s0 || !(s0.flags & (ts.SymbolFlags.Function | ts.SymbolFlags.Method | ts.SymbolFlags.Class | ts.SymbolFlags.Variable | ts.SymbolFlags.Property))) return;
        const member = vp.name.text;
        const isClass = !!(s0.flags & ts.SymbolFlags.Class);
        if (!isClass && member === "bind") { /* kept: binds the function as a value */ }
        else if (!isClass && (member === "call" || member === "apply") && isCalleeExpr(valueExpr(vp.name))) { viaCall = true; }
        else if (isClass && member === "prototype") { /* kept: `(Foo as any).prototype[..] = ..` uses the class as a value */ }
        else { if (s0.flags & (ts.SymbolFlags.Function | ts.SymbolFlags.Method | ts.SymbolFlags.Class)) drop("member_access_base"); return; }
      }
      const why = inTypeOrImport(id);
      if (why) { drop(why); return; }
      if (ts.isPropertyAccessExpression(p) && p.name === id) form = ts.isIdentifier(p.expression) || p.expression.kind === ts.SyntaxKind.ThisKeyword ? (p.expression.kind === ts.SyntaxKind.ThisKeyword ? "this_member" : "property_access") : "property_access";
      try { sym = checker.getSymbolAtLocation(id); } catch { /* ignore */ }
    }
    if (!sym) return;
    // cheap prefilter: only function-like symbols matter
    const fl = sym.flags;
    const pre = fl & (ts.SymbolFlags.Function | ts.SymbolFlags.Method | ts.SymbolFlags.Class | ts.SymbolFlags.Alias | ts.SymbolFlags.Variable | ts.SymbolFlags.Property);
    if (!pre) return;
    const t = targetOf(sym);
    const e = valueExpr(id);
    const isCallee = viaCall || isCalleeExpr(e);
    // the target of an assignment (`f = () => ..`, `this.m = ..`) is a write to the variable, not a use of the function it holds
    if (!isCallee && e.parent && ts.isBinaryExpression(e.parent) && e.parent.left === e && e.parent.operatorToken.kind >= ts.SyntaxKind.FirstAssignment && e.parent.operatorToken.kind <= ts.SyntaxKind.LastAssignment) { drop("assignment_target"); return; }
    if (t.why) { if (!isCallee && t.why !== "not_function_like" && t.why !== "unresolved" && t.why !== "anonymous") drop(t.why); return; }
    const { fns, containers } = enclosing(id);
    const nearest = fns[0];
    const caller = nearest ?? { file: rel(sf.fileName), line: 0, name: "<module>" };
    const k = `${keyOf(caller)}>${keyOf(t.loc)}`;
    if (isCallee) { called.add(k); return; }
    const site = `${rel(sf.fileName)}:${lineOf(sf, id.getStart(sf))}`;
    const ctx = form === "shorthand" ? "object_property" : ctxOf(e);
    let r = byKey.get(k);
    if (!r) {
      r = { caller, callee: t.loc, callee_kind: t.kind, sites: [site], forms: [form], ctxs: [ctx] };
      r.caller_chain = fns.slice(1).concat(containers).map(keyOf);
      if (nearest) r.caller_chain.push(`${rel(sf.fileName)}:0`);
      else if (containers.length) r.caller_alts = containers.map(keyOf);
      byKey.set(k, r);
      out.refs.push(r);
    } else {
      if (r.sites.length < 20 && !r.sites.includes(site)) r.sites.push(site);
      if (!r.forms.includes(form)) r.forms.push(form);
      if (!r.ctxs.includes(ctx)) r.ctxs.push(ctx);
      if (!nearest && containers.length) r.caller_alts = [...new Set([...(r.caller_alts ?? []), ...containers.map(keyOf)])];
      else if (nearest) r.caller_chain = [...new Set([...(r.caller_chain ?? []), ...fns.slice(1).concat(containers).map(keyOf), `${rel(sf.fileName)}:0`])];
    }
  }
  for (const r of out.refs) { r.called = called.has(`${keyOf(r.caller)}>${keyOf(r.callee)}`); if (!r.called) delete r.called; r.forms.sort(); r.ctxs.sort(); r.sites.sort(); }
  out.refs.sort((a, b) => a.caller.file.localeCompare(b.caller.file) || a.caller.line - b.caller.line || a.callee.file.localeCompare(b.callee.file) || a.callee.line - b.callee.line);
  return out;
}
