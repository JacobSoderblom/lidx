// Type hierarchy oracle for the TypeScript checker (issue #449): `extends`, `implements`, member overrides, and the
// structural-implements diagnostic. Everything comes from the type checker (getBaseTypes, resolved heritage nodes,
// getPropertyOfType, isTypeAssignableTo); nothing is derived from lidx.
//
// Keys are `file:line` of the declaration's name identifier, like the call edges.
import ts from "typescript";

export function computeHierarchy({ program, checker, rel, inRepo, inScope, isTestPath, withTests }) {
  const h = { extends: [], implements: [], overrides: [], structural_implements: [], external_base: [], types: [], ungradable: [], excluded: {} };
  const bump = (k) => { h.excluded[k] = (h.excluded[k] || 0) + 1; };
  const lineOf = (sf, pos) => sf.getLineAndCharacterOfPosition(pos).line + 1;
  const isTypeDecl = (d) => ts.isClassDeclaration(d) || ts.isClassExpression(d) || ts.isInterfaceDeclaration(d) || ts.isTypeAliasDeclaration(d);
  const nameNode = (d) => {
    if (d.name) return d.name;
    return d.modifiers?.find((x) => x.kind === ts.SyntaxKind.DefaultKeyword);
  };
  const locOf = (d) => {
    const nn = nameNode(d);
    if (!nn) return undefined;
    const sf = d.getSourceFile();
    return { file: rel(sf.fileName), line: lineOf(sf, nn.getStart(sf)), name: d.name ? d.name.text : "default" };
  };
  const key = (d) => `${d.file}:${d.line}`;
  const analysed = (sf) => inRepo(sf.fileName) && !sf.isDeclarationFile && (withTests || !isTestPath(rel(sf.fileName))) && inScope(rel(sf.fileName));

  // the in-scope declaration(s) of a type symbol: undefined when it lives outside the analysed program
  const baseDecls = (sym) => {
    if (!sym || !sym.declarations) return { ext: sym ? sym.getName() : "?", kind: "unresolved" };
    const mine = sym.declarations.map((d) => (ts.isTypeLiteralNode(d) && ts.isTypeAliasDeclaration(d.parent) ? d.parent : d)).filter((d) => isTypeDecl(d) && analysed(d.getSourceFile()));
    if (mine.length) return { decls: mine };
    const any = sym.declarations[0];
    const sf = any.getSourceFile();
    if (inRepo(sf.fileName) && !sf.isDeclarationFile) return { ext: `${rel(sf.fileName)}#${sym.getName()}`, kind: "out_of_scope" };
    return { ext: sym.getName(), kind: sf.isDeclarationFile && inRepo(sf.fileName) ? "repo_dts" : "external" };
  };
  const symOfType = (t) => {
    if (!t) return undefined;
    if (t.isIntersection?.() || t.isUnion?.()) return undefined;
    const s = t.getSymbol?.();
    // `type Shape = { .. }`: the type's own symbol is the anonymous `__type`; the alias is the declaration
    if (t.aliasSymbol && (!s || s.getName() === "__type" || s.getName() === "__object")) return t.aliasSymbol;
    return s ?? t.aliasSymbol;
  };

  const seen = new Set();
  const addPair = (list, rec, tag) => {
    const k = `${tag}|${key(rec.sub)}|${key(rec.base)}`;
    if (seen.has(k)) return;
    seen.add(k);
    list.push(rec);
  };
  const addExternal = (sub, base, relation) => {
    const k = `ext|${key(sub)}|${base.ext}|${relation}`;
    if (seen.has(k)) return;
    seen.add(k);
    h.external_base.push({ sub, base: base.ext, base_kind: base.kind, relation });
  };

  const classes = [];
  const ifaces = [];
  const typeSeen = new Set();
  const judged = (d) => {
    const l = locOf(d);
    if (!l || typeSeen.has(key(l))) return l;
    typeSeen.add(key(l));
    h.types.push(l);
    return l;
  };

  for (const sf of program.getSourceFiles()) {
    if (!analysed(sf)) continue;
    const visit = (node) => {
      if ((ts.isClassDeclaration(node) || ts.isClassExpression(node)) && nameNode(node)) { judged(node); classes.push(node); }
      else if (ts.isInterfaceDeclaration(node)) { judged(node); ifaces.push(node); }
      else if (ts.isTypeAliasDeclaration(node)) judged(node);
      ts.forEachChild(node, visit);
    };
    visit(sf);
  }

  const declaredType = (node) => {
    try { return checker.getDeclaredTypeOfSymbol(checker.getSymbolAtLocation(nameNode(node))); } catch { return undefined; }
  };

  // heritage of one declaration: direct base types from the checker, and the implemented interface types
  const heritage = (node) => {
    const type = declaredType(node);
    const out = { bases: [], implemented: [] };
    if (type && (type.isClass?.() || type.isClassOrInterface?.())) {
      let bs = [];
      try { bs = checker.getBaseTypes(type) || []; } catch { /* ignore */ }
      for (const b of bs) out.bases.push({ type: b, sym: symOfType(b) });
    }
    for (const hc of node.heritageClauses ?? []) {
      if (hc.token !== ts.SyntaxKind.ImplementsKeyword) continue;
      for (const tn of hc.types) {
        let t;
        try { t = checker.getTypeAtLocation(tn); } catch { /* ignore */ }
        out.implemented.push({ type: t, sym: symOfType(t) ?? (() => { try { return checker.getSymbolAtLocation(tn.expression); } catch { return undefined; } })(), node: tn });
      }
    }
    return out;
  };

  const resolveAlias = (s) => {
    if (s && s.flags & ts.SymbolFlags.Alias) { try { return checker.getAliasedSymbol(s); } catch { return s; } }
    return s;
  };

  const members = (node) => node.members.filter((m) => (ts.isMethodDeclaration(m) || ts.isPropertyDeclaration(m) || ts.isGetAccessorDeclaration(m) || ts.isSetAccessorDeclaration(m)) && m.name &&
    !(ts.getCombinedModifierFlags(m) & ts.ModifierFlags.Static));
  const memberLoc = (d) => {
    const sf = d.getSourceFile();
    return { file: rel(sf.fileName), line: lineOf(sf, d.name.getStart(sf)), name: d.name.getText() };
  };

  // every ancestor type (extends and implements, transitively) with the depth it was found at
  const ancestors = (node) => {
    const res = [];
    const seenT = new Set();
    const walk = (n, depth, via) => {
      const hh = heritage(n);
      const next = [...hh.bases.map((b) => ({ ...b, via: "extends" })), ...hh.implemented.map((b) => ({ ...b, via: "implements" }))];
      for (const b of next) {
        if (!b.type) continue;
        const sid = b.sym ? `${b.sym.getName()}@${b.sym.declarations?.[0]?.pos}@${b.sym.declarations?.[0]?.getSourceFile().fileName}` : undefined;
        if (!sid || seenT.has(sid)) continue;
        seenT.add(sid);
        res.push({ ...b, depth, via: depth === 1 ? b.via : via });
        const bd = b.sym?.declarations?.find((d) => ts.isClassLike(d) || ts.isInterfaceDeclaration(d));
        if (bd && depth < 12) walk(bd, depth + 1, via ?? b.via);
      }
    };
    walk(node, 1, undefined);
    return res;
  };

  const emitted = new Set();
  for (const node of [...classes, ...ifaces]) {
    const sub = locOf(node);
    const hh = heritage(node);
    const isClass = ts.isClassLike(node);
    // extends (class -> class, interface -> interfaces)
    // Each `extends` clause is read where it is written: the symbol the name in the clause resolves to (an import alias is
    // followed, a type alias is not: `extends _Alias` names the alias declaration), so merged interface declarations each keep
    // their own bases instead of all landing on the first declaration.
    for (const hc of node.heritageClauses ?? []) {
      if (hc.token !== ts.SyntaxKind.ExtendsKeyword) continue;
      for (const tn of hc.types) {
        let sym;
        try { sym = checker.getSymbolAtLocation(tn.expression); } catch { /* ignore */ }
        sym = resolveAlias(sym);
        if (!sym) { try { sym = symOfType(checker.getTypeAtLocation(tn)); } catch { /* ignore */ } }
        if (!sym) { bump("unresolved_base_expression"); continue; }
        const bd = baseDecls(sym);
        if (!bd.decls) { addExternal(sub, bd, "extends"); continue; }
        addPair(h.extends, { sub, base: locOf(bd.decls[0]), base_alts: bd.decls.slice(1).map(locOf).filter(Boolean).map(key) }, "ext");
      }
    }
    // implements (class -> interface or class), direct clauses only
    for (const b of hh.implemented) {
      const sym = resolveAlias(b.sym);
      if (!sym) { bump("unresolved_implements"); continue; }
      const bd = baseDecls(sym);
      if (!bd.decls) { addExternal(sub, bd, "implements"); continue; }
      addPair(h.implements, { sub, base: locOf(bd.decls[0]), base_alts: bd.decls.slice(1).map(locOf).filter(Boolean).map(key) }, "impl");
    }
    // overrides: every instance member of a class that a base type (extends or implements, transitively) declares too
    if (isClass) {
      const anc = ancestors(node);
      for (const m of members(node)) {
        const name = m.name.getText();
        const sm = memberLoc(m);
        for (const a of anc) {
          let prop;
          try { prop = checker.getPropertyOfType(a.type, ts.isPrivateIdentifier(m.name) ? m.name.text : (m.name.text ?? name)); } catch { /* ignore */ }
          if (!prop || !prop.declarations) continue;
          const own = prop.declarations.filter((d) => d.name && (ts.isMethodDeclaration(d) || ts.isMethodSignature(d) || ts.isPropertyDeclaration(d) || ts.isPropertySignature(d) || ts.isGetAccessorDeclaration(d) || ts.isSetAccessorDeclaration(d)));
          if (!own.length) continue;
          const d0 = own.find((d) => analysed(d.getSourceFile()));
          if (!d0) {
            const sf = own[0].getSourceFile();
            const k = `ovx|${key(sm)}|${a.sym?.getName()}`;
            if (!seen.has(k)) { seen.add(k); bump(inRepo(sf.fileName) && !sf.isDeclarationFile ? "override_base_out_of_scope" : "override_base_external"); }
            continue;
          }
          const bm = memberLoc(d0);
          if (key(bm) === key(sm)) continue; // an inherited member resolves to itself
          const k = `ov|${key(sm)}|${key(bm)}`;
          if (emitted.has(k)) {
            continue;
          }
          emitted.add(k);
          h.overrides.push({ sub: sm, base: bm, depth: a.depth, via: a.via, sub_type: key(sub), base_type: ((d) => d && analysed(d.getSourceFile()) ? key(locOf(d)) : "")(a.sym?.declarations?.find(isTypeDecl)) || (a.sym ? a.sym.getName() : "") });
        }
      }
    }
  }

  // structural implements: class instance types assignable to an interface they do not declare (diagnostic only)
  const nonGeneric = (n) => !n.typeParameters || n.typeParameters.length === 0;
  const declared = new Set(h.implements.map((p) => `${key(p.sub)}>${key(p.base)}`));
  for (const c of classes) {
    if (!nonGeneric(c) || ts.getCombinedModifierFlags(c) & ts.ModifierFlags.Abstract) continue;
    const ct = declaredType(c);
    if (!ct) continue;
    const cl = locOf(c);
    for (const i of ifaces) {
      if (!nonGeneric(i)) continue;
      const il = locOf(i);
      if (declared.has(`${key(cl)}>${key(il)}`)) continue;
      const it = declaredType(i);
      if (!it) continue;
      // a weak interface (every member optional) accepts nearly any class: only interfaces with a required member count
      const props = checker.getPropertiesOfType(it);
      if (props.length === 0 || !props.some((p) => !(p.flags & ts.SymbolFlags.Optional))) continue;
      const have = new Set(checker.getPropertiesOfType(ct).map((p) => p.getName()));
      if (!props.some((p) => have.has(p.getName()))) continue;
      let ok = false;
      try { ok = checker.isTypeAssignableTo(ct, it); } catch { /* ignore */ }
      if (ok) h.structural_implements.push({ sub: cl, base: il });
    }
  }
  h.ungradable = [];
  const cmp = (a, b) => a.file.localeCompare(b.file) || a.line - b.line;
  for (const k of ["extends", "implements", "structural_implements", "overrides"]) h[k].sort((a, b) => cmp(a.sub, b.sub) || cmp(a.base, b.base));
  h.types.sort(cmp);
  h.external_base.sort((a, b) => cmp(a.sub, b.sub) || a.base.localeCompare(b.base));
  return h;
}
