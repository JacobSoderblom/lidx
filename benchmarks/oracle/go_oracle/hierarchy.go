package main

// Type hierarchy oracle (issue #449): which in-repo named types implement which in-repo interfaces, which struct embeds
// which type, which interface embeds which interface, and which implementing method stands for which interface method.
// Everything comes from go/types (`types.Implements`, `types.NewMethodSet`, `Struct.Field(i).Embedded()`,
// `Interface.EmbeddedType(i)`); nothing is derived from lidx.

import (
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

// HPair is one relation: Sub -> Base (a type or a method, keyed at the declaration's name identifier).
type HPair struct {
	Sub  Decl `json:"sub"`
	Base Decl `json:"base"`
	// Ptr is true when only the pointer type *T implements the interface (pointer-receiver methods).
	Ptr bool `json:"ptr,omitempty"`
	// Promoted marks an override whose implementing method is declared on another type that T embeds.
	Promoted bool `json:"promoted,omitempty"`
	// Via names the type that declares the implementing method (promoted overrides).
	SubType  string `json:"sub_type,omitempty"`
	BaseType string `json:"base_type,omitempty"`
}

// HExternal is a relation whose base is declared outside the repository (`fmt.Stringer`, `error`, `io.Reader`, an embedded
// `sync.Mutex`). Judged for existence only.
type HExternal struct {
	Sub      Decl   `json:"sub"`
	Base     string `json:"base"`
	Relation string `json:"relation"`
}

// HUngradable is a declaration the oracle cannot judge (generic types and interfaces).
type HUngradable struct {
	Decl   Decl   `json:"decl"`
	Reason string `json:"reason"`
}

type Hierarchy struct {
	Implements []HPair `json:"implements"`
	// Embeds is struct embedding (struct -> embedded in-repo type); Extends is interface embedding.
	Embeds    []HPair `json:"embeds"`
	Extends   []HPair `json:"extends"`
	Overrides []HPair `json:"overrides"`
	// External lists relations to a base outside the repository.
	External []HExternal `json:"external_base"`
	// Types and Ifaces are the declarations the oracle judged (a lidx edge between two of them that the compiler does not
	// confirm is a wrong edge; an edge touching anything else is not judged).
	Types      []Decl        `json:"types"`
	Ifaces     []Decl        `json:"ifaces"`
	Ungradable []HUngradable `json:"ungradable"`
	// Excluded counts what was left out, by bucket.
	Excluded map[string]int `json:"excluded"`
}

type hdecl struct {
	obj   *types.TypeName
	named *types.Named
	d     Decl
}

func computeHierarchy(pkgs []*packages.Package, root string) *Hierarchy {
	h := &Hierarchy{Excluded: map[string]int{}, Implements: []HPair{}, Embeds: []HPair{}, Extends: []HPair{}, Overrides: []HPair{},
		External: []HExternal{}, Types: []Decl{}, Ifaces: []Decl{}, Ungradable: []HUngradable{}}
	inRepo := func(pos token.Position) (string, bool) {
		if pos.Filename == "" {
			return "", false
		}
		rel, err := filepath.Rel(root, pos.Filename)
		if err != nil || strings.HasPrefix(rel, "..") {
			return "", false
		}
		return filepath.ToSlash(rel), true
	}
	declOfPos := func(fset *token.FileSet, p token.Pos, name string) (Decl, bool) {
		if !p.IsValid() {
			return Decl{}, false
		}
		pos := fset.Position(p)
		rel, ok := inRepo(pos)
		if !ok {
			return Decl{}, false
		}
		return Decl{File: rel, Line: pos.Line, Name: name}, true
	}
	key := func(d Decl) string { return d.File + ":" + strconv.Itoa(d.Line) }

	type pairKey struct{ a, b string }
	seenImpl := map[pairKey]bool{}
	seenEmbeds := map[pairKey]bool{}
	seenExtends := map[pairKey]bool{}
	seenOver := map[pairKey]bool{}
	seenExt := map[string]bool{}
	seenType := map[string]bool{}
	seenIface := map[string]bool{}
	seenUngr := map[string]bool{}
	seenEmpty := map[string]bool{}

	// per package: the in-repo named types declared at package level
	type pkgDecls struct {
		types  []hdecl // non-interface named types
		ifaces []hdecl // interfaces (with their type sets as method sets)
	}
	cache := map[*packages.Package]*pkgDecls{}
	var declsOf func(p *packages.Package) *pkgDecls
	declsOf = func(p *packages.Package) *pkgDecls {
		if d, ok := cache[p]; ok {
			return d
		}
		d := &pkgDecls{}
		cache[p] = d
		if p.Types == nil || p.Fset == nil {
			return d
		}
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || tn.IsAlias() {
				continue
			}
			named, ok := tn.Type().(*types.Named)
			if !ok {
				continue
			}
			dd, ok := declOfPos(p.Fset, tn.Pos(), tn.Name())
			if !ok {
				continue
			}
			x := hdecl{obj: tn, named: named, d: dd}
			if _, isIface := named.Underlying().(*types.Interface); isIface {
				d.ifaces = append(d.ifaces, x)
			} else {
				d.types = append(d.types, x)
			}
		}
		return d
	}
	external := func(sub Decl, base, rel string) {
		k := key(sub) + ">" + base + ">" + rel
		if seenExt[k] {
			return
		}
		seenExt[k] = true
		h.External = append(h.External, HExternal{Sub: sub, Base: base, Relation: rel})
	}
	ungradable := func(d Decl, reason string) {
		k := key(d)
		if seenUngr[k] {
			return
		}
		seenUngr[k] = true
		h.Ungradable = append(h.Ungradable, HUngradable{Decl: d, Reason: reason})
		h.Excluded[reason]++
	}
	// named in-repo (or external) declaration of a type used as an embedded field / embedded interface
	namedOf := func(t types.Type) *types.Named {
		t = types.Unalias(t)
		if p, ok := t.(*types.Pointer); ok {
			t = types.Unalias(p.Elem())
		}
		if n, ok := t.(*types.Named); ok {
			return n.Origin()
		}
		return nil
	}

	// Every interface of every loaded package. A type and an interface need not share an import path (a codec package does not
	// import the package that declares the interface it satisfies); all packages come from one packages.Load, so their types
	// compare. With tests, `p` and `p [p.test]` are distinct objects: pairs mixing them can only miss (identical types differ), and
	// the right variant pairs with itself, so the union loses nothing.
	var ifaces []hdecl
	for _, q := range pkgs {
		ifaces = append(ifaces, declsOf(q).ifaces...)
	}
	for _, p := range pkgs {
		if p.Types == nil || p.Fset == nil {
			continue
		}
		fset := p.Fset
		own := declsOf(p)
		for _, it := range ifaces {
			iface := it.named.Underlying().(*types.Interface)
			if it.named.TypeParams().Len() > 0 {
				ungradable(it.d, "generic_interface")
				continue
			}
			if iface.NumMethods() == 0 || !iface.IsMethodSet() {
				if !seenEmpty[key(it.d)] {
					seenEmpty[key(it.d)] = true
					h.Excluded["empty_or_constraint_interface"]++
				}
				continue
			}
			if !seenIface[key(it.d)] {
				seenIface[key(it.d)] = true
				h.Ifaces = append(h.Ifaces, it.d)
			}
		}
		// interfaces declared in this package: interface embedding
		for _, it := range own.ifaces {
			iface := it.named.Underlying().(*types.Interface)
			for i := 0; i < iface.NumEmbeddeds(); i++ {
				et := iface.EmbeddedType(i)
				n := namedOf(et)
				if n == nil {
					continue // union / constraint element
				}
				if _, isIface := n.Underlying().(*types.Interface); !isIface {
					continue
				}
				bd, ok := declOfPos(fset, n.Obj().Pos(), n.Obj().Name())
				if !ok {
					if iface.NumMethods() > 0 {
						pkg := ""
						if n.Obj().Pkg() != nil {
							pkg = n.Obj().Pkg().Path() + "."
						}
						external(it.d, pkg+n.Obj().Name(), "extends")
					}
					continue
				}
				k := pairKey{key(it.d), key(bd)}
				if seenExtends[k] {
					continue
				}
				seenExtends[k] = true
				h.Extends = append(h.Extends, HPair{Sub: it.d, Base: bd})
			}
		}
		for _, tt := range own.types {
			if tt.named.TypeParams().Len() > 0 {
				ungradable(tt.d, "generic_type")
				continue
			}
			if !seenType[key(tt.d)] {
				seenType[key(tt.d)] = true
				h.Types = append(h.Types, tt.d)
			}
			// struct embedding
			if st, ok := tt.named.Underlying().(*types.Struct); ok {
				for i := 0; i < st.NumFields(); i++ {
					f := st.Field(i)
					if !f.Embedded() {
						continue
					}
					n := namedOf(f.Type())
					if n == nil {
						continue
					}
					bd, ok := declOfPos(fset, n.Obj().Pos(), n.Obj().Name())
					if !ok {
						pkg := ""
						if n.Obj().Pkg() != nil {
							pkg = n.Obj().Pkg().Path() + "."
						}
						external(tt.d, pkg+n.Obj().Name(), "embeds")
						continue
					}
					k := pairKey{key(tt.d), key(bd)}
					if seenEmbeds[k] {
						continue
					}
					seenEmbeds[k] = true
					h.Embeds = append(h.Embeds, HPair{Sub: tt.d, Base: bd})
				}
			}
			// implements
			val := tt.named
			ptr := types.NewPointer(tt.named)
			for _, it := range ifaces {
				iface := it.named.Underlying().(*types.Interface)
				if it.named.TypeParams().Len() > 0 || iface.NumMethods() == 0 || !iface.IsMethodSet() {
					continue
				}
				k := pairKey{key(tt.d), key(it.d)}
				if seenImpl[k] {
					continue
				}
				viaValue := types.Implements(val, iface)
				viaPtr := !viaValue && types.Implements(ptr, iface)
				if !viaValue && !viaPtr {
					continue
				}
				seenImpl[k] = true
				h.Implements = append(h.Implements, HPair{Sub: tt.d, Base: it.d, Ptr: viaPtr})
				var recv types.Type = val
				if viaPtr {
					recv = ptr
				}
				ms := types.NewMethodSet(recv)
				for j := 0; j < iface.NumMethods(); j++ {
					im := iface.Method(j)
					bd, ok := declOfPos(fset, im.Pos(), im.Name())
					if !ok {
						h.Excluded["override_interface_method_outside_repo"]++
						continue
					}
					sel := ms.Lookup(im.Pkg(), im.Name())
					if sel == nil {
						continue
					}
					fn, ok := sel.Obj().(*types.Func)
					if !ok {
						continue
					}
					fn = fn.Origin()
					sd, ok := declOfPos(fset, fn.Pos(), fn.Name())
					if !ok {
						h.Excluded["override_impl_method_outside_repo"]++
						continue
					}
					if key(sd) == key(bd) {
						// the struct embeds the interface itself, so the "implementing" method is the interface's own method
						h.Excluded["override_impl_is_embedded_interface_method"]++
						continue
					}
					ok2 := pairKey{key(sd), key(bd)}
					if seenOver[ok2] {
						continue
					}
					seenOver[ok2] = true
					promoted := len(sel.Index()) > 1
					h.Overrides = append(h.Overrides, HPair{Sub: sd, Base: bd, Promoted: promoted, SubType: tt.d.Name + "@" + key(tt.d), BaseType: it.d.Name + "@" + key(it.d)})
				}
			}
			// an in-repo type satisfying an external interface the repository mentions would be listed here; Go has no
			// declared implements clause, so the external bucket holds only embedded external bases (above)
		}
	}
	sortPairs := func(ps []HPair) {
		sort.Slice(ps, func(i, j int) bool {
			a, b := ps[i], ps[j]
			if key(a.Sub) != key(b.Sub) {
				return a.Sub.File < b.Sub.File || (a.Sub.File == b.Sub.File && a.Sub.Line < b.Sub.Line)
			}
			return a.Base.File < b.Base.File || (a.Base.File == b.Base.File && a.Base.Line < b.Base.Line)
		})
	}
	sortPairs(h.Implements)
	sortPairs(h.Embeds)
	sortPairs(h.Extends)
	sortPairs(h.Overrides)
	sort.Slice(h.External, func(i, j int) bool {
		a, b := h.External[i], h.External[j]
		return key(a.Sub)+a.Base+a.Relation < key(b.Sub)+b.Base+b.Relation
	})
	declLess := func(ds []Decl) {
		sort.Slice(ds, func(i, j int) bool {
			return ds[i].File < ds[j].File || (ds[i].File == ds[j].File && ds[i].Line < ds[j].Line)
		})
	}
	declLess(h.Types)
	declLess(h.Ifaces)
	sort.Slice(h.Ungradable, func(i, j int) bool {
		a, b := h.Ungradable[i].Decl, h.Ungradable[j].Decl
		return a.File < b.File || (a.File == b.File && a.Line < b.Line)
	})
	return h
}
