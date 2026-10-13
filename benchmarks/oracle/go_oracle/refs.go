package main

// Value-reference oracle (issue #452): every use of an in-repo function or method that is not the callee of a call.
// A function is a value when it is passed as an argument, stored in a struct field, map or slice, assigned, returned or
// bound (`x.M` as a method value, `T.M` as a method expression). The compiler is the only source: `types.Info.Uses`
// identifiers whose object is a *types.Func, minus the `Fun` of a CallExpr (after unwrapping parentheses, selectors and
// generic instantiation). Keyed like the call edges: (enclosing declaration, referenced declaration) at the declaration's
// name identifier; a use outside any function declaration (a package-level table) is keyed at line 0 of its file.

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Ref is one (referencing declaration, referenced declaration) pair.
type Ref struct {
	Caller Decl     `json:"caller"`
	Callee Decl     `json:"callee"`
	Sites  []string `json:"sites,omitempty"`
	// Called is true when the same caller also calls the same function somewhere (compiler evidence from the callee
	// positions of the same package, whether or not RTA reaches the caller): the pair is a call too, and a graph that keeps
	// only the call is right (README, "Value references").
	Called bool `json:"called,omitempty"`
	// Forms: ident (`f`), qualified (`pkg.F`), method_value (`x.M`), method_expr (`T.M`), iface_method_value (`i.M` on an interface).
	Forms []string `json:"forms,omitempty"`
	// Ctxs: call_argument, struct_field, map_value, composite_element, assignment, return, var_init, other.
	Ctxs []string `json:"ctxs,omitempty"`
}

type RefsOut struct {
	Refs    []Ref          `json:"refs"`
	Callers []Decl         `json:"ref_callers"`
	Dropped map[string]int `json:"dropped"`
}

func addUnique(list []string, s string) []string {
	for _, x := range list {
		if x == s {
			return list
		}
	}
	return append(list, s)
}

func unparen(e ast.Expr) ast.Expr {
	for {
		switch x := e.(type) {
		case *ast.ParenExpr:
			e = x.X
		default:
			return e
		}
	}
}

// calleeIdent returns the identifier that names the function a call expression invokes (the identifier itself, the selected
// name of a selector, either through parentheses and a generic instantiation), or nil.
func calleeIdent(fun ast.Expr) *ast.Ident {
	fun = unparen(fun)
	switch x := fun.(type) {
	case *ast.IndexExpr:
		return calleeIdent(x.X)
	case *ast.IndexListExpr:
		return calleeIdent(x.X)
	case *ast.Ident:
		return x
	case *ast.SelectorExpr:
		return x.Sel
	}
	return nil
}

func computeRefs(pkgs []*packages.Package, root string) *RefsOut {
	out := &RefsOut{Refs: []Ref{}, Callers: []Decl{}, Dropped: map[string]int{}}
	rel := func(pos token.Position) (string, bool) {
		if pos.Filename == "" {
			return "", false
		}
		r, err := filepath.Rel(root, pos.Filename)
		if err != nil || strings.HasPrefix(r, "..") {
			return "", false
		}
		return filepath.ToSlash(r), true
	}
	type pk struct{ a, b Decl }
	idx := map[pk]int{}
	called := map[pk]bool{}
	callerSeen := map[Decl]bool{}
	for _, p := range pkgs {
		if p.TypesInfo == nil || p.Fset == nil {
			continue
		}
		info := p.TypesInfo
		for _, f := range p.Syntax {
			fpos := p.Fset.Position(f.Pos())
			frel, ok := rel(fpos)
			if !ok {
				continue
			}
			callerSeen[Decl{File: frel, Line: 0, Name: "<module>"}] = true
			// callee positions of this file
			callee := map[*ast.Ident]bool{}
			ast.Inspect(f, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					if id := calleeIdent(c.Fun); id != nil {
						callee[id] = true
					}
				}
				return true
			})
			var stack []ast.Node
			ast.Inspect(f, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				stack = append(stack, n)
				if fd, ok := n.(*ast.FuncDecl); ok {
					pos := p.Fset.Position(fd.Name.Pos())
					if r, ok := rel(pos); ok {
						callerSeen[Decl{File: r, Line: pos.Line, Name: fd.Name.Name}] = true
					}
				}
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				fn, ok := info.Uses[id].(*types.Func)
				if !ok {
					return true
				}
				if o := fn.Origin(); o != nil {
					fn = o
				}
				tpos := p.Fset.Position(fn.Pos())
				trel, ok := rel(tpos)
				if !ok {
					out.Dropped["target_outside_repo"]++
					return true
				}
				// the enclosing declaration
				caller := Decl{File: frel, Line: 0, Name: "<module>"}
				for i := len(stack) - 1; i >= 0; i-- {
					if fd, ok := stack[i].(*ast.FuncDecl); ok {
						cp := p.Fset.Position(fd.Name.Pos())
						caller = Decl{File: frel, Line: cp.Line, Name: fd.Name.Name}
						break
					}
				}
				target := Decl{File: trel, Line: tpos.Line, Name: fn.Name()}
				k := pk{caller, target}
				if callee[id] {
					called[k] = true
					return true
				}
				// form and context
				form := "ident"
				top := ast.Node(id) // the expression that is the value
				if len(stack) >= 2 {
					if sel, ok := stack[len(stack)-2].(*ast.SelectorExpr); ok && sel.Sel == id {
						top = sel
						form = "qualified"
						if s, ok := info.Selections[sel]; ok {
							switch s.Kind() {
							case types.MethodVal:
								form = "method_value"
								if types.IsInterface(s.Recv()) {
									form = "iface_method_value"
								}
							case types.MethodExpr:
								form = "method_expr"
							}
						}
					}
				}
				ctx := "other"
				ti := len(stack) - 1
				for i := len(stack) - 1; i >= 0; i-- {
					if stack[i] == top {
						ti = i
						break
					}
				}
				for ti > 0 {
					switch pn := stack[ti-1].(type) {
					case *ast.ParenExpr:
						top = pn
						ti--
						continue
					case *ast.IndexExpr: // generic instantiation `f[int]`
						if pn.X == top {
							top = pn
							ti--
							continue
						}
					case *ast.IndexListExpr:
						if pn.X == top {
							top = pn
							ti--
							continue
						}
					}
					break
				}
				if ti > 0 {
					switch par := stack[ti-1].(type) {
					case *ast.CallExpr:
						ctx = "call_argument"
					case *ast.KeyValueExpr:
						if par.Value == top && ti > 1 {
							ctx = "composite_element"
							if cl, ok := stack[ti-2].(*ast.CompositeLit); ok {
								if t := info.TypeOf(cl); t != nil {
									switch t.Underlying().(type) {
									case *types.Struct:
										ctx = "struct_field"
									case *types.Map:
										ctx = "map_value"
									}
								}
							}
						}
					case *ast.CompositeLit:
						ctx = "composite_element"
					case *ast.AssignStmt:
						ctx = "assignment"
					case *ast.ReturnStmt:
						ctx = "return"
					case *ast.ValueSpec:
						ctx = "var_init"
					}
				}
				site := frel + ":" + itoa(p.Fset.Position(id.Pos()).Line)
				if i, ok := idx[k]; ok {
					r := &out.Refs[i]
					if len(r.Sites) < 20 {
						r.Sites = addUnique(r.Sites, site)
					}
					r.Forms = addUnique(r.Forms, form)
					r.Ctxs = addUnique(r.Ctxs, ctx)
					return true
				}
				idx[k] = len(out.Refs)
				out.Refs = append(out.Refs, Ref{Caller: caller, Callee: target, Sites: []string{site}, Forms: []string{form}, Ctxs: []string{ctx}})
				return true
			})
		}
	}
	for i := range out.Refs {
		r := &out.Refs[i]
		r.Called = called[pk{r.Caller, r.Callee}]
		sort.Strings(r.Forms)
		sort.Strings(r.Ctxs)
		sort.Strings(r.Sites)
	}
	sort.Slice(out.Refs, func(i, j int) bool {
		a, b := out.Refs[i], out.Refs[j]
		if a.Caller.File != b.Caller.File {
			return a.Caller.File < b.Caller.File
		}
		if a.Caller.Line != b.Caller.Line {
			return a.Caller.Line < b.Caller.Line
		}
		if a.Callee.File != b.Callee.File {
			return a.Callee.File < b.Callee.File
		}
		return a.Callee.Line < b.Callee.Line
	})
	for d := range callerSeen {
		out.Callers = append(out.Callers, d)
	}
	sort.Slice(out.Callers, func(i, j int) bool {
		a, b := out.Callers[i], out.Callers[j]
		if a.File != b.File {
			return a.File < b.File
		}
		return a.Line < b.Line
	})
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
