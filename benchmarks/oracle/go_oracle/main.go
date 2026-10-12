// Command gooracle emits the compiler-derived call graph of a Go module as JSON.
//
// It type-checks the module with go/packages, builds SSA, and runs Rapid Type
// Analysis (golang.org/x/tools/go/callgraph/rta). Every edge is reported as a
// pair of declaration positions (file, line of the declaration's name
// identifier), restricted to declarations inside the repository.
//
// Usage: gooracle -repo DIR [-tests] [-library-roots] > out.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

type Decl struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Name string `json:"name"`
}

type Edge struct {
	Caller Decl     `json:"caller"`
	Callee Decl     `json:"callee"`
	Sites  []string `json:"sites,omitempty"`
}

type Output struct {
	Repo        string   `json:"repo"`
	Tests       bool     `json:"tests"`
	Mode        string   `json:"mode"`
	Roots       int      `json:"roots"`
	AutoLibrary bool     `json:"auto_library_roots"`
	Files       []string `json:"files"`   // in-repo files that type-checked into the program
	Callers     []Decl   `json:"callers"` // in-repo declarations reachable in the graph (can be judged)
	Edges       []Edge   `json:"edges"`
	// Edges the oracle drops, with a reason, so nothing disappears silently.
	Dropped map[string]int `json:"dropped"`
	Errors  []string       `json:"load_errors"`
	// Call sites whose callee is outside the repository (or a closure): the
	// compiler knows what they call but the callee is not gradable.
	ExtSites     []string `json:"ext_sites"`
	ClosureSites []string `json:"closure_sites"`
}

func main() {
	repo := flag.String("repo", "", "repository root")
	tests := flag.Bool("tests", false, "include _test.go files; test functions become roots")
	libRoots := flag.Bool("library-roots", false, "also use every exported function/method of in-repo packages as a root")
	flag.Parse()
	if *repo == "" {
		fmt.Fprintln(os.Stderr, "-repo required")
		os.Exit(2)
	}
	root, _ := filepath.Abs(*repo)
	root, _ = filepath.EvalSymlinks(root)

	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports |
			packages.NeedDeps | packages.NeedTypes | packages.NeedSyntax | packages.NeedTypesInfo | packages.NeedModule,
		Dir:   root,
		Tests: *tests,
		Env:   append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=mod"),
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	out := &Output{Repo: root, Tests: *tests, Dropped: map[string]int{}, Errors: []string{}, Files: []string{}, Callers: []Decl{}, Edges: []Edge{}}
	for _, p := range pkgs {
		for _, e := range p.Errors {
			out.Errors = append(out.Errors, p.ID+": "+e.Error())
		}
	}

	prog, ssapkgs := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()

	inRepo := func(path string) (string, bool) {
		if path == "" {
			return "", false
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || strings.HasPrefix(rel, "..") {
			return "", false
		}
		return filepath.ToSlash(rel), true
	}

	fileSet := map[string]bool{}
	for _, p := range pkgs {
		for _, f := range p.CompiledGoFiles {
			if rel, ok := inRepo(f); ok {
				fileSet[rel] = true
			}
		}
	}

	// Roots: main + init of main packages, test functions, optionally the library surface.
	var roots []*ssa.Function
	seenRoot := map[*ssa.Function]bool{}
	addRoot := func(f *ssa.Function) {
		if f != nil && !seenRoot[f] {
			seenRoot[f] = true
			roots = append(roots, f)
		}
	}
	collect := func() {
		for _, sp := range ssapkgs {
			if sp == nil {
				continue
			}
			pkg := sp.Pkg
			isMain := pkg.Name() == "main"
			if isMain {
				addRoot(sp.Func("init"))
				addRoot(sp.Func("main"))
			}
			for _, m := range sp.Members {
				fn, ok := m.(*ssa.Function)
				if !ok {
					continue
				}
				if *tests && fn.Pos().IsValid() {
					if rel, ok := inRepo(prog.Fset.Position(fn.Pos()).Filename); ok && strings.HasSuffix(rel, "_test.go") {
						n := fn.Name()
						if strings.HasPrefix(n, "Test") || strings.HasPrefix(n, "Benchmark") || strings.HasPrefix(n, "Fuzz") || strings.HasPrefix(n, "Example") {
							addRoot(fn)
							addRoot(sp.Func("init"))
						}
					}
				}
				if *libRoots && fn.Pos().IsValid() && token.IsExported(fn.Name()) {
					if _, ok := inRepo(prog.Fset.Position(fn.Pos()).Filename); ok {
						addRoot(fn)
						addRoot(sp.Func("init"))
					}
				}
			}
			if *libRoots {
				for _, m := range sp.Members {
					t, ok := m.(*ssa.Type)
					if !ok || !token.IsExported(t.Name()) {
						continue
					}
					for _, s := range []*types.MethodSet{prog.MethodSets.MethodSet(t.Type()), prog.MethodSets.MethodSet(types.NewPointer(t.Type()))} {
						for i := 0; i < s.Len(); i++ {
							sel := s.At(i)
							if !token.IsExported(sel.Obj().Name()) {
								continue
							}
							f := prog.MethodValue(sel)
							if f != nil && f.Pos().IsValid() {
								if _, ok := inRepo(prog.Fset.Position(f.Pos()).Filename); ok {
									addRoot(f)
								}
							}
						}
					}
				}
			}
		}
	}
	collect()
	if len(roots) == 0 && !*tests && !*libRoots {
		// a library without a main package: fall back to its exported surface
		*libRoots = true
		out.AutoLibrary = true
		collect()
	}
	out.Roots = len(roots)
	switch {
	case *tests && *libRoots:
		out.Mode = "tests+library"
	case *tests:
		out.Mode = "tests"
	case *libRoots:
		out.Mode = "library"
	default:
		out.Mode = "main"
	}
	if len(roots) == 0 {
		fmt.Fprintln(os.Stderr, "no roots; load errors:", len(out.Errors), strings.Join(out.Errors[:min(len(out.Errors), 5)], "\n"))
		os.Exit(3)
	}
	res := rta.Analyze(roots, true)
	cg := res.CallGraph

	// declOf maps an SSA function to the named source declaration it belongs to.
	// Closures resolve to their outermost enclosing declaration; generic
	// instances to their origin. ok=false means there is no in-repo declaration.
	declOf := func(fn *ssa.Function) (Decl, string) {
		for fn.Parent() != nil {
			fn = fn.Parent()
		}
		if o := fn.Origin(); o != nil {
			fn = o
		}
		if fn.Synthetic != "" {
			return Decl{}, "synthetic:" + fn.Synthetic
		}
		syn, ok := fn.Syntax().(*ast.FuncDecl)
		if !ok {
			return Decl{}, "nosyntax"
		}
		pos := prog.Fset.Position(syn.Name.Pos())
		rel, ok := inRepo(pos.Filename)
		if !ok {
			return Decl{}, "external"
		}
		return Decl{File: rel, Line: pos.Line, Name: fn.Name()}, ""
	}
	isClosure := func(fn *ssa.Function) bool { return fn.Parent() != nil }

	type key struct{ a, b Decl }
	seen := map[key]int{}
	extSites := map[string]bool{}
	closureSites := map[string]bool{}
	callerSeen := map[Decl]bool{}
	emit := func(from, to *ssa.Function, site ssa.CallInstruction) {
		cf, why := declOf(from)
		if why != "" {
			if why != "external" {
				out.Dropped["caller_"+why]++
			}
			return
		}
		callerSeen[cf] = true
		siteOf := func() string {
			if site != nil && site.Pos().IsValid() {
				p := prog.Fset.Position(site.Pos())
				if rel, ok := inRepo(p.Filename); ok {
					return fmt.Sprintf("%s:%d", rel, p.Line)
				}
			}
			return ""
		}
		if isClosure(to) {
			out.Dropped["callee_closure"]++
			if s := siteOf(); s != "" {
				closureSites[s] = true
			}
			return
		}
		ct, why := declOf(to)
		if why != "" {
			if why != "external" {
				out.Dropped["callee_"+why]++
			} else if s := siteOf(); s != "" {
				extSites[s] = true
			}
			return
		}
		callerSeen[ct] = true
		k := key{cf, ct}
		siteStr := ""
		if site != nil && site.Pos().IsValid() {
			p := prog.Fset.Position(site.Pos())
			if rel, ok := inRepo(p.Filename); ok {
				siteStr = fmt.Sprintf("%s:%d", rel, p.Line)
			}
		}
		if idx, ok := seen[k]; ok {
			e := &out.Edges[idx]
			if siteStr != "" && len(e.Sites) < 20 {
				e.Sites = append(e.Sites, siteStr)
			}
			return
		}
		seen[k] = len(out.Edges)
		e := Edge{Caller: cf, Callee: ct}
		if siteStr != "" {
			e.Sites = []string{siteStr}
		}
		out.Edges = append(out.Edges, e)
	}

	// Collapse synthetic wrappers (bound-method closures, interface thunks,
	// pointer wrappers): A -> wrapper -> B becomes A -> B.
	memo := map[*ssa.Function][]*ssa.Function{}
	var resolveTargets func(fn *ssa.Function, onPath map[*ssa.Function]bool) []*ssa.Function
	resolveTargets = func(fn *ssa.Function, onPath map[*ssa.Function]bool) []*ssa.Function {
		if fn.Synthetic == "" || strings.HasPrefix(fn.Synthetic, "package initializer") {
			return []*ssa.Function{fn}
		}
		if r, ok := memo[fn]; ok {
			return r
		}
		if onPath[fn] {
			return nil
		}
		onPath[fn] = true
		seenT := map[*ssa.Function]bool{}
		var res []*ssa.Function
		if n := cg.Nodes[fn]; n != nil {
			for _, oe := range n.Out {
				for _, t := range resolveTargets(oe.Callee.Func, onPath) {
					if !seenT[t] {
						seenT[t] = true
						res = append(res, t)
					}
				}
			}
		}
		delete(onPath, fn)
		memo[fn] = res
		return res
	}

	_ = callgraph.GraphVisitEdges(cg, func(e *callgraph.Edge) error {
		from := e.Caller.Func
		to := e.Callee.Func
		if from == nil || to == nil {
			return nil
		}
		if from.Synthetic != "" && !strings.HasPrefix(from.Synthetic, "package initializer") {
			return nil // wrapper edges are collapsed through their callers below
		}
		if strings.HasPrefix(from.Synthetic, "package initializer") {
			out.Dropped["caller_package_initializer"]++
			return nil
		}
		for _, t := range resolveTargets(to, map[*ssa.Function]bool{}) {
			emit(from, t, e.Site)
		}
		return nil
	})
	for fn := range cg.Nodes {
		if fn == nil {
			continue
		}
		if d, why := declOf(fn); why == "" {
			callerSeen[d] = true
		}
	}

	for f := range fileSet {
		out.Files = append(out.Files, f)
	}
	sort.Strings(out.Files)
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
	sort.Slice(out.Edges, func(i, j int) bool {
		a, b := out.Edges[i], out.Edges[j]
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
	for s := range extSites {
		out.ExtSites = append(out.ExtSites, s)
	}
	for s := range closureSites {
		out.ClosureSites = append(out.ClosureSites, s)
	}
	sort.Strings(out.ExtSites)
	sort.Strings(out.ClosureSites)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	_ = enc.Encode(out)
}
