// Command deadoracle emits what the Go toolchain's own dead-code tools say about a module, as JSON.
//
// Two verdicts per declaration (function, method, type):
//
//   - unreferenced: staticcheck's U1000 (honnef.co/go/tools/unused, pinned in ../go.mod) run through go/analysis over the same
//     packages.Load the call-graph oracle uses. U1000 treats exported functions, types and methods as used, so the verdict
//     is only meaningful for unexported declarations; the output says which declarations those are.
//   - unreachable: not in the RTA reachable set (golang.org/x/tools/go/callgraph/rta, the machinery behind
//     golang.org/x/tools/cmd/deadcode) from the same roots ../main.go uses. Only functions and methods get this verdict.
//
// Nothing from the module is executed: packages.Load runs `go list` only, with the hardened environment of ../main.go
// (CGO_ENABLED=0, -mod=readonly, GOWORK=off, GOPACKAGESDRIVER=off). No `go build`, `go test`, `go generate` and no cgo.
//
// The root selection below is a copy of ../main.go's; keep the two in step.
//
// Usage: deadoracle -repo DIR [-tests] > out.json
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
	"time"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/checker"
	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
	"honnef.co/go/tools/unused"
)

// Decl is one function, method or type declaration of the module, keyed by file and the line of its name identifier.
type Decl struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Name string `json:"name"`
	// Kind is function, method or type; Subkind refines a type (struct, interface, alias, defined).
	Kind    string `json:"kind"`
	Subkind string `json:"subkind,omitempty"`
	Recv    string `json:"recv,omitempty"`
	Pkg     string `json:"pkg"`
	PkgName string `json:"pkg_name"`
	// Exported is the Go identifier rule; API is Exported in a package other modules can import (not main, not under
	// internal/, not a _test.go file).
	Exported  bool `json:"exported"`
	API       bool `json:"api"`
	Generated bool `json:"generated,omitempty"`
	TestFile  bool `json:"test_file,omitempty"`
	// Root: the analysis treats it as live by construction (main, init, Test*/Benchmark*/Fuzz*/Example* with an Output comment, and in a library
	// without a main package every exported function and the exported methods of exported types).
	Root string `json:"root,omitempty"`
	// Unref is U1000's verdict: "dead", "dead_quiet" (unused, but reported only through its owner, e.g. a method of an
	// unused type), "live", or "" when U1000 did not look at it. Reach is RTA's: "dead", "live", or "" for a type.
	Unref string `json:"unref,omitempty"`
	Reach string `json:"reach,omitempty"`
}

type FileInfo struct {
	File string `json:"file"`
	Pkg  string `json:"pkg"`
	// Importers: in-repo packages (other than this one and its test variants) that import this file's package.
	Importers      int  `json:"importers"`
	Main           bool `json:"main"`
	Test           bool `json:"test"`
	Generated      bool `json:"generated,omitempty"`
	Funcs          int  `json:"funcs"`
	FuncsReachable int  `json:"funcs_reachable"`
}

type Output struct {
	Repo        string         `json:"repo"`
	Tests       bool           `json:"tests"`
	Mode        string         `json:"mode"`
	Library     bool           `json:"library"` // no main package in the module: libraries are judged on unexported/internal declarations
	AutoLibrary bool           `json:"auto_library_roots"`
	Roots       int            `json:"roots"`
	Files       []string       `json:"files"`
	Decls       []Decl         `json:"decls"`
	FileInfo    []FileInfo     `json:"file_info"`
	Errors      []string       `json:"load_errors"`
	Stats       map[string]any `json:"stats"`
}

func main() {
	repo := flag.String("repo", "", "repository root")
	tests := flag.Bool("tests", false, "include _test.go files; test functions become roots")
	flag.Parse()
	if *repo == "" {
		fmt.Fprintln(os.Stderr, "-repo required")
		os.Exit(2)
	}
	root, _ := filepath.Abs(*repo)
	root, _ = filepath.EvalSymlinks(root)

	t0 := time.Now()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports |
			packages.NeedDeps | packages.NeedTypes | packages.NeedSyntax | packages.NeedTypesInfo | packages.NeedTypesSizes | packages.NeedModule,
		Dir:   root,
		Tests: *tests,
		// The corpus is untrusted: packages.Load only runs `go list`, which never runs `go generate`; CGO_ENABLED=0 stops
		// the cgo tool (and any C compiler) from running on cgo packages; -mod=readonly never rewrites go.mod/go.sum;
		// GOWORK=off and GOPACKAGESDRIVER=off keep a go.work file or a driver binary on PATH from changing what is loaded;
		// GOFLAGS is replaced so an inherited -toolexec or -overlay cannot apply. safety.go_hardened_env sets the same
		// variables for this process; both layers are asserted in test_safety.py.
		Env: append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=readonly", "GOWORK=off", "GOPACKAGESDRIVER=off"),
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	out := &Output{Repo: root, Tests: *tests, Errors: []string{}, Files: []string{}, Decls: []Decl{}, FileInfo: []FileInfo{}, Stats: map[string]any{}}
	for _, p := range pkgs {
		for _, e := range p.Errors {
			out.Errors = append(out.Errors, p.ID+": "+e.Error())
		}
	}
	out.Stats["load_seconds"] = round1(time.Since(t0))

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

	// ---- declarations -------------------------------------------------------------------------------------
	type pos struct {
		file string
		line int
	}
	declIdx := map[pos]int{}
	// Example functions without an `// Output:` comment are compiled but never run by go test (and so by deadcode -test)
	exampleNoOutput := map[pos]bool{}
	hasOutput := func(f *ast.File, fd *ast.FuncDecl) bool {
		if fd.Body == nil {
			return false
		}
		var last *ast.CommentGroup
		for _, cg := range f.Comments {
			if cg.Pos() >= fd.Body.Lbrace && cg.End() <= fd.Body.Rbrace {
				last = cg
			}
		}
		if last == nil {
			return false
		}
		t := strings.ToLower(strings.TrimSpace(last.Text()))
		return strings.HasPrefix(t, "output:") || strings.HasPrefix(t, "unordered output:")
	}
	fileSeen := map[string]bool{}
	importable := func(pkgPath, pkgName, file string) bool {
		if pkgName == "main" || strings.HasSuffix(file, "_test.go") || strings.HasSuffix(pkgName, "_test") {
			return false
		}
		for _, seg := range strings.Split(pkgPath, "/") {
			if seg == "internal" {
				return false
			}
		}
		return true
	}
	hasMain := false
	fset := (*token.FileSet)(nil)
	for _, p := range pkgs {
		if p.Name == "main" && !strings.HasSuffix(p.ID, ".test]") && !strings.HasSuffix(p.ID, ".test") {
			hasMain = true
		}
		if fset == nil {
			fset = p.Fset
		}
		for i, f := range p.Syntax {
			if i >= len(p.CompiledGoFiles) {
				break
			}
			rel, ok := inRepo(p.CompiledGoFiles[i])
			if !ok {
				continue
			}
			fileSeen[rel] = true
			pkgPath := strings.TrimSuffix(strings.Fields(p.PkgPath)[0], ".test")
			gen := ast.IsGenerated(f)
			isTest := strings.HasSuffix(rel, "_test.go")
			add := func(id *ast.Ident, kind, sub, recv string) {
				if id == nil || id.Name == "_" {
					return
				}
				ps := p.Fset.Position(id.Pos())
				k := pos{rel, ps.Line}
				if _, dup := declIdx[k]; dup {
					return
				}
				exp := token.IsExported(id.Name)
				declIdx[k] = len(out.Decls)
				out.Decls = append(out.Decls, Decl{File: rel, Line: ps.Line, Name: id.Name, Kind: kind, Subkind: sub, Recv: recv, Pkg: pkgPath, PkgName: p.Name,
					Exported: exp, API: exp && importable(pkgPath, p.Name, rel), Generated: gen, TestFile: isTest})
			}
			for _, d := range f.Decls {
				switch d := d.(type) {
				case *ast.FuncDecl:
					if isTest && d.Recv == nil && strings.HasPrefix(d.Name.Name, "Example") && !hasOutput(f, d) {
						exampleNoOutput[pos{rel, p.Fset.Position(d.Name.Pos()).Line}] = true
					}
					if d.Recv != nil && len(d.Recv.List) > 0 {
						add(d.Name, "method", "", recvName(d.Recv.List[0].Type))
					} else {
						add(d.Name, "function", "", "")
					}
				case *ast.GenDecl:
					if d.Tok != token.TYPE {
						continue
					}
					for _, s := range d.Specs {
						ts := s.(*ast.TypeSpec)
						sub := "defined"
						switch {
						case ts.Assign.IsValid():
							sub = "alias"
						default:
							switch ts.Type.(type) {
							case *ast.StructType:
								sub = "struct"
							case *ast.InterfaceType:
								sub = "interface"
							}
						}
						add(ts.Name, "type", sub, "")
					}
				}
			}
		}
	}
	out.Library = !hasMain
	for f := range fileSeen {
		out.Files = append(out.Files, f)
	}
	sort.Strings(out.Files)
	out.Stats["decls"] = len(out.Decls)

	// ---- unreferenced: U1000 ---------------------------------------------------------------------------------
	t1 := time.Now()
	graph, err := checker.Analyze([]*analysis.Analyzer{unused.Analyzer.Analyzer}, pkgs, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "analyze:", err)
		os.Exit(1)
	}
	used := map[pos]bool{}
	unusedAt := map[pos]bool{}
	quietAt := map[pos]bool{}
	nUsed, nUnused, nQuiet := 0, 0, 0
	for _, act := range graph.Roots {
		if act.Err != nil {
			out.Errors = append(out.Errors, "U1000 "+act.Package.ID+": "+act.Err.Error())
			continue
		}
		res, ok := act.Result.(unused.Result)
		if !ok {
			continue
		}
		set := func(objs []unused.Object, m map[pos]bool) {
			for _, o := range objs {
				if rel, ok := inRepo(o.Position.Filename); ok {
					m[pos{rel, o.Position.Line}] = true
				}
			}
		}
		set(res.Used, used)
		set(res.Unused, unusedAt)
		set(res.Quiet, quietAt)
		nUsed += len(res.Used)
		nUnused += len(res.Unused)
		nQuiet += len(res.Quiet)
	}
	// staticcheck's own rule (lintcmd/lint.go): an object used in any package variant (with and without tests) is used.
	for i := range out.Decls {
		d := &out.Decls[i]
		k := pos{d.File, d.Line}
		switch {
		case used[k]:
			d.Unref = "live"
		case unusedAt[k]:
			d.Unref = "dead"
		case quietAt[k]:
			d.Unref = "dead_quiet"
		}
	}
	out.Stats["u1000_seconds"] = round1(time.Since(t1))
	out.Stats["u1000_objects"] = map[string]int{"used": nUsed, "unused": nUnused, "quiet": nQuiet}

	// ---- unreachable: RTA ------------------------------------------------------------------------------------
	t2 := time.Now()
	prog, ssapkgs := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	var roots []*ssa.Function
	seenRoot := map[*ssa.Function]bool{}
	var addRoot func(f *ssa.Function)
	// A generic function (or method of a generic type) cannot be an RTA root: its body has type-parameter-typed values and rta
	// panics converting them to interfaces. Its static callees and function values are the roots instead (a generic body is
	// walked, an instantiation of it is not RTA's business until a non-generic root instantiates it).
	walkedGeneric := map[*ssa.Function]bool{}
	var walkGeneric func(f *ssa.Function)
	walkGeneric = func(f *ssa.Function) {
		if o := f.Origin(); o != nil {
			f = o
		}
		if walkedGeneric[f] {
			return
		}
		walkedGeneric[f] = true
		for _, b := range f.Blocks {
			for _, in := range b.Instrs {
				for _, op := range in.Operands(nil) {
					if g, ok := (*op).(*ssa.Function); ok && g != nil {
						addRoot(g)
					}
				}
			}
		}
		for _, anon := range f.AnonFuncs {
			addRoot(anon)
		}
	}
	isGeneric := func(f *ssa.Function) bool {
		for g := f; g != nil; g = g.Parent() {
			sig := g.Signature
			if sig.TypeParams().Len() > 0 || sig.RecvTypeParams().Len() > 0 {
				return true
			}
			for _, ta := range g.TypeArgs() {
				if hasTypeParam(ta, 0) {
					return true
				}
			}
		}
		return false
	}
	addRoot = func(f *ssa.Function) {
		if f != nil && !seenRoot[f] {
			seenRoot[f] = true
			if isGeneric(f) {
				walkGeneric(f)
				return
			}
			roots = append(roots, f)
		}
	}
	libRoots := false
	// apiRoots: besides main/init/tests, the exported declarations of importable packages are roots (they are the "public_api" bucket,
	// excluded from the universe because code outside the repository may use them, so what only they reach is not dead either).
	apiRoots := false
	rootOK := func(sp *ssa.Package, filename string) bool {
		rel, ok := inRepo(filename)
		if !ok {
			return false
		}
		if libRoots {
			return true
		}
		return apiRoots && importable(strings.TrimSuffix(strings.Fields(sp.Pkg.Path())[0], ".test"), sp.Pkg.Name(), rel)
	}
	collect := func() {
		for _, sp := range ssapkgs {
			if sp == nil {
				continue
			}
			pkg := sp.Pkg
			if pkg.Name() == "main" {
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
							if strings.HasPrefix(n, "Example") {
								if syn, ok := fn.Syntax().(*ast.FuncDecl); ok && exampleNoOutput[pos{rel, prog.Fset.Position(syn.Name.Pos()).Line}] {
									continue
								}
							}
							addRoot(fn)
							addRoot(sp.Func("init"))
						}
					}
				}
				if (libRoots || apiRoots) && fn.Pos().IsValid() && token.IsExported(fn.Name()) {
					if rootOK(sp, prog.Fset.Position(fn.Pos()).Filename) {
						addRoot(fn)
						addRoot(sp.Func("init"))
					}
				}
			}
			if libRoots || apiRoots {
				for _, m := range sp.Members {
					t, ok := m.(*ssa.Type)
					if !ok || !token.IsExported(t.Name()) {
						continue
					}
					if nt, ok := t.Type().(*types.Named); ok && nt.TypeParams().Len() > 0 {
						// a generic type has no method set of its own to enumerate; root the exported methods it declares
						// (the generic bodies; their callees are reached through the instantiations RTA follows)
						for i := 0; i < nt.NumMethods(); i++ {
							mo := nt.Method(i)
							if !token.IsExported(mo.Name()) {
								continue
							}
							f := prog.FuncValue(mo)
							if f != nil && f.Pos().IsValid() {
								if rootOK(sp, prog.Fset.Position(f.Pos()).Filename) {
									addRoot(f)
								}
							}
						}
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
								if rootOK(sp, prog.Fset.Position(f.Pos()).Filename) {
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
	var strictRoots []*ssa.Function
	if len(roots) == 0 && !*tests {
		// a library without a main package: fall back to its exported surface (the same rule as the call-graph oracle)
		libRoots = true
		out.AutoLibrary = true
		collect()
	}
	if !libRoots {
		strictRoots = append([]*ssa.Function(nil), roots...)
		apiRoots = true
		collect()
		out.Stats["api_roots"] = len(roots) - len(strictRoots)
	}
	out.Roots = len(roots)
	switch {
	case *tests:
		out.Mode = "tests"
	case libRoots:
		out.Mode = "library"
	default:
		out.Mode = "main"
	}
	if len(roots) == 0 {
		fmt.Fprintln(os.Stderr, "no roots; load errors:", len(out.Errors))
		os.Exit(3)
	}
	res := rta.Analyze(roots, true)

	// declOf: the named source declaration an SSA function belongs to (closures to their outermost declaration, generic
	// instances to their origin); ok=false when it has none in the repository.
	declOf := func(fn *ssa.Function) (pos, bool) {
		for fn.Parent() != nil {
			fn = fn.Parent()
		}
		if o := fn.Origin(); o != nil {
			fn = o
		}
		if fn.Synthetic != "" {
			return pos{}, false
		}
		syn, ok := fn.Syntax().(*ast.FuncDecl)
		if !ok {
			return pos{}, false
		}
		ps := prog.Fset.Position(syn.Name.Pos())
		rel, ok := inRepo(ps.Filename)
		if !ok {
			return pos{}, false
		}
		return pos{rel, ps.Line}, true
	}
	reach := map[pos]bool{}
	for fn := range res.Reachable {
		if fn == nil {
			continue
		}
		if k, ok := declOf(fn); ok {
			reach[k] = true
		}
	}
	for fn := range walkedGeneric {
		// a generic body entered through a root is reachable even though RTA never sees it as a node
		if k, ok := declOf(fn); ok {
			reach[k] = true
		}
	}
	if len(strictRoots) > 0 {
		// the same analysis without the API roots: what cmd/deadcode reports (kept as a validation figure)
		strict := map[pos]bool{}
		for fn := range rta.Analyze(strictRoots, true).Reachable {
			if fn == nil {
				continue
			}
			if k, ok := declOf(fn); ok {
				strict[k] = true
			}
		}
		n := 0
		for i := range out.Decls {
			d := &out.Decls[i]
			if d.Kind != "type" && !d.Generated && !strict[pos{d.File, d.Line}] {
				n++
			}
		}
		out.Stats["strict_roots_unreachable_functions"] = n
	}
	rootAt := map[pos]bool{}
	for _, r := range roots {
		if k, ok := declOf(r); ok {
			rootAt[k] = true
		}
	}
	for i := range out.Decls {
		d := &out.Decls[i]
		k := pos{d.File, d.Line}
		if d.Kind != "type" {
			if reach[k] {
				d.Reach = "live"
			} else {
				d.Reach = "dead"
			}
		}
		switch {
		case d.Kind == "function" && d.Name == "main" && d.PkgName == "main":
			d.Root = "main_init"
		case d.Kind == "function" && d.Name == "init":
			d.Root = "main_init"
		case d.TestFile && d.Kind == "function" && !exampleNoOutput[k] && (strings.HasPrefix(d.Name, "Test") || strings.HasPrefix(d.Name, "Benchmark") ||
			strings.HasPrefix(d.Name, "Fuzz") || strings.HasPrefix(d.Name, "Example")):
			d.Root = "test_root"
		case out.AutoLibrary && rootAt[k]:
			d.Root = "library_root"
		}
	}
	out.Stats["rta_seconds"] = round1(time.Since(t2))
	out.Stats["rta_reachable_functions"] = len(res.Reachable)

	// ---- file level -----------------------------------------------------------------------------------------
	importers := map[string]map[string]bool{} // package path -> importing package paths (in the repository)
	basePath := func(id string) string { return strings.TrimSuffix(strings.Fields(id)[0], ".test") }
	for _, p := range pkgs {
		if _, ok := firstInRepo(p, inRepo); !ok {
			continue
		}
		for _, imp := range p.Imports {
			if _, ok := firstInRepo(imp, inRepo); !ok {
				continue
			}
			ip, pp := strings.TrimSuffix(imp.PkgPath, "_test"), basePath(p.PkgPath)
			if ip == pp || strings.TrimSuffix(pp, "_test") == ip {
				continue
			}
			if importers[ip] == nil {
				importers[ip] = map[string]bool{}
			}
			importers[ip][pp] = true
		}
	}
	type fagg struct{ funcs, reach int }
	agg := map[string]*fagg{}
	for i := range out.Decls {
		d := &out.Decls[i]
		if d.Kind == "type" {
			continue
		}
		a := agg[d.File]
		if a == nil {
			a = &fagg{}
			agg[d.File] = a
		}
		a.funcs++
		if d.Reach == "live" {
			a.reach++
		}
	}
	filePkg := map[string]string{}
	fileName := map[string]string{}
	fileGen := map[string]bool{}
	for _, p := range pkgs {
		for i, f := range p.Syntax {
			if i >= len(p.CompiledGoFiles) {
				break
			}
			if rel, ok := inRepo(p.CompiledGoFiles[i]); ok {
				if _, seen := filePkg[rel]; !seen {
					filePkg[rel] = strings.TrimSuffix(basePath(p.PkgPath), "_test")
					fileName[rel] = p.Name
					fileGen[rel] = ast.IsGenerated(f)
				}
			}
		}
	}
	for _, f := range out.Files {
		a := agg[f]
		if a == nil {
			a = &fagg{}
		}
		out.FileInfo = append(out.FileInfo, FileInfo{File: f, Pkg: filePkg[f], Importers: len(importers[filePkg[f]]), Main: fileName[f] == "main",
			Test: strings.HasSuffix(f, "_test.go"), Generated: fileGen[f], Funcs: a.funcs, FuncsReachable: a.reach})
	}
	sort.Slice(out.Decls, func(i, j int) bool {
		a, b := out.Decls[i], out.Decls[j]
		if a.File != b.File {
			return a.File < b.File
		}
		return a.Line < b.Line
	})
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	_ = enc.Encode(out)
}

func round1(d time.Duration) float64 { return float64(int(d.Seconds()*10+0.5)) / 10 }

func firstInRepo(p *packages.Package, inRepo func(string) (string, bool)) (string, bool) {
	for _, f := range p.CompiledGoFiles {
		if rel, ok := inRepo(f); ok {
			return rel, true
		}
	}
	return "", false
}

func recvName(e ast.Expr) string {
	for {
		switch t := e.(type) {
		case *ast.StarExpr:
			e = t.X
		case *ast.ParenExpr:
			e = t.X
		case *ast.IndexExpr:
			e = t.X
		case *ast.IndexListExpr:
			e = t.X
		case *ast.Ident:
			return t.Name
		default:
			return ""
		}
	}
}

// hasTypeParam reports whether t mentions a type parameter.
func hasTypeParam(t types.Type, depth int) bool {
	if t == nil || depth > 8 {
		return false
	}
	switch t := t.(type) {
	case *types.TypeParam:
		return true
	case *types.Named:
		for i := 0; i < t.TypeArgs().Len(); i++ {
			if hasTypeParam(t.TypeArgs().At(i), depth+1) {
				return true
			}
		}
	case *types.Pointer:
		return hasTypeParam(t.Elem(), depth+1)
	case *types.Slice:
		return hasTypeParam(t.Elem(), depth+1)
	case *types.Array:
		return hasTypeParam(t.Elem(), depth+1)
	case *types.Chan:
		return hasTypeParam(t.Elem(), depth+1)
	case *types.Map:
		return hasTypeParam(t.Key(), depth+1) || hasTypeParam(t.Elem(), depth+1)
	case *types.Signature:
		for i := 0; i < t.Params().Len(); i++ {
			if hasTypeParam(t.Params().At(i).Type(), depth+1) {
				return true
			}
		}
		for i := 0; i < t.Results().Len(); i++ {
			if hasTypeParam(t.Results().At(i).Type(), depth+1) {
				return true
			}
		}
	case *types.Struct:
		for i := 0; i < t.NumFields(); i++ {
			if hasTypeParam(t.Field(i).Type(), depth+1) {
				return true
			}
		}
	}
	return false
}
