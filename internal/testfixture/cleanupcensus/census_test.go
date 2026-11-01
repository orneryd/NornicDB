// Author: L4 fixture-checked-cleanup lane (30 Oct / 1 Nov 2026).
// Verification only, committed to a clean pushed census scratch; not part of the PR.
// The best-effort mode follows the governing family audit. It reports broad
// candidates without requiring an exhaustive/zero-candidate result, while
// retaining the original strict default and every planted control.
//
// Census v4 (Rule 73 method-gap note 2). Membership comes from the contract,
// evaluated on the compiled call graph, never from the shape of a call:
//
//   - Teardown roots: every function value a shared helper (any function that
//     is not a test entry point, outside internal/testfixture) registers with
//     a Cleanup(func()) owner; every function value such a helper returns,
//     directly or in a returned value; and every method of a type the helper
//     returns that a cleanup root reaches.
//   - Reachability: SSA for the whole module with tests, and a VTA call graph
//     (closures, method values, receiver methods, function fields, interface
//     dispatch). Closures created inside reached test code are reached too,
//     because an external callee such as sync.Once.Do may run them. The walk
//     stops at internal/testfixture (delegation to the owner) and at product
//     code (the release itself).
//   - A helper is a member when its teardown, outside the owner,
//     reports an outcome (any testing failure call or any testify call), or
//     provides an early close: a returned function value that calls teardown
//     work also registered at cleanup, or a method of a returned fixture
//     type that cleanup reaches and a test also calls directly.
//   - Every root gets one listed verdict: member, drop-only (NOR-331),
//     no fixture work, owner-only, or unresolved (must be none).
//
// Completeness evidence that does not depend on this definition:
//   - reconciliation: an untyped go/ast enumerator (build constraints applied,
//     no type information) lists every one-argument Cleanup call in a
//     non-entry function; the census must account for exactly that set;
//   - recall of every member a reviewer found (NORNIC_CLEANUP_CENSUS_RECALL
//     selects the reviewed head: 4981 or ede2);
//   - a generated matrix of release kinds and indirections that must all be
//     found, with owner-only forms that must not be.
package cleanupcensus_test

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

const modulePath = "github.com/orneryd/nornicdb"
const fixturePath = modulePath + "/internal/testfixture"

// knownReleaseTypeErrors are type errors present on the release base itself
// (NOR-338). go/types still types the rest of that package, but SSA skips an
// ill-typed package, so its cleanups are listed separately.
var knownReleaseTypeErrors = map[string]string{
	"transaction_memory_reload_http_test.go": "release #1685 changed cypher.PluginFunctionLookup without updating this pkg/server test (NOR-338)",
}

func knownBaseTypeError(message string) string {
	for file := range knownReleaseTypeErrors {
		if strings.Contains(message, file) {
			return file
		}
	}
	return ""
}

func root(t *testing.T) string {
	abs, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

func load(t *testing.T, overlay map[string][]byte) []*packages.Package {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedDeps |
			packages.NeedTypes | packages.NeedTypesSizes | packages.NeedSyntax | packages.NeedTypesInfo,
		Dir:        root(t),
		Tests:      true,
		BuildFlags: []string{"-tags=noui,nolocalllm"},
		Env:        append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"),
		Overlay:    overlay,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatal(err)
	}
	var errs []string
	for _, p := range pkgs {
		for _, e := range p.Errors {
			if file := knownBaseTypeError(e.Error()); file != "" {
				t.Logf("known release type error: %s: %s", file, firstLine(e.Error()))
				continue
			}
			errs = append(errs, p.ID+": "+e.Error())
		}
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		if len(errs) > 20 {
			errs = errs[:20]
		}
		t.Fatalf("module does not type-check:\n%s", strings.Join(errs, "\n"))
	}
	return pkgs
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func hasMethod(typ types.Type, name string, check func(*types.Signature) bool) bool {
	if typ == nil {
		return false
	}
	object, _, _ := types.LookupFieldOrMethod(typ, true, nil, name)
	if object == nil {
		if tp, ok := types.Unalias(typ).(*types.TypeParam); ok {
			object, _, _ = types.LookupFieldOrMethod(tp.Constraint(), true, nil, name)
		}
	}
	function, ok := object.(*types.Func)
	return ok && check(function.Type().(*types.Signature))
}

// isOwner is true for a type with Cleanup(func()): testing.T, B, F or Boundary.
func isOwner(typ types.Type) bool {
	return hasMethod(typ, "Cleanup", func(s *types.Signature) bool {
		if s.Params().Len() != 1 || s.Results().Len() != 0 {
			return false
		}
		f, ok := s.Params().At(0).Type().Underlying().(*types.Signature)
		return ok && f.Params().Len() == 0 && f.Results().Len() == 0
	})
}

func isEntryName(name string) bool {
	if name == "TestMain" {
		return true
	}
	for _, prefix := range []string{"Test", "Benchmark", "Fuzz", "Example"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func relative(base, file string) string {
	rel, err := filepath.Rel(base, file)
	if err != nil {
		return file
	}
	return filepath.ToSlash(rel)
}

// ---- The untyped enumerator (independent of the typed census) ----

// partition accounts for every raw ".Cleanup(" text occurrence in every Go
// file under the tree, whether or not the build compiles it. The categories
// must add up to the raw total, so nothing is excluded without being counted.
type partition struct {
	raw        int
	categories map[string]int
}

// partitionCleanups walks every .go file and assigns each raw occurrence to
// exactly one category, using text, go/build file matching and go/parser only.
func partitionCleanups(t *testing.T, base string) partition {
	t.Helper()
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = "linux", "amd64", false
	ctx.BuildTags = []string{"noui", "nolocalllm"}
	p := partition{categories: map[string]int{}}
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		raw := strings.Count(string(data), ".Cleanup(")
		if raw == 0 {
			return nil
		}
		p.raw += raw
		rel := relative(base, path)
		category := ""
		for dir := filepath.Dir(path); dir != base && strings.HasPrefix(dir, base); dir = filepath.Dir(dir) {
			name := filepath.Base(dir)
			if name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				category = "ignored directory (vendor, testdata, dot or underscore)"
				break
			}
			if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
				category = "nested module (not part of ./...)"
				break
			}
		}
		if category == "" {
			if match, matchErr := ctx.MatchFile(filepath.Dir(path), entry.Name()); matchErr != nil || !match {
				category = "excluded by build constraints (linux/amd64, noui, nolocalllm)"
			}
		}
		if category != "" {
			p.categories[category] += raw
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, data, 0)
		if parseErr != nil {
			p.categories["unparsable file"] += raw
			return nil
		}
		owner := relative(base, filepath.Dir(path)) == "internal/testfixture" && file.Name.Name == "testfixture"
		isTest := strings.HasSuffix(entry.Name(), "_test.go")
		calls := 0
		count := func(n ast.Node, inEntry bool) {
			ast.Inspect(n, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Cleanup" {
					return true
				}
				calls++
				switch {
				case owner:
					p.categories["owner package internal/testfixture"]++
				case len(call.Args) != 1:
					p.categories["Cleanup call with other arity"]++
				case inEntry:
					p.categories["inline cleanup in a test entry point (NOR-331)"]++
				default:
					p.categories["one-argument Cleanup in a shared helper (census sites)"]++
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			if function, ok := decl.(*ast.FuncDecl); ok {
				if function.Body != nil {
					count(function.Body, isTest && function.Recv == nil && isEntryName(function.Name.Name))
				}
				continue
			}
			count(decl, false)
		}
		if raw > calls {
			p.categories["text in a comment, string or non-call"] += raw - calls
		}
		_ = rel
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// enumerateCleanups lists "file:line:col" of every one-argument X.Cleanup(...)
// call that is not inside a test entry point, in every Go file the build
// would compile for linux/amd64 with tags noui,nolocalllm. It uses only
// go/parser and go/build: no type information and no code shared with the
// census. The owner package's own files (package testfixture) are excluded.
func enumerateCleanups(t *testing.T, base string) map[string]bool {
	t.Helper()
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = "linux", "amd64", false
	ctx.BuildTags = []string{"noui", "nolocalllm"}
	found := map[string]bool{}
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if path != base && (name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			if path != base {
				if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
					return filepath.SkipDir // a nested module is not part of ./...
				}
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		if match, matchErr := ctx.MatchFile(filepath.Dir(path), name); matchErr != nil || !match {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil
		}
		if relative(base, filepath.Dir(path)) == "internal/testfixture" && file.Name.Name == "testfixture" {
			return nil
		}
		isTest := strings.HasSuffix(name, "_test.go")
		var visit func(n ast.Node, entry bool)
		visit = func(n ast.Node, entry bool) {
			ast.Inspect(n, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || entry {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "Cleanup" && len(call.Args) == 1 {
					position := fset.Position(call.Lparen)
					found[fmt.Sprintf("%s:%d:%d", relative(base, path), position.Line, position.Column)] = true
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok {
				visit(decl, false)
				continue
			}
			entry := isTest && function.Recv == nil && isEntryName(function.Name.Name)
			if function.Body != nil {
				visit(function.Body, entry)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// registersCleanup is true when body contains a one-argument X.Cleanup(...)
// call, found by syntax only.
func registersCleanup(body ast.Node) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Cleanup" && len(call.Args) == 1 {
				found = true
			}
		}
		return !found
	})
	return found
}

// enumerateHelperLiterals lists "file:line:col" of every function literal
// written directly in the body of a non-entry declared function that
// registers a one-argument Cleanup (a literal inside another literal belongs
// to that literal, not to the declaration). go/parser and go/build only.
func enumerateHelperLiterals(t *testing.T, base string) (map[string]bool, int) {
	t.Helper()
	product := 0
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = "linux", "amd64", false
	ctx.BuildTags = []string{"noui", "nolocalllm"}
	found := map[string]bool{}
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if path != base && (name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			if path != base {
				if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		if match, matchErr := ctx.MatchFile(filepath.Dir(path), name); matchErr != nil || !match {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil
		}
		if relative(base, filepath.Dir(path)) == "internal/testfixture" && file.Name.Name == "testfixture" {
			return nil
		}
		isTest := strings.HasSuffix(name, "_test.go")
		testCode := isTest || strings.Contains(relative(base, filepath.Dir(path)), "test")
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil || (isTest && function.Recv == nil && isEntryName(function.Name.Name)) {
				continue
			}
			ast.Inspect(function.Body, func(n ast.Node) bool {
				if literal, ok := n.(*ast.FuncLit); ok {
					if !testCode {
						product++ // a product function is not a shared test helper
						return false
					}
					position := fset.Position(literal.Pos())
					found[fmt.Sprintf("%s:%d:%d", relative(base, path), position.Line, position.Column)] = true
					return false
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found, product
}

// enumerateNamedReturns lists "file:line:col" of every return of an
// identifier or selector, from a non-entry declared function of test code,
// whose name is a function or method declared in the same directory. Syntax
// only: names are collected per directory from every file the build compiles.
func enumerateNamedReturns(t *testing.T, base string) map[string]bool {
	t.Helper()
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = "linux", "amd64", false
	ctx.BuildTags = []string{"noui", "nolocalllm"}
	type parsed struct {
		path string
		fset *token.FileSet
		file *ast.File
	}
	byDir := map[string][]parsed{}
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if path != base && (name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			if path != base {
				if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		if match, matchErr := ctx.MatchFile(filepath.Dir(path), name); matchErr != nil || !match {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr == nil {
			byDir[filepath.Dir(path)] = append(byDir[filepath.Dir(path)], parsed{path, fset, file})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for dir, files := range byDir {
		names := map[string]bool{}
		for _, f := range files {
			for _, decl := range f.file.Decls {
				if function, ok := decl.(*ast.FuncDecl); ok {
					names[function.Name.Name] = true
				}
			}
		}
		rel := relative(base, dir)
		for _, f := range files {
			if rel == "internal/testfixture" && f.file.Name.Name == "testfixture" {
				continue
			}
			isTest := strings.HasSuffix(f.path, "_test.go")
			if !isTest && !strings.Contains(rel, "test") {
				continue
			}
			for _, decl := range f.file.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok || function.Body == nil || (isTest && function.Recv == nil && isEntryName(function.Name.Name)) {
					continue
				}
				locals := map[string]bool{}
				for _, list := range []*ast.FieldList{function.Recv, function.Type.Params, function.Type.Results} {
					if list == nil {
						continue
					}
					for _, field := range list.List {
						for _, id := range field.Names {
							locals[id.Name] = true
						}
					}
				}
				ast.Inspect(function.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.AssignStmt:
						if n.Tok == token.DEFINE {
							for _, lhs := range n.Lhs {
								if id, ok := lhs.(*ast.Ident); ok {
									locals[id.Name] = true
								}
							}
						}
					case *ast.ValueSpec:
						for _, id := range n.Names {
							locals[id.Name] = true
						}
					case *ast.FuncLit:
						for _, field := range n.Type.Params.List {
							for _, id := range field.Names {
								locals[id.Name] = true
							}
						}
					}
					return true
				})
				ast.Inspect(function.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.FuncLit:
						return false
					case *ast.ReturnStmt:
						for _, result := range n.Results {
							name := ""
							switch r := result.(type) {
							case *ast.Ident:
								if !locals[r.Name] {
									name = r.Name
								}
							case *ast.SelectorExpr:
								name = r.Sel.Name
							}
							if name != "" && names[name] {
								position := f.fset.Position(result.Pos())
								found[fmt.Sprintf("%s:%d:%d", relative(base, f.path), position.Line, position.Column)] = true
							}
						}
					}
					return true
				})
			}
		}
	}
	return found
}

// enumerateReturnedLiterals lists "file:line:col" of every function literal a
// non-entry declared function that registers a cleanup returns directly: in a
// return statement of its
// own body (not of a nested literal), as the value itself, through a local
// variable bound to the literal, or as an element of a returned composite
// literal. go/parser and go/build only; no type information.
func enumerateReturnedLiterals(t *testing.T, base string) (map[string]bool, map[string]bool) {
	t.Helper()
	all := map[string]bool{}
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH, ctx.CgoEnabled = "linux", "amd64", false
	ctx.BuildTags = []string{"noui", "nolocalllm"}
	found := map[string]bool{}
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if path != base && (name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			if path != base {
				if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		if match, matchErr := ctx.MatchFile(filepath.Dir(path), name); matchErr != nil || !match {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil
		}
		if relative(base, filepath.Dir(path)) == "internal/testfixture" && file.Name.Name == "testfixture" {
			return nil
		}
		isTest := strings.HasSuffix(name, "_test.go")
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil || (isTest && function.Recv == nil && isEntryName(function.Name.Name)) {
				continue
			}
			// Only a function that registers teardown can hand a test an
			// early close; other returned closures are not teardown work.
			scoped := registersCleanup(function.Body)
			// bound maps a local variable to the values written into it: a
			// function literal or composite assigned to it, and function
			// literals assigned to its fields or elements.
			bound := map[string][]ast.Expr{}
			root := func(e ast.Expr) string {
				for {
					switch x := e.(type) {
					case *ast.Ident:
						return x.Name
					case *ast.SelectorExpr:
						e = x.X
					case *ast.IndexExpr:
						e = x.X
					case *ast.StarExpr:
						e = x.X
					case *ast.ParenExpr:
						e = x.X
					default:
						return ""
					}
				}
			}
			valueLike := func(e ast.Expr) bool {
				switch x := e.(type) {
				case *ast.FuncLit, *ast.CompositeLit:
					return true
				case *ast.UnaryExpr:
					_, ok := x.X.(*ast.CompositeLit)
					return ok
				}
				return false
			}
			ast.Inspect(function.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					if len(n.Lhs) == len(n.Rhs) {
						for i, lhs := range n.Lhs {
							if !valueLike(n.Rhs[i]) {
								continue
							}
							if _, plain := lhs.(*ast.Ident); !plain {
								if _, isLiteral := n.Rhs[i].(*ast.FuncLit); !isLiteral {
									continue
								}
							}
							if name := root(lhs); name != "" {
								bound[name] = append(bound[name], n.Rhs[i])
							}
						}
					}
				case *ast.ValueSpec:
					for i, id := range n.Names {
						if i < len(n.Values) && valueLike(n.Values[i]) {
							bound[id.Name] = append(bound[id.Name], n.Values[i])
						}
					}
				}
				return true
			})
			add := func(literal *ast.FuncLit) {
				position := fset.Position(literal.Pos())
				at := fmt.Sprintf("%s:%d:%d", relative(base, path), position.Line, position.Column)
				all[at] = true
				if scoped {
					found[at] = true
				}
			}
			visiting := map[string]bool{}
			var value func(e ast.Expr)
			value = func(e ast.Expr) {
				switch e := e.(type) {
				case *ast.FuncLit:
					add(e)
				case *ast.Ident:
					if visiting[e.Name] {
						return
					}
					visiting[e.Name] = true
					for _, written := range bound[e.Name] {
						value(written)
					}
					visiting[e.Name] = false
				case *ast.ParenExpr:
					value(e.X)
				case *ast.UnaryExpr:
					value(e.X)
				case *ast.CompositeLit:
					for _, element := range e.Elts {
						if kv, ok := element.(*ast.KeyValueExpr); ok {
							value(kv.Value)
						} else {
							value(element)
						}
					}
				}
			}
			var visit func(n ast.Node) bool
			visit = func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.FuncLit:
					return false // returns inside a nested literal belong to it
				case *ast.ReturnStmt:
					for _, result := range n.Results {
						value(result)
					}
				}
				return true
			}
			ast.Inspect(function.Body, visit)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return all, found
}

// ---- The typed census on SSA ----

type rootInfo struct {
	at      string // file:line:col of the Cleanup call or the return
	helper  string // package.Function of the shared helper
	kind    string // "cleanup" or "returned"
	fns     []*ssa.Function
	verdict string
	detail  string
}

type helperInfo struct {
	key          string
	top          *ssa.Function
	cleanupRoots []*rootInfo
	reports      []string
	earlyClose   []string
}

type census struct {
	roots               []*rootInfo
	helpers             map[string]*helperInfo
	members             []string
	dropOnly            []string
	noFixture           []string
	ownerOnly           []string
	unresolved          []string
	nonOwner            []string // one-argument Cleanup calls whose receiver is not a cleanup owner
	illTyped            []string // cleanups in a package SSA cannot build (NOR-338)
	returnedRoots       []string // every function value a shared helper returns, with its verdict
	returnedSites       map[string]bool
	returnedAll         map[string]bool    // every returned function literal, unscoped
	literalSites        map[string]string  // literal position -> verdict (registered, escapes, local)
	literalReview       []string           // local literals in helpers that return a function or a named value
	returnedTeardown    []string           // every returned function value of a shared test helper, with its teardown verdict
	namedReturns        map[string]string  // return position of a named function or method value -> verdict
	returnResolved      map[string]string  // return position of every identifier or selector result -> its types.Object kind
	synthesizedBodies   map[string]bool    // range-over-func loop bodies in test helpers
	teardownUsed        map[string]bool    // by functionID: functions some consumer registers with Cleanup or defers
	returnedReporters   []string           // returned functions that report but no consumer uses as teardown
	illTypedFiles       map[string]bool    // files of ill-typed packages, analysed from syntax only
	dynamicExt          []string           // dynamic calls in teardown of an external function value
	dynamicUnres        []string           // dynamic calls in teardown with no callee: must be none
	returnedUnres       []string           // dynamic calls with no callee in returned functions no consumer uses as teardown: code-read list
	returnedFns         []returnedFn       // every function value a shared helper returns
	verdictMemo         map[string]*judged // by functionID
	cleanupReached      map[string]bool    // by functionID: every function a cleanup root's teardown walk reaches
	returnedSplit       []string           // every judged returned function: use and verdict
	returnedUsed        int
	returnedGaps        []string // listed as not teardown, yet reached from cleanup
	fixtureMethods      []string // methods of a returned fixture type that teardown reaches
	cleanupSites        map[string]bool
	fixtureCloses       []string
	fixtureDrops        []string
	removedNames        []string
	ownCalls            int
	returnedUnresPaths  []string // the walk's path to each returnedUnres entry
	rawReleases         []string // releases the owner adopts that also escape or are called outside it
	ownedDropped        []string // v10: releases of an owned resource outside the owner whose error is dropped
	ownedChecked        []string // v10: releases of an owned resource outside the owner whose error is used
	ownedAmbiguous      []string // v11: member releases of a variable that also holds an un-owned value
	ownedPositions      map[string]bool
	adoptionEffectFacts []string            // conservative canonical-owner effects for source reconciliation
	closeFacts          []string            // compiler facts for per-site source review; not membership decisions
	ownerReleaseRanges  map[string][][2]int // physical source spans of registered test release bodies
	ownerReleaseDrops   []string            // Close outcomes discarded inside those bodies or their test helpers
	ownerAdoptionSites  int
	ledger              map[string]*ledgerEntry
	ledgerStale         []string
	ledgerProblems      []string
	enumHits            []string // syntax enumerator: releases of names the file adopts
	enumUnexplained     []string // enumerator hits neither in the census nor the ledger
	ownerAdoptions      int
	variantClasses      int // functions with more than one built variant
	functionsBuilt      int
}

type analysis struct {
	base string
	fset *token.FileSet
	cg   *callgraph.Graph
	// variants holds every function with a body by functionID.
	variants map[string][]*ssa.Function
	owner    func(*ssa.Function) bool
	lines    map[string][]string
	overlay  map[string][]byte
}

func (a *analysis) position(pos token.Pos) string {
	p := a.fset.Position(pos)
	return fmt.Sprintf("%s:%d:%d", relative(a.base, p.Filename), p.Line, p.Column)
}

// functionID names a function across package variants. A package with tests
// loads twice, plain and as its test variant, and each variant has its own
// *ssa.Function with the same name; a consumer's use of either is a use of the
// function.
func functionID(fn *ssa.Function) string { return fn.String() }

// class returns every built variant of fn. No verdict may depend on which
// variant is examined: neither is a superset, because an implementer in another
// package binds to the plain variant's interface and a type in a package's own
// tests binds to the test variant's.
func (a *analysis) class(fn *ssa.Function) []*ssa.Function {
	if variants := a.variants[functionID(fn)]; len(variants) > 0 {
		return variants
	}
	return []*ssa.Function{fn}
}

// escapingAll is escaping over every variant of top, one entry per function.
func (a *analysis) escapingAll(top *ssa.Function) []*ssa.Function {
	var out []*ssa.Function
	seen := map[string]bool{}
	for _, variant := range a.class(top) {
		for _, returned := range escaping(variant) {
			if !seen[functionID(returned)] {
				seen[functionID(returned)] = true
				out = append(out, returned)
			}
		}
	}
	return out
}

func functionPackage(fn *ssa.Function) string {
	if origin := fn.Origin(); origin != nil {
		fn = origin
	}
	if fn.Pkg != nil && fn.Pkg.Pkg != nil {
		return fn.Pkg.Pkg.Path()
	}
	if object := fn.Object(); object != nil && object.Pkg() != nil {
		return object.Pkg().Path()
	}
	return ""
}

// topLevel returns the declared function that lexically encloses fn.
func topLevel(fn *ssa.Function) *ssa.Function {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	if origin := fn.Origin(); origin != nil {
		fn = origin
	}
	return fn
}

// isTestCode is true for code written for tests: _test.go files, packages
// whose import path names test support, and synthetic wrappers.
func (a *analysis) isTestCode(fn *ssa.Function) bool {
	if fn.Synthetic != "" {
		return true
	}
	if strings.HasSuffix(a.fset.Position(fn.Pos()).Filename, "_test.go") {
		return true
	}
	path := functionPackage(fn)
	return strings.HasPrefix(path, modulePath) && strings.Contains(strings.TrimPrefix(path, modulePath), "test")
}

// isTestFile is the syntax side's test-code rule applied to fn's declaration:
// a _test.go file, or a file in a directory whose path names test support.
// Unlike isTestCode it never treats a synthetic function as test code.
func (a *analysis) isTestFile(fn *ssa.Function) bool {
	if origin := fn.Origin(); origin != nil {
		fn = origin
	}
	file := a.fset.Position(fn.Pos()).Filename
	return strings.HasSuffix(file, "_test.go") || strings.Contains(relative(a.base, filepath.Dir(file)), "test")
}

// helperKey returns the shared helper enclosing fn, or "" for test entry
// points, the owner package and code outside the module.
func (a *analysis) helperKey(fn *ssa.Function) (string, *ssa.Function) {
	top := topLevel(fn)
	path := functionPackage(top)
	if !strings.HasPrefix(path, modulePath) || path == fixturePath {
		return "", nil
	}
	if object, ok := top.Object().(*types.Func); ok && object != nil {
		signature := object.Type().(*types.Signature)
		if strings.HasSuffix(a.fset.Position(top.Pos()).Filename, "_test.go") && signature.Recv() == nil && isEntryName(object.Name()) {
			return "", nil
		}
	}
	// String is "pkg.Name" for a function and "(*pkg.T).Name" for a method.
	return top.String(), top
}

// illTypedClosure is every module package that is ill-typed or imports one,
// directly or transitively: SSA cannot build them.
func illTypedClosure(pkgs []*packages.Package) map[*packages.Package]bool {
	bad := map[*packages.Package]bool{}
	state := map[*packages.Package]int{} // 1 visiting, 2 done
	var visit func(p *packages.Package) bool
	visit = func(p *packages.Package) bool {
		if state[p] != 0 {
			return bad[p]
		}
		state[p] = 1
		result := p.IllTyped
		for _, imported := range p.Imports {
			if visit(imported) {
				result = true
			}
		}
		state[p] = 2
		bad[p] = result
		return result
	}
	for _, p := range pkgs {
		visit(p)
	}
	return bad
}

// registersCleanupSSA is true when fn, or a closure inside it, calls a
// one-argument Cleanup method: the SSA view of registersCleanup.
func registersCleanupSSA(fn *ssa.Function) bool {
	for _, block := range fn.Blocks {
		for _, instr := range block.Instrs {
			if call, ok := instr.(ssa.CallInstruction); ok {
				if _, _, ok := cleanupArg(call.Common()); ok {
					return true
				}
			}
		}
	}
	for _, anon := range fn.AnonFuncs {
		if registersCleanupSSA(anon) {
			return true
		}
	}
	return false
}

// ssaInitial picks one variant per package: the test variant when there is
// one (it contains every file of the package), otherwise the package itself.
// Test main packages and ill-typed packages (with their importers) are
// skipped. Every other variant is built with its body.
func ssaInitial(pkgs []*packages.Package, bad map[*packages.Package]bool) []*packages.Package {
	var initial []*packages.Package
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, modulePath) {
			continue
		}
		if p.Name == "main" && strings.HasSuffix(p.PkgPath, ".test") {
			continue
		}
		if bad[p] {
			continue
		}
		initial = append(initial, p)
	}
	return initial
}

// cleanupArg recognises a one-argument call of a method named Cleanup and
// returns its argument and receiver type.
func cleanupArg(common *ssa.CallCommon) (ssa.Value, types.Type, bool) {
	if common.IsInvoke() {
		if common.Method.Name() == "Cleanup" && len(common.Args) == 1 {
			return common.Args[0], common.Value.Type(), true
		}
		return nil, nil, false
	}
	callee := common.StaticCallee()
	if callee == nil || callee.Name() != "Cleanup" || callee.Signature.Recv() == nil || len(common.Args) != 2 {
		return nil, nil, false
	}
	return common.Args[1], callee.Signature.Recv().Type(), true
}

// resolve maps a function value to the functions it can be. external names an
// external callee whose result is the value (a release with no body here).
func (a *analysis) resolve(v ssa.Value, depth int) ([]*ssa.Function, string, bool) {
	if depth > 8 {
		return nil, "", false
	}
	switch v := v.(type) {
	case *ssa.Function:
		return []*ssa.Function{v}, "", true
	case *ssa.MakeClosure:
		if fn, ok := v.Fn.(*ssa.Function); ok {
			return []*ssa.Function{fn}, "", true
		}
	case *ssa.ChangeType:
		return a.resolve(v.X, depth+1)
	case *ssa.MakeInterface:
		return a.resolve(v.X, depth+1)
	case *ssa.Phi:
		var all []*ssa.Function
		external := ""
		for _, edge := range v.Edges {
			fns, ext, ok := a.resolve(edge, depth+1)
			if !ok {
				return nil, "", false
			}
			all = append(all, fns...)
			if ext != "" {
				external = ext
			}
		}
		return all, external, true
	case *ssa.UnOp:
		if alloc, ok := v.X.(*ssa.Alloc); ok && v.Op == token.MUL {
			return a.resolveStores(alloc, depth+1)
		}
	case *ssa.Call:
		return a.resolveResult(v.Common(), 0, depth+1)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok {
			return a.resolveResult(call.Common(), v.Index, depth+1)
		}
	}
	return nil, "", false
}

func (a *analysis) resolveStores(alloc *ssa.Alloc, depth int) ([]*ssa.Function, string, bool) {
	var all []*ssa.Function
	external, found := "", false
	for _, ref := range *alloc.Referrers() {
		if store, ok := ref.(*ssa.Store); ok && store.Addr == alloc {
			fns, ext, ok := a.resolve(store.Val, depth)
			if !ok {
				return nil, "", false
			}
			found = true
			all = append(all, fns...)
			if ext != "" {
				external = ext
			}
		}
	}
	return all, external, found
}

func (a *analysis) resolveResult(common *ssa.CallCommon, index, depth int) ([]*ssa.Function, string, bool) {
	callee := common.StaticCallee()
	if callee == nil {
		return nil, "", false
	}
	if callee.Blocks == nil || !strings.HasPrefix(functionPackage(callee), modulePath) {
		return nil, callee.String(), true
	}
	var all []*ssa.Function
	external, found := "", false
	for _, block := range callee.Blocks {
		for _, instr := range block.Instrs {
			ret, ok := instr.(*ssa.Return)
			if !ok || index >= len(ret.Results) {
				continue
			}
			fns, ext, ok := a.resolve(ret.Results[index], depth)
			if !ok {
				return nil, "", false
			}
			found = true
			all = append(all, fns...)
			if ext != "" {
				external = ext
			}
		}
	}
	return all, external, found
}

var reporterNames = map[string]bool{"Error": true, "Errorf": true, "Fatal": true, "Fatalf": true, "Fail": true, "FailNow": true}

// reporter names a call that reports a test outcome: a testing failure method,
// the same method through an interface that is a test owner or testing.TB,
// or any testify function.
func (a *analysis) reporter(common *ssa.CallCommon) string {
	if common.IsInvoke() {
		name := common.Method.Name()
		if !reporterNames[name] {
			return ""
		}
		typ := common.Value.Type()
		if hasMethod(typ, "FailNow", func(*types.Signature) bool { return true }) || isOwner(typ) {
			return "reports through " + types.TypeString(typ, nil) + "." + name
		}
		return ""
	}
	callee := common.StaticCallee()
	if callee == nil {
		return ""
	}
	path := functionPackage(callee)
	if path == "testing" && reporterNames[callee.Name()] {
		return "reports through testing." + callee.Name()
	}
	if strings.HasPrefix(path, "github.com/stretchr/testify/") {
		return "reports through " + callee.String()
	}
	return ""
}

// returnedFn is a function value a shared helper returns.
type returnedFn struct {
	at, key    string
	helper, fn *ssa.Function
}

// judged is the verdict on what a returned function does, made once.
type judged struct {
	at, key, verdict string
	listed           bool // on a code-read list as not teardown
}

type walk struct {
	reached    map[*ssa.Function]bool
	parent     map[*ssa.Function]*ssa.Function // first function that queued each one
	paths      map[string]string               // unresolved entry -> call path from the walk's start
	work       bool
	ownerCalls int
	reports    []string
	unresolved []string // dynamic calls with no callee and no external origin
	external   []string // dynamic calls of a function value made by external code
}

// externalCallback is true for a function value whose code lies outside the
// module: a result of an external call (context.WithCancel's cancel), or a
// value of a function type declared outside the module.
func externalCallback(v ssa.Value) bool {
	switch v := v.(type) {
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok {
			if callee := call.Common().StaticCallee(); callee != nil && !strings.HasPrefix(functionPackage(callee), modulePath) {
				return true
			}
		}
	case *ssa.Call:
		if callee := v.Common().StaticCallee(); callee != nil && !strings.HasPrefix(functionPackage(callee), modulePath) {
			return true
		}
	}
	if named, ok := types.Unalias(v.Type()).(*types.Named); ok && named.Obj().Pkg() != nil {
		return !strings.HasPrefix(named.Obj().Pkg().Path(), modulePath)
	}
	return false
}

// adoptedReleases resolves a value handed to the owner to the test-code
// functions that release it: a function value (through CloseFunc, Redacted, a
// local, a field of a value built in the same function, or a dependency list),
// or the Close method of a fixture type declared in test code.
func (a *analysis) adoptedReleases(prog *ssa.Program, v ssa.Value, depth int) []*ssa.Function {
	if v == nil || depth > 12 {
		return nil
	}
	stored := func(addr ssa.Value) []*ssa.Function {
		var out []*ssa.Function
		refs := addr.Referrers()
		if refs == nil {
			return nil
		}
		for _, ref := range *refs {
			switch ref := ref.(type) {
			case *ssa.Store:
				if ref.Addr == addr {
					out = append(out, a.adoptedReleases(prog, ref.Val, depth+1)...)
				}
			case *ssa.IndexAddr:
				if inner := ref.Referrers(); inner != nil {
					for _, use := range *inner {
						if store, ok := use.(*ssa.Store); ok && store.Addr == ref {
							out = append(out, a.adoptedReleases(prog, store.Val, depth+1)...)
						}
					}
				}
			}
		}
		return out
	}
	switch v := v.(type) {
	case *ssa.Function:
		return []*ssa.Function{v}
	case *ssa.MakeClosure:
		if fn, ok := v.Fn.(*ssa.Function); ok {
			return []*ssa.Function{fn}
		}
	case *ssa.ChangeType:
		return a.adoptedReleases(prog, v.X, depth+1)
	case *ssa.Convert:
		return a.adoptedReleases(prog, v.X, depth+1)
	case *ssa.MakeInterface:
		return a.adoptedReleases(prog, v.X, depth+1)
	case *ssa.Slice:
		return a.adoptedReleases(prog, v.X, depth+1)
	case *ssa.Alloc:
		if out := stored(v); len(out) > 0 {
			return out
		}
	case *ssa.UnOp:
		if v.Op == token.MUL {
			var out []*ssa.Function
			if field, ok := v.X.(*ssa.FieldAddr); ok {
				// A boxed variable is loaded afresh at each use, so every load
				// of the box is the same base.
				bases := []ssa.Value{field.X}
				if load, ok := field.X.(*ssa.UnOp); ok && load.Op == token.MUL {
					if refs := load.X.Referrers(); refs != nil {
						for _, ref := range *refs {
							if other, ok := ref.(*ssa.UnOp); ok && other.Op == token.MUL && other != load {
								bases = append(bases, other)
							}
						}
					}
				}
				for _, base := range bases {
					if refs := base.Referrers(); refs != nil {
						for _, ref := range *refs {
							if other, ok := ref.(*ssa.FieldAddr); ok && other.Field == field.Field {
								out = append(out, stored(other)...)
							}
						}
					}
				}
			} else {
				out = stored(v.X)
			}
			if len(out) > 0 {
				return out
			}
		}
	case *ssa.Call:
		callee := v.Common().StaticCallee()
		if callee != nil && functionPackage(callee) == fixturePath {
			var out []*ssa.Function
			for _, arg := range v.Common().Args {
				out = append(out, a.adoptedReleases(prog, arg, depth+1)...)
			}
			return out
		}
		if fns, _, ok := a.resolve(v, depth); ok && len(fns) > 0 {
			return fns
		}
	}
	// Otherwise the value is the fixture itself: the owner runs its Close.
	if selection := prog.MethodSets.MethodSet(v.Type()).Lookup(nil, "Close"); selection != nil && !types.IsInterface(v.Type()) {
		if method := prog.MethodValue(selection); method != nil && method.Blocks != nil && a.isTestCode(method) && method.Synthetic == "" {
			return []*ssa.Function{method}
		}
	}
	return nil
}

// rawReleases finds every release the owner adopts that a shared helper also
// hands out, or that test code calls outside the owner. The owner's own early
// close is produced in the fixture package, so it is neither.
func (a *analysis) rawReleases(c *census, prog *ssa.Program, functions []*ssa.Function, siteCallees map[ssa.CallInstruction][]*ssa.Function) {
	type adoption struct {
		fn      *ssa.Function
		adopter *ssa.Function
		at      string
	}
	var adoptions []adoption
	for _, fn := range functions {
		if functionPackage(fn) == fixturePath {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				callee := call.Common().StaticCallee()
				if callee == nil || functionPackage(callee) != fixturePath {
					continue
				}
				name := callee.Name()
				if index := strings.IndexByte(name, '['); index >= 0 {
					name = name[:index]
				}
				if name != "Own" && name != "OwnWithClose" {
					continue
				}
				c.ownerAdoptions++
				args := call.Common().Args
				for i := 1; i < len(args); i++ {
					for _, release := range a.adoptedReleases(prog, args[i], 0) {
						if release.Blocks != nil && a.isTestCode(release) && functionPackage(release) != fixturePath {
							adoptions = append(adoptions, adoption{fn: release, adopter: fn, at: a.position(call.Common().Pos())})
						}
					}
				}
			}
		}
	}
	// A release one adopted release runs is part of the owner's work.
	owned := map[string]bool{}
	for _, adopted := range adoptions {
		for fn := range a.reach([]*ssa.Function{adopted.fn}, siteCallees, false).reached {
			owned[functionID(fn)] = true
		}
	}
	for _, adopted := range adoptions {
		key, _ := a.helperKey(adopted.adopter)
		for _, returned := range a.escapingAll(topLevel(adopted.adopter)) {
			if functionID(returned) == functionID(adopted.fn) {
				c.rawReleases = append(c.rawReleases, fmt.Sprintf("%s adopted at %s by %s and returned beside the owner", adopted.fn.String(), adopted.at, key))
			}
		}
		targets := a.class(adopted.fn)
		for _, target := range targets {
			node := a.cg.Nodes[target]
			if node == nil {
				continue
			}
			for _, edge := range node.In {
				caller := edge.Caller.Func
				if caller == nil || caller.Synthetic != "" || edge.Site == nil || !edge.Site.Pos().IsValid() {
					continue
				}
				if functionPackage(caller) == fixturePath || !a.isTestCode(caller) || owned[functionID(caller)] {
					continue
				}
				c.rawReleases = append(c.rawReleases, fmt.Sprintf("%s adopted at %s by %s and called by %s at %s", adopted.fn.String(), adopted.at, key, caller.String(), a.position(edge.Site.Pos())))
			}
		}
	}
	c.rawReleases = unique(c.rawReleases)
}

// path names the chain of functions the walk followed to fn, each with the size
// of its package's scope, which tells a package's variants apart.
func (w walk) path(fn *ssa.Function) string {
	var chain []string
	seen := map[*ssa.Function]bool{}
	for fn != nil && !seen[fn] {
		seen[fn] = true
		scope := -1
		if fn.Pkg != nil && fn.Pkg.Pkg != nil {
			scope = fn.Pkg.Pkg.Scope().Len()
		} else if object := fn.Object(); object != nil && object.Pkg() != nil {
			scope = object.Pkg().Scope().Len()
		}
		chain = append([]string{fmt.Sprintf("%s@%d", fn.String(), scope)}, chain...)
		fn = w.parent[fn]
	}
	return strings.Join(chain, " -> ")
}

// reach walks the teardown work of fns: their bodies, closures created in
// them, and every test-code callee on the call graph. Calls into the owner
// are counted, not followed. Teardown-policy walks stop at product code;
// wrapper-identity walks follow it to the adopted type's Close as well.
// Author of the explicit reach policy: L4 lane (Codex), 1 Nov 2026.
func (a *analysis) reach(fns []*ssa.Function, siteCallees map[ssa.CallInstruction][]*ssa.Function, includeProduct bool) walk {
	w := walk{reached: map[*ssa.Function]bool{}, parent: map[*ssa.Function]*ssa.Function{}, paths: map[string]string{}}
	queue := append([]*ssa.Function(nil), fns...)
	resolved := map[string]bool{} // a call site some variant resolves is not a dynamic gap
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		if w.reached[fn] || functionPackage(fn) == fixturePath {
			continue
		}
		w.reached[fn] = true
		if fn.Blocks == nil {
			continue
		}
		for _, next := range append(append([]*ssa.Function(nil), a.class(fn)...), fn.AnonFuncs...) {
			if !w.reached[next] && w.parent[next] == nil {
				w.parent[next] = fn
			}
			queue = append(queue, next)
		}
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				common := call.Common()
				if site := a.reporter(common); site != "" {
					w.reports = append(w.reports, a.position(common.Pos())+" "+site)
				}
				if builtin, ok := common.Value.(*ssa.Builtin); ok {
					if builtin.Name() == "close" {
						w.work = true
					}
					continue
				}
				callees := siteCallees[call]
				if len(callees) == 0 {
					if callee := common.StaticCallee(); callee != nil {
						callees = []*ssa.Function{callee}
					}
				}
				entry := a.position(common.Pos()) + " in " + fn.String()
				if len(callees) > 0 {
					resolved[entry] = true
				}
				if len(callees) == 0 {
					w.paths[entry] = w.path(fn)
					if externalCallback(common.Value) {
						w.external = append(w.external, entry)
					} else {
						w.unresolved = append(w.unresolved, entry)
					}
					continue
				}
				for _, callee := range callees {
					if functionPackage(callee) == fixturePath {
						w.ownerCalls++
						continue
					}
					w.work = true
					if includeProduct || callee.Blocks != nil && a.isTestCode(callee) {
						if !w.reached[callee] && w.parent[callee] == nil {
							w.parent[callee] = fn
						}
						queue = append(queue, callee)
					}
				}
			}
		}
	}
	dynamic := func(entries []string) []string {
		var out []string
		for _, entry := range unique(entries) {
			if !resolved[entry] {
				out = append(out, entry)
			}
		}
		return out
	}
	w.reports, w.unresolved, w.external = unique(w.reports), dynamic(w.unresolved), dynamic(w.external)
	if len(w.unresolved)+len(w.external) > 0 {
		w.work = true
	}
	return w
}

// returnedVerdict classifies a function value a shared test helper returns.
// A returned closure that reports an outcome hands the caller teardown
// reporting outside the owner: the helper is a member. One that only releases
// is drop-only (NOR-331); one with no work is not teardown.
func (a *analysis) returnedVerdict(c *census, key string, helper, returned *ssa.Function, siteCallees map[ssa.CallInstruction][]*ssa.Function) string {
	if j := c.verdictMemo[functionID(returned)]; j != nil {
		return j.verdict
	}
	reporters, unres := len(c.returnedReporters), len(c.returnedUnres)
	verdict := a.judgeReturned(c, key, helper, returned, siteCallees)
	c.verdictMemo[functionID(returned)] = &judged{at: a.position(returned.Pos()), key: key, verdict: verdict, listed: len(c.returnedReporters) > reporters || len(c.returnedUnres) > unres}
	return verdict
}

func (a *analysis) judgeReturned(c *census, key string, helper, returned *ssa.Function, siteCallees map[ssa.CallInstruction][]*ssa.Function) string {
	w := a.reach([]*ssa.Function{returned}, siteCallees, false)
	c.dynamicExt = append(c.dynamicExt, w.external...)
	// An unresolved call is a census gap only in teardown work. In a returned
	// function no consumer uses as teardown it is listed with its root for the
	// code read, which decides on what the function does.
	if c.teardownUsed[functionID(returned)] {
		c.dynamicUnres = append(c.dynamicUnres, w.unresolved...)
	} else {
		for _, entry := range w.unresolved {
			c.returnedUnres = append(c.returnedUnres, a.position(returned.Pos())+" "+key+": "+entry)
			c.returnedUnresPaths = append(c.returnedUnresPaths, a.position(returned.Pos())+" "+key+": "+entry+" VIA "+w.paths[entry])
		}
	}
	switch {
	case len(w.reports) > 0 && !c.teardownUsed[functionID(returned)]:
		c.returnedReporters = append(c.returnedReporters, a.position(returned.Pos())+" "+key+" ("+w.reports[0]+")")
		return "not teardown: reports, but no consumer registers it with Cleanup or defers it"
	case len(w.reports) > 0:
		h := c.helpers[key]
		if h == nil {
			h = &helperInfo{key: key, top: helper}
			c.helpers[key] = h
		}
		h.reports = append(h.reports, "returned function at "+a.position(returned.Pos())+": "+w.reports[0])
		return "member: the returned function is used as teardown and reports (" + w.reports[0] + ")"
	case w.work:
		return "drop-only: the returned function releases without reporting"
	case w.ownerCalls > 0:
		return "owner-only: the returned function calls only the owner"
	default:
		return "no fixture work"
	}
}

// splitReturned judges every function value a shared helper returns on what
// it does, then splits all judged returned functions exactly: used as teardown
// (a consumer registers it with Cleanup or defers it, or a cleanup root's walk
// reaches it) or not. A function on a not-teardown code-read list that cleanup
// reaches is a gap.
func (a *analysis) splitReturned(c *census, siteCallees map[ssa.CallInstruction][]*ssa.Function) {
	for _, r := range c.returnedFns {
		if c.verdictMemo[functionID(r.fn)] != nil {
			continue
		}
		switch {
		case functionPackage(r.fn) == fixturePath:
			c.verdictMemo[functionID(r.fn)] = &judged{at: r.at, key: r.key, verdict: "owner closer"}
		case r.fn.Blocks == nil || !a.isTestCode(r.fn):
			c.verdictMemo[functionID(r.fn)] = &judged{at: r.at, key: r.key, verdict: "product function: its body is not test code"}
		default:
			a.returnedVerdict(c, r.key, r.helper, r.fn, siteCallees)
		}
	}
	for id, j := range c.verdictMemo {
		use := "not used as teardown"
		switch {
		case c.teardownUsed[id]:
			use = "used: consumer registers or defers it"
		case c.cleanupReached[id]:
			use = "used: reached from cleanup"
			if j.listed {
				c.returnedGaps = append(c.returnedGaps, j.at+" "+j.key+" "+j.verdict)
			}
		}
		if strings.HasPrefix(use, "used") {
			c.returnedUsed++
		}
		listed := ""
		if j.listed {
			listed = " [code-read list]"
		}
		c.returnedSplit = append(c.returnedSplit, j.at+" "+j.key+" | "+use+" | "+j.verdict+listed)
	}
	sort.Strings(c.returnedSplit)
	sort.Strings(c.returnedGaps)
}

// namedReturnPositions records every return of a named function or method
// value declared in the same package directory, from a non-entry declared
// function of test code, with its verdict.
func (a *analysis) namedReturnPositions(c *census, pkgs []*packages.Package, bad map[*packages.Package]bool, prog *ssa.Program, siteCallees map[ssa.CallInstruction][]*ssa.Function) {
	seen := map[string]bool{}
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, modulePath) || bad[p] || p.PkgPath == fixturePath || p.TypesInfo == nil {
			continue
		}
		for index, file := range p.Syntax {
			name := p.CompiledGoFiles[index]
			if seen[name] {
				continue
			}
			seen[name] = true
			isTest := strings.HasSuffix(name, "_test.go")
			if !isTest && !strings.Contains(strings.TrimPrefix(p.PkgPath, modulePath), "test") {
				continue
			}
			for _, decl := range file.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok || function.Body == nil || (isTest && function.Recv == nil && isEntryName(function.Name.Name)) {
					continue
				}
				key := p.PkgPath + "." + function.Name.Name
				ast.Inspect(function.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.FuncLit:
						return false
					case *ast.ReturnStmt:
						for _, result := range n.Results {
							var id *ast.Ident
							switch r := result.(type) {
							case *ast.Ident:
								id = r
							case *ast.SelectorExpr:
								id = r.Sel
							default:
								continue
							}
							position := p.Fset.Position(result.Pos())
							at := fmt.Sprintf("%s:%d:%d", relative(a.base, position.Filename), position.Line, position.Column)
							resolved := "unresolved"
							if used := p.TypesInfo.Uses[id]; used != nil {
								resolved = fmt.Sprintf("%T", used)
								if field, isVar := used.(*types.Var); isVar && field.IsField() {
									resolved = "field"
								}
							}
							c.returnResolved[at] = resolved
							object, ok := p.TypesInfo.Uses[id].(*types.Func)
							if !ok || object.Pkg() == nil {
								continue
							}
							if path := object.Pkg().Path(); path != p.PkgPath && path != strings.TrimSuffix(p.PkgPath, "_test") {
								continue
							}
							verdict := "product function: its body is not test code"
							if fn := prog.FuncValue(object.Origin()); fn != nil && fn.Blocks != nil && a.isTestCode(fn) {
								verdict = a.returnedVerdict(c, key, fn, fn, siteCallees)
							}
							c.namedReturns[at] = verdict
							c.returnedTeardown = append(c.returnedTeardown, at+" "+key+" "+verdict)
						}
					}
					return true
				})
			}
		}
	}
}

// directCallees are the functions called directly from fn's body.
func (a *analysis) directCallees(fn *ssa.Function, siteCallees map[ssa.CallInstruction][]*ssa.Function) []*ssa.Function {
	var out []*ssa.Function
	for _, variant := range a.class(fn) {
		for _, block := range variant.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				callees := siteCallees[call]
				if len(callees) == 0 {
					if callee := call.Common().StaticCallee(); callee != nil {
						callees = []*ssa.Function{callee}
					}
				}
				out = append(out, callees...)
			}
		}
	}
	return out
}

// escaping lists function values a helper returns, directly or stored in a
// value it returns.
func escaping(top *ssa.Function) []*ssa.Function {
	var out []*ssa.Function
	seen := map[ssa.Value]bool{}
	var collect func(v ssa.Value)
	var collectStores func(base ssa.Value, depth int)
	collectStores = func(base ssa.Value, depth int) {
		refs := base.Referrers()
		if refs == nil || depth > 8 {
			return
		}
		for _, ref := range *refs {
			switch ref := ref.(type) {
			case *ssa.Store:
				if ref.Addr == base {
					collect(ref.Val)
				}
			case *ssa.FieldAddr:
				collectStores(ref, depth+1)
			case *ssa.IndexAddr:
				collectStores(ref, depth+1)
			case *ssa.UnOp:
				// A variable a closure captures is boxed: its value is reached
				// through a load of the box.
				if ref.Op == token.MUL {
					collectStores(ref, depth+1)
				}
			case *ssa.MapUpdate:
				if ref.Map == base {
					collect(ref.Value)
				}
			}
		}
	}
	collect = func(v ssa.Value) {
		if v == nil || seen[v] {
			return
		}
		seen[v] = true
		switch v := v.(type) {
		case *ssa.Function:
			out = append(out, v)
		case *ssa.MakeClosure:
			if fn, ok := v.Fn.(*ssa.Function); ok {
				out = append(out, fn)
			}
		case *ssa.ChangeType:
			collect(v.X)
		case *ssa.MakeInterface:
			collect(v.X)
		case *ssa.UnOp:
			if v.Op == token.MUL {
				collectStores(v.X, 0)
			}
		case *ssa.Alloc:
			collectStores(v, 0)
		case *ssa.Slice:
			collect(v.X)
		case *ssa.MakeMap:
			collectStores(v, 0)
		}
	}
	for _, block := range top.Blocks {
		for _, instr := range block.Instrs {
			if ret, ok := instr.(*ssa.Return); ok {
				for _, result := range ret.Results {
					collect(result)
				}
			}
		}
	}
	return out
}

// resultTypes are the named types (pointers dereferenced) a function returns.
func resultTypes(fn *ssa.Function) []*types.Named {
	var out []*types.Named
	results := fn.Signature.Results()
	for i := 0; i < results.Len(); i++ {
		typ := results.At(i).Type()
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		if named, ok := types.Unalias(typ).(*types.Named); ok {
			out = append(out, named.Origin())
		}
	}
	return out
}

func receiverNamed(fn *ssa.Function) *types.Named {
	recv := fn.Signature.Recv()
	if recv == nil {
		return nil
	}
	typ := recv.Type()
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = pointer.Elem()
	}
	named, _ := types.Unalias(typ).(*types.Named)
	if named != nil {
		named = named.Origin()
	}
	return named
}

func run(t *testing.T, overlay map[string][]byte) census {
	t.Helper()
	base := root(t)
	pkgs := load(t, overlay)
	c := census{helpers: map[string]*helperInfo{}, cleanupSites: map[string]bool{}, returnedSites: map[string]bool{}, returnedAll: map[string]bool{}, illTypedFiles: map[string]bool{}, literalSites: map[string]string{}, namedReturns: map[string]string{}, returnResolved: map[string]string{}, verdictMemo: map[string]*judged{}, cleanupReached: map[string]bool{}, synthesizedBodies: map[string]bool{}, teardownUsed: map[string]bool{}}

	// SSA cannot build an ill-typed package (NOR-338) or its importers. Their
	// cleanups are listed from syntax so the reconciliation still accounts
	// for every one.
	bad := illTypedClosure(pkgs)
	for _, p := range pkgs {
		if !bad[p] || !strings.HasPrefix(p.PkgPath, modulePath) || (p.Name == "main" && strings.HasSuffix(p.PkgPath, ".test")) {
			continue
		}
		for _, file := range p.CompiledGoFiles {
			c.illTypedFiles[file] = true
		}
		for index, file := range p.Syntax {
			isTest := strings.HasSuffix(p.CompiledGoFiles[index], "_test.go")
			for _, decl := range file.Decls {
				var body ast.Node = decl
				name := "package scope"
				if function, ok := decl.(*ast.FuncDecl); ok {
					if function.Body == nil || (isTest && function.Recv == nil && isEntryName(function.Name.Name)) {
						continue
					}
					body, name = function.Body, function.Name.Name
				}
				ast.Inspect(body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Cleanup" && len(call.Args) == 1 {
						position := p.Fset.Position(call.Lparen)
						at := fmt.Sprintf("%s:%d:%d", relative(base, position.Filename), position.Line, position.Column)
						if !c.cleanupSites[at] {
							c.cleanupSites[at] = true
							c.illTyped = append(c.illTyped, at+" "+name)
						}
						c.illTypedFiles[p.CompiledGoFiles[index]] = true
					}
					return true
				})
			}
		}
	}

	prog, _ := ssautil.Packages(ssaInitial(pkgs, bad), ssa.InstantiateGenerics)
	prog.Build()
	funcs := ssautil.AllFunctions(prog)
	c.functionsBuilt = len(funcs)
	cg := vta.CallGraph(funcs, cha.CallGraph(prog))
	siteCallees := map[ssa.CallInstruction][]*ssa.Function{}
	for _, node := range cg.Nodes {
		for _, edge := range node.Out {
			if edge.Site != nil && edge.Callee != nil && edge.Callee.Func != nil {
				siteCallees[edge.Site] = append(siteCallees[edge.Site], edge.Callee.Func)
			}
		}
	}
	a := &analysis{base: base, fset: prog.Fset, cg: cg, variants: map[string][]*ssa.Function{}, overlay: overlay}

	var functions []*ssa.Function
	for fn := range funcs {
		if fn.Blocks != nil {
			functions = append(functions, fn)
			a.variants[functionID(fn)] = append(a.variants[functionID(fn)], fn)
		}
	}
	for _, variants := range a.variants {
		if len(variants) > 1 {
			c.variantClasses++
		}
	}
	sort.Slice(functions, func(i, j int) bool { return functions[i].String() < functions[j].String() })
	// A Cleanup argument resolves over every variant of its site.
	type cleanupValue struct {
		fns      []*ssa.Function
		external string
		resolved bool
	}
	cleanupValues := map[string]*cleanupValue{}
	for _, fn := range functions {
		if key, _ := a.helperKey(fn); key == "" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				arg, _, ok := cleanupArg(call.Common())
				if !ok {
					continue
				}
				at := a.position(call.Common().Pos())
				value := cleanupValues[at]
				if value == nil {
					value = &cleanupValue{}
					cleanupValues[at] = value
				}
				if fns, external, resolved := a.resolve(arg, 0); resolved {
					value.resolved = true
					value.fns = append(value.fns, fns...)
					if value.external == "" {
						value.external = external
					}
				}
			}
		}
	}
	for _, fn := range functions {
		key, top := a.helperKey(fn)
		if key == "" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				_, recv, ok := cleanupArg(call.Common())
				if !ok {
					continue
				}
				at := a.position(call.Common().Pos())
				if c.cleanupSites[at] {
					continue // another instance of the same generic or variant
				}
				c.cleanupSites[at] = true
				if !isOwner(recv) {
					c.nonOwner = append(c.nonOwner, at+" receiver "+types.TypeString(recv, nil))
					continue
				}
				h := c.helpers[key]
				if h == nil {
					h = &helperInfo{key: key, top: top}
					c.helpers[key] = h
				}
				r := &rootInfo{at: at, helper: key, kind: "cleanup"}
				fns, external, resolved := cleanupValues[at].fns, cleanupValues[at].external, cleanupValues[at].resolved
				r.fns = fns
				switch {
				case !resolved:
					r.verdict = "unresolved"
				case len(fns) == 0 && strings.HasPrefix(external, "context."):
					r.verdict, r.detail = "no fixture work", "context cancel "+external
				case len(fns) == 0:
					r.verdict, r.detail = "drop-only", "external release "+external
				default:
					w := a.reach(fns, siteCallees, false)
					switch {
					case len(w.reports) > 0:
						r.verdict, r.detail = "reports", strings.Join(w.reports, ", ")
						h.reports = append(h.reports, w.reports...)
					case w.work:
						r.verdict = "drop-only"
					case w.ownerCalls > 0:
						r.verdict = "owner-only"
					default:
						r.verdict = "no fixture work"
					}
					c.dynamicExt = append(c.dynamicExt, w.external...)
					c.dynamicUnres = append(c.dynamicUnres, w.unresolved...)
					for reached := range w.reached {
						c.cleanupReached[functionID(reached)] = true
					}
				}
				h.cleanupRoots = append(h.cleanupRoots, r)
				c.roots = append(c.roots, r)
			}
		}
	}

	// Every function value a shared helper returns gets a verdict; returned
	// function literals are reconciled with the untyped enumerator.
	seenReturned := map[string]bool{}
	for _, fn := range functions {
		if fn.Parent() != nil {
			continue
		}
		key, _ := a.helperKey(fn)
		if key == "" {
			continue
		}
		for _, returned := range escaping(fn) {
			at := a.position(returned.Pos())
			if seenReturned[at+key] {
				continue
			}
			seenReturned[at+key] = true
			if returned.Parent() != nil {
				c.returnedAll[at] = true
				// The same syntactic criterion as the enumerator, applied to
				// the declaration's own syntax.
				if decl, ok := fn.Syntax().(*ast.FuncDecl); ok && decl.Body != nil && registersCleanup(decl.Body) {
					c.returnedSites[at] = true
				}
			}
			verdict := "not teardown: the helper registers no cleanup"
			if functionPackage(returned) == fixturePath {
				verdict = "owner closer"
			} else if h := c.helpers[key]; h != nil && len(h.cleanupRoots) > 0 {
				verdict = "not teardown: runs no registered teardown work"
			}
			c.returnedRoots = append(c.returnedRoots, fmt.Sprintf("%s %s %s", at, key, verdict))
			c.returnedFns = append(c.returnedFns, returnedFn{at: at, key: key, helper: fn, fn: returned})
		}
	}

	// Early close: teardown work a test can run before cleanup.
	for _, h := range c.helpers {
		var rootFns []*ssa.Function
		rootSet := map[string]bool{}
		for _, r := range h.cleanupRoots {
			for _, fn := range r.fns {
				rootFns = append(rootFns, fn)
				rootSet[functionID(fn)] = true
			}
		}
		if len(rootFns) == 0 {
			continue
		}
		teardown := a.reach(rootFns, siteCallees, false).reached
		direct := map[string]bool{}
		for _, fn := range rootFns {
			if functionPackage(fn) != fixturePath {
				direct[functionID(fn)] = true
			}
			for _, callee := range a.directCallees(fn, siteCallees) {
				if functionPackage(callee) != fixturePath {
					direct[functionID(callee)] = true
				}
			}
		}
		// A returned function value that runs teardown work registered at cleanup.
		for _, returned := range a.escapingAll(h.top) {
			if functionPackage(returned) == fixturePath {
				continue
			}
			hit := rootSet[functionID(returned)]
			for _, callee := range a.directCallees(returned, siteCallees) {
				if direct[functionID(callee)] {
					hit = true
				}
			}
			if hit {
				at := a.position(returned.Pos())
				h.earlyClose = append(h.earlyClose, "returned function at "+at+" runs teardown work")
				w := a.reach([]*ssa.Function{returned}, siteCallees, false)
				h.reports = append(h.reports, w.reports...)
				c.dynamicExt = append(c.dynamicExt, w.external...)
				c.dynamicUnres = append(c.dynamicUnres, w.unresolved...)
				for i, entry := range c.returnedRoots {
					if strings.HasPrefix(entry, at+" "+h.key+" ") {
						c.returnedRoots[i] = at + " " + h.key + " early close: runs registered teardown work"
					}
				}
			}
		}
		// A method of a returned fixture type that cleanup reaches and a test also calls.
		returnedTypes := resultTypes(h.top)
		for fn := range teardown {
			named := receiverNamed(fn)
			if named == nil {
				continue
			}
			matches := false
			for _, typ := range returnedTypes {
				if typ.Obj().Name() == named.Obj().Name() && typ.Obj().Pkg() != nil && named.Obj().Pkg() != nil && typ.Obj().Pkg().Path() == named.Obj().Pkg().Path() {
					matches = true
				}
			}
			node := cg.Nodes[fn]
			if !matches || node == nil {
				continue
			}
			c.fixtureMethods = append(c.fixtureMethods, h.key+" reaches "+fn.String())
			for _, edge := range node.In {
				caller := edge.Caller.Func
				// A synthetic caller (a $bound method-value wrapper, a promoted
				// method wrapper, a call site with no position) is not a test
				// running the release early.
				if caller == nil || caller.Synthetic != "" || edge.Site == nil || !edge.Site.Pos().IsValid() {
					continue
				}
				if teardown[caller] || functionPackage(caller) == fixturePath || !a.isTestCode(caller) {
					continue
				}
				site := "-"
				if edge.Site != nil {
					site = a.position(edge.Site.Pos())
				}
				h.earlyClose = append(h.earlyClose, fmt.Sprintf("%s also called by %s at %s", fn.String(), caller.String(), site))
			}
		}
	}

	a.rawReleases(&c, prog, functions, siteCallees)
	a.ownedReleases(&c, prog, functions, siteCallees)
	fixtureChecks(&c, pkgs, base)
	a.enumerate(&c)

	// Consumers' teardown use: every function value any function (test entry
	// points included) registers with a Cleanup owner or defers.
	for _, fn := range functions {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				var value ssa.Value
				switch instr := instr.(type) {
				case *ssa.Defer:
					value = instr.Call.Value
				case ssa.CallInstruction:
					if arg, recv, ok := cleanupArg(instr.Common()); ok && isOwner(recv) {
						value = arg
					}
				}
				if value == nil {
					continue
				}
				if fns, _, ok := a.resolve(value, 0); ok {
					for _, used := range fns {
						c.teardownUsed[functionID(used)] = true
						if used.Synthetic != "" {
							for _, callee := range a.directCallees(used, siteCallees) {
								c.teardownUsed[functionID(callee)] = true
							}
						}
					}
				}
			}
		}
	}

	// Literal population (spec 2641fbde, widened after audit #33): every
	// function literal written directly in a non-entry declared function of
	// test code is one of that declaration's SSA AnonFuncs; each gets one
	// verdict. Escaping literals are returned teardown roots whether or not the
	// helper registers a cleanup.
	for _, fn := range functions {
		if fn.Parent() != nil {
			continue
		}
		key, _ := a.helperKey(fn)
		if key == "" {
			continue
		}
		decl, ok := fn.Syntax().(*ast.FuncDecl)
		if !ok || decl.Body == nil || !a.isTestFile(fn) {
			continue
		}
		rootSet := map[string]bool{}
		var rootFns []*ssa.Function
		if h := c.helpers[key]; h != nil {
			for _, r := range h.cleanupRoots {
				for _, root := range r.fns {
					rootSet[functionID(root)] = true
					rootFns = append(rootFns, root)
				}
			}
		}
		teardown := a.reach(rootFns, siteCallees, false).reached
		escapes := map[string]bool{}
		for _, returned := range a.escapingAll(fn) {
			escapes[functionID(returned)] = true
		}
		returnsValue := false
		results := fn.Signature.Results()
		for i := 0; i < results.Len(); i++ {
			typ := results.At(i).Type()
			if pointer, ok := typ.(*types.Pointer); ok {
				typ = pointer.Elem()
			}
			switch types.Unalias(typ).(type) {
			case *types.Signature, *types.Named:
				returnsValue = true
			}
		}
		// A range-over-func loop body is synthesized by the builder from a for
		// statement: it is no function literal, and literals written inside it
		// belong to the declaration in syntax, so look through it.
		literals := append([]*ssa.Function(nil), fn.AnonFuncs...)
		for i := 0; i < len(literals); i++ {
			literal := literals[i]
			at := a.position(literal.Pos())
			if literal.Synthetic != "" {
				c.synthesizedBodies[at] = true
				literals = append(literals, literal.AnonFuncs...)
				continue
			}
			if _, seen := c.literalSites[at]; seen {
				continue // another instance of the same generic
			}
			verdict := "local"
			switch {
			case rootSet[functionID(literal)] || teardown[literal]:
				verdict = "registered"
			case escapes[functionID(literal)]:
				verdict = "escapes"
			}
			c.literalSites[at] = verdict
			if verdict == "local" && returnsValue {
				c.literalReview = append(c.literalReview, at+" "+key)
			}
			if verdict == "escapes" {
				c.returnedTeardown = append(c.returnedTeardown, at+" "+key+" "+a.returnedVerdict(&c, key, fn, literal, siteCallees))
			}
		}
	}
	sort.Strings(c.literalReview)
	a.namedReturnPositions(&c, pkgs, bad, prog, siteCallees)
	a.splitReturned(&c, siteCallees)
	sort.Strings(c.returnedTeardown)

	for _, h := range c.helpers {
		if len(h.reports) > 0 || len(h.earlyClose) > 0 {
			var reasons []string
			if len(h.reports) > 0 {
				reasons = append(reasons, "reports: "+h.reports[0])
			}
			if len(h.earlyClose) > 0 {
				reasons = append(reasons, "early close: "+h.earlyClose[0])
			}
			var sites []string
			for _, r := range h.cleanupRoots {
				sites = append(sites, r.at)
			}
			c.members = append(c.members, fmt.Sprintf("%s [%s] cleanups at %s", h.key, strings.Join(reasons, "; "), strings.Join(sites, ", ")))
		}
		for _, r := range h.cleanupRoots {
			entry := fmt.Sprintf("%s %s %s", r.at, r.helper, r.detail)
			switch r.verdict {
			case "unresolved":
				c.unresolved = append(c.unresolved, entry)
			case "drop-only":
				if len(h.reports) == 0 && len(h.earlyClose) == 0 {
					c.dropOnly = append(c.dropOnly, entry)
				}
			case "no fixture work":
				c.noFixture = append(c.noFixture, entry)
			case "owner-only":
				c.ownerOnly = append(c.ownerOnly, entry)
			}
		}
	}
	c.dynamicExt, c.dynamicUnres, c.fixtureMethods = unique(c.dynamicExt), unique(c.dynamicUnres), unique(c.fixtureMethods)
	for _, list := range [][]string{c.members, c.dropOnly, c.noFixture, c.ownerOnly, c.unresolved, c.nonOwner, c.illTyped, c.returnedRoots} {
		sort.Strings(list)
	}
	return c
}

func unique(list []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range list {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func (c census) isMember(name string) bool {
	for key, h := range c.helpers {
		if (strings.HasSuffix(key, "."+name)) && (len(h.reports) > 0 || len(h.earlyClose) > 0) {
			return true
		}
	}
	return false
}

func (c census) rootVerdict(name string) string {
	for key, h := range c.helpers {
		if strings.HasSuffix(key, "."+name) && len(h.cleanupRoots) > 0 {
			return h.cleanupRoots[0].verdict
		}
	}
	return ""
}

func report(t *testing.T, label string, c census) {
	t.Logf("%s: SSA functions %d, one-argument Cleanup sites %d, teardown roots %d, helpers with roots %d", label, c.functionsBuilt, len(c.cleanupSites), len(c.roots), len(c.helpers))
	t.Logf("%s: MEMBERS %d: %s", label, len(c.members), strings.Join(c.members, " || "))
	t.Logf("%s: DROP-ONLY roots (NOR-331) %d: %s", label, len(c.dropOnly), strings.Join(c.dropOnly, " || "))
	t.Logf("%s: NO-FIXTURE-WORK roots %d: %s", label, len(c.noFixture), strings.Join(c.noFixture, " || "))
	t.Logf("%s: OWNER-ONLY roots %d: %s", label, len(c.ownerOnly), strings.Join(c.ownerOnly, " || "))
	t.Logf("%s: UNRESOLVED roots %d: %s", label, len(c.unresolved), strings.Join(c.unresolved, " || "))
	t.Logf("%s: non-owner Cleanup calls %d: %s", label, len(c.nonOwner), strings.Join(c.nonOwner, " || "))
	t.Logf("%s: ill-typed package cleanups (NOR-338) %d: %s", label, len(c.illTyped), strings.Join(c.illTyped, " || "))
	t.Logf("%s: RETURNED function values %d (literals %d): %s", label, len(c.returnedRoots), len(c.returnedSites), strings.Join(c.returnedRoots, " || "))
	t.Logf("%s: TEARDOWN fixture methods %d: %s", label, len(c.fixtureMethods), strings.Join(c.fixtureMethods, " || "))
	t.Logf("%s: dynamic teardown calls of external function values %d: %s", label, len(c.dynamicExt), strings.Join(c.dynamicExt, " || "))
	t.Logf("%s: UNRESOLVED dynamic teardown calls %d: %s", label, len(c.dynamicUnres), strings.Join(c.dynamicUnres, " || "))
	t.Logf("%s: testfixture Close calls %d: %s; skippable or dropped %d: %s", label, len(c.fixtureCloses), strings.Join(c.fixtureCloses, ", "), len(c.fixtureDrops), strings.Join(c.fixtureDrops, ", "))
	t.Logf("%s: Own/OwnWithClose call sites %d; removed names present: %v", label, c.ownCalls, c.removedNames)
	t.Logf("%s: v11 MEMBERS (releases of an adopted lifetime outside the owner) %d: %s", label, len(c.ownedDropped), strings.Join(c.ownedDropped, " || "))
	t.Logf("%s: v11 MIXED-VARIABLE MEMBERS %d: %s", label, len(c.ownedAmbiguous), strings.Join(c.ownedAmbiguous, " || "))
	t.Logf("%s: v11 LEDGER-JUSTIFIED %d: %s", label, len(c.ownedChecked), strings.Join(c.ownedChecked, " || "))
	t.Logf("%s: v11 LEDGER STALE %d: %s; LEDGER PROBLEMS %d: %s", label, len(c.ledgerStale), strings.Join(c.ledgerStale, " || "), len(c.ledgerProblems), strings.Join(c.ledgerProblems, " || "))
	t.Logf("%s: v11 ENUMERATOR hits %d, unexplained %d: %s", label, len(c.enumHits), len(c.enumUnexplained), strings.Join(c.enumUnexplained, " || "))
	t.Logf("%s: v11 ENUMERATOR HITS: %s", label, strings.Join(c.enumHits, " || "))
	t.Logf("%s: OWNER RELEASE DROPS %d: %s", label, len(c.ownerReleaseDrops), strings.Join(c.ownerReleaseDrops, " || "))
	if label == "census" {
		t.Logf("%s: CLOSE TYPE FACTS %d: %s", label, len(c.closeFacts), strings.Join(c.closeFacts, " || "))
	}
}

func isErrorClose(s *types.Signature) bool {
	return s.Params().Len() == 0 && s.Results().Len() == 1 && types.Identical(s.Results().At(0).Type(), types.Universe.Lookup("error").Type())
}

// closeCalls returns every x.Close(...) call returning one error in node.
// Author: L4 lane (Codex), 1 Nov 2026. Context-taking release adapters share
// the same error-consumption check as the zero-argument owner implementation.
func closeCalls(node ast.Node, info *types.Info) []*ast.CallExpr {
	var calls []*ast.CallExpr
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Close" {
			return true
		}
		if sig, ok := info.TypeOf(call.Fun).(*types.Signature); ok && sig.Results().Len() == 1 && types.Identical(sig.Results().At(0).Type(), types.Universe.Lookup("error").Type()) {
			calls = append(calls, call)
		}
		return true
	})
	return calls
}

// discardedErrorCalls lists every call in node whose error result is
// discarded: a bare call, deferred or started call returning an error, or an
// assignment that binds the error result to the blank identifier.
func discardedErrorCalls(node ast.Node, info *types.Info) []*ast.CallExpr {
	errorType := types.Universe.Lookup("error").Type()
	errorAt := func(call *ast.CallExpr) []bool {
		switch typ := info.TypeOf(call).(type) {
		case nil:
			return nil
		case *types.Tuple:
			result := make([]bool, typ.Len())
			for i := range result {
				result[i] = types.Identical(typ.At(i).Type(), errorType)
			}
			return result
		default:
			return []bool{types.Identical(typ, errorType)}
		}
	}
	returnsError := func(call *ast.CallExpr) bool {
		for _, isError := range errorAt(call) {
			if isError {
				return true
			}
		}
		return false
	}
	blank := func(expr ast.Expr) bool {
		id, ok := expr.(*ast.Ident)
		return ok && id.Name == "_"
	}
	var calls []*ast.CallExpr
	ast.Inspect(node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.ExprStmt:
			if call, ok := n.X.(*ast.CallExpr); ok && returnsError(call) {
				calls = append(calls, call)
			}
		case *ast.DeferStmt:
			if returnsError(n.Call) {
				calls = append(calls, n.Call)
			}
		case *ast.GoStmt:
			if returnsError(n.Call) {
				calls = append(calls, n.Call)
			}
		case *ast.AssignStmt:
			if len(n.Rhs) == 1 && len(n.Lhs) > 1 {
				if call, ok := n.Rhs[0].(*ast.CallExpr); ok {
					for i, isError := range errorAt(call) {
						if isError && i < len(n.Lhs) && blank(n.Lhs[i]) {
							calls = append(calls, call)
							break
						}
					}
				}
				return true
			}
			for i, rhs := range n.Rhs {
				if call, ok := rhs.(*ast.CallExpr); ok && i < len(n.Lhs) && blank(n.Lhs[i]) && returnsError(call) {
					calls = append(calls, call)
				}
			}
		}
		return true
	})
	return calls
}

func parents(root ast.Node) map[ast.Node]ast.Node {
	result := map[ast.Node]ast.Node{}
	var stack []ast.Node
	ast.Inspect(root, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		if len(stack) > 0 {
			result[n] = stack[len(stack)-1]
		}
		stack = append(stack, n)
		return true
	})
	return result
}

var reporters = map[string]bool{"Errorf": true, "Error": true, "Fatalf": true, "Fatal": true, "NoError": true}

func calleeName(fun ast.Expr) string {
	switch fun := fun.(type) {
	case *ast.SelectorExpr:
		return fun.Sel.Name
	case *ast.Ident:
		return fun.Name
	}
	return ""
}

var fatal = map[string]bool{"Fatal": true, "Fatalf": true, "FailNow": true}

// isFatal is true for a call that can stop the test before later reports.
func isFatal(call *ast.CallExpr) bool {
	if fatal[calleeName(call.Fun)] {
		return true
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "require"
}

func enclosingFunction(n ast.Node, up map[ast.Node]ast.Node) ast.Node {
	for n != nil {
		switch n.(type) {
		case *ast.FuncDecl, *ast.FuncLit:
			return n
		}
		n = up[n]
	}
	return nil
}

// reachesReport is true when the close error is passed on (a call argument or
// a return value), or bound to variables whose first later use is not preceded
// by a fatal assertion that could skip it. A fatal call whose own arguments
// carry the use reports it.
func reachesReport(call *ast.CallExpr, up map[ast.Node]ast.Node, info *types.Info) bool {
	switch parent := up[call].(type) {
	case *ast.CallExpr, *ast.ReturnStmt:
		return true
	case *ast.AssignStmt:
		function := enclosingFunction(parent, up)
		bound := parent.Lhs
		for i, rhs := range parent.Rhs {
			if rhs == call && len(parent.Lhs) == len(parent.Rhs) {
				bound = parent.Lhs[i : i+1]
			}
		}
		for _, lhs := range bound {
			id, ok := lhs.(*ast.Ident)
			if !ok || id.Name == "_" {
				return false
			}
			object := info.ObjectOf(id)
			var use *ast.Ident
			ast.Inspect(function, func(n ast.Node) bool {
				if candidate, ok := n.(*ast.Ident); ok && candidate.Pos() > parent.End() && info.Uses[candidate] == object && (use == nil || candidate.Pos() < use.Pos()) {
					use = candidate
				}
				return true
			})
			if use == nil {
				return false
			}
			skipped := false
			ast.Inspect(function, func(n ast.Node) bool {
				if fatalCall, ok := n.(*ast.CallExpr); ok && fatalCall.Pos() > parent.End() && fatalCall.Pos() < use.Pos() && fatalCall.End() < use.Pos() && isFatal(fatalCall) {
					skipped = true
				}
				return !skipped
			})
			if skipped {
				return false
			}
		}
		return true
	}
	return false
}

func calleeObject(fun ast.Expr, info *types.Info) types.Object {
	switch fun := fun.(type) {
	case *ast.Ident:
		return info.Uses[fun]
	case *ast.SelectorExpr:
		return info.Uses[fun.Sel]
	case *ast.IndexExpr:
		return calleeObject(fun.X, info)
	case *ast.IndexListExpr:
		return calleeObject(fun.X, info)
	}
	return nil
}

// plantedOwnerDiscards discards a release error in the owner package three
// ways: assigned to the blank identifier, as a bare call, and deferred.
const plantedOwnerDiscards = "package testfixture\n\n" +
	"import \"context\"\n\n" +
	"type plantedShutdowner interface{ Shutdown(context.Context) error }\n\n" +
	"func plantedDiscardedShutdown(s plantedShutdowner) {\n" +
	"\t_ = s.Shutdown(context.Background())\n" +
	"\ts.Shutdown(context.Background())\n" +
	"\tdefer s.Shutdown(context.Background())\n" +
	"}\n"

// plantedFixtureClose skips the second close error when the first fails.
const plantedFixtureClose = "package testfixture\n\n" +
	"import (\n\t\"testing\"\n\n\t\"github.com/stretchr/testify/require\"\n)\n\n" +
	"func plantedSequentialCloses(t *testing.T, first, second Closer) {\n" +
	"\tfirstErr, secondErr := first.Close(), second.Close()\n" +
	"\trequire.NoError(t, firstErr)\n" +
	"\trequire.NoError(t, secondErr)\n}\n"

// fixtureChecks keeps the owner package's own rules: every Close() error in
// internal/testfixture reaches a report before any fatal assertion could skip
// it, the removed unchecked API is absent, and Own call sites are counted.
func fixtureChecks(c *census, pkgs []*packages.Package, base string) {
	seen := map[string]bool{}
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, modulePath) {
			continue
		}
		if p.PkgPath == fixturePath && p.Types != nil {
			for _, name := range []string{"OwnChecked", "CheckedOwner", "own"} {
				if p.Types.Scope().Lookup(name) != nil {
					c.removedNames = append(c.removedNames, p.ID+"."+name)
				}
			}
		}
		for index, file := range p.Syntax {
			name := p.CompiledGoFiles[index]
			if seen[name] {
				continue
			}
			seen[name] = true
			up := parents(file)
			coreOwner := p.PkgPath == fixturePath && !strings.HasSuffix(name, "_test.go")
			if coreOwner {
				for _, call := range discardedErrorCalls(file, p.TypesInfo) {
					at := fmt.Sprintf("%s:%d", relative(base, name), p.Fset.Position(call.Pos()).Line)
					c.fixtureDrops = append(c.fixtureDrops, at+" discards the error of "+types.ExprString(call.Fun))
				}
			}
			for _, call := range closeCalls(file, p.TypesInfo) {
				pos := p.Fset.Position(call.Pos())
				at := fmt.Sprintf("%s:%d", relative(base, name), pos.Line)
				if coreOwner {
					c.fixtureCloses = append(c.fixtureCloses, at)
				}
				if reachesReport(call, up, p.TypesInfo) {
					continue
				}
				if coreOwner {
					c.fixtureDrops = append(c.fixtureDrops, at)
				} else {
					for _, span := range c.ownerReleaseRanges[filepath.Clean(name)] {
						if span[0] <= pos.Offset && pos.Offset < span[1] {
							c.ownerReleaseDrops = append(c.ownerReleaseDrops, at+" discards the owned outcome of "+types.ExprString(call.Fun))
							break
						}
					}
				}
			}
			ast.Inspect(file, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if object := calleeObject(call.Fun, p.TypesInfo); object != nil && object.Pkg() != nil && object.Pkg().Path() == fixturePath && (object.Name() == "Own" || object.Name() == "OwnWithClose") {
						c.ownCalls++
					}
				}
				return true
			})
		}
	}
	c.ownerReleaseDrops = unique(c.ownerReleaseDrops)
}

// ---- The generated indirection matrix ----

type matrixCase struct {
	helper string
	member bool
	root   string // expected root verdict for non-member roots, "" if none
}

// matrixSource generates every release kind under every indirection, plus
// early-close-only shapes and owner-only negatives, as one test file of
// package replication.
// plantedVariantHelpers lives in a test-support package that has its own tests,
// so its test code loads twice, plain and as the test variant. A helper's body
// is walked only in test code, so product code would never show its report.
// plantedVariantInPackage consumes one helper through the test variant and
// plantedVariantOutside consumes the other through the plain variant.
const plantedVariantHelpers = `package testutil

import "testing"

func MxVariantPlainUsed(t *testing.T) func() { return func() { t.Errorf("close failed") } }

type mxVariantTest struct{ t *testing.T }

func (r *mxVariantTest) report() { r.t.Errorf("close failed") }

func MxVariantTestUsed(t *testing.T) func() { return (&mxVariantTest{t}).report }

type MxOutsideCloser interface{ CloseChecked() }

func MxDispatchOutside(c MxOutsideCloser) func() { return c.CloseChecked }

type MxInsideCloser interface{ CloseChecked() }

func MxDispatchInside(c MxInsideCloser) func() { return c.CloseChecked }

type MxRootOutsideCloser interface{ CloseChecked() }

func MxRootDispatchOutside(t *testing.T, c MxRootOutsideCloser) { t.Cleanup(c.CloseChecked) }

type MxRootInsideCloser interface{ CloseChecked() }

func MxRootDispatchInside(t *testing.T, c MxRootInsideCloser) { t.Cleanup(c.CloseChecked) }
`

const plantedVariantInPackage = `package testutil

import "testing"

type mxInsideImpl struct{ t *testing.T }

func (i *mxInsideImpl) CloseChecked() { i.t.Errorf("close failed") }

func TestMxVariantConsumedInPackage(t *testing.T) {
	defer MxVariantTestUsed(t)()
	defer MxDispatchInside(&mxInsideImpl{t})()
	MxRootDispatchInside(t, &mxInsideImpl{t})
}
`

const plantedVariantOutside = `package main

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
)

type mxOutsideImpl struct{ t *testing.T }

func (o *mxOutsideImpl) CloseChecked() { o.t.Errorf("close failed") }

func TestMxVariantConsumedOutsidePackage(t *testing.T) {
	defer testutil.MxVariantPlainUsed(t)()
	defer testutil.MxDispatchOutside(&mxOutsideImpl{t})()
	testutil.MxRootDispatchOutside(t, &mxOutsideImpl{t})
}
`

func matrixSource() (string, []matrixCase) {
	var b strings.Builder
	b.WriteString("package replication\n\nimport (\n\t\"context\"\n\t\"testing\"\n\n\tfixtureowner \"github.com/orneryd/nornicdb/internal/testfixture\"\n)\n\n")
	b.WriteString(`type mxRes struct{}

func (*mxRes) Close() error { return nil }

type mxDrv struct{}

func (*mxDrv) Close(context.Context) error { return nil }

func (*mxDrv) Stop(context.Context) error { return nil }

var mxT *testing.T
var mxR = &mxRes{}
var mxD = &mxDrv{}
var mxHook int

func mxVoidRelease(t *testing.T, r *mxRes) {
	if r.Close() != nil {
		t.Fatal("close failed")
	}
}

type mxReleaser interface{ release() }

type mxFn struct{ fn func() }

`)
	kinds := []struct{ name, body string }{
		{"Err", "if err := R.Close(); err != nil {\n\t\tT.Error(err)\n\t}"},
		{"Void", "mxVoidRelease(T, R)"},
		{"Ctx", "if err := D.Close(context.Background()); err != nil {\n\t\tT.Errorf(\"close: %v\", err)\n\t}"},
		{"Red", "if D.Stop(context.Background()) != nil {\n\t\tT.Error(\"stop failed\")\n\t}"},
	}
	body := func(template, t, r, d string) string {
		return strings.NewReplacer("T.", t+".", "(T,", "("+t+",", "R.", r+".", " R)", " "+r+")", "D.", d+".").Replace(template)
	}
	var cases []matrixCase
	for _, k := range kinds {
		n := k.name
		local := "\tr, d := &mxRes{}, &mxDrv{}\n\t_, _ = r, d\n"
		fmt.Fprintf(&b, "func mxLit%s(t *testing.T) {\n%s\tt.Cleanup(func() {\n\t\t%s\n\t})\n}\n\n", n, local, body(k.body, "t", "r", "d"))
		fmt.Fprintf(&b, "func mxLocal%s(t *testing.T) {\n%s\trelease := func() {\n\t\t%s\n\t}\n\tt.Cleanup(release)\n}\n\n", n, local, body(k.body, "t", "r", "d"))
		fmt.Fprintf(&b, "func mxNamedFn%s() {\n\t%s\n}\n\nfunc mxNamed%s(t *testing.T) {\n\tmxT = t\n\tt.Cleanup(mxNamedFn%s)\n}\n\n", n, body(k.body, "mxT", "mxR", "mxD"), n, n)
		fmt.Fprintf(&b, "type mxHolder%s struct {\n\tt *testing.T\n\tr *mxRes\n\td *mxDrv\n}\n\nfunc (h *mxHolder%s) release() {\n\t%s\n}\n\nfunc mxMethodValue%s(t *testing.T) {\n\th := &mxHolder%s{t: t, r: &mxRes{}, d: &mxDrv{}}\n\tt.Cleanup(h.release)\n}\n\n", n, n, body(k.body, "h.t", "h.r", "h.d"), n, n)
		fmt.Fprintf(&b, "type mxFix%s struct {\n\tt *testing.T\n\tr *mxRes\n\td *mxDrv\n}\n\nfunc (f *mxFix%s) close() {\n\t%s\n}\n\nfunc mxReceiver%s(t *testing.T) {\n\tf := &mxFix%s{t: t, r: &mxRes{}, d: &mxDrv{}}\n\tt.Cleanup(func() { f.close() })\n}\n\n", n, n, body(k.body, "f.t", "f.r", "f.d"), n, n)
		fmt.Fprintf(&b, "func mxDeepB%s(t *testing.T, r *mxRes, d *mxDrv) {\n\t%s\n}\n\nfunc mxDeepA%s(t *testing.T, r *mxRes, d *mxDrv) { mxDeepB%s(t, r, d) }\n\nfunc mxDeep%s(t *testing.T) {\n\tt.Cleanup(func() { mxDeepA%s(t, &mxRes{}, &mxDrv{}) })\n}\n\n", n, body(k.body, "t", "r", "d"), n, n, n, n)
		fmt.Fprintf(&b, "func mxField%s(t *testing.T) {\n%s\ts := mxFn{fn: func() {\n\t\t%s\n\t}}\n\tt.Cleanup(func() { s.fn() })\n}\n\n", n, local, body(k.body, "t", "r", "d"))
		fmt.Fprintf(&b, "type mxImpl%s struct {\n\tt *testing.T\n\tr *mxRes\n\td *mxDrv\n}\n\nfunc (i *mxImpl%s) release() {\n\t%s\n}\n\nfunc mxInterface%s(t *testing.T) {\n\tvar rel mxReleaser = &mxImpl%s{t: t, r: &mxRes{}, d: &mxDrv{}}\n\tt.Cleanup(func() { rel.release() })\n}\n\n", n, n, body(k.body, "i.t", "i.r", "i.d"), n, n)
		fmt.Fprintf(&b, "func mxReturned%s(t *testing.T) func() {\n%s\tcloseNow := func() {\n\t\t%s\n\t}\n\tt.Cleanup(closeNow)\n\treturn closeNow\n}\n\n", n, local, body(k.body, "t", "r", "d"))
		for _, indirection := range []string{"mxLit", "mxLocal", "mxNamed", "mxMethodValue", "mxReceiver", "mxDeep", "mxField", "mxInterface", "mxReturned"} {
			cases = append(cases, matrixCase{helper: indirection + n, member: true})
		}
	}
	b.WriteString(`func mxEarlyReturned(t *testing.T) func() {
	r := &mxRes{}
	t.Cleanup(func() { _ = r.Close() })
	return func() { _ = r.Close() }
}

type mxDropFix struct{ r *mxRes }

func (f *mxDropFix) close() { _ = f.r.Close() }

func mxEarlyMethod(t *testing.T) *mxDropFix {
	f := &mxDropFix{r: &mxRes{}}
	t.Cleanup(func() { f.close() })
	return f
}

func TestMxEarlyMethodCaller(t *testing.T) { mxEarlyMethod(t).close() }

func mxOwnerOnly(t *testing.T) *mxRes { return fixtureowner.Own(t, &mxRes{}) }

func mxOwnerEarly(t *testing.T) func() {
	_, closeNow := fixtureowner.OwnWithClose(t, &mxRes{})
	return closeNow
}

func mxOwnerFunc(t *testing.T) {
	d := &mxDrv{}
	fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { return d.Close(context.Background()) }))
}

func mxOwnerServe(t *testing.T) {
	done := make(chan error, 1)
	done <- nil
	fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { return <-done }), &mxRes{})
}

func mxHookRestore(t *testing.T) {
	previous := mxHook
	mxHook = 1
	t.Cleanup(func() { mxHook = previous })
}

func mxDropOnly(t *testing.T) {
	r := &mxRes{}
	t.Cleanup(func() { _ = r.Close() })
}

type mxGate struct{ ch chan struct{} }

func (g *mxGate) open() { close(g.ch) }

func mxGateEarly(t *testing.T) *mxGate {
	g := &mxGate{ch: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func TestMxGateCaller(t *testing.T) { mxGateEarly(t).open() }

type mxQuietGate struct{ ch chan struct{} }

func (g *mxQuietGate) open() { close(g.ch) }

func mxGateBoundOnly(t *testing.T) *mxQuietGate {
	g := &mxQuietGate{ch: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func mxReturnOnly(t *testing.T) func() {
	r := &mxRes{}
	return func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}
}

type mxReturnHolder struct {
	t *testing.T
	r *mxRes
}

func (h *mxReturnHolder) stop() {
	if err := h.r.Close(); err != nil {
		h.t.Error(err)
	}
}

func mxReturnMethod(t *testing.T) func() {
	h := &mxReturnHolder{t: t, r: &mxRes{}}
	return h.stop
}

func mxReturnDrop() func() {
	r := &mxRes{}
	return func() { _ = r.Close() }
}

func TestMxReturnedTeardownConsumers(t *testing.T) {
	t.Cleanup(mxReturnOnly(t))
	defer mxReturnMethod(t)()
	defer mxReturnDrop()()
}

func mxReturnChecker(t *testing.T) func(int) {
	return func(n int) {
		if n != 1 {
			t.Errorf("got %d", n)
		}
	}
}

func TestMxReturnedCheckerConsumer(t *testing.T) { mxReturnChecker(t)(1) }

type mxHookHolder struct{ hook func() }

var mxHooks mxHookHolder

func mxReturnUnresolvedUsed(t *testing.T) func() {
	return func() { mxHooks.hook() }
}

func mxReturnUnresolvedFree(t *testing.T) func() {
	return func() { mxHooks.hook() }
}

func mxReturnCheckedAtCleanup(t *testing.T) func(int) {
	check := func(n int) {
		if n != 0 {
			t.Errorf("got %d", n)
		}
	}
	t.Cleanup(func() { check(0) })
	return check
}

func mxReturnExplicitClose(t *testing.T) func() {
	r := &mxRes{}
	closeNow := func() { _ = r.Close() }
	t.Cleanup(closeNow)
	return closeNow
}

func TestMxReturnedCleanupReached(t *testing.T) {
	mxReturnCheckedAtCleanup(t)(0)
	mxReturnExplicitClose(t)()
}

func TestMxReturnedUnresolvedConsumers(t *testing.T) {
	t.Cleanup(mxReturnUnresolvedUsed(t))
	mxReturnUnresolvedFree(t)()
}

type mxRawClient struct{ abort func() error }

func mxRawField(t *testing.T) *mxRawClient {
	client := &mxRawClient{}
	client.abort = func() error { return nil }
	fixtureowner.Own(t, fixtureowner.CloseFunc(client.abort))
	return client
}

func mxRawBoxed(t *testing.T) *mxRawClient {
	client := &mxRawClient{}
	client.abort = func() error { return nil }
	keep := func() { client.abort = client.abort }
	keep()
	fixtureowner.Own(t, fixtureowner.CloseFunc(client.abort))
	return client
}

func mxRawReturned(t *testing.T) func() error {
	release := func() error { return nil }
	fixtureowner.Own(t, fixtureowner.CloseFunc(release))
	return release
}

type mxRawFixture struct{}

func (*mxRawFixture) Close() error { return nil }

func mxRawMethod(t *testing.T) *mxRawFixture { return fixtureowner.Own(t, &mxRawFixture{}) }

func mxCanonicalAbort(t *testing.T) func() {
	_, closeNow := fixtureowner.OwnWithClose(t, fixtureowner.CloseFunc(func() error { return nil }))
	return closeNow
}

func TestMxRawReleaseConsumers(t *testing.T) {
	_ = mxRawField(t).abort()
	_ = mxRawBoxed(t).abort()
	_ = mxRawMethod(t).Close()
	mxCanonicalAbort(t)()
	mxRawReturned(t)
}

func mxNamedStop() {}

type mxNamedHolder struct{ mxNamedStop string }

func mxNamedField(h mxNamedHolder) string { return h.mxNamedStop }

func mxNamedFunc() func() { return mxNamedStop }
`)
	cases = append(cases,
		matrixCase{helper: "mxEarlyReturned", member: true},
		matrixCase{helper: "mxEarlyMethod", member: true},
		matrixCase{helper: "mxOwnerOnly"},
		matrixCase{helper: "mxOwnerEarly"},
		matrixCase{helper: "mxOwnerFunc"},
		matrixCase{helper: "mxOwnerServe"},
		matrixCase{helper: "mxHookRestore", root: "no fixture work"},
		matrixCase{helper: "mxDropOnly", root: "drop-only"},
		// A method-value cleanup whose method a test also calls is an early
		// close; with only its synthetic $bound wrapper as caller it is not.
		matrixCase{helper: "mxGateEarly", member: true},
		matrixCase{helper: "mxGateBoundOnly", root: "drop-only"},
		// A returned teardown closure is a root even when the helper registers
		// no cleanup: one that reports is a member, through a literal or a
		// method value; one that only releases is not.
		matrixCase{helper: "mxReturnOnly", member: true},
		matrixCase{helper: "mxReturnMethod", member: true},
		matrixCase{helper: "mxReturnDrop"},
		// A returned function that reports but that no consumer uses as
		// teardown (an assertion closure) is listed for the code read, not a member.
		matrixCase{helper: "mxReturnChecker"},
		// A returned assertion closure its helper's cleanup also runs is
		// teardown that reports (a member), and a gap if it was listed as
		// not teardown; a returned close the helper registers and a consumer
		// calls explicitly is teardown reached from cleanup (an early close).
		matrixCase{helper: "mxReturnCheckedAtCleanup", member: true},
		matrixCase{helper: "mxReturnExplicitClose", member: true},
		// A returned reporter in a package loaded as two variants, consumed
		// through one variant only: teardown whichever variant is judged.
		matrixCase{helper: "MxVariantPlainUsed", member: true},
		matrixCase{helper: "MxVariantTestUsed", member: true},
		// Interface dispatch only one variant can see, each way: the reporting
		// implementer is in another package's test (plain variant only) or in
		// the package's own test (test variant only).
		matrixCase{helper: "MxDispatchOutside", member: true},
		matrixCase{helper: "MxDispatchInside", member: true},
		// The same dispatch registered with Cleanup by the helper itself.
		matrixCase{helper: "MxRootDispatchOutside", member: true},
		matrixCase{helper: "MxRootDispatchInside", member: true},
	)
	return b.String(), cases
}

func runMatrix(t *testing.T, head census, l *ledger) {
	t.Helper()
	source, cases := matrixSource()
	plant := filepath.Join(root(t), "pkg", "replication", "zz_cleanup_matrix_test.go")
	fixturePlant := filepath.Join(root(t), "internal", "testfixture", "zz_planted_close.go")
	ownerPlant := filepath.Join(root(t), "internal", "testfixture", "zz_planted_discard.go")
	variantHelpers := filepath.Join(root(t), "pkg", "cypher", "testutil", "zz_cleanup_variant.go")
	variantInPackage := filepath.Join(root(t), "pkg", "cypher", "testutil", "zz_cleanup_variant_test.go")
	variantOutside := filepath.Join(root(t), "cmd", "nornicdb", "zz_cleanup_variant_test.go")
	ownedPlant := filepath.Join(root(t), "pkg", "replication", "zz_owned_release_test.go")
	productWrapperPlant := filepath.Join(root(t), "pkg", "replication", "zz_owned_product_wrapper.go")
	ownerDelegatePlant := filepath.Join(root(t), "pkg", "replication", "zz_owner_delegate_release_test.go")
	intrinsicPlant := filepath.Join(root(t), "pkg", "replication", "zz_intrinsic_adoption_test.go")
	planted := run(t, map[string][]byte{intrinsicPlant: []byte(plantedIntrinsicAdoption), plant: []byte(source), fixturePlant: []byte(plantedFixtureClose), ownerPlant: []byte(plantedOwnerDiscards), variantHelpers: []byte(plantedVariantHelpers), variantInPackage: []byte(plantedVariantInPackage), variantOutside: []byte(plantedVariantOutside), ownedPlant: []byte(plantedOwnedRelease), productWrapperPlant: []byte(plantedProductWrapper), ownerDelegatePlant: []byte(plantedOwnerDelegates), ledgerPath(t): plantedLedgerContent(t)})
	report(t, "matrix", planted)
	// Canonical-root adoption and identity controls. Marker lines avoid an
	// accidental nearby Close satisfying recall for the omitted resource.
	intrinsicMembers := strings.Join(append(append([]string{}, planted.ownedDropped...), planted.ownedAmbiguous...), " || ")
	for index, line := range strings.Split(plantedIntrinsicAdoption, "\n") {
		if !strings.Contains(line, "// intrinsic-") {
			continue
		}
		site := "pkg/replication/zz_intrinsic_adoption_test.go:" + itoa(index+1) + ":"
		want := !strings.Contains(line, "-negative")
		l.check("canonical adoption "+strings.TrimSpace(line), strings.Contains(intrinsicMembers, site) == want, "membership at %s must be %v", site, want)
	}
	// Author: L4 lane (Codex), 1 Nov 2026. Returning nil from an owned
	// adapter must not hide direct or delegated discarded Close outcomes.
	var delegatePlants, delegateOutside []string
	for _, entry := range planted.ownerReleaseDrops {
		if strings.Contains(entry, "zz_owner_delegate_release_test.go:") {
			delegatePlants = append(delegatePlants, entry)
		} else if !plantedFile(entry) {
			delegateOutside = append(delegateOutside, entry)
		}
	}
	for _, expression := range []string{"bare.Close", "blank.Close", "delayed.Close", "async.Close", "g.resource.Close", "contextual.Close"} {
		l.check("owned adapter reports discarded "+expression, strings.Contains(strings.Join(delegatePlants, " || "), expression), "not caught: %v", delegatePlants)
	}
	l.check("owned adapter discard plant count", len(delegatePlants) == 6, "got %d, want 6: %v", len(delegatePlants), delegatePlants)
	l.check("returned adapter outcome is accepted", !strings.Contains(strings.Join(delegatePlants, " || "), "returned.Close"), "flagged propagated outcome: %v", delegatePlants)
	l.check("owned adapter plants leave real drops unchanged", strings.Join(unique(delegateOutside), " || ") == strings.Join(head.ownerReleaseDrops, " || "), "real drops changed: %v vs %v", delegateOutside, head.ownerReleaseDrops)
	ownedDrops := strings.Join(planted.ownedDropped, " || ")
	// Argument flow also finds the inner release in zzOwnedWrap.Close.
	// The syntax cross-check intentionally enumerates consumers rather than
	// Close implementations, so it checks the original consumer lines below.
	// Author: L4 lane (Codex), 1 Nov 2026; retained r52d is the counter failure.
	memberLines := append(append([]string{}, plantedOwnedDropLines...), "15:")
	for _, line := range memberLines {
		l.check("v11 planted member line "+line, strings.Contains(ownedDrops, "zz_owned_release_test.go:"+line), "not caught: %v", planted.ownedDropped)
	}
	var inPlant, outsidePlants []string
	for _, entry := range planted.ownedDropped {
		switch {
		case strings.Contains(entry, "zz_owned_release_test.go:"):
			inPlant = append(inPlant, entry)
		case !plantedFile(entry):
			outsidePlants = append(outsidePlants, entry)
		}
	}
	l.check("v11 planted member count", len(inPlant) == len(memberLines),
		"the plant yields %d members, want %d: %v", len(inPlant), len(memberLines), inPlant)
	l.check("v11 plants leave the tree's members unchanged", strings.Join(outsidePlants, " || ") == strings.Join(head.ownedDropped, " || "),
		"the tree's members differ under the plants: %d vs %d", len(outsidePlants), len(head.ownedDropped))
	justified := strings.Join(planted.ownedChecked, " || ")
	for _, line := range []string{plantedOwnedAmbiguousLine, plantedOwnedLedgerLine} {
		l.check("v11 planted ledger entry line "+line, strings.Contains(justified, "zz_owned_release_test.go:"+line) && !strings.Contains(ownedDrops, "zz_owned_release_test.go:"+line),
			"not ledger-justified: %v", planted.ownedChecked)
	}
	l.check("v11 planted stale ledger entry reported", strings.Contains(strings.Join(planted.ledgerStale, " || "), "zzGone"), "stale entry not reported: %v", planted.ledgerStale)
	l.check("v11 planted members found by the enumerator", func() bool {
		for _, line := range plantedOwnedDropLines {
			if !strings.Contains(strings.Join(planted.enumHits, " || "), "zz_owned_release_test.go:"+line) {
				return false
			}
		}
		return true
	}(), "enumerator missed a planted release: %v", planted.enumHits)
	caught := 0
	for _, tc := range cases {
		member := planted.isMember(tc.helper)
		verdict := planted.rootVerdict(tc.helper)
		ok := member == tc.member && (tc.root == "" || verdict == tc.root)
		if ok {
			caught++
		}
		l.check("matrix case "+tc.helper, ok, "member %v (want %v), root verdict %q (want %q)", member, tc.member, verdict, tc.root)
	}
	t.Logf("matrix: %d of %d cases classified as required", caught, len(cases))
	l.check("matrix member count", len(planted.members) == len(head.members)+countMembers(cases),
		"matrix changed members by %d, want %d", len(planted.members)-len(head.members), countMembers(cases))
	drops := strings.Join(planted.fixtureDrops, " || ")
	l.check("planted skippable owner close", len(planted.fixtureDrops) == len(head.fixtureDrops)+4 && strings.Contains(drops, "zz_planted_close.go:10"),
		"not caught: %v", planted.fixtureDrops)
	for _, line := range []string{"8", "9", "10"} {
		l.check("planted owner Shutdown discard line "+line, strings.Contains(drops, "zz_planted_discard.go:"+line+" discards the error of s.Shutdown"),
			"not caught: %v", planted.fixtureDrops)
	}
	// The returned close its helper registers and a consumer calls explicitly
	// must be on the used side, and a cleanup root's walk must reach it.
	var explicitUse string
	for _, entry := range planted.returnedSplit {
		if parts := strings.SplitN(entry, " | ", 3); strings.HasSuffix(parts[0], ".mxReturnExplicitClose") {
			explicitUse = parts[1]
		}
	}
	explicitReached := false
	for _, r := range planted.returnedFns {
		if strings.HasSuffix(r.key, ".mxReturnExplicitClose") && planted.cleanupReached[functionID(r.fn)] {
			explicitReached = true
		}
	}
	for _, helper := range []string{"MxVariantPlainUsed", "MxVariantTestUsed"} {
		use := ""
		for _, entry := range planted.returnedSplit {
			if parts := strings.SplitN(entry, " | ", 3); strings.HasSuffix(parts[0], "."+helper) {
				use = parts[1]
			}
		}
		l.check("returned reporter consumed through one package variant "+helper, strings.HasPrefix(use, "used:"), "use %q", use)
	}
	rawList := strings.Join(planted.rawReleases, " || ")
	for name, helper := range map[string]string{"field": ".mxRawField$1", "boxed": ".mxRawBoxed$1", "returned": ".mxRawReturned$1", "method": ".mxRawFixture).Close"} {
		l.check("raw release beside the owner: "+name, strings.Contains(rawList, helper), "not found in %v", planted.rawReleases)
	}
	l.check("canonical owner close is not a raw release", !strings.Contains(rawList, "mxCanonicalAbort"), "flagged: %v", planted.rawReleases)
	l.check("explicitly called returned close is teardown", strings.HasPrefix(explicitUse, "used:") && explicitReached,
		"use %q, reached from cleanup %v", explicitUse, explicitReached)
	l.check("listed returned function reached from cleanup is a gap", strings.Contains(strings.Join(planted.returnedGaps, " "), "mxReturnCheckedAtCleanup"),
		"gaps %v", planted.returnedGaps)
	// An unresolved call in a returned function used as teardown is a gap;
	// in one no consumer uses as teardown it is listed for the code read.
	gaps, listed := strings.Join(planted.dynamicUnres, " "), strings.Join(planted.returnedUnres, " ")
	l.check("unresolved calls in returned functions split by use",
		strings.Contains(gaps, "mxReturnUnresolvedUsed$1") && !strings.Contains(listed, "mxReturnUnresolvedUsed$1") &&
			strings.Contains(listed, "mxReturnUnresolvedFree$1") && !strings.Contains(gaps, "mxReturnUnresolvedFree$1"),
		"gaps %v, listed %v", planted.dynamicUnres, planted.returnedUnres)
	// The named-return gate on the plant: the syntax enumerator runs over the
	// plant source alone; the field named like a function must be explained
	// by its typed resolution, and the function value must be on both sides.
	syntaxBase := t.TempDir()
	syntaxPlant := filepath.Join(syntaxBase, "pkg", "replication", "zz_cleanup_matrix_test.go")
	if err := os.MkdirAll(filepath.Dir(syntaxPlant), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(syntaxPlant, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	onlySyntax, onlyTyped, explained := reconcileNamedReturns(enumerateNamedReturns(t, syntaxBase), planted, "pkg/replication/zz_cleanup_matrix_test.go:")
	matched := 0
	for at := range planted.namedReturns {
		if strings.HasPrefix(at, "pkg/replication/zz_cleanup_matrix_test.go:") {
			matched++
		}
	}
	t.Logf("matrix: named-return plant: matched %d, explained %v, only syntax %v, only typed %v", matched, explained, onlySyntax, onlyTyped)
	l.check("named-return plant", len(onlySyntax)+len(onlyTyped) == 0 && len(explained) == 1 && strings.HasSuffix(explained[0], " field") && matched > 0,
		"matched %d, explained %v, only syntax %v, only typed %v", matched, explained, onlySyntax, onlyTyped)
}

// reconcileNamedReturns compares the syntax and typed named-return populations,
// restricted to positions starting with prefix. The syntax enumerator cannot
// tell a field or variable from a function with the same name; each site it
// alone lists is explained only when the type checker resolved that exact
// result to something other than a function.
func reconcileNamedReturns(syntax map[string]bool, c census, prefix string) (onlySyntax, onlyTyped, explained []string) {
	for at := range syntax {
		if !strings.HasPrefix(at, prefix) {
			continue
		}
		if _, ok := c.namedReturns[at]; !ok {
			if resolved, seen := c.returnResolved[at]; seen && resolved != "*types.Func" && resolved != "unresolved" {
				explained = append(explained, at+" "+resolved)
				continue
			}
			onlySyntax = append(onlySyntax, at)
		}
	}
	for at := range c.namedReturns {
		if strings.HasPrefix(at, prefix) && !syntax[at] {
			onlyTyped = append(onlyTyped, at)
		}
	}
	sort.Strings(onlySyntax)
	sort.Strings(onlyTyped)
	sort.Strings(explained)
	return onlySyntax, onlyTyped, explained
}

// ledger records the result of every control and gate, so a summary is read
// from one line that names every failure instead of from selected lines.
type ledger struct {
	t              *testing.T
	passed, failed []string
}

// check records a result; a failure is also reported to the test.
func (l *ledger) check(name string, ok bool, format string, args ...any) {
	l.t.Helper()
	if ok {
		l.passed = append(l.passed, name)
		return
	}
	message := fmt.Sprintf(format, args...)
	l.failed = append(l.failed, name+": "+message)
	l.t.Errorf("%s: %s", name, message)
}

// require is check for a gate whose failure stops the run.
func (l *ledger) require(name string, ok bool, format string, args ...any) {
	l.t.Helper()
	if ok {
		l.passed = append(l.passed, name)
		return
	}
	message := fmt.Sprintf(format, args...)
	l.failed = append(l.failed, name+": "+message)
	l.t.Fatalf("%s: %s", name, message)
}

func (l *ledger) report() {
	l.t.Logf("LEDGER: %d of %d controls and gates passed; FAILED %d: %s; passed: %s",
		len(l.passed), len(l.passed)+len(l.failed), len(l.failed), strings.Join(l.failed, " || "), strings.Join(l.passed, ", "))
}

func countMembers(cases []matrixCase) int {
	n := 0
	for _, tc := range cases {
		if tc.member {
			n++
		}
	}
	return n
}

// ownerDropScope retains every finding while separating the operation owners
// explicitly ruled OUT by the independent family audit. The normal adopted
// close/report implementation and its adapters remain checked. This is a
// current-source boundary decision, not a future completeness guarantee.
func ownerDropScope(t *testing.T, base string, drops []string) (in, out []string) {
	t.Helper()
	for _, drop := range drops {
		at := strings.Fields(drop)[0]
		colon := strings.LastIndex(at, ":")
		if colon < 0 {
			t.Fatalf("invalid owner drop source: %s", drop)
		}
		file := filepath.ToSlash(at[:colon])
		var line int
		if _, err := fmt.Sscanf(at[colon+1:], "%d", &line); err != nil {
			t.Fatal(err)
		}
		outside := file == "internal/testfixture/archive.go" || file == "internal/testfixture/goroutine.go" || file == "internal/testfixture/store_boundary.go"
		if file == "internal/testfixture/lifetime.go" {
			set := token.NewFileSet()
			syntax, err := parser.ParseFile(set, filepath.Join(base, filepath.FromSlash(file)), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, declaration := range syntax.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if ok && function.Name.Name == "disposeInvalidOwnerResource" && set.Position(function.Pos()).Line <= line && line <= set.Position(function.End()).Line {
					outside = true
				}
			}
		}
		if outside {
			out = append(out, drop)
		} else {
			in = append(in, drop)
		}
	}
	return in, out
}

func TestFixtureCleanupCensus(t *testing.T) {
	base := root(t)
	recall := os.Getenv("NORNIC_CLEANUP_CENSUS_RECALL")
	l := &ledger{t: t}
	defer l.report()
	c := run(t, nil)
	report(t, "census", c)

	enumerated := enumerateCleanups(t, base)
	var onlyEnumerated, onlyCensus []string
	for at := range enumerated {
		if !c.cleanupSites[at] {
			onlyEnumerated = append(onlyEnumerated, at)
		}
	}
	for at := range c.cleanupSites {
		if !enumerated[at] {
			onlyCensus = append(onlyCensus, at)
		}
	}
	sort.Strings(onlyEnumerated)
	sort.Strings(onlyCensus)
	t.Logf("reconciliation: untyped enumerator %d sites, census %d sites; only enumerator %d: %s; only census %d: %s",
		len(enumerated), len(c.cleanupSites), len(onlyEnumerated), strings.Join(onlyEnumerated, ", "), len(onlyCensus), strings.Join(onlyCensus, ", "))
	l.check("cleanup-site reconciliation", len(onlyEnumerated)+len(onlyCensus) == 0,
		"the census and the untyped enumerator disagree on the Cleanup population")
	l.check("unresolved teardown roots", len(c.unresolved) == 0, "%d teardown roots are unresolved", len(c.unresolved))
	parts := partitionCleanups(t, base)
	sum := 0
	var rows []string
	for category, n := range parts.categories {
		sum += n
		rows = append(rows, fmt.Sprintf("%s=%d", category, n))
	}
	sort.Strings(rows)
	t.Logf("partition of every raw .Cleanup( occurrence: raw %d, categories sum %d: %s", parts.raw, sum, strings.Join(rows, "; "))
	l.check("Cleanup partition sums", sum == parts.raw, "the partition does not account for every occurrence: %d of %d", sum, parts.raw)
	helperSites := parts.categories["one-argument Cleanup in a shared helper (census sites)"]
	l.check("partition helper sites", helperSites == len(enumerated), "partition helper sites %d differ from the enumerator's %d", helperSites, len(enumerated))
	l.check("unresolved dynamic teardown calls", len(c.dynamicUnres) == 0, "%d dynamic teardown calls have no callee", len(c.dynamicUnres))
	// Literal population (spec 2641fbde). The position sets must be equal:
	// a mismatch is a census defect and stops the run.
	syntaxLiterals, productLiterals := enumerateHelperLiterals(t, base)
	t.Logf("EXCLUDED by syntax: function literals in product (non-test) code %d", productLiterals)
	illTypedLiterals := 0
	for at := range syntaxLiterals {
		file := filepath.Join(base, filepath.FromSlash(at[:strings.LastIndex(at[:strings.LastIndex(at, ":")], ":")]))
		if c.illTypedFiles[file] {
			delete(syntaxLiterals, at)
			illTypedLiterals++
		}
	}
	var onlySyntax, onlySSA []string
	for at := range syntaxLiterals {
		if _, ok := c.literalSites[at]; !ok {
			onlySyntax = append(onlySyntax, at)
		}
	}
	verdicts := map[string]int{}
	var escaping []string
	for at, verdict := range c.literalSites {
		verdicts[verdict]++
		if !syntaxLiterals[at] {
			onlySSA = append(onlySSA, at)
		}
		if verdict == "escapes" {
			escaping = append(escaping, at)
		}
	}
	sort.Strings(onlySyntax)
	sort.Strings(onlySSA)
	sort.Strings(escaping)
	t.Logf("LITERAL POPULATION: syntax %d, SSA %d; only syntax %d: %s; only SSA %d: %s; ill-typed (NOR-338, syntax only) %d",
		len(syntaxLiterals), len(c.literalSites), len(onlySyntax), strings.Join(onlySyntax, ", "), len(onlySSA), strings.Join(onlySSA, ", "), illTypedLiterals)
	t.Logf("LITERAL VERDICTS: registered %d, escapes %d, local %d", verdicts["registered"], verdicts["escapes"], verdicts["local"])
	var synthesized []string
	for at := range c.synthesizedBodies {
		synthesized = append(synthesized, at)
	}
	sort.Strings(synthesized)
	t.Logf("SYNTHESIZED range-over-func loop bodies in test helpers (no FuncLit; not compared) %d: %s", len(synthesized), strings.Join(synthesized, ", "))
	t.Logf("ESCAPING literals %d: %s", len(escaping), strings.Join(escaping, " || "))
	t.Logf("REVIEW LIST (local literals in helpers returning a function or a named value) %d: %s", len(c.literalReview), strings.Join(c.literalReview, " || "))
	l.require("literal population", len(onlySyntax)+len(onlySSA) == 0,
		"census defect: the literal populations differ (only syntax %d, only SSA %d)", len(onlySyntax), len(onlySSA))
	syntaxNamed := enumerateNamedReturns(t, base)
	illTypedNamed := 0
	for at := range syntaxNamed {
		file := filepath.Join(base, filepath.FromSlash(at[:strings.LastIndex(at[:strings.LastIndex(at, ":")], ":")]))
		if c.illTypedFiles[file] {
			delete(syntaxNamed, at)
			illTypedNamed++
		}
	}
	onlySyntaxNamed, onlyTypedNamed, explainedNamed := reconcileNamedReturns(syntaxNamed, c, "")
	t.Logf("NAMED RETURN POPULATION: syntax %d, typed %d; only syntax %d: %s; only typed %d: %s; ill-typed (NOR-338, syntax only) %d",
		len(syntaxNamed), len(c.namedReturns), len(onlySyntaxNamed), strings.Join(onlySyntaxNamed, ", "), len(onlyTypedNamed), strings.Join(onlyTypedNamed, ", "), illTypedNamed)
	t.Logf("NAMED RETURN syntax-only sites the type checker resolves to a non-function %d: %s", len(explainedNamed), strings.Join(explainedNamed, ", "))
	t.Logf("RETURNED TEARDOWN VERDICTS %d: %s", len(c.returnedTeardown), strings.Join(c.returnedTeardown, " || "))
	sort.Strings(c.returnedReporters)
	t.Logf("RETURNED REPORTERS NOT USED AS TEARDOWN (code-read list) %d: %s", len(c.returnedReporters), strings.Join(c.returnedReporters, " || "))
	returnedUnres := unique(c.returnedUnres)
	t.Logf("UNRESOLVED dynamic calls in returned functions not used as teardown (code-read list) %d: %s", len(returnedUnres), strings.Join(returnedUnres, " || "))
	notUsed, byVerdict := 0, map[string]int{}
	for _, entry := range c.returnedSplit {
		parts := strings.SplitN(entry, " | ", 3)
		if parts[1] == "not used as teardown" {
			notUsed++
			byVerdict[strings.SplitN(parts[2], ":", 2)[0]]++
		}
	}
	t.Logf("RETURNED FUNCTION SPLIT: judged %d = used as teardown %d + not used %d; not used by verdict %v; returned values of shared helpers %d", len(c.returnedSplit), c.returnedUsed, notUsed, byVerdict, len(c.returnedFns))
	t.Logf("RETURNED FUNCTIONS (every judged one: use | verdict) %d: %s", len(c.returnedSplit), strings.Join(c.returnedSplit, " || "))
	t.Logf("CODE-READ PATHS %d: %s", len(c.returnedUnresPaths), strings.Join(unique(c.returnedUnresPaths), " || "))
	t.Logf("OWNER ADOPTIONS %d; RAW RELEASES (adopted by the owner and also handed out or called outside it) %d: %s", c.ownerAdoptions, len(c.rawReleases), strings.Join(c.rawReleases, " || "))
	t.Logf("VARIANT CLASSES (functions with more than one built variant) %d", c.variantClasses)
	t.Logf("ADOPTION EFFECT FACTS %d: %s", len(c.adoptionEffectFacts), strings.Join(c.adoptionEffectFacts, " || "))
	t.Logf("OWNED RELEASES OUTSIDE THE OWNER, ERROR DROPPED %d: %s", len(c.ownedDropped), strings.Join(c.ownedDropped, " || "))
	t.Logf("OWNED RELEASES OUTSIDE THE OWNER, ERROR USED (listed, not changed) %d: %s", len(c.ownedChecked), strings.Join(c.ownedChecked, " || "))
	t.Logf("RETURNED GAPS (listed as not teardown, reached from cleanup) %d: %s", len(c.returnedGaps), strings.Join(c.returnedGaps, " || "))
	l.check("returned-function split sums", c.returnedUsed+notUsed == len(c.returnedSplit),
		"census defect: the returned-function split does not sum (%d + %d != %d)", c.returnedUsed, notUsed, len(c.returnedSplit))
	l.check("returned-function gaps", len(c.returnedGaps) == 0, "%d returned functions listed as not teardown are reached from cleanup", len(c.returnedGaps))
	l.require("named-return population", len(onlySyntaxNamed)+len(onlyTypedNamed) == 0,
		"census defect: the named-return populations differ (only syntax %d, only typed %d)", len(onlySyntaxNamed), len(onlyTypedNamed))

	switch recall {
	case "4981":
		// The 4981 review's member.
		for _, name := range []string{"openTypedCountScope"} {
			l.check("recall 4981 "+name, c.isMember(name), "not found as a member")
		}
		runMatrix(t, c, l)
	case "ede2":
		// The ede2 and 6632 reviews' members, all still present at ede277392.
		for _, name := range []string{"DatabaseManager", "requireCollectionHNSWReady", "nor283OpenStore", "nor283Service", "newStatementFramingSession", "createEscapedRemoteAuthByteRef"} {
			l.check("recall ede2 "+name, c.isMember(name), "not found as a member")
		}
	case "7810":
		// v10 recall: head 781003d3a keeps owned resources released outside the owner.
		l.check("recall 7810 owned releases", len(c.ownedDropped) > 0, "no owned release outside the owner found")
		runMatrix(t, c, l)
	case "v11red":
		l.check("recall owned proxy adapter discards", strings.Contains(strings.Join(c.ownerReleaseDrops, " || "), "composite_remote_merge_key_release_test.go:241") && strings.Contains(strings.Join(c.ownerReleaseDrops, " || "), "composite_remote_merge_key_release_test.go:245"), "missing discarded proxy outcomes: %v", c.ownerReleaseDrops)
		// v11 recall on the v10 head rebased on de70: the ecc1 review's classes must be members.
		all := strings.Join(append(append([]string{}, c.ownedDropped...), c.ownedAmbiguous...), " || ")
		// Author: L4 lane (Codex), 1 Nov 2026. File-only recall accepted a
		// nearby close while missing the reviewed lifetime. Pin each release.
		for _, site := range []string{
			"pkg/replication/follower_mode_test.go:67:",
			"pkg/replication/residual_forwarded_decision_test.go:50:",
			"pkg/replication/residual_forwarded_decision_test.go:53:",
			"pkg/replication/transaction_wire_prototype_test.go:247:",
			"pkg/replication/transaction_wire_prototype_test.go:334:",
			"pkg/replication/transaction_wire_prototype_test.go:451:",
			"pkg/cypher/transaction_quotas_test.go:160:",
			"pkg/cypher/transaction_quotas_test.go:164:",
			"pkg/storage/pending_embedding_buffered_test.go:14:",
			"pkg/storage/pending_embedding_wal_fixture_test.go:16:",
			"pkg/cypher/query_node_scans_embeddings_test.go:32:",
			"pkg/cypher/materialized_path_access_regression_test.go:26:",
			"pkg/cypher/materialized_path_access_regression_test.go:64:",
			"pkg/search/build_write_queue_order_test.go:682:",
			"pkg/storage/badger_write_behind_recovery_test.go:70:",
		} {
			l.check("recall v11 "+site, strings.Contains(all, site), "not found as a member")
		}
		runMatrix(t, c, l)
	case "best-effort":
		// Governing audit: heuristic rows are search leads, not an exhaustive
		// acceptance population. Keep all output and the original strict mode.
		t.Logf("BEST-EFFORT OBSERVATIONS: adopted/mixed candidates %d/%d; stale/malformed ledger %d/%d; unexplained AST %d; raw-release candidates %d; legacy reporters %d. No zero-candidate/exhaustiveness claim.", len(c.ownedDropped), len(c.ownedAmbiguous), len(c.ledgerStale), len(c.ledgerProblems), len(c.enumUnexplained), len(c.rawReleases), len(c.members))
		in, out := ownerDropScope(t, base, c.fixtureDrops)
		t.Logf("OUT operation diagnostic drops retained (NOR-361/362/363/365): %v", out)
		l.check("normal adopted owner preserves its release outcomes", len(in) == 0, "dropped in-scope owner outcomes: %v", in)
		l.check("owned release adapters preserve Close outcomes", len(c.ownerReleaseDrops) == 0, "discarded owned Close outcomes: %v", c.ownerReleaseDrops)
		l.check("unchecked owner API stays removed", len(c.removedNames) == 0, "removed names present: %v", c.removedNames)
		l.check("Own call sites resolved", c.ownCalls > 0, "no Own call sites resolved")
		runMatrix(t, c, l)
	case "":
		l.check("v11 no release of an adopted lifetime outside the owner", len(c.ownedDropped)+len(c.ownedAmbiguous) == 0, "%d releases outside the owner (%d through a mixed variable)", len(c.ownedDropped)+len(c.ownedAmbiguous), len(c.ownedAmbiguous))
		l.check("v11 ledger has no stale or malformed entry", len(c.ledgerStale)+len(c.ledgerProblems) == 0, "stale %v, problems %v", c.ledgerStale, c.ledgerProblems)
		l.check("v11 every enumerator hit is in the census or the ledger", len(c.enumUnexplained) == 0, "%d unexplained enumerator hits", len(c.enumUnexplained))
		l.check("raw releases beside the owner", len(c.rawReleases) == 0, "%d releases escape beside the owner", len(c.rawReleases))
		// The old reporter-based helper model remains descriptive. Its scope
		// includes registry restoration, gate/join assertions and never-adopted
		// leases. The accepted gate is release identity of adopted lifetimes
		// above, reconciled against the source ledger and independent enumerator.
		t.Logf("LEGACY REPORTER-MODEL CANDIDATES (descriptive, not adopted-lifetime membership): %d; every source disposition is retained separately", len(c.members))
		l.check("owned release adapters preserve Close outcomes", len(c.ownerReleaseDrops) == 0, "discarded owned Close outcomes: %v", c.ownerReleaseDrops)
		l.check("owner discards and removed API", len(c.fixtureDrops) == 0 && len(c.removedNames) == 0,
			"a testfixture close error can be dropped or skipped (%d), or the unchecked API is back (%v)", len(c.fixtureDrops), c.removedNames)
		l.check("Own call sites resolved", c.ownCalls > 0, "no Own call sites resolved")
		runMatrix(t, c, l)
	default:
		t.Fatalf("unknown NORNIC_CLEANUP_CENSUS_RECALL %q", recall)
	}
}
