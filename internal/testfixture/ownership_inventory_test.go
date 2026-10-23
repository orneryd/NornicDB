package testfixture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type allocationPkg struct {
	Dir, ImportPath, Name, Export, ForTest   string
	GoFiles, CompiledGoFiles, IgnoredGoFiles []string
	ImportMap                                map[string]string
	Error                                    *struct{ Err string }
}
type allocationDecl struct {
	key   string
	fn    *ast.FuncDecl
	file  string
	calls []allocationCall
}
type allocationCall struct {
	key, at              string
	registered, resource bool
}

func allocationObj(e ast.Expr, info *types.Info) types.Object {
	switch e := e.(type) {
	case *ast.Ident:
		return info.ObjectOf(e)
	case *ast.SelectorExpr:
		return info.ObjectOf(e.Sel)
	case *ast.IndexExpr:
		return allocationObj(e.X, info)
	case *ast.IndexListExpr:
		return allocationObj(e.X, info)
	case *ast.ParenExpr:
		return allocationObj(e.X, info)
	case *ast.CallExpr:
		return allocationObj(e.Fun, info)
	}
	return nil
}
func allocationKey(o types.Object) string {
	if o == nil || o.Pkg() == nil {
		return ""
	}
	if f, ok := o.(*types.Func); ok {
		sig := f.Type().(*types.Signature)
		if sig.Recv() != nil {
			return o.Pkg().Path() + "." + types.TypeString(sig.Recv().Type(), func(*types.Package) string { return "" }) + "." + o.Name()
		}
	}
	return o.Pkg().Path() + "." + o.Name()
}

var censusEngineInterfaces []*types.Interface
var censusStoreTypes []types.Type

func allocationResource(typ types.Type) bool {
	typ = types.Unalias(typ)
	for _, target := range censusEngineInterfaces {
		if types.Implements(typ, target) {
			return true
		}
	}
	for _, target := range censusStoreTypes {
		if types.Identical(typ, target) {
			return true
		}
	}
	return false
}

func TestStorageFixtureConstructorInventory(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "-test", "-export", "-compiled", "-json", "./...")
	cmd.Dir = filepath.Join("..", "..")
	moduleRoot, e := filepath.Abs(cmd.Dir)
	if e != nil {
		t.Fatal(e)
	}
	cmd.Dir = moduleRoot
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	raw, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, stderr.String())
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var pkgs []allocationPkg
	exports := map[string]string{}
	for {
		var p allocationPkg
		err = dec.Decode(&p)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		pkgs = append(pkgs, p)
		if p.Export != "" {
			exports[p.ImportPath] = p.Export
		}
	}
	decls := map[string]*allocationDecl{}
	seeds := map[string]bool{}
	allCalls := []struct {
		fn, file string
		c        allocationCall
	}{}
	factorySignatures := []string{}
	filePackages := map[string]string{}
	fileNames := map[string]string{}
	packageDirs := map[string]string{}
	boundaries := map[string]bool{}
	targetPkgs := 0
	sourceFiles := 0
	testFiles := 0
	ignored := []string{}
	testRefs := 0
	for _, p := range pkgs {
		if !strings.HasPrefix(p.ImportPath, "github.com/orneryd/nornicdb/") || strings.HasSuffix(p.ImportPath, ".test") {
			continue
		}
		// Prefer the compiler's test variant over duplicate ordinary source.
		if p.ForTest == "" {
			has := false
			for _, q := range pkgs {
				if q.ForTest == p.ImportPath && q.Name == p.Name {
					has = true
				}
			}
			if has {
				continue
			}
		}
		targetPkgs++
		fs := token.NewFileSet()
		var files []*ast.File
		sourcePaths := p.CompiledGoFiles
		if len(sourcePaths) == 0 {
			sourcePaths = p.GoFiles
		}
		for _, name := range sourcePaths {
			path := name
			if !filepath.IsAbs(path) {
				path = filepath.Join(p.Dir, path)
			}
			f, e := parser.ParseFile(fs, path, nil, 0)
			if e != nil {
				t.Fatal(e)
			}
			files = append(files, f)
			sourceFiles++
			if strings.HasSuffix(path, "_test.go") {
				testFiles++
			}
		}
		path := strings.Split(p.ImportPath, " [")[0]
		if mutation := os.Getenv("NORNIC_FIXTURE_INVENTORY_MUTATION"); mutation != "" && path == storageImport && p.Name == "storage" {
			body, ok := allocationMutations[mutation]
			if mutation == "all" {
				ok = true
				var names []string
				for name := range allocationMutations {
					names = append(names, name)
				}
				sort.Strings(names)
				for _, name := range names {
					body += strings.ReplaceAll(allocationMutations[name], "allocationMutation", "allocationMutation_"+name) + "\n"
				}
			}
			if !ok {
				t.Fatal("unknown mutation", mutation)
			}
			file, e := parser.ParseFile(fs, filepath.Join(p.Dir, "allocation_mutation.go"), "package storage\n"+body, 0)
			if e != nil {
				t.Fatal(e)
			}
			files = append(files, file)
		}
		for _, name := range p.IgnoredGoFiles {
			ignored = append(ignored, filepath.Join(p.Dir, name))
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		imp := importer.ForCompiler(fs, "gc", func(path string) (io.ReadCloser, error) {
			if mapped := p.ImportMap[path]; mapped != "" {
				path = mapped
			}
			if exports[path] == "" {
				return nil, fmt.Errorf("no export for %s", path)
			}
			return os.Open(exports[path])
		})
		conf := types.Config{Importer: imp, Sizes: types.SizesFor("gc", "amd64")}
		checked, e := conf.Check(path, fs, files, info)
		if e != nil {
			t.Fatalf("type checking %s: %v", p.ImportPath, e)
		}
		censusEngineInterfaces = nil
		censusStoreTypes = nil
		resourcePkg := checked
		if path != storageImport {
			resourcePkg, e = imp.Import(storageImport)
			if e != nil {
				t.Fatal(e)
			}
		}
		if object := resourcePkg.Scope().Lookup("Engine"); object != nil {
			if target, ok := object.Type().Underlying().(*types.Interface); ok {
				censusEngineInterfaces = append(censusEngineInterfaces, target.Complete())
			}
		}
		for _, name := range []string{"MemoryEngine", "BadgerEngine", "managedBadgerDB"} {
			if object := resourcePkg.Scope().Lookup(name); object != nil {
				censusStoreTypes = append(censusStoreTypes, types.NewPointer(object.Type()))
			}
		}
		if raw, e := imp.Import("github.com/dgraph-io/badger/v4"); e == nil {
			if object := raw.Scope().Lookup("DB"); object != nil {
				censusStoreTypes = append(censusStoreTypes, types.NewPointer(object.Type()))
			}
		}
		// The reviewer's typed call graph, with fixed-point function-value aliases.
		// Sets preserve all assigned targets; no fixed depth or declaration spelling.
		aliases := map[types.Object]map[types.Object]bool{}
		obj := func(e ast.Expr) types.Object { return allocationObj(e, info) }
		changed := true
		for changed {
			changed = false
			for _, file := range files {
				ast.Inspect(file, func(n ast.Node) bool {
					var left []*ast.Ident
					var right []ast.Expr
					switch n := n.(type) {
					case *ast.ValueSpec:
						left = n.Names
						right = n.Values
					case *ast.AssignStmt:
						for _, x := range n.Lhs {
							v, _ := x.(*ast.Ident)
							left = append(left, v)
						}
						right = n.Rhs
					default:
						return true
					}
					if len(left) != len(right) {
						return true
					}
					for i, l := range left {
						if l == nil {
							continue
						}
						target := info.ObjectOf(l)
						if aliases[target] == nil {
							aliases[target] = map[types.Object]bool{}
						}
						source := obj(right[i])
						candidates := aliases[source]
						if _, ok := source.(*types.Func); ok {
							candidates = map[types.Object]bool{source: true}
						}
						for f := range candidates {
							if !aliases[target][f] {
								aliases[target][f] = true
								changed = true
							}
						}
					}
					return true
				})
			}
		}
		for _, file := range files {
			filename := fs.Position(file.Pos()).Filename
			relative, _ := filepath.Rel(cmd.Dir, filename)
			if relative == "" {
				relative = filename
			}
			isTest := strings.HasSuffix(filename, "_test.go")
			filePackages[relative] = path
			fileNames[relative] = strings.TrimSuffix(p.Name, "_test")
			packageDirs[path] = p.Dir
			parents := map[ast.Node]ast.Node{}
			var stack []ast.Node
			ast.Inspect(file, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return false
				}
				if len(stack) > 0 {
					parents[n] = stack[len(stack)-1]
				}
				stack = append(stack, n)
				return true
			})
			for _, d := range file.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				o := info.Defs[fn.Name]
				key := allocationKey(o)
				rd := &allocationDecl{key: key, fn: fn, file: relative}
				decls[key] = rd
				sig := o.Type().(*types.Signature)
				res := false
				for i := 0; i < sig.Results().Len(); i++ {
					res = res || allocationResource(sig.Results().At(i).Type())
				}
				if res {
					factorySignatures = append(factorySignatures, fmt.Sprintf("%s %s test=%v %s", fs.Position(fn.Pos()), key, isTest, sig))
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					callee := obj(call.Fun)
					targets := aliases[callee]
					if _, ok := callee.(*types.Func); ok {
						targets = map[types.Object]bool{callee: true}
					}
					for target := range targets {
						k := allocationKey(target)
						if k == "" {
							continue
						}
						registered := false
						registered = allocationRegistered(call, parents, info)
						resource := false
						if typ := info.TypeOf(call.Fun); typ != nil {
							if signature, ok := typ.Underlying().(*types.Signature); ok {
								for i := 0; i < signature.Results().Len(); i++ {
									resource = resource || allocationResource(signature.Results().At(i).Type())
								}
							}
						}
						if fn.Name.Name == "TestMain" && (k == fixtureImport+".Run" || k == fixtureImport+".StoreBoundary.Check" || k == fixtureImport+".*StoreBoundary.Check") {
							boundaries[p.Dir] = true
						}
						at := fs.Position(call.Pos())
						c := allocationCall{k, fmt.Sprintf("%s:%d", relative, at.Line), registered, resource}
						rd.calls = append(rd.calls, c)
						allCalls = append(allCalls, struct {
							fn, file string
							c        allocationCall
						}{key, relative, c})
						if strings.HasPrefix(k, "github.com/dgraph-io/badger/") && (strings.HasSuffix(k, ".Open") || strings.HasSuffix(k, ".OpenManaged")) {
							seeds[k] = true
						}
						if isTest {
							testRefs++
						}
					}
					return true
				})
			}
		}
	}
	// Transitive closure over type-resolved function objects, including helper splits and aliases.
	reaches := map[string]bool{}
	for k := range seeds {
		reaches[k] = true
	}
	changed := true
	for changed {
		changed = false
		for k, d := range decls {
			if reaches[k] {
				continue
			}
			for _, c := range d.calls {
				if reaches[c.key] {
					reaches[k] = true
					changed = true
					break
				}
			}
		}
	}
	// This generated fixture is an allocation population, not a lifetime policy
	// or a hand-maintained factory allowlist. Runtime boundaries decide leaks.
	inventory := []string{}
	for key, d := range decls {
		if !strings.HasSuffix(d.file, "_test.go") && reaches[key] {
			inventory = append(inventory, key)
			counts := map[string]int{}
			for _, call := range d.calls {
				if reaches[call.key] {
					counts[call.key]++
				}
			}
			for target, count := range counts {
				inventory = append(inventory, fmt.Sprintf("%s -> %s calls=%d", key, target, count))
			}
		}
	}
	sort.Strings(inventory)
	actual := strings.Join(inventory, "\n") + "\n"
	const population = "testdata/store_allocations.txt"
	if os.Getenv("NORNIC_UPDATE_ALLOCATION_INVENTORY") == "1" {
		if e := os.MkdirAll(filepath.Dir(population), 0755); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(population, []byte(actual), 0644); e != nil {
			t.Fatal(e)
		}
	} else {
		expected, e := os.ReadFile(population)
		if e != nil {
			t.Fatal(e)
		}
		known := map[string]bool{}
		for _, key := range strings.Split(string(expected), "\n") {
			known[key] = true
		}
		for _, key := range inventory {
			if !known[key] {
				t.Errorf("unreviewed typed allocation path: %s", key)
			}
		}
		if actual != string(expected) {
			t.Error("typed allocation population changed; regenerate and review testdata/store_allocations.txt")
		}
	}
	// Allocation identity determines the runtime-check population, not whether
	// a returned handle owns, borrows or replaces the receiver's existing store.
	// Primitive constructor registration has its separate coarse entry lint.
	// TestMain observes actual stores regardless of those ownership modes.
	required := map[string]string{}
	for _, a := range allCalls {
		if !strings.HasSuffix(a.file, "_test.go") {
			continue
		}
		if reaches[a.c.key] || a.c.resource {
			required[packageDirs[filePackages[a.file]]] = fileNames[a.file]
		}
	}
	if os.Getenv("NORNIC_GENERATE_STORE_BOUNDARIES") == "1" {
		for dir, name := range required {
			if boundaries[dir] {
				continue
			}
			source := "package " + name + "\nimport(\"os\";\"testing\";fixtureowner \"" + fixtureImport + "\")\nfunc TestMain(m *testing.M){os.Exit(fixtureowner.Run(m,\"" + name + "\"))}\n"
			if e := os.WriteFile(filepath.Join(dir, "store_boundary_test.go"), []byte(source), 0644); e != nil {
				t.Fatal(e)
			}
			boundaries[dir] = true
		}
	}
	var names []string
	for dir, name := range required {
		relative, _ := filepath.Rel(moduleRoot, dir)
		names = append(names, filepath.ToSlash(relative))
		if !boundaries[dir] {
			t.Errorf("%s (%s): allocation-reachable tests require the runtime store boundary in TestMain", relative, name)
		}
	}
	sort.Strings(names)
	for _, key := range inventory {
		t.Log("PRODUCTION_OPEN_REACHABLE " + key)
	}
	report, _ := json.Marshal(map[string]any{"packages": targetPkgs, "files": sourceFiles, "testFiles": testFiles, "resolvedTestCalls": testRefs, "allocationPaths": len(inventory), "boundaryPackages": names, "ignoredFiles": ignored})
	t.Log("ALLOCATION_CENSUS " + string(report))
}
func allocationRegistered(call *ast.CallExpr, parents map[ast.Node]ast.Node, info *types.Info) bool {
	var expression ast.Expr = call
	for {
		parent, ok := parents[expression].(*ast.ParenExpr)
		if !ok {
			break
		}
		expression = parent
	}
	wrapper, ok := parents[expression].(*ast.CallExpr)
	if !ok {
		return false
	}
	key := allocationKey(allocationObj(wrapper.Fun, info))
	if (key == fixtureImport+".Track" || key == fixtureImport+".TrackProcess") && len(wrapper.Args) >= 2 && wrapper.Args[1] == expression {
		return true
	}
	if _, ok := wrapper.Fun.(*ast.CallExpr); !ok {
		return false
	}
	return len(wrapper.Args) == 1 && wrapper.Args[0] == expression && (key == fixtureImport+".TrackResult" || key == fixtureImport+".OwnResult" || key == fixtureImport+".OwnResult3")
}

var allocationMutations = map[string]string{
	"concrete":      "func allocationMutation() *MemoryEngine{return NewMemoryEngine()}",
	"interface":     "func allocationMutation() Engine{return NewMemoryEngine()}",
	"alias":         "type allocationMutationAlias = *MemoryEngine;func allocationMutation() allocationMutationAlias{return NewMemoryEngine()}",
	"second_result": "func allocationMutation()(int,*MemoryEngine){return 0,NewMemoryEngine()}",
	"method":        "type allocationMutationReceiver struct{};func(allocationMutationReceiver)allocationMutation()*MemoryEngine{return NewMemoryEngine()}",
	"closure":       "func allocationMutation()Engine{factory:=func()Engine{return NewMemoryEngine()};return factory()}",
}

func TestFixtureInventoryRejectsAllocatorMutations(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestFixtureInventoryMutationChild$", "-test.count=1", "-test.v")
	command.Env = append(os.Environ(), "NORNIC_FIXTURE_INVENTORY_MUTATION=all", "NORNIC_UPDATE_ALLOCATION_INVENTORY=0", "NORNIC_GENERATE_STORE_BOUNDARIES=0")
	output, err := command.CombinedOutput()
	for _, name := range []string{"concrete", "interface", "alias", "second_result", "method", "closure"} {
		t.Run(name, func(t *testing.T) {
			matched := false
			for _, line := range strings.Split(string(output), "\n") {
				if strings.Contains(line, "unreviewed typed allocation path:") && strings.Contains(line, "allocationMutation_"+name) {
					matched = true
				}
			}
			if err == nil || !matched {
				t.Fatalf("allocator mutation escaped the actual typed population (%s): %v\n%s", name, err, output)
			}
		})
	}
}
func TestFixtureInventoryMutationChild(t *testing.T) {
	if os.Getenv("NORNIC_FIXTURE_INVENTORY_MUTATION") == "" {
		return
	}
	TestStorageFixtureConstructorInventory(t)
}
