package cyphertext_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const nameOwner = "github.com/orneryd/nornicdb/internal/cyphertext"

// Compiler identities recognize canonical name writers and transparent wrappers.
type emissionAtom struct {
	text, source string
	quoted       bool
	name         bool
}

type emissionGuard struct {
	info      *types.Info
	set       *token.FileSet
	bindings  map[types.Object][]ast.Expr
	quotes    map[*types.Func]bool
	functions map[*types.Func]*ast.FuncDecl
}

func (g *emissionGuard) object(expr ast.Expr) types.Object {
	switch e := expr.(type) {
	case *ast.Ident:
		return g.info.ObjectOf(e)
	case *ast.SelectorExpr:
		return g.info.ObjectOf(e.Sel)
	case *ast.IndexExpr:
		return g.object(e.X)
	case *ast.StarExpr:
		return g.object(e.X)
	case *ast.UnaryExpr:
		return g.object(e.X)
	}
	return nil
}

func (g *emissionGuard) target(expr ast.Expr) *types.Func {
	seen := map[types.Object]bool{}
	for {
		obj := g.object(expr)
		if fn, ok := obj.(*types.Func); ok {
			return fn
		}
		if obj == nil || seen[obj] {
			return nil
		}
		seen[obj] = true
		defs := g.bindings[obj]
		if len(defs) != 1 {
			return nil
		}
		expr = defs[0]
	}
}

func (g *emissionGuard) bind(target, value ast.Expr) {
	root := target
	for {
		index, ok := root.(*ast.IndexExpr)
		if !ok {
			break
		}
		root = index.X
	}
	if _, local := root.(*ast.Ident); !local {
		return
	} // field identity is always a source, never a package-global local
	if obj := g.object(target); obj != nil {
		g.bindings[obj] = append(g.bindings[obj], value)
	}
}

func (g *emissionGuard) collect(files []*ast.File) {
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			switch e := n.(type) {
			case *ast.FuncDecl:
				if f, ok := g.info.Defs[e.Name].(*types.Func); ok {
					g.functions[f] = e
				}
			case *ast.AssignStmt:
				for i, lhs := range e.Lhs {
					if i < len(e.Rhs) {
						g.bind(lhs, e.Rhs[i])
					}
				}
			case *ast.ValueSpec:
				for i, lhs := range e.Names {
					if i < len(e.Values) {
						g.bind(lhs, e.Values[i])
					}
				}
			case *ast.RangeStmt:
				if e.Value != nil {
					g.bind(e.Value, e.X)
				}
			}
			return true
		})
	}
	// Infer transparent wrappers from their typed callees and bodies. No
	// allowlist of quote-like names: replacing a wrapper with identity is red.
	for changed := true; changed; {
		changed = false
		for fn, decl := range g.functions {
			if g.quotes[fn] || decl.Body == nil || len(decl.Body.List) != 1 {
				continue
			}
			ret, ok := decl.Body.List[0].(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 1 {
				continue
			}
			call, ok := ret.Results[0].(*ast.CallExpr)
			if ok && g.isQuote(g.target(call.Fun)) {
				g.quotes[fn] = true
				changed = true
			}
		}
	}
}

func (g *emissionGuard) isQuote(fn *types.Func) bool {
	return fn != nil && (g.quotes[fn] || fn.Pkg() != nil && fn.Pkg().Path() == nameOwner && (fn.Name() == "QuoteName" || fn.Name() == "NameText"))
}

// This is a smoke check for literal Sprintf %s/%v and concatenation at
// common colon name slots. It does not cover IS, whitespace variations,
// formatter widths/indices, templates, builders, global initializers or arbitrary
// data flow. The emitter round-trip properties are the behavioral protection.
func (g *emissionGuard) value(e ast.Expr, seen map[types.Object]bool) []emissionAtom {
	if tv, ok := g.info.Types[e]; ok && tv.Value != nil && tv.Value.Kind() == constant.String {
		return []emissionAtom{{text: constant.StringVal(tv.Value)}}
	}
	switch x := e.(type) {
	case *ast.ParenExpr:
		return g.value(x.X, seen)
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			return append(g.value(x.X, seen), g.value(x.Y, seen)...)
		}
	case *ast.CallExpr:
		fn := g.target(x.Fun)
		if g.isQuote(fn) {
			return []emissionAtom{{quoted: true}}
		}
		if fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "fmt" && fn.Name() == "Sprintf" && len(x.Args) > 0 {
			tv := g.info.Types[x.Args[0]]
			if tv.Value == nil || tv.Value.Kind() != constant.String {
				break
			}
			format := constant.StringVal(tv.Value)
			var atoms []emissionAtom
			arg, start := 1, 0
			for i := 0; i < len(format); i++ {
				if format[i] != '%' {
					continue
				}
				if i+1 >= len(format) {
					return nil
				}
				verb := format[i+1]
				atoms = append(atoms, emissionAtom{text: format[start:i]})
				if verb == '%' {
					atoms = append(atoms, emissionAtom{text: "%"})
				} else if (verb == 's' || verb == 'v') && arg < len(x.Args) {
					atoms = append(atoms, g.value(x.Args[arg], seen)...)
					arg++
				} else {
					return nil
				} // other formatter syntax is outside this smoke check
				i++
				start = i + 1
			}
			return append(atoms, emissionAtom{text: format[start:]})
		}
		// Unknown calls may return complete syntax. They are outside this smoke
		// check; treating them as a decoded name would invent false positives.
		return []emissionAtom{{text: "\x01"}}
	}
	if obj := g.object(e); obj != nil && !seen[obj] {
		if len(g.bindings[obj]) > 1 {
			return []emissionAtom{{text: "\x01"}}
		}
		if defs := g.bindings[obj]; len(defs) == 1 {
			seen[obj] = true
			result := g.value(defs[0], seen)
			delete(seen, obj)
			return result
		}
	}
	name := false
	if obj := g.object(e); obj != nil {
		name = obj.Name() == "Type" || obj.Name() == "relType" || obj.Name() == "edgeType"
	}
	return []emissionAtom{{source: g.set.Position(e.Pos()).String(), name: name}}
}

var emissionNameSlot = regexp.MustCompile(`(?:\(|\[)[A-Za-z_0-9\x01]*:$`)

// Bare colon fragments also occur in address/presentation strings; only a
// conventional Type/relType/edgeType source is a smoke subject there. Pattern
// slots recognize any inserted value. Neither decision claims general coverage.
func emissionGaps(atoms []emissionAtom) []string {
	prefix := ""
	var gaps []string
	for _, atom := range atoms {
		if atom.source != "" || atom.quoted {
			if !atom.quoted && (prefix == ":" && atom.name || prefix == "[:" || prefix == "-[:" || emissionNameSlot.MatchString(prefix)) {
				gaps = append(gaps, atom.source)
			}
			prefix += "\x01"
		} else {
			prefix += atom.text
		}
	}
	return gaps
}
func (g *emissionGuard) scan(files []*ast.File) []string {
	var gaps []string
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if object, ok := g.info.Defs[fn.Name].(*types.Func); ok {
				sig := object.Type().(*types.Signature)
				if sig.Results().Len() == 1 && sig.Results().At(0).Type().String() == "github.com/orneryd/nornicdb/pkg/localization.Message" {
					continue
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if literal, ok := n.(*ast.CompositeLit); ok {
					if typ := g.info.TypeOf(literal); typ != nil && typ.String() == "github.com/orneryd/nornicdb/pkg/storage.ConstraintViolationError" {
						return false
					}
				}
				var atoms []emissionAtom
				switch e := n.(type) {
				case *ast.BinaryExpr:
					if e.Op == token.ADD {
						atoms = g.value(e, map[types.Object]bool{})
					}
				case *ast.CallExpr:
					if f := g.target(e.Fun); f != nil && f.Pkg() != nil && f.Pkg().Path() == "fmt" && f.Name() == "Sprintf" {
						atoms = g.value(e, map[types.Object]bool{})
					}
				}
				for _, source := range emissionGaps(atoms) {
					gaps = append(gaps, fmt.Sprintf("%s: unquoted name in common colon construction from %s", g.set.Position(n.Pos()), source))
				}
				return true
			})
		}
	}
	return gaps
}

func newEmissionGuard(set *token.FileSet, info *types.Info) *emissionGuard {
	return &emissionGuard{set: set, info: info, bindings: map[types.Object][]ast.Expr{}, quotes: map[*types.Func]bool{}, functions: map[*types.Func]*ast.FuncDecl{}}
}

// TestCypherNameEmissionBoundary is a smoke check for common literal Sprintf
// and colon concatenations. IS/whitespace, formatter widths, templates, builders,
// initializers and general data flow are outside it. Stored-data round trips
// across the emitter families, with production mutations, provide the protection.
func TestCypherNameEmissionBoundary(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "list", "-deps", "-export", "-json", "./...")
	cmd.Dir = root
	output, err := cmd.Output()
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			t.Fatalf("source discovery failed closed: %s", e.Stderr)
		}
		t.Fatal(err)
	}
	type sourcePackage struct {
		ImportPath, Export, Dir string
		GoFiles, CgoFiles       []string
	}
	var packages []sourcePackage
	exports := map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var p sourcePackage
		err := decoder.Decode(&p)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		exports[p.ImportPath] = p.Export
		if p.ImportPath == "github.com/orneryd/nornicdb" || strings.HasPrefix(p.ImportPath, "github.com/orneryd/nornicdb/") {
			if !strings.HasPrefix(p.ImportPath, "github.com/orneryd/nornicdb/testing/") {
				packages = append(packages, p)
			}
		}
	}
	if len(packages) < 20 {
		t.Fatalf("empty/incomplete production population: %d packages", len(packages))
	}
	filesChecked, objectsChecked := 0, 0
	for _, p := range packages {
		set := token.NewFileSet()
		var files []*ast.File
		for _, name := range append(p.GoFiles, p.CgoFiles...) {
			path := filepath.Join(p.Dir, name)
			file, err := parser.ParseFile(set, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, file)
		}
		if len(files) == 0 {
			continue
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		imp := importer.ForCompiler(set, "gc", func(path string) (io.ReadCloser, error) {
			if exports[path] == "" {
				return nil, fmt.Errorf("missing type export %s", path)
			}
			return os.Open(exports[path])
		})
		conf := types.Config{Importer: imp, FakeImportC: true}
		if _, err := conf.Check(p.ImportPath, set, files, info); err != nil {
			t.Fatalf("type analysis failed closed for %s: %v", p.ImportPath, err)
		}
		filesChecked += len(files)
		objectsChecked += len(info.Uses)
		g := newEmissionGuard(set, info)
		g.collect(files)
		for _, gap := range g.scan(files) {
			t.Error(gap)
		}
	}
	t.Logf("typed production population: %d packages, %d files, %d uses", len(packages), filesChecked, objectsChecked)
}

func TestCypherNameEmissionMutations(t *testing.T) {
	base := `package specimen; import("fmt"; "strings"); var _=fmt.Sprintf; var _ strings.Builder; type Edge struct { Type string }; type Node struct { Labels []string }; type Schema struct { Name string }`
	controls := map[string]string{
		"sprintf": `func planted(t string)string{return fmt.Sprintf("MATCH ()-[:%s]->()",t)}`,
		"concat":  `func planted(t string)string{return "MATCH ()-[:"+t+"]->()"}`,
		"colon":   `func planted(e Edge)string{return ":"+e.Type}`,
		"bracket": `func planted(t string)string{return "[:"+t+"]"}`,
		"arrow":   `func planted(t string)string{return "-[:"+t+"]->"}`,
	}
	keys := make([]string, 0, len(controls))
	for key := range controls {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, "planted.go", base+";"+controls[key], parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
			conf := types.Config{Importer: importer.Default()}
			if _, err := conf.Check("specimen", set, []*ast.File{file}, info); err != nil {
				t.Fatal(err)
			}
			if len(info.Uses) == 0 {
				t.Fatal("empty type provenance")
			}
			g := newEmissionGuard(set, info)
			g.collect([]*ast.File{file})
			if gaps := g.scan([]*ast.File{file}); len(gaps) == 0 {
				t.Fatal("type-checked unquoted mutation escaped")
			} else {
				t.Log(strings.Join(gaps, "\n"))
			}
		})
	}
}
