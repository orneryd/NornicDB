package storage

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
	"strings"
	"testing"
)

// This checks actual compiler field identities, including whole-statement
// scanners with no relationship brackets or recognizable helper names.
// QueryInfo's exported fields remain an explicit resolved-data API; product
// text adapters may not manufacture routing hints through that API.
func constraintMetadataOwnerViolations(sources map[string]string, imports types.Importer) []string {
	fset := token.NewFileSet()
	var files []*ast.File
	for name, source := range sources {
		file, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			return []string{err.Error()}
		}
		files = append(files, file)
	}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var violations []string
	config := types.Config{Importer: imports, Error: func(err error) { violations = append(violations, err.Error()) }}
	pkg, err := config.Check("github.com/orneryd/nornicdb/pkg/storage", fset, files, info)
	if err != nil {
		return violations
	}
	fields := map[types.Object]bool{}
	if object := pkg.Scope().Lookup("QueryInfo"); object != nil {
		shape, ok := object.Type().Underlying().(*types.Struct)
		if !ok {
			return []string{"routing metadata must retain its typed data boundary"}
		}
		for index := 0; index < shape.NumFields(); index++ {
			field := shape.Field(index)
			if field.Name() == "Labels" || field.Name() == "Properties" {
				fields[field] = true
			}
		}
	}
	// The constraint engine cannot replace an opaque compiled plan with names.
	if object := pkg.Scope().Lookup("contractPattern"); object != nil {
		owner, err := imports.Import("github.com/orneryd/nornicdb/internal/cyphertext")
		if err != nil {
			return []string{err.Error()}
		}
		if !types.Identical(types.Unalias(object.Type()), owner.Scope().Lookup("ConstraintPattern").Type()) {
			violations = append(violations, "constraint pattern must be the canonical compiled owner type")
		}
	}
	report := func(position token.Pos) {
		violations = append(violations, fset.Position(position).String()+": routing schema hints require an owned parsed plan, not a text-produced data field")
	}
	var slot func(ast.Expr) bool
	slot = func(expr ast.Expr) bool {
		switch value := expr.(type) {
		case *ast.SelectorExpr:
			if selection := info.Selections[value]; selection != nil {
				return fields[selection.Obj()]
			}
		case *ast.IndexExpr:
			return slot(value.X)
		}
		return false
	}
	empty := func(expr ast.Expr) bool {
		if id, ok := expr.(*ast.Ident); ok {
			return id.Name == "nil"
		}
		if value, ok := expr.(*ast.CompositeLit); ok {
			return len(value.Elts) == 0
		}
		if call, ok := expr.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "make" && len(call.Args) == 1 {
				return true
			}
		}
		return false
	}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.AssignStmt:
				for _, target := range value.Lhs {
					if slot(target) {
						report(target.Pos())
					}
				}
			case *ast.KeyValueExpr:
				if key, ok := value.Key.(*ast.Ident); ok && fields[info.Uses[key]] && !empty(value.Value) {
					report(value.Pos())
				}
			}
			return true
		})
	}
	return violations
}

func TestConstraintPatternOwnerBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-export", "-json", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	exports, sources := map[string]string{}, map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg struct {
			ImportPath, Export, Dir string
			GoFiles, CgoFiles       []string
		}
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		exports[pkg.ImportPath] = pkg.Export
		if pkg.ImportPath != "github.com/orneryd/nornicdb/pkg/storage" {
			continue
		}
		for _, name := range append(pkg.GoFiles, pkg.CgoFiles...) {
			path := filepath.Join(pkg.Dir, name)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			sources[path] = string(data)
		}
	}
	imports := importer.ForCompiler(token.NewFileSet(), "gc", func(path string) (io.ReadCloser, error) {
		if exports[path] == "" {
			return nil, fmt.Errorf("missing export %s", path)
		}
		return os.Open(exports[path])
	})
	if failures := constraintMetadataOwnerViolations(sources, imports); len(failures) != 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
	// Attack the actual routing field boundary with the original shape and
	// mutations that call a canonical identifier reader or hide an alias.
	base := `package storage; type QueryInfo struct { Labels []string; Properties map[string]interface{} }`
	for _, mutation := range []string{
		`func renamed(q string)*QueryInfo { out:=&QueryInfo{}; for _,word:=range strings.Fields(q) { if strings.HasPrefix(word,":"){out.Labels=append(out.Labels,word[1:])} }; return out }`,
		`func noBrackets(q string)*QueryInfo {return &QueryInfo{Labels:strings.Split(q,"|")}}`,
		`func alias(q string)*QueryInfo {out:=&QueryInfo{}; other:=out; other.Labels=[]string{q};return out}`,
		`func property(q string)*QueryInfo {out:=&QueryInfo{};out.Properties=map[string]interface{}{q:true};return out}`,
	} {
		failures := constraintMetadataOwnerViolations(map[string]string{"owner.go": base, "consumer.go": "package storage;import \"strings\";var _=strings.Fields;" + mutation}, imports)
		if len(failures) == 0 || !strings.Contains(strings.Join(failures, "\n"), "routing schema hints require") {
			t.Fatalf("metadata mutation escaped: %s\n%v", mutation, failures)
		}
	}
}
