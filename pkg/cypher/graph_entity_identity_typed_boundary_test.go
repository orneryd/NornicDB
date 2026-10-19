package cypher

import (
	"bytes"
	"context"
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
	"runtime"
	"sort"
	"testing"
	"time"
)

// The compiler supplies the consumer population, rather than procedure/file
// names or parameter spelling. New readers, aliases, inferred local values,
// embedded fields and function-valued references all use the same type graph.
// An unresolved import/type is an incomplete inventory and fails closed.
func TestGraphEntityIdentityTypedBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-export", "-deps", "-json", ".")
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	output, err := command.Output()
	if err != nil {
		t.Fatalf("identity type inventory: %v\n%s", err, diagnostics.String())
	}
	type listedPackage struct {
		ImportPath, Dir, Export string
		GoFiles, CgoFiles       []string
	}
	exports := map[string]string{}
	var current listedPackage
	decoder := json.NewDecoder(bytes.NewReader(output))
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for decoder.More() {
		var listed listedPackage
		if err := decoder.Decode(&listed); err != nil {
			t.Fatal(err)
		}
		exports[listed.ImportPath] = listed.Export
		if filepath.Clean(listed.Dir) == filepath.Clean(directory) {
			current = listed
		}
	}
	if current.ImportPath == "" || len(current.GoFiles) == 0 {
		t.Fatal("identity inventory omitted the current production package")
	}
	positions := token.NewFileSet()
	var files []*ast.File
	for _, name := range append(current.GoFiles, current.CgoFiles...) {
		file, err := parser.ParseFile(positions, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	// A fresh reader with an unrelated name, an alias and inferred locals must
	// be caught by the same compiler graph. It exists only in this AST fixture,
	// never in production source or the executable test package.
	const negativeName = "identity-boundary-negative-control.generated"
	negative, err := parser.ParseFile(positions, negativeName,
		"package cypher\nimport storage \"github.com/orneryd/nornicdb/pkg/storage\"\n"+
			"type identityBoundaryAlias = storage.Node\n"+
			"type identityBoundaryEmbedded struct { *identityBoundaryAlias }\n"+
			"type identityBoundaryCopy storage.Node\n"+
			"func unrelatedIdentityReader(input *identityBoundaryAlias, relationship *storage.Edge) []string {\n"+
			"candidate := identityBoundaryEmbedded{input}; inferred := relationship; copied := identityBoundaryCopy(*input); candidate.Database = inferred.Database\n"+
			"return []string{string(candidate.ID), string(copied.ID), string(inferred.ID), string(inferred.StartNode), string(inferred.EndNode)}\n}\n"+
			"type identityBoundaryKeyAlias = graphEntityKey\n"+
			"func unrelatedKeyConversion(value string) graphEntityKey { return identityBoundaryKeyAlias(value) }\n", 0)
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, negative)
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
	configuration := types.Config{
		Importer: importer.ForCompiler(positions, "gc", func(path string) (io.ReadCloser, error) {
			export := exports[path]
			if export == "" {
				return nil, fmt.Errorf("identity inventory lacks compiler export for %s", path)
			}
			return os.Open(export)
		}),
		Sizes: types.SizesFor("gc", runtime.GOARCH),
	}
	checked, err := configuration.Check(current.ImportPath, positions, files, info)
	if err != nil {
		t.Fatalf("incomplete identity consumer type graph: %v", err)
	}
	// Field objects retain their declaring type through aliases, newly defined
	// copies and promoted embedded fields. Selection.Recv alone would miss an
	// embedded Node.ID because the receiver is the enclosing wrapper type.
	nativeFields := map[types.Object]string{}
	for _, imported := range checked.Imports() {
		if imported.Path() != "github.com/orneryd/nornicdb/pkg/storage" {
			continue
		}
		for _, name := range []string{"Node", "Edge"} {
			object := imported.Scope().Lookup(name)
			if object == nil {
				t.Fatalf("native identity type %s is absent", name)
			}
			structure, ok := types.Unalias(object.Type()).Underlying().(*types.Struct)
			if !ok {
				t.Fatalf("native identity type %s is not a struct", name)
			}
			for index := 0; index < structure.NumFields(); index++ {
				field := structure.Field(index)
				switch field.Name() {
				case "ID", "Database", "StartNode", "EndNode":
					nativeFields[field] = name + "." + field.Name()
				}
			}
		}
	}
	if len(nativeFields) != 6 {
		t.Fatalf("incomplete native identity field population: %d/6", len(nativeFields))
	}
	canonicalKey := checked.Scope().Lookup("graphEntityKey")
	if canonicalKey == nil {
		t.Fatal("canonical graph key type is absent")
	}
	var findings []string
	negativeFindings := 0
	negativeConversions := 0
	for _, file := range files {
		source := positions.Position(file.Pos()).Filename
		if filepath.Base(source) == "graph_entity_identity.go" {
			continue
		}
		parents := map[ast.Node]ast.Node{}
		var stack []ast.Node
		ast.Inspect(file, func(node ast.Node) bool {
			if node == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			if len(stack) > 0 {
				parents[node] = stack[len(stack)-1]
			}
			stack = append(stack, node)
			return true
		})
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			value := info.Types[call.Fun]
			if value.IsType() && types.Identical(types.Unalias(value.Type), canonicalKey.Type()) {
				if source == negativeName {
					negativeConversions++
				} else {
					findings = append(findings, fmt.Sprintf("%s: canonical graph key constructed outside identity owner", positions.Position(call.Pos())))
				}
			}
			return true
		})
		for selector, selection := range info.Selections {
			if selector.Pos() < file.Pos() || selector.End() > file.End() || selection.Kind() != types.FieldVal {
				continue
			}
			field, native := nativeFields[selection.Obj()]
			if !native {
				continue
			}
			owner := "<initializer>"
			for parent := parents[selector]; parent != nil; parent = parents[parent] {
				if function, ok := parent.(*ast.FuncDecl); ok {
					owner = function.Name.Name
					break
				}
			}
			// The sole deferred row-similarity adapter is the accepted key-domain
			// exception. Its projection/filter siblings receive no exemption.
			if filepath.Base(source) == "gds_node_similarity_rows.go" && owner == "similarityNodeID" && field == "Node.ID" {
				continue
			}
			if source == negativeName {
				negativeFindings++
				continue
			}
			findings = append(findings, fmt.Sprintf("%s: %s accesses native %s outside identity owner (receiver %s)", positions.Position(selector.Pos()), owner, field, selection.Recv()))
		}
	}
	sort.Strings(findings)
	if negativeFindings != 7 {
		t.Fatalf("typed identity guard missed its fresh reader or provenance write: caught %d/7 native identity field accesses", negativeFindings)
	}
	t.Log("negative control: all 7 native identity accesses caught through aliases, inferred locals, embedded fields, defined copies and provenance writes")
	if negativeConversions != 1 {
		t.Fatalf("typed identity guard missed canonical key conversion through an alias: caught %d/1", negativeConversions)
	}
	t.Log("negative control: canonical key conversion through an alias caught outside the owner")
	for _, finding := range findings {
		t.Log(finding)
	}
	if len(findings) != 0 {
		t.Fatalf("%d native identity consumers still bypass the owner", len(findings))
	}
	t.Logf("complete compiler inventory: %d production files, no native identity owner bypasses", len(files)-1)
}
