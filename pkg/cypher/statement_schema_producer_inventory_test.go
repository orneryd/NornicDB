package cypher

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// This inventory deliberately includes whole-statement scanners and token
// consumers, whether or not they mention patterns or call a bracket reader.
// It is discovery evidence; admission is enforced separately by the owner
// boundary tests, not by declaring every discovered scanner harmless.
func TestStatementSchemaProducerInventory(t *testing.T) {
	type producer struct {
		File, Function string
		Line           int
		StringInput    bool
		StringOutput   bool
		SchemaSlots    []string
		Calls          []string
		RawInspection  bool
	}
	var records []producer
	for _, path := range relationshipProducerFiles(t) {
		name := filepath.Base(path)
		if strings.HasPrefix(name, "cypher_parser") || strings.HasPrefix(name, "cypher_lexer") || strings.HasPrefix(name, "cypherparser_") {
			continue
		}

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			record := producer{File: path, Function: fn.Name.Name, Line: fset.Position(fn.Pos()).Line}
			textType := func(node ast.Node) bool {
				found := false
				ast.Inspect(node, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && id.Name == "string" {
						found = true
					}
					return true
				})
				return found
			}
			if fn.Type.Params != nil {
				record.StringInput = textType(fn.Type.Params)
			}
			if fn.Type.Results != nil {
				record.StringOutput = textType(fn.Type.Results)
			}
			seenSlots, seenCalls := map[string]bool{}, map[string]bool{}
			ast.Inspect(fn, func(n ast.Node) bool {
				switch value := n.(type) {
				case *ast.Ident:
					word := strings.ToLower(value.Name)
					if (strings.Contains(word, "label") || strings.Contains(word, "reltype") || strings.Contains(word, "relationshiptype") || strings.Contains(word, "propert")) && !seenSlots[value.Name] {
						seenSlots[value.Name] = true
						record.SchemaSlots = append(record.SchemaSlots, value.Name)
					}
				case *ast.IndexExpr, *ast.SliceExpr, *ast.RangeStmt:
					record.RawInspection = true
				case *ast.CallExpr:
					callee := ""
					switch fun := value.Fun.(type) {
					case *ast.Ident:
						callee = fun.Name
					case *ast.SelectorExpr:
						callee = fun.Sel.Name
						if object, ok := fun.X.(*ast.Ident); ok {
							callee = object.Name + "." + callee
							if object.Name == "strings" || object.Name == "regexp" {
								record.RawInspection = true
							}
						}
					}
					if callee != "" && !seenCalls[callee] {
						seenCalls[callee] = true
						record.Calls = append(record.Calls, callee)
					}
				}
				return true
			})
			if len(record.SchemaSlots) > 0 || record.StringInput && record.StringOutput && record.RawInspection {
				records = append(records, record)
				encoded, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("SCHEMA_PRODUCER %s", encoded)
			}
		}
	}
	if len(records) == 0 {
		t.Fatal("empty producer inventory")
	}
	t.Logf("SCHEMA_PRODUCER_COUNT %d", len(records))
}
