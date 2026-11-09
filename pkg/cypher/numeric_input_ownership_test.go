package cypher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// Existing raw switches outside generic input conversion have explicit, frozen
// contracts (wire encoding, storage layout, compiler specialization, graph
// structure). A new switch, case or change in one of those functions requires
// review of its boundary; merely placing it in an existing file cannot bypass
// this guard. Generic numeric/list consumers belong to ReadNumber/ReadList.
func TestNumericInputOwnership(t *testing.T) {
	type exception struct {
		Reason string `json:"reason"`
		SHA256 string `json:"sha256"`
	}
	data, err := os.ReadFile("../convert/numeric_input_exceptions.json")
	if err != nil {
		t.Fatal(err)
	}
	exceptions := map[string]exception{}
	if err := json.Unmarshal(data, &exceptions); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join("..", "..")
	count := 0
	for _, directory := range []string{"pkg/cypher", "apoc", "pkg/convert"} {
		err = filepath.WalkDir(filepath.Join(root, filepath.FromSlash(directory)), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "antlr" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files := token.NewFileSet()
			file, err := parser.ParseFile(files, path, source, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok || function.Body == nil {
					continue
				}
				key := relative + ":" + function.Name.Name
				if key == "pkg/convert/number.go:ReadNumber" || key == "pkg/convert/list.go:ReadList" {
					continue
				}
				raw := false
				ast.Inspect(function.Body, func(node ast.Node) bool {
					statement, ok := node.(*ast.TypeSwitchStmt)
					if !ok {
						return true
					}
					for _, item := range statement.Body.List {
						for _, kind := range item.(*ast.CaseClause).List {
							if rawInputType(kind) {
								raw = true
							}
						}
					}
					return true
				})
				if !raw {
					continue
				}
				count++
				start, end := files.Position(function.Pos()).Offset, files.Position(function.End()).Offset
				compact := strings.Map(func(r rune) rune {
					if unicode.IsSpace(r) {
						return -1
					}
					return r
				}, string(source[start:end]))
				digest := sha256.Sum256([]byte(compact))
				approved, ok := exceptions[key]
				if !ok || approved.Reason == "" || approved.SHA256 != hex.EncodeToString(digest[:]) {
					t.Errorf("%s: raw numeric/list switch bypasses shared input readers; unclassified or changed contract (sha256 %s)", key, hex.EncodeToString(digest[:]))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("inspected all production Cypher/APOC/converter functions; %d frozen nonconsumer switches", count)
}

func rawInputType(expression ast.Expr) bool {
	switch kind := expression.(type) {
	case *ast.ArrayType:
		return true
	case *ast.Ident:
		switch kind.Name {
		case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "byte", "rune":
			return true
		}
	}
	// Function signature dispatch and named graph values have separate contracts.
	return false
}
