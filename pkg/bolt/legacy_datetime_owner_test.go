package bolt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Guard the convergence: temporal variants must use the common DateTime codec
// instead of gaining another seconds calculation when a consumer is added.
func TestDateTimeCodecCanonicalOwner(t *testing.T) {
	files, err := filepath.Glob("packstream*.go")
	require.NoError(t, err)
	decoderCases := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			functionName := function.Name.Name
			if strings.HasPrefix(functionName, "encodePackStream") && strings.Contains(functionName, "DateTime") && !strings.Contains(functionName, "Local") && functionName != "encodePackStreamCypherDateTimeInto" {
				require.Zero(t, dateTimeOwnerCalls(function.Body, "encodePackStreamIntInto"), "%s must delegate datetime fields to the common encoder", functionName)
			}
			if functionName != "decodeStructureFields" {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				clause, ok := node.(*ast.CaseClause)
				if !ok {
					return true
				}
				for _, expression := range clause.List {
					literal, ok := expression.(*ast.BasicLit)
					if !ok {
						continue
					}
					value, _ := strconv.ParseInt(literal.Value, 0, 32)
					if value == 0x46 || value == 0x49 || value == 0x66 || value == 0x69 {
						require.Equal(t, 1, dateTimeOwnerCalls(clause, "decodePackStreamDateTime"), "datetime signatures must use the common decoder")
						decoderCases++
						break
					}
				}
				return true
			})
		}
	}
	require.Equal(t, 2, decoderCases, "both offset and named-zone signature pairs must be checked")
}

func dateTimeOwnerCalls(node ast.Node, name string) int {
	count := 0
	ast.Inspect(node, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if function, ok := call.Fun.(*ast.Ident); ok && function.Name == name {
				count++
			}
		}
		return true
	})
	return count
}
