package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

// Keep the public representation matrix tied to the actual engine catalog.
// A newly registered collection consumer must join the matrix at registration.
func TestListInputConsumerInventory(t *testing.T) {
	source, err := os.ReadFile("function_catalog.go")
	require.NoError(t, err)
	names := regexp.MustCompile(`name: "(apoc\.coll\.[^"]+)"`).FindAllSubmatch(source, -1)
	require.NotEmpty(t, names)
	queries := strings.Join(listinputs.Queries(), "\n")
	for _, name := range names {
		require.Contains(t, queries, string(name[1])+"(", "missing public representation coverage")
	}
	// These runtime-list adapters previously asserted a boxed list and silently
	// discarded other representations. Conversion belongs only to ListValue.
	for _, path := range []string{
		"apoc_collections.go", "functions_helpers.go", "functions_neo5.go", "functions_eval_math.go",
	} {
		files := token.NewFileSet()
		file, err := parser.ParseFile(files, path, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			assertion, ok := node.(*ast.TypeAssertExpr)
			if !ok {
				return true
			}
			list, ok := assertion.Type.(*ast.ArrayType)
			if !ok || list.Len != nil {
				return true
			}
			if element, ok := list.Elt.(*ast.InterfaceType); ok && len(element.Methods.List) == 0 {
				t.Errorf("%s: boxed-list assertion bypasses shared normalization", files.Position(assertion.Pos()))
			}
			return true
		})
	}
}
