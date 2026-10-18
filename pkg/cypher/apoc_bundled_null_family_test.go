package cypher

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"

	apocpkg "github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Read the current wrapper's procedure exclusions: develop has not yet
// incorporated the built-in export exclusions from the separate #1422 PR.
func bundledAPOCPluginFunctions(t *testing.T) map[string]interface{} {
	t.Helper()
	require.NoError(t, apocpkg.Initialize(apocstorage.NewInMemoryStorage(), nil))
	file, err := parser.ParseFile(token.NewFileSet(), "../../apoc/plugin-src/apoc/apoc_plugin.go", nil, 0)
	require.NoError(t, err)
	procedures := map[string]bool{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "isAPOCProcedureName" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			clause, ok := node.(*ast.CaseClause)
			if ok {
				for _, expression := range clause.List {
					literal, ok := expression.(*ast.BasicLit)
					if ok && literal.Kind == token.STRING {
						name, err := strconv.Unquote(literal.Value)
						require.NoError(t, err)
						procedures[name] = true
					}
				}
			}
			return true
		})
	}
	require.NotEmpty(t, procedures)
	functions := map[string]interface{}{}
	for _, name := range apocpkg.List() {
		if procedures[name] {
			continue
		}
		info, ok := apocpkg.GetInfo(name)
		require.True(t, ok)
		functions[name] = info.Handler
	}
	return functions
}

func TestBundledAPOCMapNullFamily(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	testAPOCMapNullFamily(t, true)
}

func TestBundledAPOCDigestNullFamily(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	testApocUtilHashRecursiveNulls(t)
}

func TestBundledAPOCMapGetNullError(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_get_error"))
	// Maps.java#get dereferences containsKey for a null default and
	// getOrDefault otherwise; neither accepts a null source map.
	for _, value := range []string{"null", "{}", "1"} {
		_, err := exec.Execute(context.Background(), "RETURN apoc.map.get(null,'x',"+value+")", nil)
		requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
		require.ErrorContains(t, err, "apoc.map.get")
		require.ErrorContains(t, err, "`map` argument is null")
	}
	result, err := exec.Execute(context.Background(), "RETURN apoc.map.get({},'x',{}), apoc.map.get({x:{keep:1}},'x',{})", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{}, result.Rows[0][0])
	require.Equal(t, map[string]interface{}{"keep": int64(1)}, result.Rows[0][1])
}
