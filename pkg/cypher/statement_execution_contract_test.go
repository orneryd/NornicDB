package cypher

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementExecutionIgnoresLiteralContents(t *testing.T) {
	for _, keyword := range []string{"CREATE", "MERGE", "SET", "DELETE", "REMOVE", "FOREACH", "CALL"} {
		for _, form := range []string{"string", "comment", "line-comment", "parameter", "map-key", "quoted-key", "property", "variable"} {
			for _, route := range []string{"native", "fabric", "union"} {
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/explicit=%v", keyword, form, route, explicit), func(t *testing.T) {
						body := "RETURN '" + keyword + "' AS value"
						values := map[string]interface{}{keyword: keyword, "data": map[string]interface{}{keyword: keyword}}
						switch form {
						case "comment": body = "RETURN 1 AS value /* " + keyword + " */"
						case "line-comment": body = "// " + keyword + "\nRETURN 1 AS value"
						case "parameter": body = "RETURN $" + keyword + " AS value"
						case "map-key": body = "RETURN {" + keyword + ":1} AS value"
						case "quoted-key": body = "RETURN {`" + keyword + "`:1} AS value"
						case "property": body = "RETURN $data." + keyword + " AS value"
						case "variable": body = "WITH 1 AS `" + keyword + "` RETURN `" + keyword + "` AS value"
						}
						exec, _, _ := newDynamicModeExecutor(t, route != "native")
						ctx := context.Background()
						if explicit { _, err := exec.Execute(ctx, "BEGIN", nil); require.NoError(t, err) }
						call := "CALL apoc.cypher.run($body,$values) YIELD value RETURN value"
						query := call
						if route != "native" { query = "CALL { USE mode_comp.one " + call + " } RETURN value" }
						if route == "union" { query += " UNION ALL CALL { USE mode_comp.two " + call + " } RETURN value" }
						result, err := exec.Execute(ctx, query, map[string]interface{}{"body": body, "values": values})
						require.NoError(t, err)
						want := 1; if route == "union" { want = 2 }; require.Len(t, result.Rows, want)
						if route == "native" { mode, known := ResolvedStatementQueryType(result.Metadata); require.True(t, known); require.Equal(t, "r", mode) }
						if explicit { _, err = exec.Execute(ctx, "COMMIT", nil); require.NoError(t, err) }
					})
				}
			}
		}
	}
}

func TestStatementExecutionClauseContracts(t *testing.T) {
	for _, body := range []string{
		"CREATE (:Contract)", "MERGE (:Contract)", "MATCH (n) SET n.v=1", "MATCH (n) DELETE n",
		"MATCH (n) DETACH DELETE n", "MATCH (n) REMOVE n.v", "FOREACH (x IN [1] | CREATE (:Contract))",
		"CALL { CREATE (:Contract) RETURN 1 AS x } RETURN x", "CALL apoc.create.node(['Contract'],{})",
	} {
		contract, err := readStatementExecutionContract(body); require.NoError(t, err, body)
		require.True(t, contract.writes, body); require.True(t, contract.transaction, body)
	}
	for _, body := range []string{"EXPLAIN CREATE (:Contract)", "RETURN {CREATE:1} AS x", "RETURN 'SET' AS x"} {
		contract, err := readStatementExecutionContract(body); require.NoError(t, err, body); require.False(t, contract.writes, body)
	}
}

// The entire production package is scanned. A new function, typed alias or
// admission helper cannot read the cache-only flag outside its quarantined
// cache/telemetry owners. The mode owner's dependency closure may not call any
// of those owners, including through another package-local helper.
func TestStatementExecutionAdmissionBoundary(t *testing.T) {
	files, err := os.ReadDir("."); require.NoError(t, err)
	trees := map[string]*ast.FuncDecl{}
	hints := map[string]bool{}
	calls := map[string][]string{}
	for _, file := range files {
		name := file.Name(); if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") { continue }
		tree, err := parser.ParseFile(token.NewFileSet(), filepath.Clean(name), nil, 0); require.NoError(t, err)
		for _, declaration := range tree.Decls {
			fn, ok := declaration.(*ast.FuncDecl); if !ok || fn.Body == nil { continue }
			trees[fn.Name.Name] = fn
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if field, ok := node.(*ast.SelectorExpr); ok && field.Sel.Name == "IsWriteQuery" {
					hints[fn.Name.Name] = true
					require.Contains(t, map[string]bool{"query_info.go":true, "cache_policy.go":true, "op_type.go":true}, name,
						"cache-only write hint escaped into %s.%s", name, fn.Name.Name)
				}
				if call, ok := node.(*ast.CallExpr); ok {
					switch callee := call.Fun.(type) { case *ast.Ident: calls[fn.Name.Name] = append(calls[fn.Name.Name], callee.Name); case *ast.SelectorExpr: calls[fn.Name.Name] = append(calls[fn.Name.Name], callee.Sel.Name) }
				}
				return true
			})
		}
	}
	for _, root := range []string{"readStatementExecutionContract", "statementNeedsTransaction", "admitResolvedStatementWrite", "observeResolvedStatementMode", "fabricProcedureWritePlan", "QueryPermissionRequirements"} {
		require.Contains(t, trees, root)
		seen := map[string]bool{}
		var visit func(string)
		visit = func(name string) {
			if seen[name] || name == "ensureBuiltInProceduresRegistered" { return }; seen[name] = true
			require.False(t, hints[name], "%s reaches cache-only write hints through %s", root, name)
			for _, next := range calls[name] { visit(next) }
		}
		visit(root)
	}
}
