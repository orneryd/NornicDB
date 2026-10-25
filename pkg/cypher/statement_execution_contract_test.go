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
						case "comment":
							body = "RETURN 1 AS value /* " + keyword + " */"
						case "line-comment":
							body = "// " + keyword + "\nRETURN 1 AS value"
						case "parameter":
							body = "RETURN $" + keyword + " AS value"
						case "map-key":
							body = "RETURN {" + keyword + ":1} AS value"
						case "quoted-key":
							body = "RETURN {`" + keyword + "`:1} AS value"
						case "property":
							body = "RETURN $data." + keyword + " AS value"
						case "variable":
							body = "WITH 1 AS `" + keyword + "` RETURN `" + keyword + "` AS value"
						}
						exec, _, _ := newDynamicModeExecutor(t, route != "native")
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						call := "CALL apoc.cypher.run($body,$values) YIELD value RETURN value"
						query := call
						if route != "native" {
							query = "CALL { USE mode_comp.one " + call + " } RETURN value"
						}
						if route == "union" {
							query += " UNION ALL CALL { USE mode_comp.two " + call + " } RETURN value"
						}
						result, err := exec.Execute(ctx, query, map[string]interface{}{"body": body, "values": values})
						require.NoError(t, err)
						want := 1
						if route == "union" {
							want = 2
						}
						require.Len(t, result.Rows, want)
						if route == "native" {
							mode, known := ResolvedStatementQueryType(result.Metadata)
							require.True(t, known)
							require.Equal(t, "r", mode)
						}
						if explicit {
							_, err = exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
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
		"CALL { CREATE (:Contract) RETURN 1 AS x } RETURN x", "CALL gds.graph.drop('contract')",
	} {
		contract, err := readStatementExecutionContract(t.Context(), body)
		require.NoError(t, err, body)
		require.True(t, contract.writes, body)
		require.True(t, contract.transaction, body)
	}
	for _, body := range []string{"EXPLAIN CREATE (:Contract)", "RETURN {CREATE:1} AS x", "RETURN 'SET' AS x"} {
		contract, err := readStatementExecutionContract(t.Context(), body)
		require.NoError(t, err, body)
		require.False(t, contract.writes, body)
	}
}

// The entire production package is scanned. A new function, typed alias or
// admission helper cannot read the cache-only flag outside its quarantined
// cache/telemetry owners. The mode owner's dependency closure may not call any
// of those owners, including through another package-local helper.
func TestStatementExecutionAdmissionBoundary(t *testing.T) {
	files, err := os.ReadDir(".")
	require.NoError(t, err)
	functions := map[string]*ast.FuncDecl{}
	origins := map[string]string{}
	fields := map[string]map[string]string{}
	globals := map[string]string{}
	var declarations []*ast.GenDecl
	typeName := func(expr ast.Expr) string { return admissionTypeName(expr) }
	for _, file := range files {
		name := file.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		tree, err := parser.ParseFile(token.NewFileSet(), filepath.Clean(name), nil, 0)
		require.NoError(t, err)
		for _, declaration := range tree.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Body != nil {
				key := fn.Name.Name
				if fn.Recv != nil {
					key = typeName(fn.Recv.List[0].Type) + "." + key
				}
				functions[key], origins[key] = fn, name
			}
			if decl, ok := declaration.(*ast.GenDecl); ok {
				declarations = append(declarations, decl)
				for _, spec := range decl.Specs {
					if typ, ok := spec.(*ast.TypeSpec); ok {
						if structure, ok := typ.Type.(*ast.StructType); ok {
							fields[typ.Name.Name] = map[string]string{}
							for _, field := range structure.Fields.List {
								for _, name := range field.Names {
									fields[typ.Name.Name][name.Name] = typeName(field.Type)
								}
							}
						}
					}
				}
			}
		}
	}
	// Resolve concrete receivers rather than conflating unrelated Get/Analyze
	// methods. Imports cannot reach the package-private cache hint owners.
	var resolveType func(ast.Expr, map[string]string) string
	var resolveFunction func(ast.Expr, map[string]string) string
	resolveFunction = func(expr ast.Expr, variables map[string]string) string {
		switch value := expr.(type) {
		case *ast.Ident:
			if functions[value.Name] != nil {
				return value.Name
			}
		case *ast.SelectorExpr:
			key := resolveType(value.X, variables) + "." + value.Sel.Name
			if functions[key] != nil {
				return key
			}
		}
		return ""
	}
	resolveType = func(expr ast.Expr, variables map[string]string) string {
		switch value := expr.(type) {
		case *ast.Ident:
			if value := variables[value.Name]; value != "" {
				return value
			}
			return globals[value.Name]
		case *ast.SelectorExpr:
			return fields[resolveType(value.X, variables)][value.Sel.Name]
		case *ast.UnaryExpr:
			return resolveType(value.X, variables)
		case *ast.CompositeLit:
			return typeName(value.Type)
		case *ast.CallExpr:
			if fn := functions[resolveFunction(value.Fun, variables)]; fn != nil && fn.Type.Results != nil && len(fn.Type.Results.List) > 0 {
				return typeName(fn.Type.Results.List[0].Type)
			}
		}
		return ""
	}
	for pass := 0; pass < 3; pass++ {
		for _, declaration := range declarations {
			for _, spec := range declaration.Specs {
				if variable, ok := spec.(*ast.ValueSpec); ok {
					for index, name := range variable.Names {
						typ := typeName(variable.Type)
						if typ == "" && index < len(variable.Values) {
							typ = resolveType(variable.Values[index], nil)
						}
						if typ != "" {
							globals[name.Name] = typ
						}
					}
				}
			}
		}
	}
	hints := map[string]bool{}
	calls := map[string][]string{}
	for key, fn := range functions {
		variables := map[string]string{}
		declare := func(list *ast.FieldList) {
			if list != nil {
				for _, field := range list.List {
					for _, name := range field.Names {
						variables[name.Name] = typeName(field.Type)
					}
				}
			}
		}
		declare(fn.Recv)
		declare(fn.Type.Params)
		declare(fn.Type.Results)
		for pass := 0; pass < 3; pass++ {
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if assignment, ok := node.(*ast.AssignStmt); ok {
					for index, target := range assignment.Lhs {
						if name, ok := target.(*ast.Ident); ok && index < len(assignment.Rhs) {
							if typ := resolveType(assignment.Rhs[index], variables); typ != "" {
								variables[name.Name] = typ
							}
						}
					}
				}
				if variable, ok := node.(*ast.ValueSpec); ok {
					for index, name := range variable.Names {
						typ := typeName(variable.Type)
						if typ == "" && index < len(variable.Values) {
							typ = resolveType(variable.Values[index], variables)
						}
						if typ != "" {
							variables[name.Name] = typ
						}
					}
				}
				return true
			})
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if field, ok := node.(*ast.SelectorExpr); ok {
				switch field.Sel.Name {
				case "IsWriteQuery", "IsReadOnly", "HasCreate", "HasMerge", "HasDelete", "HasSet", "HasRemove", "HasForeach":
					hints[key] = true
				}
				if field.Sel.Name == "IsWriteQuery" {
					require.Contains(t, map[string]bool{"query_info.go": true, "cache_policy.go": true, "op_type.go": true}, origins[key], "cache-only write hint escaped into %s", key)
				}
			}
			// References, rather than just calls, retain function-valued aliases too.
			if expr, ok := node.(ast.Expr); ok {
				if target := resolveFunction(expr, variables); target != "" {
					calls[key] = append(calls[key], target)
				}
			}
			return true
		})
	}
	for _, root := range []string{"readStatementExecutionContract", "statementNeedsTransaction", "StorageExecutor.admitResolvedStatementWrite", "StorageExecutor.observeResolvedStatementMode", "StorageExecutor.fabricProcedureWritePlan", "QueryPermissionRequirements"} {
		require.Contains(t, functions, root)
		seen := map[string]bool{}
		var visit func(string, []string)
		visit = func(key string, path []string) {
			// This producer constructs fixed registry contracts; invoking handlers is
			// an execution boundary, not part of deciding their declared mode.
			if seen[key] || key == "ensureBuiltInProceduresRegistered" {
				return
			}
			seen[key] = true
			path = append(path, key)
			require.False(t, hints[key], "admission reaches cache-only hints: %s", strings.Join(path, " -> "))
			for _, next := range calls[key] {
				visit(next, path)
			}
		}
		visit(root, nil)
	}
}

func admissionTypeName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return admissionTypeName(value.X)
	case *ast.IndexExpr:
		return admissionTypeName(value.X)
	case *ast.IndexListExpr:
		return admissionTypeName(value.X)
	}
	return ""
}
