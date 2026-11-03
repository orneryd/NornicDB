package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
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

// USE [GRAPH] g is read by the USE grammar owner; the statement grammar has no
// optional GRAPH keyword. A statement's write, transaction and procedure
// contract, and the permissions derived from it, do not depend on its graph
// references, so every USE spelling yields the contract of the statement.
func TestStatementExecutionContractReadsEveryUseSpelling(t *testing.T) {
	for _, body := range []string{
		"CREATE (:Contract)", "MATCH (n) DETACH DELETE n", "CALL { CREATE (:Contract) RETURN 1 AS x } RETURN x",
		"CALL { USE GRAPH g.one CREATE (:Contract) RETURN 1 AS x } RETURN x", "MATCH (n) RETURN n",
	} {
		want, err := readStatementExecutionContract(t.Context(), body)
		require.NoError(t, err, body)
		for _, use := range []string{"USE g ", "USE GRAPH g ", "USE GRAPH limit ", "USE GRAPH `call` ", "USE graph "} {
			query := use + body
			got, err := readStatementExecutionContract(t.Context(), query)
			require.NoError(t, err, query)
			expected := want
			// Effects/permissions are graph-independent; the new private routing
			// fact separately records a grammar USE for static atomic statements.
			expected.selectsDatabase = expected.transaction && !expected.dynamic
			require.Equal(t, expected, got, query)
			require.Equal(t, want.writes, QueryPermissionRequirements(t.Context(), query, RawSource).Write, query)
		}
	}
	_, err := readStatementExecutionContract(t.Context(), "USE GRAPH g MATCH (n RETURN n")
	require.Error(t, err)
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
	typeName := func(expr ast.Expr) string { return testfixture.ReceiverName(expr, testfixture.ReceiverBaseName) }
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
	require.NotContains(t, functions, "statementNeedsTransaction", "retired forwarding owner must not return")
	require.True(t, implicitExecutionUsesCanonicalTransactionDecision(functions["StorageExecutor.executeImplicitAsync"]), "implicit admission must bind and branch on the canonical transaction contract")
	for _, root := range []string{"readStatementExecutionContract", "StorageExecutor.admitResolvedStatementWrite", "StorageExecutor.observeResolvedStatementMode", "StorageExecutor.fabricProcedureWritePlan", "QueryPermissionRequirements"} {
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

// Only the transaction decision is an admission root. Later read projection
// legitimately consumes optimizer hints and is not part of this contract guard.
func implicitExecutionUsesCanonicalTransactionDecision(fn *ast.FuncDecl) bool {
	if fn == nil || fn.Body == nil {
		return false
	}
	var binding *ast.Object
	for _, statement := range fn.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) < 1 || len(assignment.Rhs) != 1 {
			continue
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok {
			continue
		}
		owner, ok := call.Fun.(*ast.Ident)
		if !ok || owner.Name != "readStatementExecutionContract" {
			continue
		}
		name, ok := assignment.Lhs[0].(*ast.Ident)
		if ok {
			binding = name.Obj
		}
	}
	if binding == nil {
		return false
	}
	for _, statement := range fn.Body.List {
		branch, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		field, ok := branch.Cond.(*ast.SelectorExpr)
		if !ok || field.Sel.Name != "transaction" {
			continue
		}
		value, ok := field.X.(*ast.Ident)
		if !ok || value.Obj != binding {
			continue
		}
		native, ordinary := false, false
		ast.Inspect(branch.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			native = native || method.Sel.Name == "executeNativeCoordinatedStatement"
			ordinary = ordinary || method.Sel.Name == "executeWithImplicitTransaction"
			return true
		})
		return native && ordinary
	}
	return false
}

func TestStatementExecutionAdmissionGuardRejectsPoisonedDecision(t *testing.T) {
	source, err := os.ReadFile("executor.go")
	require.NoError(t, err)
	for _, change := range []struct {
		name, old, new string
		want           bool
	}{
		{"actual", "", "", true},
		{"cache write hint", "if contract.transaction {", "if info.IsWriteQuery {", false},
		{"cache create hint", "if contract.transaction {", "if info.HasCreate {", false},
		{"different producer", "contract, err := readStatementExecutionContract(ctx, cypher)", "contract, err := otherProducer(ctx, cypher)", false},
	} {
		t.Run(change.name, func(t *testing.T) {
			text := string(source)
			if change.old != "" {
				require.Contains(t, text, change.old)
				text = strings.Replace(text, change.old, change.new, 1)
			}
			tree, err := parser.ParseFile(token.NewFileSet(), "executor.go", text, 0)
			require.NoError(t, err)
			var target *ast.FuncDecl
			for _, decl := range tree.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "executeImplicitAsync" {
					target = fn
				}
			}
			require.Equal(t, change.want, implicitExecutionUsesCanonicalTransactionDecision(target))
		})
	}
}

func TestStatementExecutionPoisonedCacheHintCannotBypassAtomicAdmission(t *testing.T) {
	for _, setting := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(setting.name, func(t *testing.T) {
			t.Cleanup(setting.setup())
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "admission_poison")
			executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			query := "CREATE (:AdmissionPoison) WITH 1 AS x RETURN x / $zero AS value"
			executor.analyzer.cacheMu.Lock()
			poison := &QueryInfo{IsReadOnly: true, IsWriteQuery: false, HasReturn: true}
			executor.analyzer.raw[query] = poison
			executor.analyzer.cacheMu.Unlock()
			require.Same(t, poison, executor.analyzer.Analyze(t.Context(), query), "exact query must hit the poisoned optimizer entry")
			result, err := executor.Execute(t.Context(), query, map[string]interface{}{"zero": int64(0)})
			require.ErrorContains(t, err, "zero")
			if result != nil {
				require.Empty(t, result.Rows, "error framing may retain columns, never successful staged rows")
			}
			nodes, err := store.GetNodesByLabel(t.Context(), "AdmissionPoison")
			require.NoError(t, err)
			require.Empty(t, nodes, "actual write must roll back despite poisoned optimizer hints")
		})
	}
}
