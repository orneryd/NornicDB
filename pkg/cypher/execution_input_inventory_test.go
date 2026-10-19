package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reconcile declarations against a repeatable extraction of owned handlers.
// Follow direct StorageExecutor calls to the two text execution boundaries;
// dynamic/plugin handlers have no declared purity and are never cacheable.
func TestExecutionInputRegistryInventory(t *testing.T) {
	packages, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	require.NoError(t, err)
	methods := map[string]*ast.FuncDecl{}
	for _, file := range packages["cypher"].Files {
		for _, declaration := range file.Decls {
			method, ok := declaration.(*ast.FuncDecl)
			if !ok || method.Recv == nil {
				continue
			}
			receiver, ok := method.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			if name, ok := receiver.X.(*ast.Ident); ok && name.Name == "StorageExecutor" {
				methods[method.Name.Name] = method
			}
		}
	}
	callees := func(body ast.Node, receiver string) []string {
		var names []string
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			object, ok := selector.X.(*ast.Ident)
			if ok && object.Name == receiver {
				names = append(names, selector.Sel.Name)
			}
			return true
		})
		return names
	}
	var reachesText func(string, map[string]bool) bool
	reachesText = func(name string, visited map[string]bool) bool {
		if name == "executeUserStatement" || name == "compileNodeWhereFilter" || name == "compileRelationshipWhereFilter" {
			return true
		}
		// The generic expression engine evaluates syntax already present in
		// the outer statement; walking through it reaches every query handler,
		// not a dependency of this procedure's evaluated argument values.
		// Re-interpretation of a string value (the legacy statistics config)
		// is audited separately below.
		if strings.HasPrefix(name, "evaluate") || strings.HasPrefix(name, "parse") {
			return false
		}
		if visited[name] {
			return false
		}
		visited[name] = true
		method := methods[name]
		if method == nil || len(method.Recv.List[0].Names) == 0 {
			return false
		}
		for _, callee := range callees(method.Body, method.Recv.List[0].Names[0].Name) {
			if reachesText(callee, visited) {
				return true
			}
		}
		return false
	}
	ensureBuiltInProceduresRegistered()
	var discovered []string
	for _, file := range packages["cypher"].Files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}
			function, ok := call.Fun.(*ast.Ident)
			if !ok || !strings.HasPrefix(function.Name, "register") {
				return true
			}
			nameLiteral, ok := call.Args[0].(*ast.BasicLit)
			if !ok || nameLiteral.Kind != token.STRING {
				return true
			}
			name, err := strconv.Unquote(nameLiteral.Value)
			require.NoError(t, err)
			handler, ok := call.Args[len(call.Args)-1].(*ast.FuncLit)
			if !ok {
				return true
			}
			for _, parameter := range handler.Type.Params.List {
				pointer, ok := parameter.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				kind, ok := pointer.X.(*ast.Ident)
				if !ok || kind.Name != "StorageExecutor" || len(parameter.Names) == 0 {
					continue
				}
				for _, callee := range callees(handler.Body, parameter.Names[0].Name) {
					if !reachesText(callee, map[string]bool{}) {
						continue
					}
					procedure, registered := globalProcedureRegistry.Get(name)
					require.True(t, registered, name)
					require.NotEmpty(t, procedure.Spec.ResultCache.CypherInputs, "%s reaches executable text through %s", name, callee)
					discovered = append(discovered, name)
					break
				}
			}
			return true
		})
	}
	require.GreaterOrEqual(t, len(discovered), 14)
	t.Logf("source-derived executable-text procedures (%d): %v", len(discovered), discovered)
	for _, name := range []string{"db.stats.collect", "db.stats.retrieve", "apoc.custom.installProcedure", "apoc.custom.declareProcedure"} {
		procedure, ok := globalProcedureRegistry.Get(name)
		require.True(t, ok, name)
		require.NotEmpty(t, procedure.Spec.ResultCache.CypherInputs, name)
		require.False(t, procedure.Spec.ResultCache.Deterministic, name)
	}
}
