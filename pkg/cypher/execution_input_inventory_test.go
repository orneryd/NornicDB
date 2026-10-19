package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reconcile live registry handlers against owned source, including spec factories
// and aliases. Casting an argument value back to executable text introduces a
// dependency even when the handler's mode already forbids result caching.
func TestExecutionInputRegistryInventory(t *testing.T) {
	positions := token.NewFileSet()
	packages, err := parser.ParseDir(positions, ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	require.NoError(t, err)
	methods := map[string]*ast.FuncDecl{}
	literals := map[string][]*ast.FuncLit{}
	for filename, file := range packages["cypher"].Files {
		ast.Inspect(file, func(node ast.Node) bool {
			if literal, ok := node.(*ast.FuncLit); ok {
				literals[filepath.Base(filename)] = append(literals[filepath.Base(filename)], literal)
			}
			return true
		})
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
	// Source-derived string-value adapters. Minimum argument count separates
	// statistics collect/retrieve config from status/clear/stop and fixed calls.
	rawText := map[string]int{}
	for name, method := range methods {
		variables := map[string]int{}
		ast.Inspect(method.Body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Rhs) != 1 || len(assignment.Lhs) == 0 {
				return true
			}
			cast, ok := assignment.Rhs[0].(*ast.TypeAssertExpr)
			if !ok {
				return true
			}
			kind, ok := cast.Type.(*ast.Ident)
			variable, named := assignment.Lhs[0].(*ast.Ident)
			if !ok || kind.Name != "string" || !named {
				return true
			}
			minimum := 0
			if indexed, ok := cast.X.(*ast.IndexExpr); ok {
				if literal, ok := indexed.Index.(*ast.BasicLit); ok && literal.Kind == token.INT {
					if index, err := strconv.Atoi(literal.Value); err == nil {
						minimum = index + 1
					}
				}
			}
			variables[variable.Name] = minimum
			return true
		})
		ast.Inspect(method.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !(strings.HasPrefix(selector.Sel.Name, "evaluate") || selector.Sel.Name == "executeInternal" || selector.Sel.Name == "Eval") {
				return true
			}
			for _, argument := range call.Args {
				if variable, ok := argument.(*ast.Ident); ok {
					if minimum, valueText := variables[variable.Name]; valueText {
						rawText[name] = minimum
					}
				}
			}
			return true
		})
	}
	var reachesText func(string, int, map[string]bool) bool
	reachesText = func(name string, maxArgs int, visited map[string]bool) bool {
		if name == "executeUserStatement" || name == "compileNodeWhereFilter" || name == "compileRelationshipWhereFilter" {
			return true
		}
		// Generic engines consume syntax already present in the outer text.
		// Do not walk their downstream fanout as a handler dependency.
		if strings.HasPrefix(name, "evaluate") || strings.HasPrefix(name, "parse") || name == "executeInternal" || name == "executeWithoutTransaction" {
			return false
		}
		if minimum, adapter := rawText[name]; adapter && (maxArgs < 0 || maxArgs >= minimum) {
			return true
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
			if reachesText(callee, maxArgs, visited) {
				return true
			}
		}
		return false
	}
	ensureBuiltInProceduresRegistered()
	var discovered []string
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		globalProcedureRegistry.mu.RLock()
		procedure := globalProcedureRegistry.builtins[spec.Name]
		globalProcedureRegistry.mu.RUnlock()
		function := runtime.FuncForPC(reflect.ValueOf(procedure.Handler).Pointer())
		source, line := function.FileLine(function.Entry())
		var handler *ast.FuncLit
		for _, literal := range literals[filepath.Base(source)] {
			if positions.Position(literal.Pos()).Line <= line && positions.Position(literal.End()).Line >= line {
				if handler == nil || literal.End()-literal.Pos() < handler.End()-handler.Pos() {
					handler = literal
				}
			}
		}
		require.NotNil(t, handler, "%s: resolve actual registered handler at %s:%d", spec.Name, source, line)
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
				if !reachesText(callee, spec.MaxArgs, map[string]bool{}) {
					continue
				}
				require.NotEmpty(t, spec.ResultCache.CypherInputs, "%s interprets text through %s", spec.Name, callee)
				discovered = append(discovered, spec.Name)
				break
			}
		}
	}
	require.GreaterOrEqual(t, len(discovered), 19)
	t.Logf("source-derived executable-text procedures (%d): %v; string-value adapters: %v", len(discovered), discovered, rawText)
	// Installation defers interpretation until a later custom procedure CALL.
	for _, name := range []string{"apoc.custom.installProcedure", "apoc.custom.declareProcedure"} {
		procedure, ok := globalProcedureRegistry.Get(name)
		require.True(t, ok, name)
		require.NotEmpty(t, procedure.Spec.ResultCache.CypherInputs, name)
		require.False(t, procedure.Spec.ResultCache.Deterministic, name)
	}
}
