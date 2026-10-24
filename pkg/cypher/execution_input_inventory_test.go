package cypher

import (
	"go/ast"
	"go/token"
	"go/types"
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
	positions, files, info := relationshipBoundarySource(t, "")
	// Reuse invocation edges: constructing or registering a handler does not
	// execute its body. Receiver names alone cannot distinguish those events.
	functions, _ := relationshipBoundaryGraph(files, positions, info)
	methods := map[*types.Func]*ast.FuncDecl{}
	literals := map[string][]*ast.FuncLit{}
	for _, file := range files {
		filename := positions.Position(file.Pos()).Filename
		ast.Inspect(file, func(node ast.Node) bool {
			if literal, ok := node.(*ast.FuncLit); ok {
				literals[filepath.Base(filename)] = append(literals[filepath.Base(filename)], literal)
			}
			return true
		})
		for _, declaration := range file.Decls {
			method, ok := declaration.(*ast.FuncDecl)
			if !ok || method.Body == nil {
				continue
			}
			object, _ := info.Defs[method.Name].(*types.Func)
			methods[object] = method
		}
	}
	callees := func(body ast.Node) []*types.Func {
		var names []*types.Func
		ast.Inspect(body, func(node ast.Node) bool {
			if _, literal := node.(*ast.FuncLit); literal {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if target := relationshipBoundaryTarget(info, call.Fun); target != nil {
				names = append(names, target)
			}
			return true
		})
		return names
	}
	// Source-derived string-value adapters. Minimum argument count separates
	// statistics collect/retrieve config from status/clear/stop and fixed calls.
	rawText := map[*types.Func]int{}
	for name, method := range methods {
		variables := map[string]int{}
		ast.Inspect(method.Body, func(node ast.Node) bool {
			if _, literal := node.(*ast.FuncLit); literal {
				return false
			}
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
			if _, literal := node.(*ast.FuncLit); literal {
				return false
			}
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
	var reachesText func(*types.Func, int, map[*types.Func]bool) bool
	var witness []string
	reachesText = func(object *types.Func, maxArgs int, visited map[*types.Func]bool) bool {
		name := object.Name()
		if name == "executeUserStatement" || name == "compileNodeWhereFilter" || name == "compileRelationshipWhereFilter" {
			witness = append(witness, relationshipBoundaryFunctionName(object))
			return true
		}
		if minimum, adapter := rawText[object]; adapter && (maxArgs < 0 || maxArgs >= minimum) {
			witness = append(witness, relationshipBoundaryFunctionName(object))
			return true
		}
		// A proven string-value adapter remains a dependency regardless of
		// its receiver or name. Only stop generic downstream fanout after it.
		if strings.HasPrefix(name, "evaluate") || strings.HasPrefix(name, "parse") || name == "executeInternal" || name == "executeWithoutTransaction" {
			return false
		}
		if visited[object] {
			return false
		}
		visited[object] = true
		function := functions[object]
		if function == nil {
			return false
		}

		for callee := range function.calls {
			if reachesText(callee, maxArgs, visited) {
				witness = append(witness, function.name)
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
		handlerValue := reflect.ValueOf(procedure.Handler)
		if procedure.admittedHandler != nil {
			handlerValue = reflect.ValueOf(procedure.admittedHandler)
		}
		function := runtime.FuncForPC(handlerValue.Pointer())
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
			for _, callee := range callees(handler.Body) {
				witness = nil
				if !reachesText(callee, spec.MaxArgs, map[*types.Func]bool{}) {
					continue
				}
				require.NotEmpty(t, spec.ResultCache.CypherInputs, "%s interprets text through %s via %v", spec.Name, callee, witness)
				discovered = append(discovered, spec.Name)
				break
			}
		}
	}
	t.Logf("source-derived executable-text procedures (%d): %v; string-value adapters: %v", len(discovered), discovered, rawText)
	require.GreaterOrEqual(t, len(discovered), 19)
	// Installation defers interpretation until a later custom procedure CALL.
	for _, name := range []string{"apoc.custom.installProcedure", "apoc.custom.declareProcedure"} {
		procedure, ok := globalProcedureRegistry.Get(name)
		require.True(t, ok, name)
		require.NotEmpty(t, procedure.Spec.ResultCache.CypherInputs, name)
		require.False(t, procedure.Spec.ResultCache.Deterministic, name)
	}
}
