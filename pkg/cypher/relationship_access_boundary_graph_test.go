package cypher

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
)

// Keep function creation separate from invocation. In particular, registering a
// procedure does not execute its handler. Function values flow through compiler
// objects (including parameters, result slots and struct fields), so callbacks
// and method values retain their real callers instead of their lexical parent.
func relationshipBoundaryGraph(files []*ast.File, fset *token.FileSet, info *types.Info) (map[*types.Func]*relationshipBoundaryFunction, []string) {
	functions := map[*types.Func]*relationshipBoundaryFunction{}
	bodies := map[*types.Func]*ast.BlockStmt{}
	literals := map[*ast.FuncLit]*types.Func{}
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			object, _ := info.Defs[function.Name].(*types.Func)
			if object == nil {
				continue
			}
			signature := object.Type().(*types.Signature)
			owner := signature.Recv() != nil && relationshipBoundaryNamedType(signature.Recv().Type()) == "relationshipAccessExecution"
			adapter := relationshipBoundaryStorageAdapter(object)
			functions[object] = &relationshipBoundaryFunction{object: object, name: relationshipBoundaryFunctionName(object), position: fset.Position(function.Pos()).String(), owner: owner, adapter: adapter, calls: map[*types.Func]bool{}}
			bodies[object] = function.Body
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if literal, ok := node.(*ast.FuncLit); ok {
					name := fmt.Sprintf("%s.closure@%s", relationshipBoundaryFunctionName(object), fset.Position(literal.Pos()))
					closure := types.NewFunc(literal.Pos(), object.Pkg(), name, info.TypeOf(literal).(*types.Signature))
					literals[literal] = closure
					functions[closure] = &relationshipBoundaryFunction{object: closure, name: name, position: fset.Position(literal.Pos()).String(), owner: owner, adapter: adapter, calls: map[*types.Func]bool{}}
					bodies[closure] = literal.Body
				}
				return true
			})
		}
	}

	type binding struct {
		object types.Object
		value  ast.Expr
	}
	var bindings []binding
	calls := map[*types.Func][]*ast.CallExpr{}
	var violations []string
	valueObject := func(expression ast.Expr) types.Object {
		for {
			switch value := expression.(type) {
			case *ast.Ident:
				if object := info.Defs[value]; object != nil {
					return object
				}
				return info.Uses[value]
			case *ast.SelectorExpr:
				if selection := info.Selections[value]; selection != nil {
					return selection.Obj()
				}
				return info.Uses[value.Sel]
			case *ast.ParenExpr:
				expression = value.X
			case *ast.IndexExpr:
				expression = value.X
			default:
				return nil
			}
		}
	}
	for object, body := range bodies {
		function := functions[object]
		ast.Inspect(body, func(node ast.Node) bool {
			if _, ok := node.(*ast.FuncLit); ok {
				return false
			}
			switch node := node.(type) {
			case *ast.CompositeLit:
				if relationshipBoundaryNamedType(info.TypeOf(node)) == "relationshipAccessExecution" && function.name != "StorageExecutor.relationshipExecutionFor" {
					violations = append(violations, fmt.Sprintf("%s: %s constructs relationshipAccessExecution outside the program", fset.Position(node.Pos()), function.name))
				}
				if structure, ok := info.TypeOf(node).Underlying().(*types.Struct); ok {
					for index, element := range node.Elts {
						if keyed, ok := element.(*ast.KeyValueExpr); ok {
							if field, ok := keyed.Key.(*ast.Ident); ok {
								bindings = append(bindings, binding{info.Uses[field], keyed.Value})
							}
						} else if index < structure.NumFields() {
							bindings = append(bindings, binding{structure.Field(index), element})
						}
					}
				}
			case *ast.AssignStmt:
				if len(node.Lhs) == len(node.Rhs) {
					for index, lhs := range node.Lhs {
						bindings = append(bindings, binding{valueObject(lhs), node.Rhs[index]})
					}
				}
			case *ast.ValueSpec:
				if len(node.Names) == len(node.Values) {
					for index, name := range node.Names {
						bindings = append(bindings, binding{info.Defs[name], node.Values[index]})
					}
				}
			case *ast.ReturnStmt:
				results := object.Type().(*types.Signature).Results()
				if len(node.Results) == results.Len() {
					for index, expression := range node.Results {
						bindings = append(bindings, binding{results.At(index), expression})
					}
				}
			case *ast.CallExpr:
				calls[object] = append(calls[object], node)
			}
			return true
		})
	}

	values := map[types.Object]map[*types.Func]bool{}
	dispatch := map[*types.Func]map[*types.Func]bool{}
	functionTargets := func(function *types.Func) map[*types.Func]bool {
		if cached := dispatch[function]; cached != nil {
			return cached
		}
		result := map[*types.Func]bool{function: true}
		dispatch[function] = result
		signature := function.Type().(*types.Signature)
		if signature.Recv() == nil {
			return result
		}
		contract, ok := signature.Recv().Type().Underlying().(*types.Interface)
		if !ok || relationshipStoragePrimitive(function) {
			return result
		}
		for candidate := range functions {
			implementation := candidate.Type().(*types.Signature)
			if candidate.Name() == function.Name() && implementation.Recv() != nil && types.Implements(implementation.Recv().Type(), contract) {
				result[candidate] = true
			}
		}
		return result
	}
	var targets func(ast.Expr) map[*types.Func]bool
	targets = func(expression ast.Expr) map[*types.Func]bool {
		if literal, ok := expression.(*ast.FuncLit); ok {
			return map[*types.Func]bool{literals[literal]: true}
		}
		if call, ok := expression.(*ast.CallExpr); ok {
			result := map[*types.Func]bool{}
			for function := range targets(call.Fun) {
				results := function.Type().(*types.Signature).Results()
				if results.Len() == 1 {
					for value := range values[results.At(0)] {
						result[value] = true
					}
				}
			}
			return result
		}
		object := valueObject(expression)
		if function, ok := object.(*types.Func); ok {
			return functionTargets(function)
		}
		return values[object]
	}
	merge := func(object types.Object, sources map[*types.Func]bool) bool {
		if object == nil || len(sources) == 0 {
			return false
		}
		if values[object] == nil {
			values[object] = map[*types.Func]bool{}
		}
		changed := false
		for source := range sources {
			if !values[object][source] {
				values[object][source], changed = true, true
			}
		}
		return changed
	}
	for changed := true; changed; {
		changed = false
		for _, binding := range bindings {
			changed = merge(binding.object, targets(binding.value)) || changed
		}
		for caller, sites := range calls {
			for _, call := range sites {
				for target := range targets(call.Fun) {
					if relationshipStoragePrimitive(target) {
						functions[caller].directIO = true
					}
					if functions[target] != nil {
						functions[caller].calls[target] = true
					}
					parameters := target.Type().(*types.Signature).Params()
					for index, argument := range call.Args {
						callbacks := targets(argument)
						if index < parameters.Len() {
							changed = merge(parameters.At(index), callbacks) || changed
						}
						if functions[target] == nil {
							// An opaque dependency can invoke its function arguments.
							for callback := range callbacks {
								if functions[callback] != nil {
									functions[caller].calls[callback] = true
								}
							}
						}
					}
				}
			}
		}
	}
	return functions, violations
}
