package storage_test

import (
	"go/ast"
	"go/types"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// The catalog holds actual interface signatures and actual execution-owner
// objects. Identifier spelling is not a capability or callback permission.
type bypassReadTypes map[string][]*types.Signature

func sameBypassSignature(actual *types.Signature, offset int, contract *types.Signature) bool {
	if actual == nil || contract == nil || actual.Variadic() != contract.Variadic() || actual.Params().Len()-offset != contract.Params().Len() || actual.Results().Len() != contract.Results().Len() {
		return false
	}
	for i := 0; i < contract.Params().Len(); i++ {
		if !types.Identical(actual.Params().At(i+offset).Type(), contract.Params().At(i).Type()) {
			return false
		}
	}
	for i := 0; i < contract.Results().Len(); i++ {
		if !types.Identical(actual.Results().At(i).Type(), contract.Results().At(i).Type()) {
			return false
		}
	}
	return true
}

func (catalog bypassReadTypes) matches(selection *types.Selection) bool {
	signature, ok := selection.Type().(*types.Signature)
	if !ok {
		return false
	}
	offset := 0
	if selection.Kind() == types.MethodExpr {
		offset = 1
	}
	for _, contract := range catalog[selection.Obj().Name()] {
		if sameBypassSignature(signature, offset, contract) {
			return true
		}
	}
	return false
}

func bypassFunctionObject(info *types.Info, expression ast.Expr) *types.Func {
	switch node := expression.(type) {
	case *ast.IndexExpr:
		return bypassFunctionObject(info, node.X)
	case *ast.IndexListExpr:
		return bypassFunctionObject(info, node.X)
	case *ast.Ident:
		object := info.Uses[node]
		if object == nil {
			object = info.Defs[node]
		}
		function, _ := object.(*types.Func)
		return function
	case *ast.SelectorExpr:
		if selected := info.Selections[node]; selected != nil {
			function, _ := selected.Obj().(*types.Func)
			return function
		}
		function, _ := info.Uses[node.Sel].(*types.Func)
		return function
	}
	return nil
}

// These are definition anchors for the already implemented execution owners,
// not name patterns applied to callers. Only the actual package-scope object
// authorizes a callback. Its generic signature supplies the callback position.
func bypassExecutionOwners(sources []capabilitySource) (map[*types.Func]int, map[string]int) {
	objects := map[*types.Func]int{}
	symbols := map[string]int{}
	packages := map[*types.Package]bool{}
	for _, source := range sources {
		for _, member := range source.members {
			if member.Syntax != nil && member.Syntax.Package != nil {
				packages[member.Syntax.Package] = true
			}
		}
	}
	for pkg := range packages {
		if pkg.Path() != "github.com/orneryd/nornicdb/pkg/storage" {
			continue
		}
		for _, anchor := range []string{"ReadWithCapability", "ReadValueWithCapability", "VisitWithCapability", "VisitWhileWithCapability", "visitCompositeRead", "ExecuteBoundRead"} {
			function, ok := pkg.Scope().Lookup(anchor).(*types.Func)
			if !ok {
				continue
			}
			signature := function.Type().(*types.Signature)
			if signature.TypeParams().Len() == 0 {
				continue
			}
			reader := signature.TypeParams().At(0)
			for i := 0; i < signature.Params().Len(); i++ {
				callback, ok := signature.Params().At(i).Type().Underlying().(*types.Signature)
				if ok && callback.Params().Len() > 0 && types.Identical(callback.Params().At(0).Type(), reader) {
					objects[function.Origin()] = i
					symbols[function.FullName()] = i
					break
				}
			}
		}
	}
	return objects, symbols
}

func bypassCallbackArgument(member workcensus.Member, source capabilitySource, call *ast.CallExpr) (int, bool) {
	if function := bypassFunctionObject(source.info, call.Fun); function != nil {
		argument, found := source.executions[function.Origin()]
		return argument, found
	}
	// Function-variable aliases use the common engine's resolved callee facts.
	// No private alias walk or spelling-based fallback is permitted.
	for _, symbol := range member.Callees {
		if argument, found := source.executionSymbols[symbol]; found {
			return argument, true
		}
	}
	return 0, false
}
