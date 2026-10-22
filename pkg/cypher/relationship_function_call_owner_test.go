package cypher

import (
	"go/ast"
	"go/types"
	"sort"
	"strings"
	"testing"
)

// The syntax owner is pure and shared with the pattern reader. Enumerate its
// complete compiler-selected reverse population, including fixture closures;
// the behavioral proof uses Execute with quoted argument tokens.
func TestRelationshipAccessFunctionCallSyntaxOwner(t *testing.T) {
	fset, files, info := relationshipFixtureTypedSource(t, "", false)
	functions, _ := relationshipBoundaryGraph(files, fset, info)
	var target *types.Func
	var mathTarget *types.Func
	for object := range functions {
		if object.Name() == "parseFunctionCallWS" {
			target = object
		}
		if object.Name() == "evaluateExpressionWithContextFullMath" {
			mathTarget = object
		}
	}
	if target == nil || mathTarget == nil {
		t.Fatal("complete syntax/fallback owner population missing")
	}
	canonicalCalls, localLoops := 0, 0
	ast.Inspect(functions[target].body, func(node ast.Node) bool {
		switch node.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			localLoops++
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if callee := relationshipBoundaryTarget(info, call.Fun); callee != nil && callee.Name() == "findMatchingDelimiter" {
				canonicalCalls++
			}
		}
		return true
	})
	if canonicalCalls != 1 || localLoops != 0 {
		t.Errorf("call syntax must delegate once to shared quoted delimiter owner: calls=%d private loops=%d", canonicalCalls, localLoops)
	}
	var callers []string
	var mathCallers []string
	for object, function := range functions {
		if function.calls[target] {
			callers = append(callers, function.name+" "+function.position)
		}
		if function.calls[mathTarget] {
			mathCallers = append(mathCallers, function.name+" "+function.position)
			if strings.HasSuffix(fset.Position(object.Pos()).Filename, "_test.go") || object.Name() != "evaluateExpressionWithContextFullFunctions" {
				t.Errorf("fallback must be reached after common dispatch, including private fixtures: %s", function.position)
			}
		}
	}
	if len(callers) == 0 || len(mathCallers) != 1 {
		t.Fatalf("incomplete reverse census: syntax=%d fallback=%d", len(callers), len(mathCallers))
	}
	sort.Strings(callers)
	sort.Strings(mathCallers)
	for _, caller := range callers {
		t.Log("syntax caller: " + caller)
	}
	t.Logf("full typed reverse census: syntax=%d fallback=%v", len(callers), mathCallers)
}
