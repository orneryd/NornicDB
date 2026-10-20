package cypher

import (
	"github.com/stretchr/testify/require"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"
)

func admissionFactAcquisitions(source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "admission.go", source, 0)
	if err != nil {
		return nil, err
	}
	var violations []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		// These are the recursive consumers of the statement's fact capability.
		guarded := false
		switch function.Name.Name {
		case "invalidAggregationInListComprehension", "validateListComprehensionVariables", "containsNotInOperator", "validateSyntax", "validateSyntaxNornic", "validateSyntaxNornicInput", "validateSyntaxANTLR", "parsePipelineClauses", "withLocalTypes", "withPatternComprehensionBindings", "validateExpressionPatternBindings", "checkNativeBindingScope", "checkCase":
			guarded = true
		}
		if !guarded {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if target, ok := call.Fun.(*ast.Ident); ok && target.Name == "parseListComprehension" {
				violations = append(violations, function.Name.Name+" reacquires native statement facts through an evaluator")
			}
			target, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			owner, ok := target.X.(*ast.Ident)
			if !ok || owner.Name != "fabric" {
				return true
			}
			switch target.Sel.Name {
			case "ReadQueryStructure", "ExpressionScopes", "ExpressionReferences", "RootExpressionScope", "ComprehensionSyntax", "ReduceSyntax":
				violations = append(violations, function.Name.Name+" reacquires native statement facts")
			}
			return true
		})
	}
	return violations, nil
}

func TestAdmissionFactOwnerGuard(t *testing.T) {
	for _, name := range []string{"semantic_validation.go", "expression_validation.go", "match_semantic_validation.go", "static_native_binding_types.go", "static_operator_types.go", "pipeline_executor.go", "validator_strictness.go", "executor_query_routing.go"} {
		source, err := os.ReadFile(name)
		require.NoError(t, err)
		violations, err := admissionFactAcquisitions(source)
		require.NoError(t, err)
		require.Empty(t, violations, name)
	}
	mutation := []byte(`package cypher;func withLocalTypes(scope staticTypeScope,text string,at int){for range fabric.ExpressionScopes(text){_ = fabric.ReadQueryStructure(text)}}`)
	violations, err := admissionFactAcquisitions(mutation)
	require.NoError(t, err)
	require.Len(t, violations, 2)
	mutation = []byte(`package cypher;func parsePipelineClauses(text string){_ = fabric.ReadQueryStructure(text)}`)
	violations, err = admissionFactAcquisitions(mutation)
	require.NoError(t, err)
	require.Len(t, violations, 1)
	mutation = []byte(`package cypher;func containsNotInOperator(text string){_ = fabric.ExpressionScopes(text)}`)
	violations, err = admissionFactAcquisitions(mutation)
	require.NoError(t, err)
	require.Len(t, violations, 1)

	mutation = []byte(`package cypher;func validateListComprehensionVariables(text string){for range fabric.ExpressionScopes(text){}}`)
	violations, err = admissionFactAcquisitions(mutation)
	require.NoError(t, err)
	require.Len(t, violations, 1)

	mutation = []byte(`package cypher;func invalidAggregationInListComprehension(text string){parseListComprehension(text)}`)
	violations, err = admissionFactAcquisitions(mutation)
	require.NoError(t, err)
	require.Len(t, violations, 1)

}
