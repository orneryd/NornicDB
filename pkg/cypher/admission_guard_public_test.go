package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Scan the entire production package, including functions with previously
// unseen names. These capabilities belong to their producers, not validators.
func independentAdmissionFacts(source []byte, paths ...string) ([]string, error) {
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
		bindingScope, withTransition := false, false
		scopeMap := false
		producer := len(paths) > 0 && filepath.Base(paths[0]) == "semantic_statement_input.go"
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if clause, ok := node.(*ast.CaseClause); ok {
				for _, value := range clause.List {
					if name, ok := value.(*ast.Ident); ok && name.Name == "pipelineClauseWith" {
						withTransition = true
					}
				}
			}
			var mapType *ast.MapType
			switch value := node.(type) {
			case *ast.CompositeLit:
				mapType, _ = value.Type.(*ast.MapType)
			case *ast.CallExpr:
				if name, ok := value.Fun.(*ast.Ident); ok && name.Name == "make" && len(value.Args) > 0 {
					mapType, _ = value.Args[0].(*ast.MapType)
				}
			}
			if mapType != nil {
				key, kok := mapType.Key.(*ast.Ident)
				value, vok := mapType.Value.(*ast.Ident)
				scopeMap = scopeMap || (kok && vok && key.Name == "string" && value.Name == "string")
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if target, ok := call.Fun.(*ast.SelectorExpr); ok && !producer {
				switch target.Sel.Name {
				case "ReadQueryStructure", "ProjectionBodyAndTail", "ProjectionSyntax", "ClauseBody":
					violations = append(violations, function.Name.Name+": native acquisition outside statement owner")
				}
			}
			if target, ok := call.Fun.(*ast.Ident); ok && !producer {
				switch target.Name {
				case "pipelineStatementFor", "describePipelineStatement", "legacyPipelineClauses":
					violations = append(violations, function.Name.Name+": physical acquisition outside statement owner")
				}
			}
			if target, ok := call.Fun.(*ast.Ident); ok && !producer && (target.Name == "logicalStatementSyntax" || target.Name == "logicalProjectionSyntax") {
				for _, argument := range call.Args {
					if _, synthetic := argument.(*ast.BinaryExpr); synthetic {
						violations = append(violations, function.Name.Name+": standalone clause reconstruction")
					}
				}
			}
			if target, ok := call.Fun.(*ast.Ident); ok && target.Name == "returnProjectionPlanFor" && len(call.Args) == 1 {
				if source, ok := call.Args[0].(*ast.SelectorExpr); ok && source.Sel.Name == "text" {
					violations = append(violations, function.Name.Name+": retained projection descriptor dropped")
				}
			}
			if target, ok := call.Fun.(*ast.Ident); ok && target.Name == "TransactionDatabaseError" {
				allowed := function.Name.Name == "useDatabase"
				if function.Name.Name == "admit" && function.Recv != nil && len(function.Recv.List) == 1 {
					if pointer, ok := function.Recv.List[0].Type.(*ast.StarExpr); ok {
						if owner, ok := pointer.X.(*ast.Ident); ok && owner.Name == "fabricGraphAdmission" {
							allowed = true
						}
					}
				}
				if !allowed {
					violations = append(violations, function.Name.Name+": transaction policy outside graph admission owner")
				}
			}
			if target, ok := call.Fun.(*ast.Ident); ok {
				switch target.Name {
				case "cypherGrammarVersion":
					if function.Name.Name != "statementCypherVersion" {
						violations = append(violations, function.Name.Name+": raw version reconstructed")
					}
				case "projectedBindingScope":
					violations = append(violations, function.Name.Name+": independent projection scope")
				case "newSemanticBindingScope":
					bindingScope = true
				}
			}
			if target, ok := call.Fun.(*ast.SelectorExpr); ok && target.Sel.Name == "scope" && function.Name.Name != "admitOperandFrames" {
				if value, ok := target.X.(*ast.Ident); ok && value.Name == "operand" {
					violations = append(violations, function.Name.Name+": typed operand frame reconstructed")
				}
			}
			if target, ok := call.Fun.(*ast.SelectorExpr); ok && target.Sel.Name == "Operands" && function.Name.Name != "admitClauseOperands" {
				violations = append(violations, function.Name.Name+": operand selection reconstructed")
			}
			return true
		})
		parameterConsumer := len(paths) > 0 && (filepath.Base(paths[0]) == "property_access_semantic_validation.go" || filepath.Base(paths[0]) == "static_operator_types.go" || filepath.Base(paths[0]) == "static_function_argument_types.go")
		if scopeMap && parameterConsumer {
			violations = append(violations, function.Name.Name+": parameter consumer constructed a scope map")
		}
		if scopeMap && withTransition && function.Name.Name != "semanticScopeWalk" {
			violations = append(violations, function.Name.Name+": independent typed-scope map")
		}
		if bindingScope && withTransition && function.Name.Name != "semanticScopeWalk" {
			violations = append(violations, function.Name.Name+": second WITH binding walk")
		}
	}
	return violations, nil
}

func TestAdmissionDecisionOwnerGuard(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		violations, err := independentAdmissionFacts(source, file)
		require.NoError(t, err)
		require.Empty(t, violations, file)
	}
	for _, source := range []string{
		`package cypher;func renamed(clause pipelineClause){_ = fabric.ReadQueryStructure(clause.text)}`,
		`package cypher;func renamed(text string){_ = fabric.ReadQueryStructure(text)}`,
		`package cypher;func renamed(text string){_ = pipelineStatementFor(text)}`,
		`package cypher;func renamed(text string){helper(text)};func helper(raw string){_ = describePipelineStatement(raw,false)}`,
		`package cypher;func renamed(text string){_,_,_ = legacyPipelineClauses(text)}`,
		`package cypher;func renamed(clause pipelineClause){_ = returnProjectionPlanFor(clause.text)}`,
		`package cypher;func renamed(text string){_ = logicalStatementSyntax("RETURN "+text)}`,
		`package cypher;func renamed(text string){helper(text)};func helper(raw string){_ = fabric.ProjectionSyntax(raw)}`,
		`package cypher;func renamed(text string){helper(text)};func helper(raw string){_ = fabric.ReadQueryStructure("RETURN "+raw)}`,
		`package cypher;func renamed(clause pipelineClause){_ = fabric.ProjectionBodyAndTail(clause.text,"RETURN")}`,
		`package cypher;func renamed(clauses []pipelineClause){types:=map[string]string{};for _,clause:=range clauses {switch clause.kind {case pipelineClauseWith:types["x"]="Integer"}}}`,

		`package cypher;func renamedPolicy(ctx any){_ = TransactionDatabaseError(nil,"a","b","RETURN 1")}`,
		`package cypher;func renamedValidator(operand admittedClauseOperand,outer staticTypeScope){_ = operand.scope(outer)}`,
		`package cypher;func renamedGate(text string){_ = cypherGrammarVersion(text)}`,
		`package cypher;func renamedFunctionCheck(clause pipelineClause){for range clause.syntax.Operands(){}}`,
		`package cypher;func renamedScopeWalk(clauses []pipelineClause){scope:=newSemanticBindingScope();for _,clause:=range clauses{switch clause.kind{case pipelineClauseWith:scope=projectedBindingScope(scope,clause.text,clause.syntax)}}}`,
	} {
		violations, err := independentAdmissionFacts([]byte(source))
		require.NoError(t, err)
		require.NotEmpty(t, violations, source)
	}
	// Prove that an indirect, renamed parameter adapter cannot construct
	// an independent type environment, even without a local WITH switch.
	violations, err := independentAdmissionFacts([]byte(`package cypher;func renamed(text string){scope:=make(map[string]string);_ = scope}`), "static_operator_types.go")
	require.NoError(t, err)
	require.NotEmpty(t, violations)
}
