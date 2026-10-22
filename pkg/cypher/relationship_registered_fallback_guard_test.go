package cypher

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// These names are installed unconditionally by functions_neo5.go init and
// fn/builtins_core.go. FullFunctions returns on found even for null/errors;
// its sole production call to FullMath occurs after that dispatch. A private
// fixture must use that shared evaluator rather than preserve a dead copy.
func relationshipFallbackIsRegistered(name string) bool {
	// With no Eval, EvaluateFunction reports membership without invoking the
	// implementation. Discover the live registry, not a list of observed cases.
	_, found, _ := cypherfn.EvaluateFunction(name, nil, cypherfn.Context{})
	return found
}

func relationshipRegisteredFallbackBranches(fset *token.FileSet, file *ast.File) []string {
	var result []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil || (function.Name.Name != "evaluateExpressionWithContextFullFunctions" && function.Name.Name != "evaluateExpressionWithContextFullMath") {
			continue
		}
		for _, statement := range function.Body.List {
			branch, ok := statement.(*ast.IfStmt)
			if !ok {
				continue
			}
			name := ""
			if call, ok := branch.Cond.(*ast.CallExpr); ok {
				if callee, ok := call.Fun.(*ast.Ident); ok && callee.Name == "isCaseExpression" && function.Name.Name == "evaluateExpressionWithContextFullMath" {
					result = append(result, fmt.Sprintf("%s: FullFunctions already returns for CASE before FullMath", fset.Position(branch.Pos())))
				}
				if callee, ok := call.Fun.(*ast.Ident); ok && callee.Name == "matchFuncStartAndSuffix" && len(call.Args) == 2 {
					if literal, ok := call.Args[1].(*ast.BasicLit); ok {
						name, _ = strconv.Unquote(literal.Value)
					}
				}
			}
			if comparison, ok := branch.Cond.(*ast.BinaryExpr); ok && comparison.Op == token.EQL {
				if variable, ok := comparison.X.(*ast.Ident); ok && variable.Name == "lowerExpr" {
					if literal, ok := comparison.Y.(*ast.BasicLit); ok {
						call, _ := strconv.Unquote(literal.Value)
						if strings.HasSuffix(call, "()") {
							name = call[:len(call)-2]
						}
					}
				}
			}
			if name != "" && relationshipFallbackIsRegistered(name) {
				result = append(result, fmt.Sprintf("%s: %s: registered %s has a shadowed fallback implementation", fset.Position(branch.Pos()), function.Name.Name, name))
			}
		}
	}
	return result
}

func TestRelationshipAccessRegisteredFallbackOwnership(t *testing.T) {
	for _, path := range []string{"functions_eval_functions.go", "functions_eval_math.go"} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, failure := range relationshipRegisteredFallbackBranches(fset, file) {
			t.Error(failure)
		}
	}
	// This reader guard must reject a reintroduced copy and accept an unrelated
	// unregistered evaluator branch; it never changes registry dispatch.
	const control = `package cypher
func (e *StorageExecutor) evaluateExpressionWithContextFullMath() {
 if matchFuncStartAndSuffix(expr, "sin") { return }
 if matchFuncStartAndSuffix(expr, "kalman.init") { return }
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "control.go", control, 0)
	if err != nil {
		t.Fatal(err)
	}
	if failures := relationshipRegisteredFallbackBranches(fset, file); len(failures) != 1 {
		t.Fatalf("guard must reject only the shadowed registered branch: %v", failures)
	}
}
