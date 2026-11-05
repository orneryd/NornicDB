package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// These boundaries forbid the two retired inference paths from growing back.
func TestCourseChangeOwnerBoundaries(t *testing.T) {
	for _, check := range []struct {
		path, function string
		forbidden      []string
		required       string
	}{
		{"admin_error_redaction.go", "redactAdminCommandError", []string{"scanCypherQuotedText", "decodeCypherQuotedString", "parseCypherValueToken", "tokenize"}, "redactAdminCommandErrorSource"},
		{"admin_error_redaction.go", "redactAdminCommandErrorSource", []string{"scanCypherQuotedText", "decodeCypherQuotedString", "parseCypherValueToken", "tokenize"}, "PasswordBoundary"},
		{"statement_execution_contract.go", "readStatementExecutionContract", []string{"fabricSingleGraphStatement"}, "ParseSyntax"},
		{"../fabric/use_clause.go", "ReadUseClause", []string{"scanGraphReference", "matchingParen", "splitArguments", "scanBacktickName"}, "ReadUseReference"},
	} {
		source, err := os.ReadFile(check.path)
		require.NoError(t, err)
		tree, err := parser.ParseFile(token.NewFileSet(), check.path, source, 0)
		require.NoError(t, err)
		calls := map[string]int{}
		var syntaxStages []string
		for _, decl := range tree.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != check.function {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					switch name := call.Fun.(type) {
					case *ast.Ident:
						calls[name.Name]++
					case *ast.SelectorExpr:
						calls[name.Sel.Name]++
						if check.required == "ParseSyntax" && name.Sel.Name == "ParseSyntax" {
							require.Len(t, call.Args, 5, "source stage is required")
							stage, ok := call.Args[4].(*ast.SelectorExpr)
							require.True(t, ok, "contract must name explicit accepted stage")
							if ok {
								syntaxStages = append(syntaxStages, stage.Sel.Name)
							}
						}
					}
				}
				return true
			})
		}
		require.Positive(t, calls[check.required], check.function)
		for _, name := range check.forbidden {
			require.Zero(t, calls[name], check.function+" duplicates "+name)
		}
		if check.required == "ParseSyntax" {
			require.Equal(t, 1, calls[check.required], "the contract must not retry a rewritten source")
			require.Equal(t, []string{"AcceptedSource"}, syntaxStages, "the execution contract consumes accepted source exactly once")
		}
	}
}
