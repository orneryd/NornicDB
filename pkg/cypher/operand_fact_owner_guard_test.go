package cypher

import (
	"github.com/stretchr/testify/require"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func operandCompilerOwnerViolations(source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "operand.go", source, 0)
	if err != nil {
		return nil, err
	}
	var violations []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil || function.Name.Name == "compileRowValueProgram" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			var constructed ast.Expr
			switch value := node.(type) {
			case *ast.CompositeLit:
				constructed = value.Type
			case *ast.ValueSpec:
				constructed = value.Type
			case *ast.CallExpr:
				if name, ok := value.Fun.(*ast.Ident); ok && name.Name == "new" && len(value.Args) == 1 {
					constructed = value.Args[0]
				}
			}
			if name, ok := constructed.(*ast.Ident); ok && name.Name == "rowValueCompiler" {
				violations = append(violations, function.Name.Name)
			}
			return true
		})
	}
	return violations, nil
}

func TestOperandCompilerHasOneOwner(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		source, err := os.ReadFile(entry.Name())
		require.NoError(t, err)
		violations, err := operandCompilerOwnerViolations(source)
		require.NoError(t, err)
		require.Empty(t, violations, entry.Name())
	}
	for _, mutation := range []string{
		`package cypher;func parseProjectionExprAlias(){compiler:=rowValueCompiler{};_=compiler}`,
		`package cypher;func compiledReadFootprint(){var compiler rowValueCompiler;_=compiler}`,
		`package cypher;func projectionAdapter(){_=new(rowValueCompiler)}`,
	} {
		violations, err := operandCompilerOwnerViolations([]byte(mutation))
		require.NoError(t, err)
		require.Len(t, violations, 1)
	}
}
