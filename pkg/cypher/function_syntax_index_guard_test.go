package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxFactRangeGuard(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "function_syntax.go", nil, 0)
	require.NoError(t, err)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "source" && function.Name.Name != "nextCall" {
			continue
		}
		require.False(t, unindexedFunctionFactRange(function.Body), function.Name.Name+" must not revisit preceding spans")
	}
}

func unindexedFunctionFactRange(body ast.Node) bool {
	aliases := map[string]bool{}
	var inventory func(ast.Expr) bool
	inventory = func(expression ast.Expr) bool {
		switch value := expression.(type) {
		case *ast.SelectorExpr:
			return value.Sel.Name == "comments" || value.Sel.Name == "subqueries"
		case *ast.Ident:
			return aliases[value.Name]
		case *ast.SliceExpr:
			if value.Low == nil {
				return inventory(value.X)
			}
			if low, literal := value.Low.(*ast.BasicLit); literal && low.Value == "0" {
				return inventory(value.X)
			}
		}
		return false
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Lhs) != len(assignment.Rhs) {
				return true
			}
			for index, left := range assignment.Lhs {
				if name, named := left.(*ast.Ident); named && !aliases[name.Name] && inventory(assignment.Rhs[index]) {
					aliases[name.Name], changed = true, true
				}
			}
			return true
		})
	}
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if loop, ok := node.(*ast.RangeStmt); ok && inventory(loop.X) {
			found = true
		}
		return true
	})
	return found
}

func TestFunctionSyntaxFactRangeGuardMutants(t *testing.T) {
	for _, sample := range []struct {
		body string
		bad  bool
	}{
		{"for _, c := range syntax.comments {}", true},
		{"for _, s := range syntax.subqueries {}", true},
		{"all := syntax.comments; alias := all; for _, c := range alias {}", true},
		{"all := syntax.comments[:]; for _, c := range all {}", true},
		{"for _, c := range syntax.comments[0:] {}", true},
		{"for _, c := range syntax.comments[syntax.commentIndex(start):] {}", false},
		{"for _, c := range syntax.calls[syntax.callIndex(start):] {}", false},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "mutant.go", "package cypher; func f(){"+sample.body+"}", 0)
		require.NoError(t, err)
		require.Equal(t, sample.bad, unindexedFunctionFactRange(file.Decls[0].(*ast.FuncDecl).Body), sample.body)
	}
}
