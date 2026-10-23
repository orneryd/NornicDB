package cypher

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A function that begins and commits its own transaction (a transaction
// script) leaves through one of its transaction's owners once it has begun:
// abortTransaction (fail and roll back), handleRollback, the commit (which
// releases the transaction when it fails), or a success. Any other error
// exit would leave the private transaction open, holding its snapshot.
func TestTransactionScriptExitsReleaseTransaction(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		files = append(files, file)
	}
	violations, guarded := transactionScriptExitViolations(fset, files)
	require.Empty(t, violations, "a transaction script exit that skips abortTransaction")
	require.Contains(t, guarded, "executeSimpleTransactionScript")
	require.Contains(t, guarded, "executeCaseRollbackTransactionScript")

	fixture, err := parser.ParseFile(fset, "transaction_script_exit_fixture.go", transactionScriptExitFixture, 0)
	require.NoError(t, err)
	violations, _ = transactionScriptExitViolations(fset, []*ast.File{fixture})
	require.Equal(t, []string{"transaction_script_exit_fixture.go:11 script: return nil, batchErr"}, violations)
}

const transactionScriptExitFixture = `package cypher

func (e *StorageExecutor) script(ctx context.Context, rows int) (*ExecuteResult, error) {
	if _, err := e.handleBegin(); err != nil {
		return nil, err
	}
	if err := e.run(); err != nil {
		return nil, e.abortTransaction(ctx, err)
	}
	for batchErr := range e.batches(rows) {
		return nil, batchErr
	}
	if rows == 0 {
		return e.handleRollback()
	}
	if _, err := e.handleCommit(); err != nil {
		return nil, err
	}
	return &ExecuteResult{}, nil
}
`

func transactionScriptExitViolations(fset *token.FileSet, files []*ast.File) (violations, guarded []string) {
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			begin := token.NoPos
			commits := false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch method := calledMethod(node); method {
				case "handleBegin":
					if begin == token.NoPos {
						begin = node.End()
					}
				case "handleCommit":
					commits = true
				}
				return true
			})
			if begin == token.NoPos || !commits {
				continue
			}
			guarded = append(guarded, function.Name.Name)
			var owners []ast.Node
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch statement := node.(type) {
				case *ast.FuncLit:
					return false
				case *ast.IfStmt:
					// The begin's and the commit's own failures: nothing has begun,
					// or the commit released the transaction.
					if statement.Init != nil && containsCall(statement.Init, "handleBegin", "handleCommit") {
						owners = append(owners, statement.Body)
					}
				case *ast.ReturnStmt:
					if statement.Pos() < begin || releasesTransaction(statement) || inside(statement, owners) {
						return true
					}
					var text strings.Builder
					for i, result := range statement.Results {
						if i > 0 {
							text.WriteString(", ")
						}
						text.WriteString(types.ExprString(result))
					}
					violations = append(violations, fmt.Sprintf("%s:%d %s: return %s", filepath.Base(fset.Position(statement.Pos()).Filename), fset.Position(statement.Pos()).Line, function.Name.Name, text.String()))
				}
				return true
			})
		}
	}
	sort.Strings(violations)
	sort.Strings(guarded)
	return violations, guarded
}

// releasesTransaction reports a success (a nil error) or an exit through
// abortTransaction, handleRollback or the commit itself.
func releasesTransaction(statement *ast.ReturnStmt) bool {
	if len(statement.Results) == 0 {
		return false
	}
	last := statement.Results[len(statement.Results)-1]
	if ident, ok := last.(*ast.Ident); ok && ident.Name == "nil" {
		return true
	}
	switch calledMethod(last) {
	case "abortTransaction", "handleRollback", "handleCommit":
		return true
	}
	return false
}

func calledMethod(node ast.Node) string {
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return ""
	}
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
		return selector.Sel.Name
	}
	return ""
}

func containsCall(node ast.Node, methods ...string) bool {
	found := false
	ast.Inspect(node, func(child ast.Node) bool {
		for _, method := range methods {
			if calledMethod(child) == method {
				found = true
			}
		}
		return !found
	})
	return found
}

func inside(node ast.Node, scopes []ast.Node) bool {
	for _, scope := range scopes {
		if scope.Pos() <= node.Pos() && node.End() <= scope.End() {
			return true
		}
	}
	return false
}
