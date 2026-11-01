package testfixture

// Author of fixture cleanup changes: L4 implementation lane (Codex), 1 November 2026.

import (
	"go/ast"
	"strings"
)

func fixtureUnwrap(expression ast.Expr) ast.Expr {
	for {
		switch value := expression.(type) {
		case *ast.ParenExpr:
			expression = value.X
		case *ast.IndexExpr:
			expression = value.X
		case *ast.IndexListExpr:
			expression = value.X
		default:
			return expression
		}
	}
}

func fixtureCallTarget(expression ast.Expr, imports map[string]string, dots map[string]bool, localStorage bool) (string, string) {
	switch expression := fixtureUnwrap(expression).(type) {
	case *ast.SelectorExpr:
		if qualifier, ok := expression.X.(*ast.Ident); ok && qualifier.Obj == nil {
			return imports[qualifier.Name], expression.Sel.Name
		}
	case *ast.Ident:
		if expression.Obj != nil {
			return "", ""
		}
		if localStorage && fixtureFactory(storageImport, expression.Name) != "" {
			return storageImport, expression.Name
		}
		for path := range dots {
			if fixtureFactory(path, expression.Name) != "" || (path == fixtureImport && (expression.Name == "Track" || expression.Name == "TrackResult" || expression.Name == "TrackProcess" || expression.Name == "OwnResult" || expression.Name == "OwnResult3")) {
				return path, expression.Name
			}
		}
	}
	return "", ""
}

func fixtureFactory(path, name string) string {
	if path == storageImport {
		switch name {
		case "NewMemoryEngine", "NewMemoryEngineWithMVCCHistory", "NewBadgerEngine", "NewBadgerEngineInMemory", "NewBadgerEngineWithOptions", "openManagedBadger":
			return name
		case "RecoverFromWAL", "RecoverFromWALWithLogger", "RecoverFromWALWithResult", "RecoverWithTransactions":
			return name
		}
	}
	if strings.HasPrefix(path, "github.com/dgraph-io/badger/") && (name == "Open" || name == "OpenManaged") {
		return "badger." + name
	}
	return ""
}

// fixtureAdjacentConstructorTrack recognizes a constructor's first result
// entering Track in the immediately following statement of the same block.
// No assertion, branch, alias, reassignment or unknown call can intervene.
// The syntax lint and the typed allocation inventory share this one matcher.
func fixtureAdjacentConstructorTrack(expression ast.Expr, parents map[ast.Node]ast.Node, isTrack func(ast.Expr) bool) bool {
	for {
		parent, ok := parents[expression].(*ast.ParenExpr)
		if !ok {
			break
		}
		expression = parent
	}
	assignment, ok := parents[expression].(*ast.AssignStmt)
	if !ok || len(assignment.Rhs) != 1 || assignment.Rhs[0] != expression || len(assignment.Lhs) < 2 {
		return false
	}
	resource, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok || resource.Name == "_" {
		return false
	}
	block, ok := parents[assignment].(*ast.BlockStmt)
	if !ok {
		return false
	}
	for index, statement := range block.List {
		if statement != assignment || index+1 >= len(block.List) {
			continue
		}
		next, ok := block.List[index+1].(*ast.AssignStmt)
		if !ok || len(next.Lhs) != 1 || len(next.Rhs) != 1 {
			return false
		}
		left, ok := next.Lhs[0].(*ast.Ident)
		if !ok || left.Name != resource.Name || (left.Obj != nil && resource.Obj != nil && left.Obj != resource.Obj) {
			return false
		}
		tracked, ok := fixtureUnwrap(next.Rhs[0]).(*ast.CallExpr)
		if !ok || !isTrack(tracked.Fun) || len(tracked.Args) != 3 {
			return false
		}
		argument, ok := fixtureUnwrap(tracked.Args[1]).(*ast.Ident)
		return ok && argument.Name == resource.Name && (argument.Obj == nil || resource.Obj == nil || argument.Obj == resource.Obj)
	}
	return false
}

func fixtureConstructorCall(stack []ast.Node, reference ast.Expr) (*ast.CallExpr, int) {
	for index := len(stack) - 2; index >= 0; index-- {
		switch parent := stack[index].(type) {
		case *ast.ParenExpr, *ast.IndexExpr, *ast.IndexListExpr:
			if fixtureUnwrap(parent.(ast.Expr)) != reference {
				return nil, -1
			}
		case *ast.CallExpr:
			if fixtureUnwrap(parent.Fun) == reference {
				return parent, index
			}
			return nil, -1
		default:
			return nil, -1
		}
	}
	return nil, -1
}
