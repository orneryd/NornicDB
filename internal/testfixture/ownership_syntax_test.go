package testfixture

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
