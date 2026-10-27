package testfixture

import "go/ast"

// ReceiverNaming selects whether a syntax identity includes pointer markers.
type ReceiverNaming uint8

const (
	// ReceiverWithPointers preserves pointer markers in declaration identities.
	ReceiverWithPointers ReceiverNaming = iota
	// ReceiverBaseName names the underlying type for admission dependency lookup.
	ReceiverBaseName
)

// ReceiverName reads the base name of an AST receiver, ignoring generic type
// arguments. For example, *Box[T] is "*Box" with ReceiverWithPointers and
// "Box" with ReceiverBaseName. Unrecognized syntax retains only pointer markers.
func ReceiverName(expression ast.Expr, naming ReceiverNaming) string {
	prefix := ""
	for {
		switch value := expression.(type) {
		case *ast.Ident:
			return prefix + value.Name
		case *ast.StarExpr:
			if naming == ReceiverWithPointers {
				prefix += "*"
			}
			expression = value.X
		case *ast.IndexExpr:
			expression = value.X
		case *ast.IndexListExpr:
			expression = value.X
		default:
			return prefix
		}
	}
}
