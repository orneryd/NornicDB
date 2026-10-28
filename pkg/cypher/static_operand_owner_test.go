package cypher

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// A static operand's raw category collapses a CASE union to the operator
// category its alternatives share. Only the owner file reads it; every other
// consumer asks through the union-aware methods (typeName, fits, excludes,
// elementOperand, is, …), so no consumer can take that category for the type.
func TestStaticOperandCategoryOwner(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if path == "static_operand.go" {
			continue
		}
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.SelectorExpr:
				if node.Sel.Name == "operandCategory" {
					t.Errorf("%s: reads a static operand's raw category outside static_operand.go", fs.Position(node.Pos()))
				}
			case *ast.CallExpr:
				// A list's element type has one reader (elementOfType): no
				// other code strips "List<" from a type name, which loses a
				// union's alternatives ("List<Map> or List<Any>").
				for _, argument := range node.Args {
					if literal, ok := argument.(*ast.BasicLit); ok && literal.Kind == token.STRING {
						if value, _ := strconv.Unquote(literal.Value); value == "List<" {
							if selector, ok := node.Fun.(*ast.SelectorExpr); ok && (selector.Sel.Name == "TrimPrefix" || selector.Sel.Name == "CutPrefix") {
								t.Errorf("%s: reads a list element type outside static_operand.go", fs.Position(node.Pos()))
							}
						}
					}
				}
				if identifier, ok := node.Fun.(*ast.Ident); ok && identifier.Name == "len" && len(node.Args) == 1 {
					if literal, ok := node.Args[0].(*ast.BasicLit); ok && literal.Value == `"List<"` {
						t.Errorf("%s: slices a list type name outside static_operand.go", fs.Position(node.Pos()))
					}
				}
			case *ast.KeyValueExpr:
				if key, ok := node.Key.(*ast.Ident); ok && key.Name == "operandCategory" {
					t.Errorf("%s: sets a static operand's raw category outside static_operand.go", fs.Position(node.Pos()))
				}
			}
			return true
		})
	}
}

// staticTypeNameDecisionExceptions are the functions outside the owner that
// compare a Cypher type name. Each reads a runtime value kind or a declared
// signature, never a static operand's derived type, and is pinned to its
// source hash: a change, or a new comparison anywhere else, fails the guard.
var staticTypeNameDecisionExceptions = map[string]struct{ reason, sha256 string }{
	"gds_projection.go:gdsGraphSchema":                                        {"GDS projection reads the stored property value kind or its Java class name at run time, not a static operand.", "0fb60fead4b3e00c30dc649b519c0de32c3518767883806350dcdbd4d429b589"},
	"gds_projection.go:gdsJavaClass":                                          {"GDS projection reads the stored property value kind or its Java class name at run time, not a static operand.", "dbbb995c12e82d89eb41bc0379692fd2ca2915bb782e70a086752e2877548cfc"},
	"gds_projection.go:gdsJavaTypeTarget":                                     {"GDS projection reads the stored property value kind or its Java class name at run time, not a static operand.", "3af32eccf910abee2ea4d92f6a7e6287cdfb2c54a01a0a204cd0d6c7776faa87"},
	"gds_property_values.go:gdsConvertPropertyValue":                          {"GDS projection reads the stored property value kind or its Java class name at run time, not a static operand.", "9b0539412034ba6485c954a676665abc0fc98ce299ff1d27810ec121273d833b"},
	"gds_property_values.go:*GraphProjection.nodePropertyValue":               {"GDS projection reads the stored property value kind or its Java class name at run time, not a static operand.", "13de77dfc3699eb4d528ce2e0e94d7500d6b4748500f506aa28d9b8b56923330"},
	"property_access_semantic_validation.go:parameterMayRejectPropertyAccess": {"Types a bound parameter from its runtime value (parameterPropertyAccessType) before a property access; parameters carry values, never a static union.", "939f9951606a7d04476e14c67ef15fdfa6595c84091acce48b50d95ded5dfe15"},
	"expression_validation.go:staticListOperand":                              {"Names a bound parameter's value in a list position by its runtime kind (cypherTypeName; the parameter boundary); every other operand is typed by operandOf.", "99f2601cef82f95f2401513f4ac8ca762b323978aec2620b3823967dae678ec4"},
}

// staticTypingOwnerFiles hold the one typing pass: the operand type and its
// predicates, and the deriver that computes every expression's operand.
var staticTypingOwnerFiles = map[string]bool{
	"static_operand.go": true, "static_operator_types.go": true,
	"static_comprehension_types.go": true, "static_type_scope.go": true,
}

// staticTypeNameDerivers return a derived static type name: comparing their
// result with anything but "" decides by type name.
var staticTypeNameDerivers = map[string]bool{
	"typeName": true, "typeOf": true, "staticExpressionType": true, "staticLiteralType": true,
	"staticTextType": true,
	"elementOfType":  true, "listOfType": true, "propertyAccessExpressionType": true,
	"staticFunctionResultType": true, "procedureArgumentTypeName": true,
}

func isNilIdent(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "nil"
}

// Outside the owner, no code decides by comparing a static type name with a
// Cypher type or with another derived type name (==, !=, a switch case), or
// reads a list type by its "List<" prefix: a type is read through operandOf
// and decided through fits or excludes, so a union is never mistaken for one
// of its alternatives.
func TestStaticTypeNameDecisionOwner(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	isTypeName := func(expression ast.Expr) bool {
		literal, ok := expression.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return false
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return false
		}
		switch value {
		case "Integer", "Float", "String", "Boolean", "Map", "Node", "Relationship", "Path", "Null",
			"Number", "Duration", "Date", "Time", "LocalTime", "LocalDateTime", "DateTime", "Point",
			"Vector", "UUID", "Any", "Map, Node or Relationship", "Float, Integer or Number":
			return true
		}
		return strings.HasPrefix(value, "List<")
	}
	// A derived type name: a deriver's result, or a variable the function
	// assigns one to. Comparing one with "" asks whether the type is known,
	// which decides nothing about the type.
	isDeriverCall := func(expression ast.Expr) bool {
		call, ok := expression.(*ast.CallExpr)
		if !ok {
			return false
		}
		switch function := call.Fun.(type) {
		case *ast.Ident:
			return staticTypeNameDerivers[function.Name]
		case *ast.SelectorExpr:
			return staticTypeNameDerivers[function.Sel.Name]
		}
		return false
	}
	derivedVariables := func(body *ast.BlockStmt) map[string]bool {
		derived := map[string]bool{}
		ast.Inspect(body, func(node ast.Node) bool {
			if assign, ok := node.(*ast.AssignStmt); ok && len(assign.Lhs) == len(assign.Rhs) {
				for index, target := range assign.Lhs {
					if identifier, ok := target.(*ast.Ident); ok && isDeriverCall(assign.Rhs[index]) {
						derived[identifier.Name] = true
					}
				}
			}
			return true
		})
		return derived
	}
	isEmptyString := func(expression ast.Expr) bool {
		literal, ok := expression.(*ast.BasicLit)
		return ok && literal.Kind == token.STRING && literal.Value == `""`
	}
	found := map[string]bool{}
	for _, path := range files {
		if staticTypingOwnerFiles[path] || strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, path, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			derived := derivedVariables(function.Body)
			derivesTypeName := func(expression ast.Expr) bool {
				if identifier, ok := expression.(*ast.Ident); ok {
					return derived[identifier.Name]
				}
				return isDeriverCall(expression)
			}
			decides := false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.BinaryExpr:
					if node.Op == token.EQL || node.Op == token.NEQ {
						if isTypeName(node.X) || isTypeName(node.Y) ||
							derivesTypeName(node.X) && derivesTypeName(node.Y) ||
							(derivesTypeName(node.X) || derivesTypeName(node.Y)) && !isEmptyString(node.X) && !isEmptyString(node.Y) && !isNilIdent(node.X) && !isNilIdent(node.Y) {
							decides = true
						}
					}
				case *ast.CaseClause:
					for _, item := range node.List {
						if isTypeName(item) {
							decides = true
						}
					}
				case *ast.CallExpr:
					if selector, ok := node.Fun.(*ast.SelectorExpr); ok && (selector.Sel.Name == "HasPrefix" || selector.Sel.Name == "TrimPrefix" || selector.Sel.Name == "CutPrefix") {
						for _, argument := range node.Args {
							if literal, ok := argument.(*ast.BasicLit); ok && literal.Value == `"List<"` {
								decides = true
							}
						}
					}
				}
				return true
			})
			if !decides {
				continue
			}
			key := path + ":" + function.Name.Name
			if function.Recv != nil && len(function.Recv.List) > 0 {
				key = path + ":" + types.ExprString(function.Recv.List[0].Type) + "." + function.Name.Name
			}
			found[key] = true
			start, end := fs.Position(function.Pos()).Offset, fs.Position(function.End()).Offset
			compact := strings.Map(func(r rune) rune {
				if unicode.IsSpace(r) {
					return -1
				}
				return r
			}, string(source[start:end]))
			digest := sha256.Sum256([]byte(compact))
			exception, ok := staticTypeNameDecisionExceptions[key]
			if !ok || exception.reason == "" || exception.sha256 != hex.EncodeToString(digest[:]) {
				t.Errorf("%s: compares a Cypher type name outside static_operand.go (sha256 %s)", key, hex.EncodeToString(digest[:]))
			}
		}
	}
	for key := range staticTypeNameDecisionExceptions {
		if !found[key] {
			t.Errorf("%s: stale type-name decision exception", key)
		}
	}
}
