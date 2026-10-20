package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Discover types across the whole package, including aliases and future
// parameter-carrying validators, rather than listing the reviewed consumers.
func checkerConstructionViolations(sources map[string][]byte) ([]string, error) {
	files := make(map[string]*ast.File)
	protected := map[string]bool{"staticOperatorChecker": true}
	checkerMethods := make(map[string]bool)
	typedCheckerMethods := make(map[string]bool)
	var violations []string
	for name, source := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
		if err != nil {
			return nil, err
		}
		files[name] = file
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			// A fielded frame starts only at the admission or explicitly
			// binding-kind-only compatibility owners. All other consumers
			// must retain an incoming frame (withTypes), not copy its fields.
			owner := filepath.Base(name) + ":" + function.Name.Name
			frameOwner := owner == "match_semantic_validation.go:semanticScopeWalk" ||
				owner == "static_type_scope.go:static" ||
				owner == "static_type_scope.go:projectMatchSemanticScope" ||
				owner == "expression_validation.go:graphListOperandTypeError" ||
				owner == "executor_show.go:applyShowTail"
			if !strings.HasSuffix(name, "_test.go") {
				freshTypeViewOwner := owner == "semantic_checker.go:withTypes" ||
					owner == "static_type_scope.go:projectTypeScope" ||
					owner == "static_type_scope.go:projectStaticValueTypes" ||
					owner == "static_type_scope.go:projectStaticValueMembers" ||
					owner == "static_type_scope.go:callSubqueryReturnTypesInput" ||
					owner == "fabric_semantic_compilation.go:resolve"
				ast.Inspect(function.Body, func(node ast.Node) bool {
					if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "static" && !freshTypeViewOwner {
						violations = append(violations, owner+": fresh type view outside its producer")
					}
					return true
				})
			}
			if !frameOwner && !strings.HasSuffix(name, "_test.go") {
				ast.Inspect(function.Body, func(node ast.Node) bool {
					literal, ok := node.(*ast.CompositeLit)
					if ok && len(literal.Elts) != 0 {
						if named, ok := literal.Type.(*ast.Ident); ok && named.Name == "staticTypeScope" {
							violations = append(violations, owner+": partial typed frame outside admission owner")
						}
					}
					return true
				})
			}
			frame := false
			for _, fields := range []*ast.FieldList{function.Recv, function.Type.Params} {
				if fields == nil {
					continue
				}
				ast.Inspect(fields, func(node ast.Node) bool {
					if named, ok := node.(*ast.Ident); ok && named.Name == "staticTypeScope" {
						frame = true
					}
					return true
				})
			}
			if !frame {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if name, ok := call.Fun.(*ast.Ident); ok && (name.Name == "staticLiteralTypeName" || name.Name == "literalFunctionResultType" || name.Name == "staticListTypeName" || name.Name == "staticArithmeticTypeName") {
						violations = append(violations, name.Name+": literal inference outside admitted operand fallback")
					}
				}
				return true
			})
		}
		for _, imported := range file.Imports {
			if imported.Path.Value == `"github.com/orneryd/nornicdb/internal/admittedcheck"` && filepath.Base(name) != "semantic_checker.go" {
				violations = append(violations, name+": checker factory import outside frame owner")
			}
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || len(function.Recv.List) != 1 || !(strings.HasPrefix(strings.ToLower(function.Name.Name), "check") || strings.HasPrefix(strings.ToLower(function.Name.Name), "validate")) {
				continue
			}
			receiver := function.Recv.List[0].Type
			if pointer, ok := receiver.(*ast.StarExpr); ok {
				receiver = pointer.X
			}
			if named, ok := receiver.(*ast.Ident); ok {
				checkerMethods[named.Name] = true
				method := strings.ToLower(function.Name.Name)
				typed := method == "check" || method == "validate"
				if function.Type.Results != nil {
					for _, result := range function.Type.Results.List {
						if operand, ok := result.Type.(*ast.Ident); ok && operand.Name == "staticOperand" {
							typed = true
						}
					}
				}
				typedCheckerMethods[named.Name] = typedCheckerMethods[named.Name] || typed
			}
		}
	}
	for name, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if spec.Name.Name == "staticTypeScope" && filepath.Base(name) == "static_function_argument_types.go" {
				return true // the one admitted parameter/frame owner
			}
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			isChecker := typedCheckerMethods[spec.Name.Name] || strings.Contains(strings.ToLower(spec.Name.Name), "checker") || strings.Contains(strings.ToLower(spec.Name.Name), "validator")
			for _, field := range structure.Fields.List {
				for _, fieldName := range field.Names {
					parameter := strings.ToLower(fieldName.Name)
					// Executor shell-parameter storage is not a typed checker.
					// Discover renamed validators from canonical parameter fields,
					// and typed checkers from their operand-producing methods.
					if (isChecker && strings.Contains(parameter, "param")) || checkerMethods[spec.Name.Name] && (parameter == "params" || parameter == "parameters") {
						protected[spec.Name.Name] = true
						violations = append(violations, name+": checker has an independent parameter source")
					}
				}
			}
			return true
		})
	}
	var containsProtected func(ast.Expr) bool
	containsProtected = func(expression ast.Expr) bool {
		found := false
		ast.Inspect(expression, func(node ast.Node) bool {
			if named, ok := node.(*ast.Ident); ok && protected[named.Name] {
				found = true
			}
			return !found
		})
		return found
	}
	for changed := true; changed; {
		changed = false
		for _, file := range files {
			ast.Inspect(file, func(node ast.Node) bool {
				if spec, ok := node.(*ast.TypeSpec); ok {
					if _, structure := spec.Type.(*ast.StructType); !structure && containsProtected(spec.Type) && !protected[spec.Name.Name] {
						protected[spec.Name.Name], changed = true, true
					}
				}
				return true
			})
		}
	}
	for path, file := range files {
		for _, declaration := range file.Decls {
			owner := false
			if function, ok := declaration.(*ast.FuncDecl); ok {
				owner = filepath.Base(path) == "semantic_checker.go" && (function.Name.Name == "checkOperand" || function.Name.Name == "checkPostfixOperand") && function.Recv != nil && len(function.Recv.List) == 1
				if owner {
					receiver, ok := function.Recv.List[0].Type.(*ast.Ident)
					owner = ok && receiver.Name == "staticTypeScope"
				}
			}
			if owner {
				continue
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.SelectorExpr:
					if value.Sel.Name == "executeInternalWithAdmission" && filepath.Base(path) != "executor_internal.go" && filepath.Base(path) != "executor_fabric.go" {
						violations = append(violations, path+": physical admission entrance outside its adapter")
					}
					if value.Sel.Name == "checkExpression" || value.Sel.Name == "checkPostfixRequest" {
						violations = append(violations, path+": raw checker operation outside frame owner")
					}
					if qualifier, ok := value.X.(*ast.Ident); ok && value.Sel.Name == "New" {
						for _, imported := range file.Imports {
							if imported.Path.Value == `"github.com/orneryd/nornicdb/internal/admittedcheck"` {
								alias := "admittedcheck"
								if imported.Name != nil {
									alias = imported.Name.Name
								}
								if qualifier.Name == alias {
									violations = append(violations, path+": checker constructor outside frame owner")
								}
							}
						}
					}
				case *ast.CompositeLit:
					if value.Type != nil && containsProtected(value.Type) {
						violations = append(violations, path+": checker literal outside frame owner")
					}
				case *ast.Field:
					if containsProtected(value.Type) {
						violations = append(violations, path+": checker type outside frame owner")
					}
				case *ast.ValueSpec:
					if value.Type != nil && containsProtected(value.Type) && len(value.Values) == 0 {
						violations = append(violations, path+": zero checker outside frame owner")
					}
				case *ast.CallExpr:
					if containsProtected(value.Fun) {
						violations = append(violations, path+": checker conversion outside frame owner")
					}
					if name, ok := value.Fun.(*ast.Ident); ok && name.Name == "new" && len(value.Args) == 1 {
						if containsProtected(value.Args[0]) {
							violations = append(violations, path+": checker allocation outside frame owner")
						}
					}
				}
				return true
			})
		}
	}
	return violations, nil
}

func TestSemanticCheckerConstructionOwner(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	sources := make(map[string][]byte)
	for _, path := range paths {
		sources[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}
	violations, err := checkerConstructionViolations(sources)
	require.NoError(t, err)
	require.Empty(t, violations)
	// A runtime owner that validates statements and stores shell parameters
	// is not itself a typed operand checker.
	violations, err = checkerConstructionViolations(map[string][]byte{"executor.go": []byte(`package cypher;type executor struct{shellParams map[string]interface{}};func(executor)validateStatement(){}`)})
	require.NoError(t, err)
	require.Empty(t, violations)
	for _, mutation := range []string{
		`package cypher;func consumer(input staticTypeScope){_ = typeScopeOf(input).static(input.cypher25)}`,
		`package cypher;func consumer(input staticTypeScope){_ = staticTypeScope{values:input.values}}`,
		`package cypher;func consumer(input staticTypeScope){_ = staticTypeScope{params:input.params,kinds:input.kinds}}`,
		`package cypher;func renamed(scope staticTypeScope){_ = staticOperatorChecker{scope:scope}}`,
		`package cypher;type renamed=staticOperatorChecker;func helper(scope staticTypeScope){_ = renamed{scope:scope}}`,
		`package cypher;func renamed(){var lost staticOperatorChecker;_ = lost}`,
		`package cypher;func renamed(){_ = new(staticOperatorChecker)}`,
		`package cypher;func renamed()(r staticOperatorChecker){return}`,
		`package cypher;func renamed(){var a [1]staticOperatorChecker;_ = a[0]}`,
		`package cypher;func renamed(){_ = new((staticOperatorChecker))}`,
		`package cypher;func renamed(s staticTypeScope){_ = staticOperatorChecker(struct{scope staticTypeScope}{scope:s})}`,
		`package cypher;func renamed(s staticTypeScope){_,_ = s.checkExpression("x")}`,
		`package cypher;import a "github.com/orneryd/nornicdb/internal/admittedcheck";func renamed(){_ = a.New(1,func(x int,y string)(int,error){return x,nil})}`,

		`package cypher;func renamed(scope staticTypeScope){_ = []staticOperatorChecker{{scope:scope}}}`,
		`package cypher;func renamed(scope staticTypeScope){_ = map[string]*staticOperatorChecker{"x":{scope:scope}}}`,
		`package cypher;type hidden=[]staticOperatorChecker;func renamed(scope staticTypeScope){_ = hidden{{scope:scope}}}`,
		`package cypher;type unrelated struct{params map[string]interface{}};func(unrelated)validateOperand(){};func renamed(){_ = unrelated{}}`,
		`package cypher;type futureValidator struct{params map[string]interface{}};func renamed(){_ = futureValidator{}}`,
		`package cypher;type unrelated struct{parameters map[string]interface{}};func(unrelated)check(){};func renamed(){_ = unrelated{}}`,
	} {
		violations, err := checkerConstructionViolations(map[string][]byte{"renamed_consumer.go": []byte(mutation)})
		require.NoError(t, err)
		require.NotEmpty(t, violations, mutation)
	}
	testOpaqueCheckerCompiler(t)
}

type checkerFixtureImporter struct{ owner *types.Package }

func (value checkerFixtureImporter) Import(path string) (*types.Package, error) {
	if path == value.owner.Path() {
		return value.owner, nil
	}
	return nil, os.ErrNotExist
}

// Type-check the ACTUAL opaque owner package, then compile external mutations.
// No facsimile checker declaration can accidentally prove a different boundary.
func testOpaqueCheckerCompiler(t *testing.T) {
	set := token.NewFileSet()
	path := filepath.Join("..", "..", "internal", "admittedcheck", "checker.go")
	source, err := os.ReadFile(path)
	require.NoError(t, err)
	file, err := parser.ParseFile(set, path, source, 0)
	require.NoError(t, err)
	ownerPath := "github.com/orneryd/nornicdb/internal/admittedcheck"
	owner, err := (&types.Config{}).Check(ownerPath, set, []*ast.File{file}, nil)
	require.NoError(t, err)
	require.False(t, owner.Scope().Lookup("checker").Exported())
	compile := func(body string) error {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, "external.go", `package external;import owner "`+ownerPath+`";`+body, 0)
		require.NoError(t, err)
		_, err = (&types.Config{Importer: checkerFixtureImporter{owner: owner}}).Check("external", set, []*ast.File{file}, nil)
		return err
	}
	require.NoError(t, compile(`var value=owner.New(1,func(frame int,input string)(int,error){return frame,nil});var result,err=value.Check("x")`))
	for _, body := range []string{
		`var _=owner.checker[int,string,int]{}`,
		`func zero()(result owner.checker[int,string,int]){return}`,
		`var values [1]owner.checker[int,string,int]`,
		`var _=new((owner.checker[int,string,int]))`,
		`var _=owner.checker[int,string,int](struct{frame int;run func(int,string)(int,error)}{})`,
		`type alias=owner.checker[int,string,int];var value alias`,
	} {
		require.ErrorContains(t, compile(body), "not exported", body)
	}
}

func TestSemanticCheckerRejectsLiteralFactOverride(t *testing.T) {
	for _, expression := range []string{"staticLiteralTypeName(text)", "literalFunctionResultType(text, text)", "staticListTypeName(text)", "staticArithmeticTypeName(text)"} {
		source := "package cypher; func consumer(frame staticTypeScope, text string) string { return " + expression + " }"
		violations, err := checkerConstructionViolations(map[string][]byte{"consumer.go": []byte(source)})
		require.NoError(t, err)
		require.NotEmpty(t, violations, expression)
	}
}

func TestSemanticCheckerAdmissionPurposeGuard(t *testing.T) {
	source := "package cypher; func consumer(e *StorageExecutor) { e.executeInternalWithAdmission(nil, \"RETURN 1\", nil, nil) }"
	violations, err := checkerConstructionViolations(map[string][]byte{"consumer.go": []byte(source)})
	require.NoError(t, err)
	require.NotEmpty(t, violations)
}
