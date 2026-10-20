package cypher

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
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
				owner = filepath.Base(path) == "semantic_checker.go" && function.Name.Name == "operatorChecker" && function.Recv != nil && len(function.Recv.List) == 1
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
				case *ast.CompositeLit:
					if value.Type != nil && containsProtected(value.Type) {
						violations = append(violations, path+": checker literal outside frame owner")
					}
				case *ast.ValueSpec:
					if named, ok := value.Type.(*ast.Ident); ok && protected[named.Name] && len(value.Values) == 0 {
						violations = append(violations, path+": zero checker outside frame owner")
					}
				case *ast.CallExpr:
					if name, ok := value.Fun.(*ast.Ident); ok && name.Name == "new" && len(value.Args) == 1 {
						if named, ok := value.Args[0].(*ast.Ident); ok && protected[named.Name] {
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
		`package cypher;func renamed(scope staticTypeScope){_ = staticOperatorChecker{scope:scope}}`,
		`package cypher;type renamed=staticOperatorChecker;func helper(scope staticTypeScope){_ = renamed{scope:scope}}`,
		`package cypher;func renamed(){var lost staticOperatorChecker;_ = lost}`,
		`package cypher;func renamed(){_ = new(staticOperatorChecker)}`,
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
	// Compiler proof uses the ACTUAL checker declaration. Supplying a second
	// parameter map is no longer a valid Go construction, even in the owner.
	file, err := parser.ParseFile(token.NewFileSet(), "semantic_checker.go", sources["semantic_checker.go"], 0)
	require.NoError(t, err)
	var declaration bytes.Buffer
	for _, value := range file.Decls {
		if general, ok := value.(*ast.GenDecl); ok {
			for _, candidate := range general.Specs {
				if spec, ok := candidate.(*ast.TypeSpec); ok && spec.Name.Name == "staticOperatorChecker" {
					declaration.WriteString("type ")
					require.NoError(t, printer.Fprint(&declaration, token.NewFileSet(), spec))
				}
			}
		}
	}
	require.NotEmpty(t, declaration.String())
	set := token.NewFileSet()
	invalid, err := parser.ParseFile(set, "mutation.go", "package fixture;type staticTypeScope struct{};"+declaration.String()+";var _=staticOperatorChecker{params:map[string]interface{}{}}", 0)
	require.NoError(t, err)
	_, err = (&types.Config{}).Check("fixture", set, []*ast.File{invalid}, nil)
	require.ErrorContains(t, err, "unknown field params")
}
