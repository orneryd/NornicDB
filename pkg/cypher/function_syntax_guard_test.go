package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Generic splitters are call readers too. Inspect AST operations rather than
// formatting, and exercise the detector with alternate-reader mutants below.
func TestFunctionSyntaxOwnerGuard(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "function_syntax.go" {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			for _, reason := range alternateFunctionSyntax(function.Body) {
				if reason == "independent quote/comma/group scanner" && functionGrammarBoundary[name+":"+function.Name.Name] != "" {
					continue
				}
				t.Errorf("%s:%s: %s; consume functionSyntax instead", name, function.Name.Name, reason)
			}
		}
	}
}

var rawFunctionPrefix = regexp.MustCompile(`^[A-Za-z_][A-Za-z_0-9.]*[ \t]*\(`)
var callTextInput = regexp.MustCompile(`(?i)(expr|arg|param|signature|call|content|^text$|^s$)`)

func alternateFunctionSyntax(body ast.Node) []string {
	reasons := map[string]bool{}
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			quote, comma, group := false, false, false
			ast.Inspect(n, func(child ast.Node) bool {
				if identifier, ok := child.(*ast.Ident); ok && (identifier.Name == "delim" || identifier.Name == "delimiter") {
					comma = true
				}
				if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.CHAR {
					value, _ := strconv.Unquote(literal.Value)
					quote = quote || value == "'" || value == `"` || value == "`"
					comma = comma || value == ","
					group = group || value == "(" || value == "[" || value == "{"
				}
				return true
			})
			if quote && comma && group {
				reasons["independent quote/comma/group scanner"] = true
			}
		case *ast.CallExpr:
			selector, ok := n.Fun.(*ast.SelectorExpr)
			if !ok || len(n.Args) < 2 {
				return true
			}
			library, ok := selector.X.(*ast.Ident)
			if !ok || library.Name != "strings" {
				return true
			}
			switch selector.Sel.Name {
			case "HasPrefix", "Contains", "Index", "IndexByte", "IndexRune", "LastIndex", "Split", "SplitN":
			default:
				return true
			}
			if expression, ok := n.Args[1].(*ast.BinaryExpr); ok && expression.Op == token.ADD {
				if literal, ok := expression.Y.(*ast.BasicLit); ok {
					value, _ := strconv.Unquote(literal.Value)
					if value == "(" {
						reasons["independent function-prefix reader"] = true
					}
				}
			}
			ast.Inspect(n.Args[1], func(child ast.Node) bool {
				if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.STRING {
					value, _ := strconv.Unquote(literal.Value)
					if rawFunctionPrefix.MatchString(value) {
						reasons["independent function-prefix reader"] = true
					}
				}
				return true
			})
			literal, ok := n.Args[1].(*ast.BasicLit)
			if !ok {
				return true
			}
			value, _ := strconv.Unquote(literal.Value)
			if rawFunctionPrefix.MatchString(value) {
				reasons["independent function-prefix reader"] = true
			}
			if (value == "(" || value == ",") && selector.Sel.Name != "HasPrefix" && selector.Sel.Name != "Contains" {
				ast.Inspect(n.Args[0], func(child ast.Node) bool {
					if identifier, ok := child.(*ast.Ident); ok && callTextInput.MatchString(identifier.Name) {
						reasons["raw call-text delimiter reader"] = true
					}
					return true
				})
			}
		}
		return true
	})
	var out []string
	for reason := range reasons {
		out = append(out, reason)
	}
	return out
}

func TestFunctionSyntaxGuardDetectsAlternateReaders(t *testing.T) {
	for _, source := range []string{
		"package p; func f(s string) { for _, c := range s { switch c { case '\\'': case ',': case '(': } } }",
		"package p; func f(s string, delim rune) { for _, c := range s { switch c { case '\\'': case '(': }; if c == delim {} } }",
		`package p; func f(s string) bool { return strings.HasPrefix(s, "substring(") }`,
		`package p; func f(s, name string) bool { return strings.Contains(s, name+"(") }`,
		`package p; func f(expr string) int { return strings.IndexByte(expr, '(') }`,
		`package p; func f(args string) []string { return strings.Split(args, ",") }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "mutant.go", source, 0)
		require.NoError(t, err)
		require.NotEmpty(t, alternateFunctionSyntax(file.Decls[0].(*ast.FuncDecl).Body), source)
	}
}

// Genuine protocol grammars classify graph steps or variable bindings; they do
// not produce callable identities or argument spans. Raw call-text and prefix
// reading remain forbidden even inside these explicitly named boundaries.
var functionGrammarBoundary = map[string]string{
	"parser.go:tokenize":                                 "Execution grammar tokens.",
	"create.go:scanCreatePatterns":                       "Graph node/relationship steps and offsets.",
	"executor_subqueries.go:firstVariableOccurrence":     "Free-variable diagnostic position.",
	"label_expression_rewrite.go:nameSingleNodePaths":    "Generated graph node names.",
	"set_semantic_validation.go:expressionFreeVariables": "Local variable bindings.",
}
