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
	callIdentity, delimiterMatcher := false, false
	ast.Inspect(body, func(node ast.Node) bool {
		if name, ok := node.(*ast.Ident); ok {
			switch name.Name {
			case "parseFunctionCallWS", "functionCallAt", "functionCallBeforeParen", "bindingFunctionBeforeParen", "parseReduceForm":
				callIdentity = true
			case "findMatchingDelimiter", "findMatchingParen":
				delimiterMatcher = true
			}
		}
		return true
	})
	if callIdentity && delimiterMatcher { reasons["independent callable group matcher"] = true }
	patterns := map[string]bool{}
	for changed := true; changed; {
		changed = false
		ast.Inspect(body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Lhs) != len(assignment.Rhs) {
				return true
			}
			for index, left := range assignment.Lhs {
				name, ok := left.(*ast.Ident)
				if ok && !patterns[name.Name] && rawCallablePattern(assignment.Rhs[index], patterns) {
					patterns[name.Name] = true
					changed = true
				}
			}
			return true
		})
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			quote, comma, group, dot := false, false, false, false
			ast.Inspect(n, func(child ast.Node) bool {
				if identifier, ok := child.(*ast.Ident); ok && (identifier.Name == "delim" || identifier.Name == "delimiter") {
					comma = true
				}
				if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.CHAR {
					value, _ := strconv.Unquote(literal.Value)
					quote = quote || value == "'" || value == `"` || value == "`"
					comma = comma || value == ","
					dot = dot || value == "."
					group = group || value == "(" || value == "[" || value == "{"
				}
				return true
			})
			if quote && group && (comma || dot) {
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
			if rawCallablePattern(n.Args[1], patterns) {
				reasons["independent function-prefix reader"] = true
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
		"package p; func f(s string) { for _, c := range s { switch c { case '\\'': case '.': case '(': } } }",
		`package p; func f(s string) { functionCallAt(s,0); findMatchingDelimiter(s,0,'(',')') }`,
		`package p; func f(q string) bool { prefix:="count("; alias:=prefix; return strings.Contains(q,alias) }`,
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

func rawCallablePattern(node ast.Node, aliases map[string]bool) bool {
	found := false
	ast.Inspect(node, func(child ast.Node) bool {
		if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			value, _ := strconv.Unquote(literal.Value)
			found = found || rawFunctionPrefix.MatchString(value)
		}
		if identifier, ok := child.(*ast.Ident); ok {
			found = found || aliases[identifier.Name]
		}
		return !found
	})
	return found
}
