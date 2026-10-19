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

// A call reader must consume the lexical owner's spans. In particular a generic
// comma splitter is part of this contract even when its name says nothing about
// functions. Inspect syntax rather than matching formatting or a list of files.
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
			if reason := alternateFunctionSyntax(function.Body); reason != "" {
                if reason == "independent quote/comma/group scanner" && functionGrammarBoundary[name+":"+function.Name.Name] != "" {continue}
				t.Errorf("%s:%s: %s; consume functionSyntax instead", name, function.Name.Name, reason)
			}
		}
	}
}

var rawFunctionPrefix = regexp.MustCompile("^[A-Za-z_][A-Za-z_0-9.]*[ \\t]*\\($")

func alternateFunctionSyntax(body ast.Node) string {
	reason := ""
	ast.Inspect(body, func(node ast.Node) bool {
		if reason != "" {
			return false
		}
		switch n := node.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			quote, comma, group := false, false, false
			ast.Inspect(n, func(child ast.Node) bool {
				if identifier, ok := child.(*ast.Ident); ok && (identifier.Name == "delim" || identifier.Name == "delimiter" || identifier.Name == "separator") {comma=true}
                literal, ok := child.(*ast.BasicLit)
				if !ok || literal.Kind != token.CHAR {
					return true
				}
				value, _ := strconv.Unquote(literal.Value)
				quote = quote || value == "'" || value == "\"" || value == "`"
				comma = comma || value == ","
				group = group || value == "(" || value == "[" || value == "{"
				return true
			})
			if quote && comma && group && reason == "" {
				reason = "independent quote/comma/group scanner"
			}
		case *ast.CallExpr:
			selector, ok := n.Fun.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "HasPrefix" && selector.Sel.Name != "Index" && selector.Sel.Name != "IndexByte") || len(n.Args) < 2 {
				return true
			}
			literal, ok := n.Args[1].(*ast.BasicLit)
			if ok && literal.Kind == token.STRING {
				value, _ := strconv.Unquote(literal.Value)
				if rawFunctionPrefix.MatchString(value) {
					reason = "independent function-prefix reader"
				}
			}
		}
		return true
	})
	return reason
}

func TestFunctionSyntaxGuardDetectsAlternateReaders(t *testing.T) {
	for _, source := range []string{
		"package p; func f(s string) { for _, c := range s { switch c { case '\\'': case ',': case '(': } } }",
		"package p; func f(s string) bool { return strings.HasPrefix(s, \"substring(\") }",
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "mutant.go", source, 0)
		require.NoError(t, err)
		require.NotEmpty(t, alternateFunctionSyntax(file.Decls[0].(*ast.FuncDecl).Body), source)
	}
}

// These readers implement genuine graph/scope/parser protocols, not callable
// identity or argument splitting. No call reader or generic splitter belongs
// here. Raw function-prefix reading is forbidden even within these boundaries.
var functionGrammarBoundary = map[string]string{
 "parser.go:tokenize":"Execution parser emits grammar tokens, never function argument spans.",
 "create.go:scanCreatePatterns":"Graph pattern grammar classifies node/relationship steps and offsets.",
 "executor_subqueries.go:firstVariableOccurrence":"Scope diagnostic finds first free variable occurrence.",
 "label_expression_rewrite.go:nameSingleNodePaths":"Graph path grammar supplies generated node names.",
 "set_semantic_validation.go:expressionFreeVariables":"Scope grammar classifies local variable bindings.",
}
