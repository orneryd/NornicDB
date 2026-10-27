package cypher

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This smoke check covers known byte classifiers and literal space
// boundary checks. It does not infer variable values or query-text provenance.
// Generated behavior properties and source mutations cover the real consumers.
var byteLexicalHelper = regexp.MustCompile(`(?i)^(is(name|ident|identifier|word).*(byte|char|start)|is(ascii)?(white)?space(byte)?|iscypherwhitespace)$`)

func lexicalOwnerViolations(source []byte) ([]string, error) {
	set := token.NewFileSet()
	tree, err := parser.ParseFile(set, "source.go", source, 0)
	if err != nil {
		return nil, err
	}
	var violations []string
	add := func(node ast.Node, reason string) {
		violations = append(violations, fmt.Sprintf("%d: %s", set.Position(node.Pos()).Line, reason))
	}
	space := func(expr ast.Expr) bool {
		literal, ok := expr.(*ast.BasicLit)
		if !ok || literal.Kind != token.CHAR {
			return false
		}
		value, err := strconv.Unquote(literal.Value)
		return err == nil && (value == " " || value == "\t")
	}
	ast.Inspect(tree, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.CallExpr:
			if id, ok := node.Fun.(*ast.Ident); ok && byteLexicalHelper.MatchString(id.Name) {
				add(node, "byte lexical helper")
			}
			if fn, ok := node.Fun.(*ast.SelectorExpr); ok && (fn.Sel.Name == "IsSpace" || fn.Sel.Name == "IsLetter" || fn.Sel.Name == "IsDigit") && len(node.Args) == 1 {
				if cast, ok := node.Args[0].(*ast.CallExpr); ok && len(cast.Args) == 1 {
					if _, ok := cast.Args[0].(*ast.IndexExpr); ok {
						add(node, "byte cast passed to Unicode classification")
					}
				}
			}
			if fn, ok := node.Fun.(*ast.SelectorExpr); ok && len(node.Args) == 2 {
				switch fn.Sel.Name {
				case "Trim", "TrimLeft", "TrimRight", "ContainsAny", "IndexAny":
					if literal, ok := node.Args[1].(*ast.BasicLit); ok && literal.Kind == token.STRING {
						cutset, err := strconv.Unquote(literal.Value)
						if err == nil && strings.ContainsRune(cutset, ' ') && strings.ContainsRune(cutset, '\t') {
							add(node, "ASCII whitespace cutset")
						}
					}
				}
			}
		case *ast.FuncDecl:
			if byteLexicalHelper.MatchString(node.Name.Name) {
				add(node, "duplicate lexical helper")
			}
		case *ast.BinaryExpr:
			if (node.Op == token.EQL || node.Op == token.NEQ) && (space(node.X) || space(node.Y)) {
				add(node, "literal whitespace boundary")
			}
		case *ast.CaseClause:
			for _, expr := range node.List {
				// Tab-only cases also encode log escaping, which is a data
				// transformation. Space-bearing cases are scanner classifiers.
				if literal, ok := expr.(*ast.BasicLit); ok && literal.Kind == token.CHAR && literal.Value == "' '" {
					add(node, "literal whitespace switch")
					break
				}
			}
		}
		return true
	})
	return violations, nil
}

func TestLexicalOwnerGuard(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, directory := range []string{"internal/cyphertext", "pkg/cypher", "pkg/fabric", "pkg/observability", "pkg/storage/constraint_contracts.go", "pkg/knowledgepolicy/on_access_runtime.go", "pkg/adminimport/importer.go", "pkg/server/server_helpers.go"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if filepath.ToSlash(path) == filepath.ToSlash(filepath.Join(root, "internal/cyphertext/lexical.go")) {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			violations, err := lexicalOwnerViolations(source)
			if err != nil {
				return err
			}
			for _, violation := range violations {
				t.Errorf("%s:%s", path, violation)
			}
			return nil
		})
		require.NoError(t, err)
	}
}

func TestLexicalOwnerGuardMutation(t *testing.T) {
	for _, body := range []string{
		`func isNameByte(b byte) bool { return b >= 0x80 }`,
		`func isWhitespace(b byte) bool { return b == ' ' }`,
		`func boundary(q string, i int) bool { return q[i] == ' ' || q[i] == '\t' }`,
		`func boundary(q string, i int) bool { return q[i] != ' ' }`,
		`func boundary(q string, i int) bool { switch q[i] { case ' ', '\t': return true }; return false }`,
		`func boundary(q string, i int) bool { return unicode.IsSpace(rune(q[i])) }`,
		`func boundary(q string) string { return strings.TrimLeft(q, " \t\r\n") }`,
	} {
		violations, err := lexicalOwnerViolations([]byte("package probe\n" + body))
		require.NoError(t, err)
		require.NotEmpty(t, violations, "mutation survived")
	}
	violations, err := lexicalOwnerViolations([]byte(`package probe
func boundary(q string, i int) bool { return cyphertext.IsWhitespaceAt(q, i) || cyphertext.NamePartAt(q, i) }`))
	require.NoError(t, err)
	require.Empty(t, violations)
}
