package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A consumer must use the recorded query facts. Retokenizing a query or an
// argument inside its invocation loop repeats work even without allocations.
func TestFunctionSyntaxConsumersUseRecordedPositions(t *testing.T) {
	for file, functions := range map[string]map[string]bool{
		"deleted_entity_projection.go": {"deletedEntityReadsIn": true},
		"execution_determinism.go":     {"compileExecutionPolicy": true},
	} {
		source, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		require.NoError(t, err)
		for _, declaration := range source.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || !functions[function.Name.Name] {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if name, ok := call.Fun.(*ast.Ident); ok {
					require.NotContains(t, []string{"forEachFunctionInvocation", "lexicalProcedureInvocations", "functionArgumentsAt", "appendFunctionArgumentsAt", "queryCommentEnd", "queryGapEnd", "skipCypherLexicalToken", "QueryPermissionRequirements"}, name.Name, file+":"+function.Name.Name)
				}
				return true
			})
		}
	}
}

func TestFunctionSyntaxCursorRetainsPositions(t *testing.T) {
	prefix := strings.Repeat("/* a quoted-looking ' ( , */ 0 + ", 1200)
	text := prefix + "labels(/* input */ `n`) + n.p IS NOT NULL"
	syntax := functionSyntax{text: text, leading: -1}
	request := functionSyntaxRequest{activeOpen: -1}
	cursor := syntaxCursor{text: text}
	projection := syntaxProjection{syntax: &syntax, request: &request}
	previous := 0
	var names []string
	for {
		first := *cursor.peek(&projection, nil)
		offset := cursor.offset
		for repeat := 0; repeat < 3; repeat++ {
			require.Equal(t, first, *cursor.peek(&projection, nil))
			require.Equal(t, offset, cursor.offset, "lookahead must reuse its recorded token")
		}
		got := *cursor.take(&projection, nil)
		if got.kind == syntaxEOF {
			break
		}
		require.GreaterOrEqual(t, got.start, previous)
		require.Greater(t, got.end, got.start)
		previous = got.end
		if got.kind == syntaxIdentifier {
			names = append(names, got.name)
		}
	}
	require.Equal(t, []string{"labels", "n", "n", "p", "IS", "NOT", "NULL"}, names)
	require.Equal(t, len(text), cursor.offset)
	require.Empty(t, syntax.comments, "sparse token positions need no comment inventory")
}

func TestFunctionSyntaxReferenceFactsPreserveNullPolicy(t *testing.T) {
	for _, sample := range []struct {
		text string
		null bool
	}{
		{"n.p IS NULL", true},
		{"n.p IS\nNOT\tNULL", true},
		{"n.p /* comment */ IS NULL", false},
		{"n. p IS NULL", false},
		{"n.`p` IS NULL", false},
		{"n.p IS NOT 1", false},
	} {
		var buffer [8]syntaxReference
		facts := readFunctionReferenceFacts(sample.text, buffer[:0], []string{"labels", "keys", "properties"})
		require.Len(t, facts, 1, sample.text)
		require.Equal(t, "n", facts[0].variable, sample.text)
		require.Equal(t, sample.null, facts[0].nullTest, sample.text)
	}
}
