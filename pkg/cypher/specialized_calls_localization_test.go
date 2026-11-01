package cypher

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestSpecializedCallFilesContainNoInlineErrorf(t *testing.T) {
	files := []string{
		"call_vector.go",
		"call_fulltext.go",
		"call_temporal.go",
		"call_txlog.go",
	}

	for _, path := range files {
		t.Run(path, func(t *testing.T) {
			fileSet := token.NewFileSet()
			file, err := parser.ParseFile(fileSet, path, nil, 0)
			require.NoError(t, err)

			var positions []token.Position
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Errorf" {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if ok && identifier.Name == "fmt" {
					positions = append(positions, fileSet.Position(call.Pos()))
				}
				return true
			})

			require.Empty(t, positions, "inline fmt.Errorf calls must use localizedError descriptors")
		})
	}
}

func TestSpecializedCallErrorsHaveTypedIdentity(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	t.Run("parameter machine data", func(t *testing.T) {
		// A parameter the statement never supplied is ParameterMissing before
		// the vector procedure runs, as in Neo4j (#657).
		_, err := exec.Execute(context.Background(), "CALL db.index.vector.queryNodes('idx', 10, $embedding)", map[string]interface{}{})
		require.EqualError(t, err, "Neo.ClientError.Statement.ParameterMissing: Expected parameter(s): embedding")

		code, message := nerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.ParameterMissing", code)
		require.Equal(t, "Expected parameter(s): embedding", message)
	})

	t.Run("public descriptor", func(t *testing.T) {
		// Search text without an embedder keeps the node vector procedure's
		// localized message identity on the public path.
		_, err := exec.Execute(context.Background(), "CALL db.index.vector.queryNodes('idx', 10, 'hello')", nil)
		require.EqualError(t, err, "Failed to invoke procedure `db.index.vector.queryNodes`: Caused by: string query provided but no embedder configured; use vector array or configure embedding service")

		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, localization.MessageCypherSpecializedCallsStringQueryEmbedderRequired, localizedErr.Message.ID)
		require.Equal(t, string(localization.MessageCypherSpecializedCallsStringQueryEmbedderRequired), localizedErr.Code)
	})

	t.Run("errors as", func(t *testing.T) {
		cause := errors.New("forced lookup failure")
		err := localizedError(localization.CypherSpecializedCallsTemporalLookupFailed("History", cause), cause)
		var localizedErr *localization.LocalizedError
		require.True(t, errors.As(err, &localizedErr))
		require.True(t, errors.Is(err, cause))
	})
}
