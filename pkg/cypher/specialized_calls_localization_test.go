package cypher

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"

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
		ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{})
		_, err := exec.callDbIndexVectorQueryNodes(ctx, "CALL db.index.vector.queryNodes('idx', 10, $embedding)")
		require.EqualError(t, err, "parameter $embedding not provided")

		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, localization.MessageCypherSpecializedCallsParameterNotProvided, localizedErr.Message.ID)
		require.Equal(t, "embedding", localizedErr.Message.Data["Parameter"])
	})

	t.Run("private parser descriptor", func(t *testing.T) {
		_, _, _, err := exec.parseVectorQueryParams("CALL db.index.vector.queryNodes")
		require.EqualError(t, err, "missing parameters")

		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, localization.MessageCypherSpecializedCallsVectorParametersMissing, localizedErr.Message.ID)
	})

	t.Run("wrapped cause", func(t *testing.T) {
		_, err := exec.callDbTxlogEntries(context.Background(), "CALL db.txlog.entries(nope)")
		require.EqualError(t, err, `invalid fromSeq: strconv.ParseUint: parsing "nope": invalid syntax`)
		require.ErrorIs(t, err, strconv.ErrSyntax)

		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, localization.MessageCypherSpecializedCallsTxlogInvalidSequence, localizedErr.Message.ID)
		require.Equal(t, "fromSeq", localizedErr.Message.Data["Argument"])
	})

	t.Run("errors as", func(t *testing.T) {
		cause := errors.New("forced lookup failure")
		err := localizedError(localization.CypherSpecializedCallsTemporalLookupFailed("History", cause), cause)
		var localizedErr *localization.LocalizedError
		require.True(t, errors.As(err, &localizedErr))
		require.True(t, errors.Is(err, cause))
	})
}
