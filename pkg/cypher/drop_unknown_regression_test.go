package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnknownDropFailsWithoutMutatingGraph(t *testing.T) {
	for _, query := range []string{"DROP FOOBAR x", "DROP GARBAGE"} {
		t.Run(query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:Kept {id: 1})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, query, nil)
			var semantic *SemanticError
			require.ErrorAs(t, err, &semantic)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code)
			result, err := exec.Execute(ctx, "MATCH (n:Kept) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, int64(1), result.Rows[0][0])
		})
	}
}

func TestDropMissingSchemaObjectClassification(t *testing.T) {
	for _, testCase := range []struct {
		statement string
		code      string
	}{
		{"DROP INDEX does_not_exist", "Neo.DatabaseError.Schema.IndexDropFailed"},
		{"DROP CONSTRAINT does_not_exist", "Neo.DatabaseError.Schema.ConstraintDropFailed"},
	} {
		t.Run(testCase.statement, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			_, err := exec.Execute(context.Background(), testCase.statement, nil)
			var semantic *SemanticError
			require.ErrorAs(t, err, &semantic)
			require.Equal(t, testCase.code, semantic.Code)
			_, err = exec.Execute(context.Background(), testCase.statement+" IF EXISTS", nil)
			require.NoError(t, err)
		})
	}
}
