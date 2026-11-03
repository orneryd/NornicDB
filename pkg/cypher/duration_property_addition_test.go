package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDurationAdditionPropertyUsesRuntimeType(t *testing.T) {
	exec := newClassificationExecutor(t)
	for _, expression := range []string{"duration('P1D') + n.v", "du + n.v"} {
		query := "MATCH (n:A) WITH n, duration('P1D') AS du RETURN " + expression + " AS v"
		requireStatus(t, exec, query, nil, "Neo.ClientError.Statement.TypeError", "Cannot add `Duration` and `Long`")
	}
	for _, expression := range []string{"duration('P1D') + n.missing", "du + n.missing"} {
		result, err := exec.Execute(context.Background(), "MATCH (n:A) WITH n, duration('P1D') AS du RETURN "+expression+" AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{nil}}, result.Rows)
	}
}
