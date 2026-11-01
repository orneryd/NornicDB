package cypher

// Author: #1665 implementation lane (Claude Opus 5.5).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// toString of the smallest positive FLOAT is Java's Double.MIN_VALUE text,
// as Neo4j returns it.
func TestToStringSmallestFloatMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	result, err := exec.Execute(context.Background(), "RETURN toString(4.9E-324) AS positive, toString(-4.9E-324) AS negative", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"4.9E-324", "-4.9E-324"}, result.Rows[0])
}
