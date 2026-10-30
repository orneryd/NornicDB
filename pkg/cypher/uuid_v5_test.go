package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// uuid.v5 is RFC 9562's name-based UUID.
func TestUUIDv5Function(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "uuid"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN toString(uuid.v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'python.org')) AS v":                                            "886313e1-3b8a-5372-9b90-0c9aee199e5d",
		"RETURN uuid.v5(uuid('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org') = uuid('886313e1-3b8a-5372-9b90-0c9aee199e5d') AS v": true,
		"RETURN uuid.v5(null, 'x') AS v": nil,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	for _, query := range []string{
		"RETURN uuid.v5('not a uuid', 'x')", "RETURN uuid.v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 1)", "RETURN uuid.v5('x')",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
}
