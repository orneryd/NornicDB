package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A predicate scan in an explicit transaction answers from the transaction's
// projected label cache. A list property it returns must be the caller's own:
// changing it must not show in the next read of the same statement (NOR-160).
func TestNOR160ProjectedScanReturnedListIsTheCallersOwn(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scan"))
	_, err := exec.Execute(ctx, "CREATE (:P {g: 0, tags: ['a', 'b']})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })

	const query = "MATCH (n:P) WHERE n.g = 0 RETURN n.tags AS tags"
	read := func() any {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		return result.Rows[0][0]
	}
	first := func(list any) any {
		switch values := list.(type) {
		case []any:
			return values[0]
		case []string:
			return values[0]
		}
		t.Fatalf("unexpected list type %T", list)
		return nil
	}

	read() // fills the transaction's projected label cache
	got := read()
	require.Equal(t, "a", first(got))
	switch values := got.(type) {
	case []any:
		values[0] = "changed"
	case []string:
		values[0] = "changed"
	}
	require.Equal(t, "a", first(read()), "a returned list must not share the transaction's cached value")
}
