package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A row's node comes from a read-only scan candidate in an explicit
// transaction. Its statement reads the node through property access only, so
// what can reach the caller is a property value. A candidate holding a value
// that can be changed in place (here a list) is the row's own copy: changing
// it reaches neither a later read of the same scan nor the rows of other
// candidates (NOR-160).
func TestNOR160PipelineRowOwnsItsReadOnlyCandidate(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where"))
	_, err := exec.Execute(ctx, "CREATE (:P {k: 'a', v: [1]}), (:P {k: 'b', v: [2]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })

	match := func() *storage.Node {
		matched, handled, err := exec.pipelineApplyInitialNodeMatch(ctx, []pipelineRow{{"id": "b"}}, "MATCH (n:P) WHERE n.k = id", pipelineMatchPhysicalHint{readTail: []string{"n.v"}})
		require.NoError(t, err)
		require.True(t, handled)
		require.Len(t, matched, 1)
		return matched[0]["n"].(*storage.Node)
	}
	first := match()
	first.Properties["v"] = int64(99)

	second := match()
	require.NotSame(t, first, second)
	require.Equal(t, "[2]", fmt.Sprint(second.Properties["v"]))
}

// A candidate whose values are all immutable can share nothing a caller could
// change, so its row binds it without a copy; the statement still returns the
// right values from it (NOR-160).
func TestNOR160PipelineRowOfImmutableValuesIsNotCopied(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where"))
	_, err := exec.Execute(ctx, "CREATE (:P {k: 'a', v: 1}), (:P {k: 'b', v: 2})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })

	match := func() *storage.Node {
		matched, handled, err := exec.pipelineApplyInitialNodeMatch(ctx, []pipelineRow{{"id": "b"}}, "MATCH (n:P) WHERE n.k = id", pipelineMatchPhysicalHint{readTail: []string{"n.v"}})
		require.NoError(t, err)
		require.True(t, handled)
		require.Len(t, matched, 1)
		return matched[0]["n"].(*storage.Node)
	}
	match() // fills the transaction's projected label cache
	require.Same(t, match(), match(), "a row of immutable values binds the scan's candidate")

	result, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.k = 'b' RETURN n.v AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(2)}}, result.Rows)
}
