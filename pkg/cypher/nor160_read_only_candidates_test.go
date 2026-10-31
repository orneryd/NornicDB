package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A row's node comes from a read-only scan candidate in an explicit
// transaction: it must be the caller's copy, so changing it reaches neither a
// later read of the same scan nor the rows of other candidates (NOR-160).
func TestNOR160PipelineRowOwnsItsReadOnlyCandidate(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where"))
	_, err := exec.Execute(ctx, "CREATE (:P {k: 'a', v: 1}), (:P {k: 'b', v: 2})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })

	matched, handled, err := exec.pipelineApplyInitialNodeMatch(ctx, []pipelineRow{{"id": "b"}}, "MATCH (n:P) WHERE n.k = id", pipelineMatchPhysicalHint{readTail: []string{"n.v"}})
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, matched, 1)
	first := matched[0]["n"].(*storage.Node)
	first.Properties["v"] = int64(99)

	again, _, err := exec.pipelineApplyInitialNodeMatch(ctx, []pipelineRow{{"id": "b"}}, "MATCH (n:P) WHERE n.k = id", pipelineMatchPhysicalHint{readTail: []string{"n.v"}})
	require.NoError(t, err)
	require.Len(t, again, 1)
	second := again[0]["n"].(*storage.Node)
	require.NotSame(t, first, second)
	require.Equal(t, int64(2), second.Properties["v"])
}
