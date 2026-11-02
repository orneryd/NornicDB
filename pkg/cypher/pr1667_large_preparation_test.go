package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPR1667LargeIndexedPreparationCancellation(t *testing.T) {
	b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	probe := &pr1667PreparationProbe{NamespacedEngine: storage.NewNamespacedEngine(b, "pr1667")}
	e := NewStorageExecutor(probe)
	const count = 50000
	for offset := 0; offset < count; offset += 1000 {
		nodes := make([]*storage.Node, 0, 1000)
		for i := offset; i < offset+1000; i++ {
			nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("pr1667:n%06d", i)), Labels: []string{"P"}, Properties: map[string]any{"k": int64(1), "payload": strings.Repeat("x", 512)}})
		}
		require.NoError(t, b.BulkCreateNodes(t.Context(), nodes))
	}
	_, err = e.Execute(context.Background(), pr1667IndexK, nil)
	require.NoError(t, err)
	ctx, cancel := probe.arm()
	defer cancel()
	result, err := e.Execute(ctx, "MATCH (n:P) WHERE n.k = 1 RETURN n.k", nil)
	elapsed := time.Since(probe.canceledAt)
	requireCanonicalCase(t, probe.canceledAt, probe.seam)
	t.Logf("population=%d cancel-to-return=%s reads-after-cancel=%d", count, elapsed, probe.readsAfterCancel)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.Zero(t, probe.readsAfterCancel, "canceled preparation must not materialize candidates")
	require.Less(t, elapsed, pr1667PreparationCancellationBudget)
}
