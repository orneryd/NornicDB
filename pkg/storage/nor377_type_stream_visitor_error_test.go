package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// EdgeTypeStreamer documents that a callback error stops the stream and is
// returned. Its result must not depend on whether the same edge has drained.
func TestNOR377TypeStreamReturnsVisitorSentinelForAckedAndStoredEdges(t *testing.T) {
	for _, drained := range []bool{false, true} {
		name := "acked"
		if drained {
			name = "stored"
		}
		t.Run(name, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			relatedEndpointNodes(t, engine)
			edge := &Edge{ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b"}
			if drained {
				require.NoError(t, engine.CreateEdge(t.Context(), edge))
				require.Zero(t, engine.writeBehind.PendingOps())
			} else {
				relatedAckedBufferedEdge(t, engine, edge)
				require.Greater(t, engine.writeBehind.PendingOps(), 0)
			}
			baseline := engine.work.Active()
			var ids []EdgeID
			err := engine.StreamEdgesByType(t.Context(), "LINK", func(edge *Edge) error {
				ids = append(ids, edge.ID)
				return ErrIterationStopped
			})
			require.Equal(t, []EdgeID{"test:holder"}, ids)
			require.Equal(t, baseline, engine.work.Active())
			require.ErrorIs(t, err, ErrIterationStopped, "EdgeTypeStreamer promises to return the callback error")
		})
	}
}
