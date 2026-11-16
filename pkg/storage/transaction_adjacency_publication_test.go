package storage

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// The existing metadata barrier is between optimistic validation and final
// entity admission. A peer filling the range there must still be rejected by
// the protected validation, before the writer's staged work is accepted.
func TestAdjacencyDecisionRejectsPeerPublishedAfterOptimisticValidation(t *testing.T) {
	for _, highPerformance := range []bool{false, true} {
		t.Run(map[bool]string{false: "low-memory", true: "default"}[highPerformance], func(t *testing.T) {
			barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
			engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
				DataDir: t.TempDir(), LowMemory: !highPerformance, HighPerformance: highPerformance,
				Logger: slog.New(fixtureowner.RawLogHandler(barrier.Handle)),
			}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			for _, id := range []NodeID{adjacencyDecisionAnchor, adjacencyDecisionOther} {
				_, err := engine.CreateNode(t.Context(), &Node{ID: id})
				require.NoError(t, err)
			}
			tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			edges, err := tx.ReadEdges(t.Context(), AdjacentEdgeRead{
				Node: adjacencyDecisionAnchor, Types: []string{adjacencyDecisionLink}, ReadPolicy: CurrentEdgeMatch,
			})
			require.NoError(t, err)
			require.Empty(t, edges)
			_, err = tx.LockNodeForWrite(t.Context(), adjacencyDecisionAnchor)
			require.NoError(t, err)
			_, err = tx.CreateNode(t.Context(), &Node{ID: adjacencyDecisionStaged})
			require.NoError(t, err)
			require.NoError(t, tx.SetMetadata(map[string]interface{}{"guard_publication": true}))
			barrier.target = tx.ID
			var release sync.Once
			unblock := func() { release.Do(func() { close(barrier.resume) }) }
			t.Cleanup(unblock)
			done := make(chan error, 1)
			go func() { done <- tx.Commit() }()
			select {
			case <-barrier.reached:
			case err := <-done:
				t.Fatalf("writer ended before its validation barrier: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("writer never reached its post-validation barrier")
			}
			peer := &Edge{ID: "test:post-validation-peer", StartNode: adjacencyDecisionAnchor, EndNode: adjacencyDecisionOther, Type: adjacencyDecisionLink}
			require.NoError(t, engine.CreateEdge(t.Context(), peer))
			unblock()
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrConflict)
				require.NotErrorIs(t, err, ErrDeadlock)
			case <-time.After(10 * time.Second):
				t.Fatal("writer did not finish after the peer publication")
			}
			_, err = engine.GetNode(t.Context(), adjacencyDecisionStaged)
			require.ErrorIs(t, err, ErrNotFound, "refused work must not become accepted")
			stored, err := engine.GetEdge(t.Context(), peer.ID)
			require.NoError(t, err)
			require.Equal(t, peer.StartNode, stored.StartNode)
			require.Equal(t, peer.EndNode, stored.EndNode)
			require.Equal(t, peer.Type, stored.Type)
		})
	}
}
