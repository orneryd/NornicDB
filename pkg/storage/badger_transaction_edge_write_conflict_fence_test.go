// SPDX-License-Identifier: MIT
package storage

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// edgeWriteConflictHandler pauses the real Commit path at its existing
// metadata log, after snapshot validation and before materialization. No
// production hook or extra commit-time observation is introduced by the test.
type edgeWriteConflictHandler struct {
	target  string
	reached chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (h *edgeWriteConflictHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *edgeWriteConflictHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message != "transaction committing with metadata" {
		return nil
	}
	target := false
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == "transaction_id" && a.Value.String() == h.target {
			target = true
		}
		return true
	})
	if target {
		h.once.Do(func() { close(h.reached) })
		select {
		case <-h.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (h *edgeWriteConflictHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *edgeWriteConflictHandler) WithGroup(string) slog.Handler      { return h }

func TestTransactionEdgeUpdatePreservesPostValidationPeerWrite(t *testing.T) {
	for _, highPerformance := range []bool{false, true} {
		name := "low_memory"
		if highPerformance {
			name = "default_server"
		}
		t.Run(name, func(t *testing.T) { testEdgeUpdatePostValidationPeerWrite(t, highPerformance) })
	}
}

func testEdgeUpdatePostValidationPeerWrite(t *testing.T, highPerformance bool) {
	for _, scenario := range []string{"peer_updates_edge", "peer_deletes_edge"} {
		t.Run(scenario, func(t *testing.T) {
			barrier := &edgeWriteConflictHandler{reached: make(chan struct{}), resume: make(chan struct{})}
			engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), HighPerformance: highPerformance, LowMemory: !highPerformance, Logger: slog.New(barrier)})
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			source, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)
			writer, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = writer.Rollback() })
			edge, err := writer.GetEdge(edgeID)
			require.NoError(t, err)
			edge.Properties = map[string]interface{}{"writer": "stale"}
			require.NoError(t, writer.UpdateEdge(edge))
			require.NoError(t, writer.SetMetadata(map[string]interface{}{"validation_publication_control": true}))
			barrier.target = writer.ID
			var release sync.Once
			unblock := func() { release.Do(func() { close(barrier.resume) }) }
			t.Cleanup(unblock)
			committed := make(chan error, 1)
			go func() { committed <- writer.Commit() }()
			select {
			case <-barrier.reached:
			case err := <-committed:
				t.Fatalf("writer finished before validation barrier: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("writer did not reach post-validation metadata barrier")
			}
			peer, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = peer.Rollback() })
			if scenario == "peer_updates_edge" {
				edge, err := peer.GetEdge(edgeID)
				require.NoError(t, err)
				edge.Properties = map[string]interface{}{"writer": "peer"}
				require.NoError(t, peer.UpdateEdge(edge))
			} else {
				require.NoError(t, peer.DeleteEdge(edgeID))
			}
			require.NoError(t, peer.Commit())
			unblock()
			select {
			case err := <-committed:
				assert.ErrorIs(t, err, ErrConflict, "a peer publication after validation must reject the stale writer")
			case <-time.After(5 * time.Second):
				t.Fatal("writer did not finish after peer publication")
			}
			reader, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Rollback() })
			_, err = reader.GetNode(source)
			require.NoError(t, err, "failed writer must not remove the source node")
			edge, err = reader.GetEdge(edgeID)
			if scenario == "peer_updates_edge" {
				require.NoError(t, err)
				require.Equal(t, "peer", edge.Properties["writer"])
			} else {
				require.ErrorIs(t, err, ErrNotFound, "failed writer must not resurrect the peer-deleted edge")
			}
		})
	}
}

func TestTransactionHighPerformanceCascadePreservesPostValidationPeerWrite(t *testing.T) {
	barrier := &edgeWriteConflictHandler{reached: make(chan struct{}), resume: make(chan struct{})}
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), HighPerformance: true, Logger: slog.New(barrier)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	source, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)
	writer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback() })
	require.NoError(t, writer.DeleteNode(source))
	require.NoError(t, writer.SetMetadata(map[string]interface{}{"validation_publication_control": true}))
	barrier.target = writer.ID
	var release sync.Once
	unblock := func() { release.Do(func() { close(barrier.resume) }) }
	t.Cleanup(unblock)
	committed := make(chan error, 1)
	go func() { committed <- writer.Commit() }()
	select {
	case <-barrier.reached:
	case err := <-committed:
		t.Fatalf("writer finished before validation barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not reach post-validation metadata barrier")
	}
	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	edge, err := peer.GetEdge(edgeID)
	require.NoError(t, err)
	edge.Properties = map[string]interface{}{"writer": "peer"}
	require.NoError(t, peer.UpdateEdge(edge))
	require.NoError(t, peer.Commit())
	unblock()
	select {
	case err := <-committed:
		assert.ErrorIs(t, err, ErrConflict, "late peer update must reject the stale cascade delete")
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not finish after peer publication")
	}
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	_, err = reader.GetNode(source)
	assert.NoError(t, err, "failed writer must not remove the source node")
	edge, err = reader.GetEdge(edgeID)
	require.NoError(t, err)
	require.Equal(t, "peer", edge.Properties["writer"])
}
