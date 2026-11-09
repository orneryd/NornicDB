package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// This decorator owns the completion boundary so a failed snapshot drain can
// be observed without adding a production failure-injection API.
type snapshotCompletionEngine struct {
	Engine
	flushes int
	err     error
}

func (e *snapshotCompletionEngine) FlushWriteBehind(ctx context.Context) error {
	e.flushes++
	return e.err
}

func TestRecoveryStopsBeforeWALWhenSnapshotCompletionFails(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "namespaced"}[wrapped], func(t *testing.T) {
			base := newTestEngine(t)
			injected := errors.New("snapshot completion refused")
			owner := &snapshotCompletionEngine{Engine: base, err: injected}
			var destination Engine = owner
			if wrapped {
				destination = NewNamespacedEngine(owner, "test")
			}
			snapshot := filepath.Join(t.TempDir(), "snapshot.bin")
			require.NoError(t, SaveSnapshot(&Snapshot{
				Timestamp: time.Now(), Version: "1.0",
				Nodes: []*Node{{ID: "test:snapshot", Properties: map[string]any{"value": "snapshot"}}},
			}, snapshot))
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			require.NoError(t, wal.Append(OpCreateNode, &Node{ID: "test:later"}))
			require.NoError(t, wal.Sync())
			result, status, err := RecoverIntoEngine(t.Context(), destination, wal.Config().Dir, snapshot)
			require.ErrorIs(t, err, injected)
			require.ErrorContains(t, err, "complete snapshot restore")
			require.Equal(t, 1, owner.flushes)
			require.Zero(t, result.Applied)
			require.Zero(t, status.WALEntries)
			_, err = base.GetNode(t.Context(), "test:later")
			require.ErrorIs(t, err, ErrNotFound, "WAL must not overtake an incomplete snapshot")
		})
	}
}

func TestRecoveryPublishesReplayedRelabelAndRelationshipCounts(t *testing.T) {
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	namespace := defaultRecoveryDatabase()
	first, second := NodeID(namespace+":a"), NodeID(namespace+":b")
	for _, node := range []*Node{{ID: first, Labels: []string{"Old"}}, {ID: second, Labels: []string{"End"}}} {
		_, err := source.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	edge := &Edge{ID: EdgeID(namespace + ":r"), StartNode: first, EndNode: second, Type: "OldR"}
	require.NoError(t, source.CreateEdge(t.Context(), edge))
	snapshot := filepath.Join(t.TempDir(), "snapshot.bin")
	require.NoError(t, source.bufferedCommitWAL.Append(OpBulkNodes, WALBulkNodesData{Nodes: []*Node{{ID: first, Labels: []string{"Old"}}, {ID: second, Labels: []string{"End"}}}}))
	require.NoError(t, source.bufferedCommitWAL.Append(OpBulkEdges, WALBulkEdgesData{Edges: []*Edge{copyEdge(edge)}}))
	require.NoError(t, SaveSnapshot(&Snapshot{Timestamp: time.Now(), Version: "1.0", Sequence: source.bufferedCommitWAL.sequence.Load(), Nodes: []*Node{{ID: "a", Labels: []string{"Old"}}, {ID: "b", Labels: []string{"End"}}}, Edges: []*Edge{{ID: "r", StartNode: "a", EndNode: "b", Type: "OldR"}}}, snapshot))
	tx, err := source.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace(namespace))
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: first, Labels: []string{"New"}}, Policy: ClientNodeEdits}))
	changed := copyEdge(edge)
	changed.Type = "NewR"
	require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: changed}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	walDir := source.bufferedCommitWAL.Config().Dir
	require.NoError(t, source.FlushWriteBehind(context.Background()))
	require.NoError(t, source.bufferedCommitWAL.Sync())
	check := func(t *testing.T, destination *BadgerEngine) {
		t.Helper()
		node, err := destination.GetNode(t.Context(), first)
		require.NoError(t, err)
		require.Equal(t, []string{"New"}, node.Labels)
		relationship, err := destination.GetEdge(t.Context(), edge.ID)
		require.NoError(t, err)
		require.Equal(t, "NewR", relationship.Type)
		for label, want := range map[string]int64{"Old": 0, "New": 1, "End": 1} {
			got, err := destination.NodeCountByLabelInNamespace(t.Context(), namespace, label)
			require.NoError(t, err)
			require.Equal(t, want, got, "label=%s must agree with restored bodies before reopen", label)
		}
		for kind, want := range map[string]int64{"OldR": 0, "NewR": 1} {
			got, err := destination.EdgeCountByTypeInNamespace(t.Context(), namespace, kind)
			require.NoError(t, err)
			require.Equal(t, want, got, "type=%s must agree with restored bodies before reopen", kind)
		}
		for _, row := range []struct {
			label, kind string
			start, end  int64
		}{
			{"Old", "OldR", 0, 0}, {"New", "NewR", 1, 0}, {"End", "NewR", 0, 1}, {"End", "OldR", 0, 0},
		} {
			start, err := destination.EdgeCountByStartLabelInNamespace(t.Context(), namespace, row.label, row.kind)
			require.NoError(t, err)
			require.Equal(t, row.start, start)
			end, err := destination.EdgeCountByEndLabelInNamespace(t.Context(), namespace, row.label, row.kind)
			require.NoError(t, err)
			require.Equal(t, row.end, end)
		}
	}
	for _, buffered := range []bool{false, true} {
		for _, withSnapshot := range []bool{false, true} {
			name := map[bool]string{false: "native-sync", true: "native-buffered"}[buffered] + "/" + map[bool]string{false: "wal-only", true: "snapshot"}[withSnapshot]
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				destination := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, synchronous: !buffered, manualDrain: true})
				snapshotPath := ""
				if withSnapshot {
					snapshotPath = snapshot
				}
				result, _, err := RecoverIntoEngine(t.Context(), destination, walDir, snapshotPath)
				require.NoError(t, err)
				require.Zero(t, result.Failed)
				check(t, destination)
				// Recovered counters must also support the next ordinary write.
				// A stale New=0 delta previously underflowed on this delete and
				// prevented the database from closing or reopening successfully.
				require.NoError(t, destination.DeleteNode(t.Context(), first))
				require.NoError(t, destination.FlushWriteBehind(t.Context()))
				require.NoError(t, destination.Close())
				reopened := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, synchronous: !buffered, manualDrain: true})
				for label, want := range map[string]int64{"Old": 0, "New": 0, "End": 1} {
					got, err := reopened.NodeCountByLabelInNamespace(t.Context(), namespace, label)
					require.NoError(t, err)
					require.Equal(t, want, got)
				}
				for _, kind := range []string{"OldR", "NewR"} {
					got, err := reopened.EdgeCountByTypeInNamespace(t.Context(), namespace, kind)
					require.NoError(t, err)
					require.Zero(t, got)
				}
				_, err = reopened.GetNode(t.Context(), first)
				require.ErrorIs(t, err, ErrNotFound)
				_, err = reopened.GetEdge(t.Context(), edge.ID)
				require.ErrorIs(t, err, ErrNotFound)
			})
		}
	}
	t.Run("public-recovery", func(t *testing.T) {
		destination, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(t.Context(), walDir, snapshot))
		require.NoError(t, err)
		require.Zero(t, result.Failed)
		defer func() { require.NoError(t, destination.Close()) }()
		check(t, destination.BadgerEngine)
	})
	t.Run("transaction-recovery", func(t *testing.T) {
		destination, result, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), walDir, snapshot))
		require.NoError(t, err)
		require.False(t, result.HasErrors(), result.Summary())
		defer func() { require.NoError(t, destination.Close()) }()
		check(t, destination.BadgerEngine)
	})
	t.Run("applied-prefix-before-invalid-record", func(t *testing.T) {
		cloneDir := t.TempDir()
		require.NoError(t, os.CopyFS(cloneDir, os.DirFS(walDir)))
		cfg := *source.bufferedCommitWAL.Config()
		cfg.Dir = cloneDir
		clone, err := NewWAL(cloneDir, &cfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, clone.Close()) })
		require.NoError(t, clone.Append(OpBufferedCommit, "invalid buffered commit"))
		require.NoError(t, clone.Sync())
		destination := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
		result, _, err := RecoverIntoEngine(t.Context(), destination, cloneDir, snapshot)
		require.NoError(t, err, "record failures are reported in ReplayResult")
		require.Positive(t, result.Applied)
		require.Equal(t, 1, result.Failed)
		require.Len(t, result.Errors, 1)
		require.ErrorContains(t, result.Errors[0].Error, "buffered")
		check(t, destination)
	})

}
