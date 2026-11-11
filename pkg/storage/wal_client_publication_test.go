package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCoreWALCancelDuringAppendDoesNotLeaveRejectedReplay(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := runAtHeldWALAppend(t, wal, func() error {
		_, err := wrapped.CreateNode(ctx, &Node{ID: "test:rejected", Properties: map[string]any{"v": 1}})
		return err
	}, cancel)
	require.ErrorIs(t, err, context.Canceled)
	_, err = native.GetNode(t.Context(), "test:rejected")
	require.ErrorIs(t, err, ErrNotFound)
	entries := walClientEntries(t, wal)
	require.Empty(t, entries, "rejected client input is not replayable intent")
	recovered := newTestMemoryEngine(t)
	for _, entry := range entries {
		require.NoError(t, ReplayWALEntry(ctx, recovered, entry))
	}
	_, err = recovered.GetNode(t.Context(), "test:rejected")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestWALClientRejectsDuplicateBeforeJournal(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	_, err := native.CreateNode(t.Context(), &Node{ID: "test:duplicate", Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:duplicate", Properties: map[string]any{"v": 2}})
	require.ErrorIs(t, err, ErrAlreadyExists)
	require.Empty(t, walClientEntries(t, wal))
	got, err := native.GetNode(t.Context(), "test:duplicate")
	require.NoError(t, err)
	require.EqualValues(t, 1, got.Properties["v"])
}

func TestWALClientAcceptedDecisionKeepsNativeAndReplayTogether(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	var decisions atomic.Int32
	ctx := WithPublicationDecision(request, func(_ context.Context, _ PublicationIdentity, action func() error) error {
		decisions.Add(1)
		err := action()
		cancel()
		return err
	})
	published := make(chan struct{}, 1)
	native.OnNodeCreated(func(node *Node) {
		if node.ID == "test:accepted" {
			published <- struct{}{}
		}
	})
	_, err := wrapped.CreateNode(ctx, &Node{ID: "test:accepted", Properties: map[string]any{"v": 7}})
	if err != nil {
		var accepted *AcceptedCommitError
		require.ErrorAs(t, err, &accepted)
		require.NotEqual(t, CommitDiscarded, accepted.State)
	}
	require.EqualValues(t, 1, decisions.Load())
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted native publication did not finish")
	}
	node, err := native.GetNode(t.Context(), "test:accepted")
	require.NoError(t, err)
	require.EqualValues(t, 7, node.Properties["v"])
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	require.Equal(t, OpEntityCommit, entries[0].Operation)
	recovered := newTestMemoryEngine(t)
	for range 2 {
		for _, entry := range entries {
			require.NoError(t, ReplayWALEntry(ctx, recovered, entry))
		}
	}
	node, err = recovered.GetNode(t.Context(), "test:accepted")
	require.NoError(t, err)
	require.EqualValues(t, 7, node.Properties["v"])
	count, err := recovered.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestWALClientBulkDeleteKeepsSkipMissingAndReplay(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	nodes := []*Node{{ID: "test:a"}, {ID: "test:b"}}
	require.NoError(t, native.BulkCreateNodes(t.Context(), nodes))
	require.NoError(t, native.CreateEdge(t.Context(), &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, wrapped.BulkDeleteEdges(t.Context(), []EdgeID{"test:e", "test:e", "", "test:absent"}))
	require.NoError(t, wrapped.BulkDeleteNodes(t.Context(), []NodeID{"test:a", "test:a", "", "test:absent"}))
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 2)
	recovered := newTestMemoryEngine(t)
	require.NoError(t, recovered.BulkCreateNodes(t.Context(), []*Node{{ID: "test:a"}, {ID: "test:b"}}))
	require.NoError(t, recovered.CreateEdge(t.Context(), &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	for _, entry := range entries {
		require.Equal(t, OpEntityCommit, entry.Operation)
		var data walEntityCommitData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
		_, ops, err := DecodeEntityTransaction(data.Payload)
		require.NoError(t, err)
		require.Len(t, ops, 1, "only an actual deletion enters the accepted snapshot")
		require.NoError(t, ReplayWALEntry(t.Context(), recovered, entry))
		require.NoError(t, ReplayWALEntry(t.Context(), recovered, entry))
	}
	for _, engine := range []*MemoryEngine{native, recovered} {
		nodes, err := engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 1, nodes)
		edges, err := engine.EdgeCount(t.Context())
		require.NoError(t, err)
		require.Zero(t, edges)
	}
}

func TestWALClientNamespaceInputsPreserveReturnedIDAndReplay(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, func(engine *MemoryEngine) Engine { return NewNamespacedEngine(engine, "tenant") })
	input := &Node{ID: "local", Properties: map[string]any{"v": 1}}
	id, err := wrapped.CreateNode(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, NodeID("local"), id)
	require.Equal(t, NodeID("local"), input.ID)
	node, err := native.GetNode(t.Context(), "tenant:local")
	require.NoError(t, err)
	require.EqualValues(t, 1, node.Properties["v"])
	recovered := newTestMemoryEngine(t)
	for _, entry := range walClientEntries(t, wal) {
		require.NoError(t, ReplayWALEntry(t.Context(), recovered, entry))
	}
	node, err = recovered.GetNode(t.Context(), "tenant:local")
	require.NoError(t, err)
	require.EqualValues(t, 1, node.Properties["v"])
}

func TestWALClientNamespaceEdgeInputsAndReplay(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, func(engine *MemoryEngine) Engine { return NewNamespacedEngine(engine, "tenant") })
	for _, id := range []NodeID{"start", "end"} {
		_, err := wrapped.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	require.ErrorIs(t, wrapped.CreateEdge(t.Context(), nil), ErrInvalidData)
	input := &Edge{ID: "edge", StartNode: "start", EndNode: "end", Type: "KNOWS", Properties: map[string]any{"since": 2020}}
	require.NoError(t, wrapped.CreateEdge(t.Context(), input))
	require.Equal(t, EdgeID("edge"), input.ID)
	require.Equal(t, NodeID("start"), input.StartNode)
	require.Equal(t, NodeID("end"), input.EndNode)
	recovered := newTestMemoryEngine(t)
	for _, entry := range walClientEntries(t, wal) {
		require.NoError(t, ReplayWALEntry(t.Context(), recovered, entry))
	}
	for _, engine := range []*MemoryEngine{native, recovered} {
		edge, err := engine.GetEdge(t.Context(), "tenant:edge")
		require.NoError(t, err)
		require.Equal(t, NodeID("tenant:start"), edge.StartNode)
		require.Equal(t, NodeID("tenant:end"), edge.EndNode)
		require.Equal(t, "KNOWS", edge.Type)
		require.EqualValues(t, 2020, edge.Properties["since"])
	}
}

func TestWALNativeBufferedUsesExistingJournalAndAcceptedLifetime(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	native, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 1000000}))
	require.NoError(t, err)
	fixtureowner.Own(t, native)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, wal)
	wrapped := fixtureowner.OwnChecked(t, NewWALEngine(native, wal))
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	var decisions int
	ctx := WithPublicationDecision(request, func(_ context.Context, _ PublicationIdentity, action func() error) error {
		decisions++
		err := action()
		cancel()
		return err
	})
	require.NoError(t, wrapped.BulkCreateNodes(ctx, []*Node{{ID: "test:buffer-one"}, {ID: "test:buffer-two"}}))
	require.Equal(t, 1, decisions)
	require.Positive(t, native.writeBehind.PendingOps(), "public ACK retains buffer admission")
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	require.Equal(t, OpBufferedCommit, entries[0].Operation, "no second semantic envelope or receipt")
	recovered := newTestMemoryEngine(t)
	require.NoError(t, ReplayWALEntry(ctx, recovered.BadgerEngine, entries[0]))
	require.NoError(t, ReplayWALEntry(ctx, recovered.BadgerEngine, entries[0]))
	for _, engine := range []Engine{native, recovered} {
		count, err := engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 2, count)
	}
	require.NoError(t, native.FlushWriteBehind(t.Context()))
}

func TestWALNativeBulkCancellationBeforeDecisionIsRejected(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var decisions atomic.Int32
	selected := WithPublicationDecision(ctx, func(_ context.Context, _ PublicationIdentity, action func() error) error {
		decisions.Add(1)
		return action()
	})
	err := runAtHeldWALAppend(t, wal, func() error {
		return wrapped.BulkCreateNodes(selected, []*Node{{ID: "test:bulk-rejected-a"}, {ID: "test:bulk-rejected-b"}})
	}, cancel)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, decisions.Load())
	require.Empty(t, walClientEntries(t, wal))
	count, err := native.NodeCount(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestWALNativeLargeFinalJournalAndRecovery(t *testing.T) {
	for _, failFinal := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "postappend_native_failure"}[failFinal], func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			native := openLargeCommitTestEngine(t, dir)
			wal, err := NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			fixtureowner.OwnChecked(t, wal)
			injected := errors.New("final native batch failed after accepted journal")
			wrapped := fixtureowner.Track(t, NewWALEngine(native, wal), ErrStorageClosed)
			t.Cleanup(func() {
				closeErr := wrapped.Close()
				if failFinal {
					require.ErrorIs(t, closeErr, injected)
				} else {
					require.NoError(t, closeErr)
				}
			})
			var observed []uint64
			restore := setLargeCommitBatchHook(func(int) error {
				sequence := wal.sequence.Load()
				observed = append(observed, sequence)
				if failFinal && sequence > 0 {
					return injected
				}
				return nil
			})
			defer restore()
			const n = 6000
			nodes := make([]*Node, n)
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:large-wal-%d", i)), Labels: []string{"Large"}, Properties: map[string]any{"v": i, "pad": "0123456789abcdef"}}
			}
			err = wrapped.BulkCreateNodes(t.Context(), nodes)
			restore()
			if failFinal {
				require.ErrorIs(t, err, injected)
				var accepted *AcceptedCommitError
				require.ErrorAs(t, err, &accepted)
				require.Equal(t, CommitRecoveryPending, accepted.State)
				require.NotEmpty(t, accepted.LogicalID)
				_, nextErr := wrapped.CreateNode(t.Context(), &Node{ID: "test:large-wal-0"})
				require.Error(t, nextErr)
				require.ErrorIs(t, nextErr, injected)
			} else {
				require.NoError(t, err)
			}
			require.Greater(t, len(observed), 1, "actual public bulk must cross native batch boundary")
			for _, sequence := range observed[:len(observed)-1] {
				require.Zero(t, sequence, "intermediate/token batches cannot journal entity intent")
			}
			require.EqualValues(t, 1, observed[len(observed)-1])
			entries := walClientEntries(t, wal)
			require.Len(t, entries, 1)
			require.Equal(t, OpEntityCommit, entries[0].Operation)
			if failFinal {
				require.ErrorIs(t, wrapped.createSnapshotAndCompact(t.Context()), injected)
				_, snapshotErr := wal.CreateSnapshot(native)
				require.ErrorIs(t, snapshotErr, injected)
				var snapshot bytes.Buffer
				require.ErrorIs(t, WriteSnapshot(t.Context(), native, &snapshot, SnapshotOptions{}), injected)
				require.Zero(t, snapshot.Len(), "failed owner cannot publish a partial snapshot")
				require.Len(t, walClientEntries(t, wal), 1, "unapplied semantic intent must survive failed compaction")
				require.ErrorIs(t, wrapped.Close(), injected)
				require.ErrorIs(t, wrapped.Close(), injected)
			} else {
				require.NoError(t, wrapped.Close())
			}

			reopened := openLargeCommitTestEngine(t, dir)
			require.NoError(t, ReplayWALEntry(t.Context(), reopened, entries[0]))
			require.NoError(t, ReplayWALEntry(t.Context(), reopened, entries[0]))
			count, err := reopened.NodeCount(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, n, count)
			_, err = reopened.CreateNode(t.Context(), &Node{ID: "test:after-recovery"})
			require.NoError(t, err)
		})
	}
}

func TestWALNativeNoopAndRejectedClosedJournalDoNotFence(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	require.NoError(t, wrapped.BulkCreateNodes(t.Context(), nil))
	require.NoError(t, wrapped.BulkDeleteNodes(t.Context(), []NodeID{"", "test:absent"}))
	require.Empty(t, walClientEntries(t, wal))
	require.NoError(t, wal.Close())
	_, err := wrapped.CreateNode(t.Context(), &Node{ID: "test:closed-journal"})
	require.ErrorIs(t, err, ErrWALClosed)
	require.Nil(t, native.db.oracle.failure())
	_, err = native.CreateNode(t.Context(), &Node{ID: "test:healthy-native"})
	require.NoError(t, err)
}

func TestWALNativeReceiptKeepsUnrelatedCacheWarmAndCallerIsolated(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	_, err := native.CreateNode(t.Context(), &Node{ID: "test:unrelated", Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	_, err = native.GetNode(t.Context(), "test:unrelated")
	require.NoError(t, err)
	native.nodeCacheMu.RLock()
	warm := native.nodeCache["test:unrelated"]
	native.nodeCacheMu.RUnlock()
	require.NotNil(t, warm)
	native.durableCaches.mu.Lock()
	floor := native.durableCaches.all
	native.durableCaches.mu.Unlock()
	input := &Node{ID: "test:owned", Properties: map[string]any{"v": int64(7)}}
	_, err = wrapped.CreateNode(t.Context(), input)
	require.NoError(t, err)
	input.Properties["v"] = int64(99)
	native.nodeCacheMu.RLock()
	after := native.nodeCache["test:unrelated"]
	native.nodeCacheMu.RUnlock()
	require.Same(t, warm, after)
	native.durableCaches.mu.Lock()
	afterFloor := native.durableCaches.all
	native.durableCaches.mu.Unlock()
	require.Equal(t, floor, afterFloor, "receipt metadata cannot broaden the graph invalidation scope")
	node, err := native.GetNode(t.Context(), "test:owned")
	require.NoError(t, err)
	require.EqualValues(t, 7, node.Properties["v"])
	recovered := newTestMemoryEngine(t)
	for _, entry := range walClientEntries(t, wal) {
		require.NoError(t, ReplayWALEntry(t.Context(), recovered, entry))
	}
	node, err = recovered.GetNode(t.Context(), "test:owned")
	require.NoError(t, err)
	require.EqualValues(t, 7, node.Properties["v"])
}

func TestWALNativeDisabledAtDecisionCannotInventAcceptedIntent(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	native := openLargeCommitTestEngine(t, t.TempDir())
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, wal)
	wrapped := fixtureowner.OwnChecked(t, NewWALEngine(native, wal))
	injected := errors.New("component final batch failed with WAL explicitly disabled")
	var decisions int
	ctx := WithPublicationDecision(t.Context(), func(_ context.Context, _ PublicationIdentity, action func() error) error {
		decisions++
		restore := config.WithWALDisabled()
		defer restore()
		return action()
	})
	restore := setLargeCommitBatchHook(func(int) error {
		if decisions > 0 {
			return injected
		}
		return nil
	})
	defer restore()
	nodes := make([]*Node, 6000)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:no-journal-%d", i)), Labels: []string{"NoJournal"}, Properties: map[string]any{"v": i}}
	}
	err = wrapped.BulkCreateNodes(ctx, nodes)
	restore()
	require.ErrorIs(t, err, injected)
	require.Equal(t, 1, decisions)
	require.Empty(t, walClientEntries(t, wal))
	var accepted *AcceptedCommitError
	require.False(t, errors.As(err, &accepted), "zero append position is no accepted/replayable intent")
	require.Nil(t, native.db.oracle.failure(), "explicitly disabled WAL cannot create an accepted-intent fence")
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:healthy-after-no-journal"})
	require.NoError(t, err)
}

func TestWALNativeBorrowedPayloadSurvivesInterleavedEncoder(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	var decisions int
	ctx := WithPublicationDecision(t.Context(), func(_ context.Context, _ PublicationIdentity, action func() error) error {
		decisions++
		for i := 0; i < 16; i++ {
			_, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "other", LogicalID: fmt.Sprintf("interleaved-%d", i), Mode: CommitModeDirect, Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "other:unrelated", Properties: map[string]any{"overwrite": "different-payload-larger-than-native-record"}}}}})
			require.NoError(t, err)
		}
		return action()
	})
	_, err := wrapped.CreateNode(ctx, &Node{ID: "test:leased", Properties: map[string]any{"v": int64(7)}})
	require.NoError(t, err)
	require.Equal(t, 1, decisions)
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	recovered := newTestMemoryEngine(t)
	require.NoError(t, ReplayWALEntry(ctx, recovered, entries[0]))
	for _, engine := range []Engine{native, recovered} {
		node, err := engine.GetNode(t.Context(), "test:leased")
		require.NoError(t, err)
		require.EqualValues(t, 7, node.Properties["v"])
		_, err = engine.GetNode(t.Context(), "other:unrelated")
		require.ErrorIs(t, err, ErrNotFound)
	}
}

// A transaction whose journal record is larger than one WAL frame commits
// (NOR-412): the record is written as one spanned record, reads back as one
// logical entry and leaves the wrapper healthy.
func TestWALNativeRecordLargerThanOneFrameCommits(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	native := openLargeCommitTestEngine(t, t.TempDir())
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, wal)
	wrapped := fixtureowner.OwnChecked(t, NewWALEngine(native, wal))
	nodes := make([]*Node, 512)
	payload := strings.Repeat("x", 32<<10)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:frame-%d", i)), Properties: map[string]any{"payload": payload}}
	}
	require.NoError(t, wrapped.BulkCreateNodes(t.Context(), nodes))
	var commits int
	for _, entry := range walClientEntries(t, wal) {
		if entry.Operation == OpEntityCommit {
			commits++
			require.Greater(t, len(entry.Data), int(walMaxEntrySize), "one logical record larger than one frame")
		}
		require.NotEqual(t, OpRecordPart, entry.Operation, "readers see only the logical record")
	}
	require.Equal(t, 1, commits)
	require.Nil(t, native.db.oracle.failure())
	count, err := native.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, len(nodes), count)
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:healthy-after-large-record"})
	require.NoError(t, err)
}

type rejectedWALPayload struct{ cause error }

func (p rejectedWALPayload) MarshalJSON() ([]byte, error) { return nil, p.cause }

func TestWALPureEncoderRejectionRetainsCauseAndHealthyRetry(t *testing.T) {
	_, wal, _ := newWALClientFixture(t, nil)
	injected := errors.New("test-owned pure encoder rejection")
	sequence, err := wal.AppendWithDatabaseReturningSeq(OpCreateNode, rejectedWALPayload{cause: injected}, "test")
	require.Zero(t, sequence)
	require.ErrorIs(t, err, injected)
	var rejected *walRecordRejection
	require.ErrorAs(t, err, &rejected)
	require.Empty(t, walClientEntries(t, wal))
	require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "test:healthy-encoding"}}, "test"))
	require.Len(t, walClientEntries(t, wal), 1)
}

// A prepared transaction whose record is larger than one frame is accepted,
// journaled once as a spanned record and applied (NOR-412).
func TestWALPreparedRecordLargerThanOneFrameIsAccepted(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	operations := make([]Operation, 512)
	payload := strings.Repeat("x", 32<<10)
	for i := range operations {
		operations[i] = Operation{Type: OpCreateNode, Node: &Node{ID: NodeID(fmt.Sprintf("test:prepared-frame-%d", i)), Properties: map[string]any{"payload": payload}}}
	}
	prepared, _, err := PrepareEntityChanges(t.Context(), native, "test", operations, CommitModeDirect)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	receipt, err := wrapped.SubmitEntityCommit(t.Context(), prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(t.Context()))
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	require.Equal(t, OpEntityCommit, entries[0].Operation)
	require.Greater(t, len(entries[0].Data), int(walMaxEntrySize))
	count, err := native.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, len(operations), count)
	pending := 0
	wrapped.pendingEntityCommits.Range(func(_, _ any) bool { pending++; return true })
	require.Zero(t, pending)
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:after-prepared-large-record"})
	require.NoError(t, err)
}
