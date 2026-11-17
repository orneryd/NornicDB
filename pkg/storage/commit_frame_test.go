package storage

// A commit frame is one commit scope's operations together with the
// before/final fold of exactly those operations (newCommitStates). These tests
// pin the prepared frame's contract: the endpoints every consumer of a frame
// reads, the merge that lets a publication boundary of several frames publish
// without folding their operations again, distinct frames staying distinct, a
// canceled preparation selecting nothing, the frame a commit writes, and the
// one prepared fold materialization and publication both read.

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// mustCommitFrame prepares the frame a materialization test hands to
// materializeMVCCCommit: the operations folded once, as a commit's frame owner
// prepares them.
func mustCommitFrame(t testing.TB, operations []Operation) commitFrame {
	t.Helper()
	frame, err := prepareCommitFrame(t.Context(), operations)
	require.NoError(t, err)
	return frame
}

func TestPrepareCommitFrameSelectsFrameEndpointStates(t *testing.T) {
	committed := &Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{"p": "committed"}}
	first := &Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{"p": "first"}}
	second := &Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{"p": "second"}}
	recreated := &Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{"p": "recreated"}}
	committedEdge := &Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:n", Type: "R", Properties: map[string]any{"p": "committed"}}
	nextEdge := &Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:n", Type: "R", Properties: map[string]any{"p": "next"}}

	tests := []struct {
		name       string
		operations []Operation
		nodes      map[NodeID]commitEntityState[*Node]
		edges      map[EdgeID]commitEntityState[*Edge]
	}{
		{
			name: "repeated updates keep the committed predecessor and the final image",
			operations: []Operation{
				{Type: OpUpdateNode, Node: first, OldNode: committed},
				{Type: OpUpdateNode, Node: second, OldNode: first},
			},
			nodes: map[NodeID]commitEntityState[*Node]{"test:n": {before: committed, after: second}},
		},
		{
			name: "create then update is a creation with the last image",
			operations: []Operation{
				{Type: OpCreateNode, Node: first},
				{Type: OpUpdateNode, Node: second, OldNode: first},
			},
			nodes: map[NodeID]commitEntityState[*Node]{"test:n": {after: second}},
		},
		{
			name: "create update delete ends as a creation and a deletion",
			operations: []Operation{
				{Type: OpCreateNode, Node: first},
				{Type: OpUpdateNode, Node: second, OldNode: first},
				{Type: OpDeleteNode, NodeID: "test:n", OldNode: second},
			},
			nodes: map[NodeID]commitEntityState[*Node]{"test:n": {}},
		},
		{
			name: "create then delete",
			operations: []Operation{
				{Type: OpCreateNode, Node: first},
				{Type: OpDeleteNode, NodeID: "test:n", OldNode: first},
			},
			nodes: map[NodeID]commitEntityState[*Node]{"test:n": {}},
		},
		{
			name: "delete then recreate keeps the committed predecessor",
			operations: []Operation{
				{Type: OpDeleteNode, NodeID: "test:n", OldNode: committed},
				{Type: OpCreateNode, Node: recreated},
			},
			nodes: map[NodeID]commitEntityState[*Node]{"test:n": {before: committed, after: recreated}},
		},
		{
			name: "a node deletion folds its cascaded relationships as bodyless deletions",
			operations: []Operation{
				{Type: OpDeleteNode, NodeID: "test:n", OldNode: committed, DeletedEdgeIDs: []EdgeID{"test:e"}},
			},
			nodes: map[NodeID]commitEntityState[*Node]{"test:n": {before: committed}},
			edges: map[EdgeID]commitEntityState[*Edge]{"test:e": {}},
		},
		{
			name: "a relationship update",
			operations: []Operation{
				{Type: OpUpdateEdge, Edge: nextEdge, OldEdge: committedEdge},
			},
			edges: map[EdgeID]commitEntityState[*Edge]{"test:e": {before: committedEdge, after: nextEdge}},
		},
		{
			name: "a relationship deletion",
			operations: []Operation{
				{Type: OpDeleteEdge, EdgeID: "test:e", OldEdge: committedEdge},
			},
			edges: map[EdgeID]commitEntityState[*Edge]{"test:e": {before: committedEdge}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			frame := mustCommitFrame(t, tc.operations)
			require.Equal(t, tc.nodes, frame.states.nodes)
			require.Equal(t, tc.edges, frame.states.edges)
			require.Equal(t, tc.operations, frame.ops, "a frame retains the operations it folded")
		})
	}
}

// TestPrepareCommitFrameCanceledContextSelectsNothing pins the preparation
// lifetime: a canceled preparation folds nothing and contributes no entity to
// the publication boundary it would have joined.
func TestPrepareCommitFrameCanceledContextSelectsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	frame, err := prepareCommitFrame(ctx, []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:c"}}})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, frame.ops)
	require.Nil(t, frame.states.nodes)
	require.Nil(t, frame.states.edges)

}

// TestCommitPhysicalFrameSelectsTheFrameTheCommitWrites pins the frame a
// commit materializes: physicalOperationsLocked returns a transaction's own
// operations while it has no cancelled relationship write, so the commit
// materializes the logical frame it already prepared instead of preparing
// those operations twice; a cancelled relationship write makes the physical
// operations a frame of their own.
func TestCommitPhysicalFrameSelectsTheFrameTheCommitWrites(t *testing.T) {
	node := &Node{ID: "test:select-n", Labels: []string{"L"}}
	cancelled := &Edge{ID: "test:select-e", StartNode: "test:select-n", EndNode: "test:select-n", Type: "R"}
	operations := []Operation{
		{Type: OpCreateNode, Node: node},
		{Type: OpCreateEdge, EdgeID: cancelled.ID, Edge: cancelled},
	}

	t.Run("without a cancelled relationship write the physical frame is the logical frame", func(t *testing.T) {
		tx := &BadgerTransaction{operations: operations}
		logical := mustCommitFrame(t, operations)
		physical, err := tx.physicalCommitFrame(t.Context(), logical, tx.physicalOperationsLocked())
		require.NoError(t, err)
		require.Same(t, &logical.ops[0], &physical.ops[0], "the commit materializes the frame it prepared, not a second fold of it")
		require.Equal(t, logical.states, physical.states)
	})

	t.Run("a cancelled relationship write makes the physical operations their own frame", func(t *testing.T) {
		tx := &BadgerTransaction{operations: operations, cancelledEdgeWrites: map[EdgeID]struct{}{"test:select-e": {}}}
		logical := mustCommitFrame(t, operations)
		physical, err := tx.physicalCommitFrame(t.Context(), logical, tx.physicalOperationsLocked())
		require.NoError(t, err)
		require.NotSame(t, &logical.ops[0], &physical.ops[0])
		require.Equal(t, []Operation{{Type: OpCreateNode, Node: node}}, physical.ops, "the cancelled relationship write is not materialized")
		require.Equal(t, map[NodeID]commitEntityState[*Node]{"test:select-n": {after: node}}, physical.states.nodes)
		require.Empty(t, physical.states.edges)
		require.Contains(t, logical.states.edges, EdgeID("test:select-e"), "the logical frame still reports the cancelled relationship")
	})

	t.Run("the owner condition decides, not the operations it leaves", func(t *testing.T) {
		unmatched := []Operation{{Type: OpCreateNode, Node: node}}
		tx := &BadgerTransaction{operations: unmatched, cancelledEdgeWrites: map[EdgeID]struct{}{"test:absent": {}}}
		logical := mustCommitFrame(t, unmatched)
		physical, err := tx.physicalCommitFrame(t.Context(), logical, tx.physicalOperationsLocked())
		require.NoError(t, err)
		require.Equal(t, logical.ops, physical.ops, "no operation was filtered")
		require.Equal(t, logical.states, physical.states)
		require.NotSame(t, &logical.ops[0], &physical.ops[0], "the frame follows the owner's condition, not the operations it left")
	})
}

// TestPreparedCommitFrameServesMaterializationAndPublication is the read-only
// reuse a prepared frame promises: the frame a commit prepares once is
// materialized and then published, each consumer's outcome is that frame's
// fold, and neither consumer rewrites it, so the frame still holds the
// endpoints its owner prepared.
func TestPreparedCommitFrameServesMaterializationAndPublication(t *testing.T) {
	engine := newTestEngine(t)
	engine.activeMVCCSnapshotReaders.Store(1)
	defer engine.activeMVCCSnapshotReaders.Store(0)

	committed := &Node{ID: "test:frame", Labels: []string{"L"}, Properties: map[string]any{"p": "committed"}}
	_, err := engine.CreateNode(t.Context(), committed)
	require.NoError(t, err)
	createdHead, err := engine.GetNodeCurrentHead(context.Background(), "test:frame")
	require.NoError(t, err)

	written := &Node{ID: "test:frame", Labels: []string{"L"}, Properties: map[string]any{"p": "final"}}
	operations := []Operation{{Type: OpUpdateNode, Node: written, OldNode: committed}}
	frame := mustCommitFrame(t, operations)

	version := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(time.Second), CommitSequence: createdHead.Version.CommitSequence + 1}
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return engine.materializeMVCCCommit(t.Context(), mvccCommitMaterialization{writer: singleBatchWriter(txn), version: version, frame: frame})
	}))

	// The same frame publishes next, and publication reads the fold the
	// materialization read: the cache images hold the very maps the frame's
	// owner prepared.
	var images commitCacheImages
	engine.publishCommitOperations(namespaceForNodeID(written.ID), nil, frame, false, false, &images)
	require.Equal(t, reflect.ValueOf(frame.states.nodes).Pointer(), reflect.ValueOf(images.first.nodes).Pointer(), "publication reads the frame's states instead of folding its operations again")

	updated, err := engine.GetNodeCurrentHead(context.Background(), "test:frame")
	require.NoError(t, err)
	require.Equal(t, version.CommitSequence, updated.Version.CommitSequence, "materialization headed the frame's final image")

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		record, err := engine.loadNodeMVCCRecordExactInTxn(txn, "test:frame", createdHead.Version)
		require.NoError(t, err, "materialization archived the frame's committed predecessor at its own version")
		require.NotNil(t, record.Node)
		require.Equal(t, "committed", record.Node.Properties["p"])
		return nil
	}))

	require.Equal(t, mustCommitFrame(t, operations), frame, "both consumers left the frame as its owner prepared it")
}

// TestBufferedFlushPublishesPreparedFrames drives the prepared frames through
// the buffered generation the flusher materializes and publishes: several
// acknowledged logical frames whose repeated updates, deletion, cascaded
// relationship and recreation must land exactly once, in commit order.
func TestBufferedFlushPublishesPreparedFrames(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	ctx := context.Background()

	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		if _, err := tx.CreateNode(ctx, &Node{ID: "test:frame", Labels: []string{"L"}, Properties: map[string]any{"p": "one"}}); err != nil {
			return err
		}
		if _, err := tx.CreateNode(ctx, &Node{ID: "test:gone", Labels: []string{"L"}, Properties: map[string]any{"p": "gone"}}); err != nil {
			return err
		}
		return tx.CreateEdge(ctx, &Edge{ID: "test:edge", StartNode: "test:frame", EndNode: "test:gone", Type: "R"})
	})
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:frame", Labels: []string{"L"}, Properties: map[string]any{"p": "two"}}})
	})
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:frame", Labels: []string{"L"}, Properties: map[string]any{"p": "three"}}})
	})
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.DeleteGraphNode(ctx, "test:gone", CascadeEdges)
	})
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(ctx, &Node{ID: "test:gone", Labels: []string{"L"}, Properties: map[string]any{"p": "recreated"}})
		return err
	})
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the frames must be buffered before the flush")

	require.NoError(t, engine.FlushWriteBehind(ctx))
	require.Zero(t, engine.writeBehind.PendingOps())

	frame, err := engine.GetNode(ctx, "test:frame")
	require.NoError(t, err)
	require.Equal(t, "three", frame.Properties["p"], "the last acknowledged update of the frame wins")

	gone, err := engine.GetNode(ctx, "test:gone")
	require.NoError(t, err)
	require.Equal(t, "recreated", gone.Properties["p"], "the recreation follows the deletion")

	_, err = engine.GetEdge(ctx, "test:edge")
	require.ErrorIs(t, err, ErrNotFound, "the cascade removed the relationship the deletion folded")

	count, err := engine.NodeCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}
