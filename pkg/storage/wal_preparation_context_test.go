package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A mutation rejected during caller-owned preparation must not reappear after
// WAL recovery. The accepted semantic commit owner is tested separately.
func TestWALCanceledPreparationDoesNotRecover(t *testing.T) {
	for _, owner := range []string{"before call", "createBulkEntitiesWithContext", "semanticTransactionWriter", "EncodeEntityTransactionRequestWithContext", "retainPreparedAuthorityWithContext", "tryContextEntityOperations"} {
		t.Run(owner, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
			require.NoError(t, err)
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			w := NewWALEngine(b, wal)
			t.Cleanup(func() { _ = w.Close() })
			if owner == "before call" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err = w.CreateNodeWithContext(ctx, &Node{ID: "test:canceled"})
				require.ErrorIs(t, err, context.Canceled)
			} else {
				ctx := preparationContext(t, owner, 2)
				if owner == "createBulkEntitiesWithContext" {
					ctx.additionalOwner = "submitDecodedEntityTransactionWithPolicy"
				}
				nodes := make([]*Node, 20)
				for i := range nodes {
					nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:canceled%d", i))}
				}
				err = w.BulkCreateNodesWithContext(ctx, nodes)
				t.Logf("owner=%s observed=%s hits=%d write=%v", owner, ctx.observed, ctx.hits, err)
				require.GreaterOrEqual(t, ctx.hits, 2)
				require.ErrorIs(t, err, context.Canceled)
			}
			_, err = w.CreateNode(&Node{ID: "test:healthy"})
			require.NoError(t, err)
			require.NoError(t, w.Close())
			recovered, replay, err := RecoverFromWALWithResult(dir, "")
			require.NoError(t, err)
			defer recovered.Close()
			require.Empty(t, replay.Errors)
			nodes, err := recovered.AllNodes()
			require.NoError(t, err)
			t.Logf("owner=%s recovered=%d", owner, len(nodes))
			require.Len(t, nodes, 1, "canceled preparation must never become replayable")
			require.Equal(t, NodeID("test:healthy"), nodes[0].ID)
		})
	}
}

func TestWALContextUpdateRetainsUpsert(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	b, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	w := NewWALEngine(b, wal)
	t.Cleanup(func() { _ = w.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, w.UpdateNodeWithContext(ctx, &Node{ID: "test:new", Properties: map[string]interface{}{"v": "created"}}))
	require.NoError(t, w.UpdateNodeWithContext(ctx, &Node{ID: "test:new", Properties: map[string]interface{}{"v": "updated"}}))
	node, err := b.GetStoredNode("test:new")
	require.NoError(t, err)
	require.Equal(t, "updated", node.Properties["v"])
	require.NoError(t, w.Close())
	recovered, replay, err := RecoverFromWALWithResult(dir, "")
	require.NoError(t, err)
	defer recovered.Close()
	require.Empty(t, replay.Errors)
	node, err = recovered.GetStoredNode("test:new")
	require.NoError(t, err)
	require.Equal(t, "updated", node.Properties["v"])
	count, err := recovered.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestAcceptedCommandDecodePreparationCancellation(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	b, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	w := NewWALEngine(b, wal)
	t.Cleanup(func() { _ = w.Close() })
	ops := make([]Operation, 20)
	for i := range ops {
		ops[i] = Operation{Type: OpCreateNode, Node: &Node{ID: NodeID(fmt.Sprintf("test:command%d", i))}}
	}
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "cancel-decode", Operations: ops, Mode: CommitModeImplicit})
	require.NoError(t, err)
	ctx := preparationContext(t, "DecodeEntityTransactionWithContext", 2)
	envelope, decoded, err := DecodeEntityTransactionWithContext(ctx, payload)
	var receipt *CommitReceipt
	if err == nil {
		receipt, err = SubmitAcceptedEntityTransaction(ctx, w, envelope, decoded, nil)
	}
	// A failing old implementation may have accepted this command; settle it so
	// assertions cannot abandon a live native transaction or recovery owner.
	if receipt != nil {
		require.NoError(t, receipt.Wait(context.Background()))
	}
	t.Logf("owner=%s hits=%d submit=%v", ctx.owner, ctx.hits, err)
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, receipt)
	require.NoError(t, w.Close())
	recovered, replay, err := RecoverFromWALWithResult(dir, "")
	require.NoError(t, err)
	defer recovered.Close()
	require.Empty(t, replay.Errors)
	nodes, err := recovered.AllNodes()
	require.NoError(t, err)
	require.Empty(t, nodes)
}

type walPreparationDecorator struct{ Engine }

func (w *walPreparationDecorator) GetInnerEngine() Engine { return w.Engine }

func TestWALFramePreparationCancellation(t *testing.T) {
	for _, owner := range []string{"BeginEntityTransactionWithContext", "EntityCommitSubmitterForWithContext"} {
		t.Run(owner, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
			require.NoError(t, err)
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			var decorated Engine = b
			for i := 0; i < 20; i++ {
				decorated = &walPreparationDecorator{Engine: decorated}
			}
			w := NewWALEngine(decorated, wal)
			t.Cleanup(func() { _ = w.Close() })
			ctx := preparationContext(t, owner, 2)
			_, err = w.CreateNodeWithContext(ctx, &Node{ID: "test:rejected"})
			t.Logf("owner=%s hits=%d create=%v", ctx.owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, w.Close())
			recovered, replay, err := RecoverFromWALWithResult(dir, "")
			require.NoError(t, err)
			defer recovered.Close()
			require.Empty(t, replay.Errors)
			nodes, err := recovered.AllNodes()
			require.NoError(t, err)
			require.Empty(t, nodes)
		})
	}
}
