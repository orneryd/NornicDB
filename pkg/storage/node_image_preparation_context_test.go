package storage

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func preparationImageNode(kind string) *Node {
	node := &Node{ID: "test:image", Labels: []string{"ImageCandidate"}}
	switch kind {
	case "properties":
		node.Properties = make(map[string]any)
		for i := 0; i < 20; i++ {
			node.Properties[fmt.Sprintf("property%d", i)] = int64(i)
		}
	case "metadata":
		node.EmbedMeta = make(map[string]any)
		for i := 0; i < 20; i++ {
			node.EmbedMeta[fmt.Sprintf("metadata%d", i)] = int64(i)
		}
	case "chunks":
		for i := 0; i < 20; i++ {
			node.ChunkEmbeddings = append(node.ChunkEmbeddings, []float32{1, 2, 3, 4})
		}
	case "named":
		node.NamedEmbeddings = make(map[string][]float32)
		for i := 0; i < 20; i++ {
			node.NamedEmbeddings[fmt.Sprintf("vector%d", i)] = []float32{1, 2, 3, 4}
		}
	}
	return node
}

func TestNativeNodeImagePreparationCancellation(t *testing.T) {
	for _, kind := range []string{"properties", "metadata", "chunks", "named"} {
		t.Run(kind, func(t *testing.T) {
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			owner := "copyNodeWithoutEmbeddingsWithContext"
			if kind == "chunks" || kind == "named" {
				owner = "copyNodeWithContext"
			}
			ctx := preparationContext(t, owner, 3)
			_, mutationErr := tx.CreateNodeWithContext(ctx, preparationImageNode(kind))
			commitErr := tx.CommitContext(context.Background())
			nodes, err := b.GetStoredNodesByLabel("ImageCandidate")
			require.NoError(t, err)
			t.Logf("kind=%s observations=%d mutation=%v commit=%v stored=%d", kind, ctx.hits, mutationErr, commitErr, len(nodes))
			require.GreaterOrEqual(t, ctx.hits, 3)
			require.ErrorIs(t, mutationErr, context.Canceled)
			require.Error(t, commitErr, "a canceled partial staging pass cannot later be committed")
			require.Empty(t, nodes)
		})
	}
}

func TestNativeNodeImageUpdateDeleteCancellation(t *testing.T) {
	for _, operation := range []string{"update", "delete"} {
		for _, kind := range []string{"properties", "metadata", "chunks", "named"} {
			t.Run(operation+"/"+kind, func(t *testing.T) {
				b, err := NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				node := preparationImageNode(kind)
				_, err = tx.CreateNode(node)
				require.NoError(t, err)
				owner := "copyNodeWithoutEmbeddingsWithContext"
				if kind == "chunks" || kind == "named" {
					owner = "copyNodeWithContext"
				}
				ctx := preparationContext(t, owner, 3)
				var mutationErr error
				if operation == "update" {
					mutationErr = tx.UpdateGraphNodeWithContext(ctx, node, StoredNodeImages)
				} else {
					mutationErr = tx.DeleteGraphNodeWithContext(ctx, node.ID, CascadeEdges)
				}
				commitErr := tx.CommitContext(context.Background())
				nodes, err := b.GetStoredNodesByLabel("ImageCandidate")
				require.NoError(t, err)
				t.Logf("operation=%s kind=%s observations=%d mutation=%v commit=%v stored=%d", operation, kind, ctx.hits, mutationErr, commitErr, len(nodes))
				require.GreaterOrEqual(t, ctx.hits, 3)
				require.ErrorIs(t, mutationErr, context.Canceled)
				require.Error(t, commitErr, "canceled staging must not later be accepted")
				require.Empty(t, nodes)
			})
		}
	}
}

// Exercise prepared command copying through the actual public WAL submitter,
// with preparation already complete, so an earlier native staging check cannot
// supply credit for the later immutable-payload copy.
func TestWALPreparedNodeImageCopyCancellation(t *testing.T) {
	for _, kind := range []string{"properties", "metadata", "chunks", "named"} {
		t.Run(kind, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			dir := t.TempDir()
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
			require.NoError(t, err)
			w := NewWALEngine(b, wal)
			t.Cleanup(func() { require.NoError(t, w.Close()) })
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = tx.CreateNode(preparationImageNode(kind))
			require.NoError(t, err)
			prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
			require.NoError(t, err)
			defer prepared.Abort()
			before := wal.Sequence()
			// The retained private prepared snapshot is encoded directly. Its
			// former extra public copy is gone; qualify cancellation inside
			// the canonical wire writer over the same four image populations.
			owner := "semanticTransactionWriter"
			ctx := preparationContext(t, owner, 3)
			receipt, submitErr := w.SubmitEntityCommit(ctx, prepared, nil)
			if receipt != nil {
				require.NoError(t, receipt.Wait(context.Background()))
			}
			prepared.Abort()
			nodes, err := b.GetStoredNodesByLabel("ImageCandidate")
			require.NoError(t, err)
			t.Logf("kind=%s observations=%d submit=%v stored=%d WAL=%d->%d", kind, ctx.hits, submitErr, len(nodes), before, wal.Sequence())
			require.GreaterOrEqual(t, ctx.hits, 3)
			require.ErrorIs(t, submitErr, context.Canceled)
			require.Nil(t, receipt)
			require.Empty(t, nodes)
			require.Equal(t, before, wal.Sequence(), "rejected image-copy preparation cannot become replayable")
		})
	}
}
