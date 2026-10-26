package replication

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAcceptedCommandMemoryWALRetiresNativeFrameRetainsPayload(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	standalone := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
	engine, adapter := standalone.engine, standalone.adapter

	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.CreateNode(&storage.Node{ID: "test:a", ChunkEmbeddings: [][]float32{make([]float32, 16000)}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), tx.ID)
	require.NoError(t, err)
	defer p.Abort()
	receipt, err := engine.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, receipt.Wait(ctx))
	require.NoError(t, adapter.FlushWAL())
	adapter.memWALMu.RLock()
	entries := append([]*WALEntry(nil), adapter.memWAL...)
	adapter.memWALMu.RUnlock()
	var accepted []*Command
	for _, entry := range entries {
		if entry.Command.Type == CmdCommitTransaction && entry.Command.RequestID == p.LogicalID() {
			accepted = append(accepted, entry.Command)
		}
	}
	require.Len(t, accepted, 1)
	cmd := accepted[0]
	require.NotNil(t, cmd.accepted)
	_, err = cmd.accepted.prepared.Submit(ctx, base)
	require.ErrorIs(t, err, storage.ErrStorageClosed, "terminal command no longer owns native frame")
	peer, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(peer, cmd.Data))
	node, err := peer.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Len(t, node.ChunkEmbeddings, 1)
	require.Len(t, node.ChunkEmbeddings[0], 16000)
}

func TestAcceptedInnerWALRejectionReleasesNativeFrameKeepsLogicalPending(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	owner := storage.NewWALEngine(base, wal)
	engine := setupTestAdapter(t, adapterFixtureOptions{backing: owner, standalone: true, timeout: time.Second}).engine

	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.CreateNode(&storage.Node{ID: "test:pending"})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), tx.ID)
	require.NoError(t, err)
	defer p.Abort()
	require.NoError(t, wal.Close())
	receipt, err := engine.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome := receipt.WaitOutcome(ctx)
	require.Equal(t, storage.CommitRecoveryPending, outcome.State)
	require.ErrorIs(t, outcome.Err, storage.ErrWALClosed)
	select {
	case <-receipt.Done():
		t.Fatal("recorded command falsely discarded")
	default:
	}
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() { defer close(joined); done <- owner.Close() }()
	t.Cleanup(func() {
		p.Abort()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("owner close worker did not join")
		}
	})
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("inner rejection retained native frame through close")
	}
}
