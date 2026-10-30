package storage

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestAcceptedEntityHandleRetiresTerminalNativeFrame(t *testing.T) {
	b := newTestEngine(t)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:a", ChunkEmbeddings: [][]float32{make([]float32, 16000)}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	defer p.Abort()
	h := NewAcceptedEntityCommitHandle(p)
	r, err := h.Submit(context.Background(), b)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, r.Wait(ctx))
	h.Retire()
	require.Nil(t, h.prepared.Load())
	_, err = h.Submit(ctx, b)
	require.ErrorIs(t, err, ErrStorageClosed)
	node, err := b.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Len(t, node.ChunkEmbeddings, 1)
	require.Len(t, node.ChunkEmbeddings[0], 16000)
}

type parkedAcceptedSubmitter struct {
	Engine
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

func (p *parkedAcceptedSubmitter) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	defer close(p.returned)
	close(p.entered)
	<-p.release
	route, _ := EntityCommitSubmitterFor(p.Engine)
	return route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
}
func TestAcceptedWALCloseRetiresPendingNativeFrameWithoutDiscard(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	b := newTestEngine(t)
	inner := &parkedAcceptedSubmitter{Engine: b, entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(inner.release) }) }
	t.Cleanup(release)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	w := NewWALEngine(inner, wal)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:pending", ChunkEmbeddings: [][]float32{make([]float32, 16000)}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	defer p.Abort()
	settled := false
	receipt, err := w.SubmitEntityCommit(context.Background(), p, func(CommitOutcome) { settled = true })
	require.NoError(t, err)
	select {
	case <-inner.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("inner submission did not enter")
	}
	pending, ok := w.pendingEntityCommits.Load(p.LogicalID())
	require.True(t, ok)
	handle := pending.(*walAcceptedEntityCommit).prepared
	snapshotDone := make(chan error, 1)
	snapshotJoined := make(chan struct{})
	go func() { defer close(snapshotJoined); snapshotDone <- w.createSnapshotAndCompact() }()
	closeDone := make(chan error, 1)
	closeJoined := make(chan struct{})
	go func() { defer close(closeJoined); closeDone <- w.Close() }()
	t.Cleanup(func() {
		release()
		p.Abort()
		pending.(*walAcceptedEntityCommit).releaseMutation()
		select {
		case <-snapshotJoined:
		case <-time.After(5 * time.Second):
			t.Error("snapshot worker did not join")
		}
		select {
		case <-closeJoined:
		case <-time.After(5 * time.Second):
			t.Error("close worker did not join")
		}
		select {
		case <-inner.returned:
		case <-time.After(5 * time.Second):
			t.Error("inner submitter did not join")
		}
	})
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("close retained pending native lifecycle")
	}
	select {
	case err := <-snapshotDone:
		require.ErrorIs(t, err, ErrWALClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot retained pending mutation lease")
	}
	require.Nil(t, handle.prepared.Load())
	require.Equal(t, CommitRecoveryPending, receipt.Outcome().State)
	require.False(t, settled)
	select {
	case <-receipt.Done():
		t.Fatal("shutdown falsely settled accepted logical work")
	default:
	}
	release()
	// The original journal payload remains available for startup recovery.
	require.Greater(t, wal.sequence.Load(), uint64(0))
}
