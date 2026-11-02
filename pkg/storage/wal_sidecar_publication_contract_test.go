package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func newWALClientFixture(t testing.TB, decorate func(*MemoryEngine) Engine) (*MemoryEngine, *WAL, *WALEngine) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	native := newTestMemoryEngine(t)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, wal)
	var inner Engine = native
	if decorate != nil {
		inner = decorate(native)
	}
	wrapped := fixtureowner.OwnChecked(t, NewWALEngine(inner, wal))
	return native, wal, wrapped
}

// runAtHeldWALAppend controls actual writer admission, not a sleep or a mock
// WAL. It releases the lock and joins the public operation on every exit.
func runAtHeldWALAppend(t *testing.T, wal *WAL, run func() error, blocked func()) error {
	t.Helper()
	waiting := make(chan struct{}, 1)
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &wal.mu {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	})
	defer restore()
	wal.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(wal.mu.Unlock) }
	done := make(chan error, 1)
	go func() { done <- run() }()
	joined := false
	defer func() {
		unlock()
		if !joined {
			<-done
		}
	}()
	select {
	case <-waiting:
		blocked()
	case err := <-done:
		joined = true
		t.Fatalf("public write returned before reaching held WAL admission: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("public write did not reach held WAL admission")
	}
	unlock()
	err := <-done
	joined = true
	return err
}

func walClientEntries(t *testing.T, wal *WAL) []WALEntry {
	t.Helper()
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntries(wal.file.Name(), nil)
	require.NoError(t, err)
	return entries
}

func sidecarClientPayload(t *testing.T, engine Engine) *Node {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:sidecar-client", Labels: []string{"Doc"}, Properties: map[string]any{"title": "source"}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	return embeddingWriteback(t, engine, "test:sidecar-client", [][]float32{{1, 2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, now)
}

func TestWALSidecarCancelBeforeNativeDecisionWritesNoAudit(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	payload := sidecarClientPayload(t, native)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hook := func() { cancel() }
	old := embeddingSourceCheckedHook.Swap(&hook)
	defer embeddingSourceCheckedHook.Store(old)
	err := wrapped.UpdateNodeEmbeddingSidecar(ctx, payload)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, walClientEntries(t, wal))
	node, err := native.GetNode(t.Context(), payload.ID)
	require.NoError(t, err)
	require.Empty(t, node.ChunkEmbeddings)
}

func TestWALSidecarDecisionAcceptedBeforeAppendCompletesNatively(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	payload := sidecarClientPayload(t, native)
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	var decisions atomic.Int32
	ctx := WithPublicationDecision(request, func(_ context.Context, _ PublicationIdentity, action func() error) error {
		decisions.Add(1)
		return action()
	})
	err := runAtHeldWALAppend(t, wal, func() error { return wrapped.UpdateNodeEmbeddingSidecar(ctx, payload) }, func() { require.EqualValues(t, 1, decisions.Load()); cancel() })
	require.NoError(t, err)
	require.EqualValues(t, 1, decisions.Load())
	node, err := native.GetNode(t.Context(), payload.ID)
	require.NoError(t, err)
	require.Equal(t, payload.ChunkEmbeddings, node.ChunkEmbeddings)
	require.Equal(t, payload.Properties, node.Properties)
	require.Equal(t, payload.UpdatedAt, node.UpdatedAt)
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	require.Equal(t, OpUpdateEmbedding, entries[0].Operation)
	// A healthy audit record is skipped so later graph records still replay.
	recovered := newTestMemoryEngine(t)
	require.NoError(t, ReplayWALEntry(recovered, entries[0]))
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:after-audit"})
	require.NoError(t, err)
	entries = walClientEntries(t, wal)
	require.Len(t, entries, 2)
	require.NoError(t, ReplayWALEntry(recovered, entries[1]))
	_, err = recovered.GetNode(t.Context(), payload.ID)
	require.ErrorIs(t, err, ErrNotFound, "audit payload never becomes a graph node")
	count, err := recovered.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

type failingSidecarPublication struct {
	Engine
	failure error
}

func (e *failingSidecarPublication) UpdateNodeEmbeddingSidecar(ctx context.Context, _ *Node) error {
	return DecidePublication(ctx, func() error { return e.failure })
}

// This is a component fault at the existing atomic sidecar action boundary;
// the healthy and cancellation cases above execute the real native owner.
func TestWALSidecarPreservesNativeFailureWithoutRecoveryPromise(t *testing.T) {
	injected := errors.New("native sidecar action rejected")
	native, wal, wrapped := newWALClientFixture(t, func(engine *MemoryEngine) Engine {
		return &failingSidecarPublication{Engine: engine, failure: injected}
	})
	payload := sidecarClientPayload(t, native)
	err := wrapped.UpdateNodeEmbeddingSidecar(t.Context(), payload)
	require.ErrorIs(t, err, injected)
	var accepted *AcceptedCommitError
	require.False(t, errors.As(err, &accepted), "an audit record is not a semantic recovery receipt")
	node, readErr := native.GetNode(t.Context(), payload.ID)
	require.NoError(t, readErr)
	require.Empty(t, node.ChunkEmbeddings)
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	require.Equal(t, OpUpdateEmbedding, entries[0].Operation)
}

func TestWALSidecarAppendFailureDoesNotPublishNativeData(t *testing.T) {
	native, wal, wrapped := newWALClientFixture(t, nil)
	payload := sidecarClientPayload(t, native)
	require.NoError(t, wal.Close())
	err := wrapped.UpdateNodeEmbeddingSidecar(t.Context(), payload)
	require.ErrorIs(t, err, ErrWALClosed)
	node, readErr := native.GetNode(t.Context(), payload.ID)
	require.NoError(t, readErr)
	require.Empty(t, node.ChunkEmbeddings)
}
