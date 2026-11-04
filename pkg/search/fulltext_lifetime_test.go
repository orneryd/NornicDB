package search

import (
	"context"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A fulltext callback is native service work. Shutdown must cancel the read
// context the service supplies to that callback, wait for its return, and
// reject new reads before disposal.
func TestFulltextDocumentsParticipateInServiceLifetime(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	readDone := make(chan error, 1)
	go func() {
		readDone <- svc.ReadFulltextDocuments(t.Context(), store, FulltextDocumentScope{Ordered: true}, nil, func(readCtx context.Context, _ FulltextDocuments) error {
			close(entered)
			<-readCtx.Done()
			close(canceled)
			<-release
			return readCtx.Err()
		})
	}()
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("fulltext lifetime boundary did not complete")
		}
	}
	await(entered)
	closed := make(chan error, 1)
	go func() { closed <- svc.Close() }()
	await(canceled)
	select {
	case err := <-closed:
		t.Fatalf("Close disposed the service while its callback was active: %v", err)
	default:
	}
	err := svc.ReadFulltextDocuments(t.Context(), store, FulltextDocumentScope{Ordered: true}, nil, func(context.Context, FulltextDocuments) error {
		t.Error("new fulltext read entered a closing service")
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	unblock()
	require.ErrorIs(t, <-readDone, context.Canceled)
	require.NoError(t, <-closed)
}

func TestFulltextResetInvalidatesAnUnchangedGraphCertificate(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "reset:one", Properties: map[string]any{"text": "orchard"}})
	require.NoError(t, err)
	builds := 0
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		builds++
		return &FulltextDocument{Text: node.Properties["text"].(string), Tokens: 1}
	}
	read := func() {
		t.Helper()
		require.NoError(t, svc.ReadFulltextDocuments(t.Context(), store, FulltextDocumentScope{Name: "reset", Definition: "text"}, build, func(_ context.Context, d FulltextDocuments) error {
			require.Equal(t, 1, d.Count())
			return nil
		}))
	}
	read()
	require.Equal(t, 1, builds)
	read()
	require.Equal(t, 1, builds, "control: unchanged graph is reused")
	// A reset is independently authoritative; an equal revision cannot certify
	// a view that predates it. This exercises the subscriber transition itself.
	svc.documentOwner().observeReset()
	read()
	require.Equal(t, 2, builds, "reset must force a new population even if the graph token is unchanged")
}

func TestFulltextCanceledFirstReadDoesNotPinAnEmptyProjection(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	build := func(*storage.Node, *storage.Edge) *FulltextDocument { return nil }
	consume := func(context.Context, FulltextDocuments) error { return nil }
	require.NoError(t, svc.ReadFulltextDocuments(t.Context(), store, FulltextDocumentScope{Name: "anchor"}, build, consume))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Cancel inside the real registration interval, after initial admission.
	// Either select branch must retire the new projection when no view exists.
	err := svc.ReadFulltextDocuments(ctx, store, FulltextDocumentScope{Name: "canceled", Retain: func(string) bool { cancel(); return true }}, build, consume)
	require.ErrorIs(t, err, context.Canceled)
	owner := svc.documentOwner()
	owner.changes.Lock()
	_, pinned := owner.views["canceled"]
	owner.changes.Unlock()
	require.False(t, pinned, "an uninitialized canceled projection must not pin journal history")
}
