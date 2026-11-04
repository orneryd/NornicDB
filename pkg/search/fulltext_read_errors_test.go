package search

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fulltextReadFaultStore struct {
	*storage.BadgerEngine
	openErr, rowErr error
	scans           atomic.Int64
}

func (s *fulltextReadFaultStore) GetInnerEngine() storage.Engine { return s.BadgerEngine }
func (*fulltextReadFaultStore) PreservesEventView()              {}
func (s *fulltextReadFaultStore) StreamNodes(ctx context.Context, visit func(*storage.Node) error) error {
	s.scans.Add(1)
	return s.BadgerEngine.StreamNodes(ctx, visit)
}
func (s *fulltextReadFaultStore) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	if s.openErr != nil {
		return nil, s.openErr
	}
	view, err := s.BadgerEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	reader, ok := view.(storage.EntityReadSnapshot)
	if !ok {
		view.Close()
		return nil, storage.ErrReadSnapshotUnsupported
	}
	return &fulltextReadFaultSnapshot{EntityReadSnapshot: reader, store: s}, nil
}

type fulltextReadFaultSnapshot struct {
	storage.EntityReadSnapshot
	store *fulltextReadFaultStore
}

func (s *fulltextReadFaultSnapshot) ReadNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if s.store.rowErr != nil {
		return nil, s.store.rowErr
	}
	return s.EntityReadSnapshot.ReadNode(ctx, id)
}

func TestFulltextRetainedReadDoesNotHideStorageFailures(t *testing.T) {
	for _, phase := range []string{"capture", "row"} {
		t.Run(phase, func(t *testing.T) {
			base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed).BadgerEngine
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := &fulltextReadFaultStore{BadgerEngine: base}
			svc := NewService(store, DefaultVectorDimensions, nil)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			_, err := store.CreateNode(t.Context(), &storage.Node{ID: "errors:one", Properties: map[string]any{"text": "orchard"}})
			require.NoError(t, err)
			read := func() error {
				return svc.ReadFulltextDocuments(t.Context(), store, FulltextDocumentScope{Name: "errors", Definition: "text", EntityRows: true}, func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
					return &FulltextDocument{Text: node.Properties["text"].(string), Tokens: 1}
				}, func(readCtx context.Context, d FulltextDocuments) error {
					return d.Each(readCtx, func(id string, doc *FulltextDocument) error {
						_, _, rowErr := d.Entity(readCtx, id, doc)
						return rowErr
					})
				})
			}
			require.NoError(t, read())
			before := store.scans.Load()
			failure := errors.New("controlled fulltext storage I/O failure")
			if phase == "capture" {
				store.openErr = failure
			} else {
				store.rowErr = failure
			}
			require.ErrorIs(t, read(), failure, "an operational read failure must reach the consumer")
			require.Equal(t, before, store.scans.Load(), "a failed retained read must not silently run a full-scan replacement")
			store.openErr, store.rowErr = nil, nil
			require.NoError(t, read(), "a failed view must not poison the next read")
		})
	}
}
