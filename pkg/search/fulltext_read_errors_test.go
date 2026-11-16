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
	edgeScanErr     error
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

func (s *fulltextReadFaultStore) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	s.scans.Add(1)
	if err := s.BadgerEngine.StreamEdges(ctx, visit); err != nil {
		return err
	}
	return s.edgeScanErr
}

func (s *fulltextReadFaultSnapshot) ReadEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if s.store.rowErr != nil {
		return nil, s.store.rowErr
	}
	return s.EntityReadSnapshot.ReadEdge(ctx, id)
}

func TestFulltextRetainedReadDoesNotHideStorageFailures(t *testing.T) {
	for _, relationships := range []bool{false, true} {
		for _, phase := range []string{"capture", "row", "scan"} {
			if phase == "scan" && !relationships {
				continue
			}
			kind := "nodes"
			if relationships {
				kind = "relationships"
			}
			t.Run(kind+"/"+phase, func(t *testing.T) {
				base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed).BadgerEngine
				t.Cleanup(func() { require.NoError(t, base.Close()) })
				store := &fulltextReadFaultStore{BadgerEngine: base}
				svc := NewService(store, DefaultVectorDimensions, nil)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				_, err := store.CreateNode(t.Context(), &storage.Node{ID: "errors:one", Properties: map[string]any{"text": "orchard"}})
				require.NoError(t, err)
				if relationships {
					_, err = store.CreateNode(t.Context(), &storage.Node{ID: "errors:two"})
					require.NoError(t, err)
					require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "errors:link", StartNode: "errors:one", EndNode: "errors:two", Type: "LINK", Properties: map[string]any{"text": "orchard"}}))
				}
				read := func() error {
					return svc.ReadFulltextDocuments(t.Context(), store, FulltextDocumentScope{Name: "errors", Definition: "text", EntityRows: true, Relationships: relationships, Fresh: phase == "scan"}, func(node *storage.Node, edge *storage.Edge) *FulltextDocument {
						text := ""
						if relationships {
							text = edge.Properties["text"].(string)
						} else {
							text = node.Properties["text"].(string)
						}
						return &FulltextDocument{Text: text, Tokens: 1}
					}, func(readCtx context.Context, d FulltextDocuments) error {
						return d.Each(readCtx, func(id string, doc *FulltextDocument) error { _, _, err := d.Entity(readCtx, id, doc); return err })
					})
				}
				require.NoError(t, read())
				before := store.scans.Load()
				failure := errors.New("controlled fulltext storage I/O failure")
				switch phase {
				case "capture":
					store.openErr = failure
				case "row":
					store.rowErr = failure
				case "scan":
					store.edgeScanErr = failure
				}
				require.ErrorIs(t, read(), failure, "storage failures must reach the public consumer")
				if phase != "scan" {
					require.Equal(t, before, store.scans.Load(), "a failed retained read must not run a full-scan replacement")
				} else {
					require.Greater(t, store.scans.Load(), before, "the failing relationship stream must actually be consumed")
				}
				store.openErr, store.rowErr, store.edgeScanErr = nil, nil, nil
				require.NoError(t, read(), "a failed read must not poison the next read")
			})
		}
	}
}
