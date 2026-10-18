package search

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type relationshipRestoreTraversalGuard struct {
	*storage.NamespacedEngine
	nodeTraversals int
	edgeStreams    int
	edgeFallbacks  int
	edgeErr        error
}

func (e *relationshipRestoreTraversalGuard) IterateNodes(func(*storage.Node) bool) error {
	e.nodeTraversals++
	return errors.New("restored node artifacts must skip node traversal")
}

func (e *relationshipRestoreTraversalGuard) AllNodes() ([]*storage.Node, error) {
	e.nodeTraversals++
	return nil, errors.New("restored node artifacts must skip node traversal")
}

func (e *relationshipRestoreTraversalGuard) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	e.edgeStreams++
	if e.edgeErr != nil {
		return e.edgeErr
	}
	return e.NamespacedEngine.StreamEdges(ctx, fn)
}

// OpenReadSnapshot counts the reads through the snapshot and fails them as
// the engine's own IterateNodes and StreamEdges do.
func (e *relationshipRestoreTraversalGuard) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := e.NamespacedEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &hookedReadSnapshot{ReadSnapshotView: inner, beforeNodes: func() error {
		e.nodeTraversals++
		return errors.New("restored node artifacts must skip node traversal")
	}, beforeEdges: func() error {
		e.edgeStreams++
		return e.edgeErr
	}}, nil
}

// relationshipRestoreWithoutSnapshot hides the guard's streaming and read
// snapshots, like an engine that has neither.
type relationshipRestoreWithoutSnapshot struct{ storage.Engine }

func (e *relationshipRestoreTraversalGuard) AllEdges() ([]*storage.Edge, error) {
	e.edgeFallbacks++
	if e.edgeErr != nil {
		return nil, e.edgeErr
	}
	return e.NamespacedEngine.AllEdges()
}

func TestBaseRestoreLoadsCompleteRelationshipPopulation(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, wrapper := range []string{"none", "traced", "snapshotless"} {
			for _, kind := range []string{"oversized_payload", "aggregate_payload", "oversized_metadata", "aggregate_metadata", "existing_edge_headroom", "traversal_error", "within_budget", "unlimited"} {
				t.Run(fmt.Sprintf("%s/%s/%s", backend, wrapper, kind), func(t *testing.T) {
					ctx := context.Background()
					base := storage.NewMemoryEngine()
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					engine := storage.NewNamespacedEngine(base, "nornic")
					_, err := engine.CreateNode(&storage.Node{ID: "a", NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}, Properties: map[string]any{"text": "seed"}})
					require.NoError(t, err)
					_, err = engine.CreateNode(&storage.Node{ID: "b"})
					require.NoError(t, err)
					edges := make([]*storage.Edge, 0, 2)
					for i := 0; i < 2; i++ {
						edge := &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: "a", EndNode: "b", Type: "REL", Properties: map[string]any{"embedding": []float32{1, 0, 0}}}
						require.NoError(t, engine.CreateEdge(edge))
						edges = append(edges, edge)
					}
					dir := t.TempDir()
					configure := func(svc *Service) {
						svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
						svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
						svc.SetPersistenceEnabled(true)
					}
					first := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, backend, nil)
					t.Cleanup(func() { require.NoError(t, first.Close()) })
					configure(first)
					require.NoError(t, first.BuildIndexes(ctx))
					require.Greater(t, first.fulltext().Count(), 0)
					require.NotNil(t, first.vectorFileStore)
					require.Greater(t, first.vectorFileStore.Count(), 0)
					_, _, _, _, _, metadataOK := first.vectorFileStore.NodeQueryMetadataWithTypes()
					require.True(t, metadataOK)
					nodeMetadata := first.vectorMetadataBytes
					for _, usage := range first.indexCapacityByEdge {
						nodeMetadata -= usage.vectorMetadata
					}
					require.NoError(t, first.Close())
					guard := &relationshipRestoreTraversalGuard{NamespacedEngine: engine}
					var source storage.Engine = guard
					switch wrapper {
					case "traced":
						// No StreamEdges of its own; it forwards the read snapshot.
						source = storage.NewTracedEngine(guard)
					case "snapshotless":
						// Neither: relationships are read through the AllEdges fallback.
						source = relationshipRestoreWithoutSnapshot{guard}
					}
					_, streaming := source.(storage.StreamingEngine)
					require.Equal(t, wrapper == "none", streaming)
					require.Equal(t, wrapper != "snapshotless", storage.SupportsReadSnapshot(ctx, source))
					options := &ServiceOptions{}
					wantEdges, wantResident, reject := 1, int64(12), true
					wantError := ErrIndexMemoryBudgetExceeded
					switch kind {
					case "oversized_payload":
						options.VectorMemoryMaxBytes = 8
						wantEdges, wantResident = 0, 0
					case "aggregate_payload":
						options.VectorMemoryMaxBytes = 12
					case "oversized_metadata":
						options.MetadataMemoryMaxBytes = nodeMetadata + 45
						wantEdges, wantResident = 0, 0
					case "aggregate_metadata":
						options.MetadataMemoryMaxBytes = nodeMetadata + 91
					case "existing_edge_headroom":
						options.VectorMemoryMaxBytes = 24
						wantEdges, wantResident = 2, 24
					case "traversal_error":
						guard.edgeErr = errors.New("relationship traversal unavailable")
						wantError = guard.edgeErr
						wantEdges, wantResident = 0, 0
					case "within_budget":
						options.VectorMemoryMaxBytes = 24
						wantEdges, wantResident, reject = 2, 24, false
					case "unlimited":
						wantEdges, wantResident, reject = 2, 24, false
					}
					svc := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, backend, options)
					t.Cleanup(func() { require.NoError(t, svc.Close()) })
					configure(svc)
					if kind == "existing_edge_headroom" {
						require.NoError(t, svc.IndexEdge(&storage.Edge{ID: "other", Type: "OTHER", Properties: map[string]any{"embedding": []float32{1, 0, 0}}}))
					}
					for pass := 0; pass < 2; pass++ {
						err := svc.BuildIndexes(ctx)
						if reject {
							assert.ErrorIs(t, err, wantError)
							assert.False(t, svc.IsReady(), "rejected restored population must remain not-ready")
						} else {
							require.NoError(t, err)
							assert.True(t, svc.IsReady())
						}
						assert.Zero(t, guard.nodeTraversals, "valid artifacts must retain the node-skip optimization")
						if wrapper == "snapshotless" {
							assert.Greater(t, guard.edgeFallbacks, pass, "restoration must populate relationships through fallback")
						} else {
							assert.Greater(t, guard.edgeStreams, pass, "restoration must populate relationships through its read snapshot")
						}
						assert.Len(t, svc.edgePropVector, wantEdges)
						assert.Len(t, svc.edgeTypes, wantEdges)
						assert.Len(t, svc.indexCapacityByEdge, wantEdges)
						assert.Equal(t, wantResident, svc.vectorResidentBytes)
						if !reject {
							query := func() {
								hits, err := svc.VectorQueryRelationships(ctx, []float32{1, 0, 0}, RelationshipVectorQuerySpec{Type: "REL", Property: "embedding", Similarity: "cosine", Limit: 2})
								require.NoError(t, err)
								ids := make([]string, 0, len(hits))
								for _, hit := range hits {
									ids = append(ids, hit.ID)
									assert.InDelta(t, 1, hit.Score, 1e-6)
								}
								assert.ElementsMatch(t, []string{"r0", "r1"}, ids, "complete population must survive a single-edge live update")
							}
							query()
							require.NoError(t, svc.IndexEdge(edges[1]))
							query()
						}
					}
				})
			}
		}
	}
}
