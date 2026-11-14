package search

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// publicationOrderVectors are the report's fixture vectors: V0, V1, V2 and V3.
var publicationOrderVectors = [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {0.6, 0.8, 0}}

// publicationOrderTexts are the texts "node id at k" stores, so each write is
// distinguishable in the full-text index. Letters only, so no analyzer setting
// decides whether the term survives.
var publicationOrderTexts = []string{"alpha", "bravo", "charlie", "delta"}

// publicationOrderNode is node id at k of the report's fixture: the text of
// that write, the property vector Vk and the named vector V((k+1) mod 4).
func publicationOrderNode(id string, k int) *storage.Node {
	property := publicationOrderVectors[k%len(publicationOrderVectors)]
	named := publicationOrderVectors[(k+1)%len(publicationOrderVectors)]
	return &storage.Node{
		ID:              storage.NodeID(id),
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": publicationOrderTexts[k%len(publicationOrderTexts)], "p": property},
		NamedEmbeddings: map[string][]float32{"q": named},
	}
}

// requireServedNodeVector is the one assertion that the index serves want as
// the vector a query on property reads for nodeID (VectorSourcePrecedence).
func requireServedNodeVector(t *testing.T, svc *Service, nodeID, property string, want []float32) {
	t.Helper()
	svc.indexMu.RLock()
	keys := svc.indexVectorKeysLocked(nodeID, property)
	svc.indexMu.RUnlock()
	require.Len(t, keys, 1, "the index serves one vector of %s for %s", property, nodeID)
	requireBuildWriteVector(t, svc, keys[0], want)
}

// newPublicationOrderService builds the report's seed with the selected warm
// and persistence path. Both the delayed and serial schedules use this fixture.
func newPublicationOrderService(t *testing.T, ctx context.Context, firstBuild string) (*storage.MemoryEngine, *Service) {
	t.Helper()
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dir := t.TempDir()
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.FixVectorDimensions()
	if strings.HasSuffix(firstBuild, "_persistent") {
		svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
		svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
		svc.SetPersistenceEnabled(true)
	}
	svc.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if strings.HasPrefix(firstBuild, "lazy") {
		svc.SetLazyWarming(true, func() { _ = svc.BuildIndexes(ctx) })
	}

	// The seed: node n0 at 0 and node n1 at 1, then the first build.
	for k, id := range []string{"nornic:n0", "nornic:n1"} {
		_, err := engine.CreateNode(ctx, publicationOrderNode(id, k))
		require.NoError(t, err)
	}
	if strings.HasPrefix(firstBuild, "lazy") {
		require.NoError(t, svc.EnsureWarm(ctx))
	} else {
		require.NoError(t, svc.BuildIndexes(ctx))
	}
	return engine, svc
}

// A node's serving vectors are published by the refresh that is still the
// node's newest reservation, and each refresh installs what storage holds when
// it is applied. An older reserved refresh therefore cannot bring back the
// vectors of an earlier write once a newer write's refresh (or a rebuild) has
// published the newer ones. Report 49 (group B17).
func TestOlderReservedRefreshKeepsTheNewestVectors(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	ctx := context.Background()

	for _, firstBuild := range []string{"eager", "lazy", "eager_persistent", "lazy_persistent"} {
		t.Run(firstBuild, func(t *testing.T) {
			engine, svc := newPublicationOrderService(t, ctx, firstBuild)
			// Two writes of n0, each reserving the refresh it is owed, a
			// rebuild between them, then the newer refresh, another rebuild
			// and the older refresh last.
			require.NoError(t, engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: publicationOrderNode("nornic:n0", 2)}))
			older := svc.ReserveNodeMutation("nornic:n0")
			require.NoError(t, engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: publicationOrderNode("nornic:n0", 3)}))
			newer := svc.ReserveNodeMutation("nornic:n0")

			require.NoError(t, svc.BuildIndexes(ctx))
			require.NoError(t, svc.RefreshNodeWithMutation(ctx, newer))
			require.NoError(t, svc.BuildIndexes(ctx))
			require.NoError(t, svc.RefreshNodeWithMutation(ctx, older))

			serialEngine, serial := newPublicationOrderService(t, ctx, firstBuild)
			for _, version := range []int{2, 3} {
				require.NoError(t, serialEngine.UpdateNode(ctx, storage.NodeUpdateRequest{After: publicationOrderNode("nornic:n0", version)}))
				require.NoError(t, serial.RefreshNode(ctx, "nornic:n0"))
			}
			require.NoError(t, serial.BuildIndexes(ctx))
			require.NoError(t, serial.BuildIndexes(ctx))

			stored, err := engine.GetNode(ctx, "nornic:n0")
			require.NoError(t, err)
			require.Equal(t, "delta", stored.Properties["text"], "storage holds the last write")

			// The index serves the last write, not the write the older
			// reservation belonged to: p = V3 and q = V0.
			requireServedNodeVector(t, svc, "nornic:n0", "p", publicationOrderVectors[3])
			requireServedNodeVector(t, svc, "nornic:n0", "q", publicationOrderVectors[0])

			// The node's full-text entry is the last write's text, so the
			// vector entries and the lexical entry agree.
			require.Equal(t, []string{"nornic:n0"}, buildWriteSearchIDs(t, svc, "delta"))

			// A vector query of the index ranks the node by the last write.
			hits, err := svc.VectorQueryNodes(ctx, publicationOrderVectors[3], VectorQuerySpec{
				Label:      "Doc",
				Property:   "p",
				Similarity: "cosine",
				Limit:      10,
			})
			require.NoError(t, err)
			require.NotEmpty(t, hits)
			require.Equal(t, "nornic:n0", hits[0].ID, "the last write's vector ranks first")
			require.InDelta(t, 1.0, hits[0].Score, 1e-6)
			// Reconcile every reported probe against the serial-write control,
			// including named-property vectors and Search's raw similarity.
			for _, probe := range [][]float32{publicationOrderVectors[3], publicationOrderVectors[0], publicationOrderVectors[1]} {
				for _, property := range []string{"p", "q"} {
					var scores [2]map[string]float64
					for i, source := range []*Service{svc, serial} {
						rows, err := source.VectorQueryNodes(ctx, probe, VectorQuerySpec{Label: "Doc", Property: property, Similarity: "cosine", Limit: 10})
						require.NoError(t, err)
						scores[i] = map[string]float64{}
						for _, row := range rows {
							scores[i][row.ID] = row.Score
						}
					}
					require.Equal(t, scores[1], scores[0], "property %s, probe %v", property, probe)
				}
				var similarity [2]map[string]float64
				for i, source := range []*Service{svc, serial} {
					result, err := source.Search(ctx, "", probe, &SearchOptions{Limit: 10})
					require.NoError(t, err)
					similarity[i] = map[string]float64{}
					for _, row := range result.Results {
						similarity[i][row.ID] = row.Similarity
					}
				}
				require.Equal(t, similarity[1], similarity[0], "Search probe %v", probe)
			}

		})
	}
}

// Certificates from the count-based producer cannot establish source equality:
// that producer could certify stale vectors after rejecting their saved tuple.
func TestVectorCheckpointRejectsCountBasedProducerPolicy(t *testing.T) {
	ctx := t.Context()
	_, svc := newPublicationOrderService(t, ctx, "eager_persistent")
	path := populationRecordPath([3]string{svc.fulltextIndexPath, svc.vectorIndexPath, svc.hnswIndexPath})
	record, exists, err := readPopulationRecord(path)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, record.Complete)
	svc.mu.RLock()
	store := svc.vectorFileStore
	svc.mu.RUnlock()
	admitted, err := svc.vectorStoreSourceAdmitted(store, record.SourceStamp)
	require.NoError(t, err)
	require.True(t, admitted, "the current certified tuple remains reusable")
	current := record
	space, configured := svc.embeddingSpace.Load().(string)
	// This is the exact policy spelling emitted before the admission fix.
	record.VectorSettings = fmt.Sprintf("%s;embedding-space-configured=%t;embedding-space=%q", svc.composeVectorBuildSettings(), configured, space)
	require.NoError(t, writeMsgpackSnapshot(path, &record))
	admitted, err = svc.vectorStoreSourceAdmitted(store, record.SourceStamp)
	require.NoError(t, err)
	require.False(t, admitted, "an older producer may have certified stale live entries")
	require.NoError(t, writeMsgpackSnapshot(path, &current))
	admitted, err = svc.vectorStoreSourceAdmitted(store, current.SourceStamp)
	require.NoError(t, err)
	require.True(t, admitted, "restoring the current policy preserves the warm-load path")
}
