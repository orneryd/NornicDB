package search

import (
	"context"
	"io"
	"log/slog"
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

// A node's serving vectors are published by the refresh that is still the
// node's newest reservation, and each refresh installs what storage holds when
// it is applied. An older reserved refresh therefore cannot bring back the
// vectors of an earlier write once a newer write's refresh (or a rebuild) has
// published the newer ones. Report 49 (group B17).
func TestOlderReservedRefreshKeepsTheNewestVectors(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	ctx := context.Background()

	for _, firstBuild := range []string{"eager", "lazy"} {
		t.Run(firstBuild, func(t *testing.T) {
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			svc := NewService(engine, 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			svc.FixVectorDimensions()
			svc.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
			if firstBuild == "lazy" {
				svc.SetLazyWarming(true, func() { _ = svc.BuildIndexes(ctx) })
			}

			// The seed: node n0 at 0 and node n1 at 1, then the first build.
			for k, id := range []string{"nornic:n0", "nornic:n1"} {
				_, err := engine.CreateNode(ctx, publicationOrderNode(id, k))
				require.NoError(t, err)
			}
			if firstBuild == "lazy" {
				require.NoError(t, svc.EnsureWarm(ctx))
			} else {
				require.NoError(t, svc.BuildIndexes(ctx))
			}

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
		})
	}
}
