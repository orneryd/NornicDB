package search

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// writeLegacyVectorStore writes a version-2 vector store keyed with the legacy
// suffix grammar, as a release before vector_entry_key.go saved it: node x's
// chunk 1 and node x-chunk-1's main vector share the key "x-chunk-1", so the
// store holds only one of the two embeddings and its metadata names the key
// for both nodes.
func writeLegacyVectorStore(t *testing.T, vectorPath string) {
	t.Helper()
	store, err := NewVectorFileStore(vectorPath, vectorOwnerDims)
	require.NoError(t, err)
	require.NoError(t, store.Add("x", vectorOwnerAxis(0)))
	require.NoError(t, store.Add("x-chunk-1", vectorOwnerAxis(3))) // x's chunk 1 (axis 1) was overwritten
	require.NoError(t, store.Add("x-chunk-2", vectorOwnerAxis(2)))
	store.SetNodeQueryMetadataWithTypes(
		map[string][]string{"x": {"Doc"}, "x-chunk-1": {"Doc"}},
		map[string]string{},
		map[string]map[string]string{},
		map[string]map[string]string{},
		map[string][]string{"x": {"x", "x-chunk-1", "x-chunk-2"}, "x-chunk-1": {"x-chunk-1"}},
	)
	require.NoError(t, store.Save())
	require.NoError(t, store.Close())

	// Stamp the release's format version into the header and the metadata.
	vec, err := os.ReadFile(vectorPath + ".vec")
	require.NoError(t, err)
	vec[4] = 2
	require.NoError(t, os.WriteFile(vectorPath+".vec", vec, 0o644))
	raw, err := os.ReadFile(vectorPath + ".meta")
	require.NoError(t, err)
	var meta VectorFileStoreMeta
	require.NoError(t, msgpack.Unmarshal(raw, &meta))
	meta.Version = 2
	raw, err = msgpack.Marshal(&meta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(vectorPath+".meta", raw, 0o644))
}

// A vector store saved with legacy keys is never loaded or decoded: it is
// removed and rebuilt from storage, which holds every node's embeddings, and
// every owner is then found under its own entries. Covered with the release's
// build settings (no key format) and with no settings file at all.
func TestLegacyVectorStoreIsRebuiltFromStorage(t *testing.T) {
	for _, settings := range []string{"legacy", "missing"} {
		t.Run("settings="+settings, func(t *testing.T) {
			ctx := context.Background()
			engine := newNamespacedEngine(t)
			nodes := []*storage.Node{vectorOwnerChunkNode("x", 0, 1, 2), vectorOwnerChunkNode("x-chunk-1", 3)}
			for _, node := range nodes {
				_, err := engine.CreateNode(cloneVectorOwnerNode(node))
				require.NoError(t, err)
			}
			dir := t.TempDir()
			fulltextPath, vectorPath := filepath.Join(dir, "bm25"), filepath.Join(dir, "vectors")
			configure := func(svc *Service) {
				svc.SetFulltextIndexPath(fulltextPath)
				svc.SetVectorIndexPath(vectorPath)
				svc.SetPersistenceEnabled(true)
			}

			// A current build leaves BM25 and build settings on disk, so a
			// loadable vector store would let the reopen skip node iteration.
			first := NewService(engine, vectorOwnerDims, nil)
			configure(first)
			require.NoError(t, first.BuildIndexes(ctx))
			first.PersistIndexesToDisk()
			require.NoError(t, first.Close())

			require.NoError(t, os.Remove(vectorPath+".vec"))
			require.NoError(t, os.Remove(vectorPath+".meta"))
			writeLegacyVectorStore(t, vectorPath)
			settingsPath := searchBuildSettingsPath(fulltextPath, vectorPath, "")
			switch settings {
			case "legacy":
				saved, err := loadSearchBuildSettings(settingsPath)
				require.NoError(t, err)
				require.NotNil(t, saved)
				for _, field := range []*string{&saved.Vector, &saved.HNSW, &saved.Routing} {
					require.Contains(t, *field, ";keys="+vectorEntryKeyFormat)
					*field = strings.Replace(*field, ";keys="+vectorEntryKeyFormat, "", 1)
				}
				require.NoError(t, saveSearchBuildSettings(settingsPath, *saved))
			case "missing":
				require.NoError(t, os.Remove(settingsPath))
			}

			second := NewService(engine, vectorOwnerDims, nil)
			t.Cleanup(func() { _ = second.Close() })
			configure(second)
			require.NoError(t, second.BuildIndexes(ctx))
			require.NotNil(t, second.vectorFileStore, "the rebuilt store must be file-backed again")
			assert.Equal(t, 4, second.EmbeddingCount(), "every embedding must come back from storage")
			assert.False(t, second.vectorFileStore.Has("x-chunk-2"), "no legacy key may survive the rebuild")
			assertVectorOwners(t, second, []vectorOwnerQuery{{axis: 1, owner: "x"}, {axis: 2, owner: "x"}, {axis: 3, owner: "x-chunk-1"}})

			// The rebuilt store is saved in the current format.
			second.PersistIndexesToDisk()
			vec, err := os.ReadFile(vectorPath + ".vec")
			require.NoError(t, err)
			assert.Equal(t, byte(vecFileVersion), vec[4])
		})
	}
}

// Graph artifacts whose IDs are legacy keys are treated as absent, never read:
// the global/per-cluster HNSW graph and the IVF cluster member lists.
func TestLegacyGraphArtifactsAreNotLoaded(t *testing.T) {
	dir := t.TempDir()
	legacy := &hnswIndexSnapshot{Version: "1.2.0", Dimensions: 2, InternalToID: []string{"x", "x-chunk-1"}}
	writeHNSWSnapshot(t, filepath.Join(dir, "hnsw"), legacy)
	index, err := LoadHNSWIndex(filepath.Join(dir, "hnsw"), func(string) ([]float32, bool) { return nil, false })
	require.NoError(t, err)
	assert.Nil(t, index)

	ivfDir := filepath.Join(dir, "hnsw_ivf")
	writeHNSWSnapshot(t, filepath.Join(ivfDir, "0"), legacy)
	members, err := loadIVFClusterMemberIDs(filepath.Join(ivfDir, "0"))
	require.NoError(t, err)
	assert.Nil(t, members)

	current := &hnswIndexSnapshot{Version: hnswIndexFormatVersionGraphOnly, Dimensions: 2, InternalToID: []string{"x", nodeChunkVectorKey("x", 1)}}
	writeHNSWSnapshot(t, filepath.Join(ivfDir, "1"), current)
	members, err = loadIVFClusterMemberIDs(filepath.Join(ivfDir, "1"))
	require.NoError(t, err)
	assert.Equal(t, current.InternalToID, members)
}
