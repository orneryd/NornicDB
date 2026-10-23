package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIVFPQPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "hnsw")
	vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 8)
	require.NoError(t, err)
	defer vfs.Close()

	for i := 0; i < 900; i++ {
		vec := []float32{1, 0, 0, 0, 0, 0, 0, 0}
		if i%2 == 0 {
			vec = []float32{0, 1, 0, 0, 0, 0, 0, 0}
		}
		require.NoError(t, vfs.Add(fmt.Sprintf("doc-%d", i), vec))
	}
	idx, _, err := BuildIVFPQFromVectorStore(context.Background(), vfs, IVFPQProfile{
		Dimensions:          8,
		IVFLists:            20,
		PQSegments:          4,
		PQBits:              4,
		NProbe:              5,
		RerankTopK:          50,
		TrainingSampleMax:   800,
		KMeansMaxIterations: 6,
	}, nil)
	require.NoError(t, err)

	require.NoError(t, SaveIVFPQBundle(base, idx))
	loaded, err := LoadIVFPQBundle(base)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, idx.Count(), loaded.Count())
	require.True(t, loaded.compatibleProfile(idx.profile))
}

func TestIVFPQPersistHelpersAndErrorBranches(t *testing.T) {
	require.Equal(t, "", ivfpqBundleDir(""))
	require.NoError(t, SaveIVFPQBundle("", nil))
	require.NoError(t, SaveIVFPQBundle("base", nil))

	idx, err := LoadIVFPQBundle("")
	require.NoError(t, err)
	require.Nil(t, idx)

	idx, err = LoadIVFPQBundle(filepath.Join(t.TempDir(), "missing-base"))
	require.NoError(t, err)
	require.Nil(t, idx)

	// Unsupported format metadata should return nil without error.
	base := filepath.Join(t.TempDir(), "hnsw")
	dir := ivfpqBundleDir(base)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, writeMsgpackSnapshots(dir, map[string]any{
		"meta":      ivfpqMetaSnapshot{FormatVersion: ivfpqBundleFormatVersion + 1},
		"centroids": [][]float32{},
		"codebooks": ivfpqCodebooksSnapshot{},
		"lists":     ivfpqListsSnapshot{},
	}))
	idx, err = LoadIVFPQBundle(base)
	require.NoError(t, err)
	require.Nil(t, idx)

	// Invalid msgpack decode path.
	bad := filepath.Join(t.TempDir(), "bad.msgpack")
	require.NoError(t, os.WriteFile(bad, []byte("not-msgpack"), 0o644))
	var dst ivfpqMetaSnapshot
	require.Error(t, decodeMsgpackFile(bad, &dst))
}

func TestIVFPQPersist_ServiceBranches(t *testing.T) {
	profile := IVFPQProfile{
		Dimensions:          8,
		IVFLists:            16,
		PQSegments:          4,
		PQBits:              4,
		NProbe:              4,
		RerankTopK:          20,
		TrainingSampleMax:   128,
		KMeansMaxIterations: 4,
	}

	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 8)

	// vfs nil branch when no compatible in-memory/persisted index exists.
	_, err := svc.getOrBuildIVFPQIndex(context.Background(), profile, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "vector file store unavailable")

	// persistIVFPQBackground no-op branches: no base path and no index.
	svc.persistIVFPQBackground("", "")
	svc.persistIVFPQBackground(filepath.Join(t.TempDir(), "vectors"), "")
}

func tinyIVFPQIndexForTest(profile IVFPQProfile) *IVFPQIndex {
	idx := &IVFPQIndex{
		profile:         profile,
		centroids:       [][]float32{{1, 0}, {0, 1}},
		centroidNorm:    [][]float32{{1, 0}, {0, 1}},
		codebooks:       []ivfpqCodebook{{SubDim: 2, Codeword: [][]float32{{0, 0}, {1, 1}}}},
		lists:           []ivfpqList{{IDs: []string{"doc-1"}, CodeSize: 1, Codes: []byte{1}}},
		formatVersion:   ivfpqBundleFormatVersion,
		builtAtUnixNano: 123,
	}
	idx.initScratchPool()
	return idx
}

func TestIVFPQPersist_LoadErrorBranches(t *testing.T) {
	base := filepath.Join(t.TempDir(), "hnsw")
	dir := ivfpqBundleDir(base)
	require.NoError(t, os.WriteFile(dir, []byte("not-a-dir"), 0o644))
	idx, err := LoadIVFPQBundle(base)
	require.Error(t, err)
	require.Nil(t, idx)

	for _, tc := range []struct {
		name      string
		files     map[string]any
		badFile   string
		badBytes  []byte
		wantError string
	}{
		{
			name:      "bad meta",
			badFile:   "meta",
			badBytes:  []byte("not-msgpack"),
			wantError: "msgpack",
		},
		{
			name: "missing centroids",
			files: map[string]any{
				"meta":      ivfpqMetaSnapshot{FormatVersion: ivfpqBundleFormatVersion},
				"codebooks": ivfpqCodebooksSnapshot{},
				"lists":     ivfpqListsSnapshot{},
			},
			wantError: "missing centroids",
		},
		{
			name: "bad codebooks",
			files: map[string]any{
				"meta":      ivfpqMetaSnapshot{FormatVersion: ivfpqBundleFormatVersion},
				"centroids": [][]float32{},
				"lists":     ivfpqListsSnapshot{},
			},
			badFile:   "codebooks",
			badBytes:  []byte("not-msgpack"),
			wantError: "msgpack",
		},
		{
			name: "bad lists",
			files: map[string]any{
				"meta":      ivfpqMetaSnapshot{FormatVersion: ivfpqBundleFormatVersion},
				"centroids": [][]float32{},
				"codebooks": ivfpqCodebooksSnapshot{},
			},
			badFile:   "lists",
			badBytes:  []byte("not-msgpack"),
			wantError: "msgpack",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "hnsw")
			dir := ivfpqBundleDir(base)
			require.NoError(t, os.MkdirAll(dir, 0o755))
			if len(tc.files) > 0 {
				require.NoError(t, writeMsgpackSnapshots(dir, tc.files))
			}
			if tc.badFile != "" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, tc.badFile), tc.badBytes, 0o644))
			}
			idx, err := LoadIVFPQBundle(base)
			require.Error(t, err)
			require.Nil(t, idx)
			if tc.wantError == "missing centroids" {
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.Contains(t, err.Error(), tc.wantError)
			}
		})
	}
}

func TestIVFPQPersist_ServiceSuccessAndCacheBranches(t *testing.T) {
	profile := IVFPQProfile{Dimensions: 2, IVFLists: 2, PQSegments: 1, PQBits: 1, NProbe: 1, RerankTopK: 2}
	idx := tinyIVFPQIndexForTest(profile)
	idx.overflow = []ivfpqOverflowVector{{ID: "outlier", Vector: []float32{1, 0}}}

	base := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFPQBundle(base, idx))
	loaded, err := LoadIVFPQBundle(base)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, 1, loaded.Count())
	require.Equal(t, idx.overflow, loaded.overflow)
	require.True(t, loaded.compatibleProfile(profile))

	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	svc.ivfpqIndex = idx
	var nilCtx context.Context
	cached, err := svc.getOrBuildIVFPQIndex(nilCtx, profile, nil, nil)
	require.NoError(t, err)
	require.Same(t, idx, cached)

	svc = NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	svc.hnswIndexPath = base
	loadedFromDisk, err := svc.getOrBuildIVFPQIndex(context.Background(), profile, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, loadedFromDisk)
	require.Equal(t, 1, loadedFromDisk.Count())

	svc.persistIVFPQBackground("", base)
	stat, err := os.Stat(ivfpqBundleDir(base))
	require.NoError(t, err)
	require.True(t, stat.IsDir())
}

func TestIVFPQPersistRestoresLiveMutationOverlay(t *testing.T) {
	profile := IVFPQProfile{Dimensions: 2, IVFLists: 2, PQSegments: 1, PQBits: 1, NProbe: 1, RerankTopK: 2}
	idx := tinyIVFPQIndexForTest(profile)
	overlay := newANNMutationOverlay()
	overlay.Add("fresh", []float32{1, 0})
	overlay.Remove("removed")
	base := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, saveIVFPQBundle(base, idx, overlay, nil))

	loaded, mutations, err := loadIVFPQBundle(base)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	restored := newANNMutationOverlay()
	restored.restore(mutations)
	require.Equal(t, []string{"fresh"}, candidateIDs(restored.Search(context.Background(), []float32{1, 0}, 10, -1)))
	require.True(t, restored.IsRemoved("removed"))
}

func TestIVFPQPersistedMutationsDetectVectorStoreChanges(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vectors")
	store, err := NewVectorFileStore(base, 2)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.Add("base", []float32{1, 0}))

	snapshot := newANNMutationOverlay().snapshot()
	snapshot.VectorStoreCount, snapshot.VectorStoreSlots = store.stateVersion()
	require.True(t, snapshot.matchesVectorStore(store))

	require.NoError(t, store.Add("fresh", []float32{0, 1}))
	require.False(t, snapshot.matchesVectorStore(store))
}
