package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompressedIndexIsRetiredWithItsVectorSource(t *testing.T) {
	ctx := context.Background()
	profile := IVFPQProfile{Dimensions: 2, IVFLists: 2, PQSegments: 1, PQBits: 1, NProbe: 1, RerankTopK: 2}
	base := filepath.Join(t.TempDir(), "hnsw")
	bundleDir := ivfpqBundleDir(base)
	svc := NewServiceWithDimensions(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.hnswIndexPath = base

	// A query loads the saved compressed index for the current population A.
	stale := tinyIVFPQIndexForTest(profile)
	require.NoError(t, SaveIVFPQBundle(base, stale))
	epochA := svc.vectorSourceEpoch.Load()
	loaded, err := svc.getOrBuildIVFPQIndex(ctx, profile, nil, epochA)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	svc.ivfpqMu.RLock()
	require.Same(t, loaded, svc.ivfpqIndex)
	svc.ivfpqMu.RUnlock()

	// The base build replaces the source with population B.
	require.NoError(t, svc.resetBaseVectorSource())
	epochB := svc.vectorSourceEpoch.Load()
	require.NotEqual(t, epochA, epochB)
	_, statErr := os.Stat(bundleDir)
	require.True(t, os.IsNotExist(statErr), "the saved compressed bundle for A survived source replacement: %v", statErr)

	// Profile compatibility alone must not reuse A's index for B.
	reused, err := svc.getOrBuildIVFPQIndex(ctx, profile, nil, epochB)
	require.Error(t, err, "no source to build B from, and A's index must not be reused")
	require.Nil(t, reused)

	// A reader that captured A's epoch before replacement gets its own answer,
	// but the index it loaded is not published for later readers.
	require.NoError(t, SaveIVFPQBundle(base, stale))
	svc.ivfpqMu.Lock()
	svc.ivfpqIndex = nil
	svc.ivfpqMu.Unlock()
	late, err := svc.getOrBuildIVFPQIndex(ctx, profile, nil, epochA)
	require.NoError(t, err)
	require.NotNil(t, late)
	svc.ivfpqMu.RLock()
	published := svc.ivfpqIndex
	svc.ivfpqMu.RUnlock()
	require.Nil(t, published, "a load for a retired source was published")

	// Background persistence does not save a retired index under B's version.
	require.NoError(t, os.RemoveAll(bundleDir))
	svc.ivfpqMu.Lock()
	svc.ivfpqIndex, svc.ivfpqEpoch = stale, epochA
	svc.ivfpqMu.Unlock()
	svc.persistIVFPQBackground("", base)
	_, statErr = os.Stat(bundleDir)
	require.True(t, os.IsNotExist(statErr), "a retired compressed index was persisted: %v", statErr)
}
