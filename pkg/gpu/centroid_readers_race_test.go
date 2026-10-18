package gpu

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Centroid readers (GetCentroids, FindNearestCentroid, AssignToClustering)
// read the published clustering's coordinates under its lock, the lock
// UpdateCentroidsBatch holds while it rewrites them. Run with -race.
func TestCentroidReadersHoldTheClusteringLockDuringUpdates(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(64), &KMeansConfig{NumClusters: 2, MaxIterations: 2})
	ids, vectors := generateTestEmbeddings(32, 64)
	for i, v := range vectors {
		require.NoError(t, ci.Add(ids[i], v))
	}
	require.Nil(t, ci.GetCentroids(), "an unclustered index has no centroids")
	require.Equal(t, -1, ci.FindNearestCentroid(vectors[0]))
	require.Nil(t, (&Clustering{}).Centroids())
	require.NoError(t, ci.Cluster())

	const iterations = 2000
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			centroids := ci.GetCentroids()
			if len(centroids) != 2 || len(centroids[0]) != 64 {
				t.Errorf("GetCentroids returned %d centroids", len(centroids))
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if c := ci.FindNearestCentroid(vectors[0]); c < 0 || c > 1 {
				t.Errorf("FindNearestCentroid returned %d", c)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if _, _, _, ok := ci.AssignToClustering(ci.PublishedClustering(), ids[1]); !ok {
				t.Error("AssignToClustering found no embedding")
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if err := ci.OnNodeUpdate(ids[0], vectors[i%len(vectors)]); err != nil {
				t.Error(err)
				return
			}
			ci.UpdateCentroidsBatch()
		}
	}()
	close(start)
	wg.Wait()
}
