package gpu

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Both operations are supported public methods, with ordinary concurrent calls.
func TestClusterIndex_CentroidBatchVersusClusterCompletes(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(64), &KMeansConfig{NumClusters: 2, MaxIterations: 2})
	ids, vectors := generateTestEmbeddings(32, 64)
	for i, v := range vectors {
		require.NoError(t, ci.Add(ids[i], v))
	}
	require.NoError(t, ci.Cluster())
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		for i := 0; i < 2000; i++ {
			if err := ci.OnNodeUpdate(ids[0], vectors[i%len(vectors)]); err != nil {
				done <- err
				return
			}
			ci.UpdateCentroidsBatch()
		}
		done <- nil
	}()
	go func() {
		<-start
		for i := 0; i < 300; i++ {
			if err := ci.Cluster(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	close(start)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-timer.C:
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("ordinary Cluster/UpdateCentroidsBatch calls deadlocked:\n%s", buf[:n])
		}
	}
}
