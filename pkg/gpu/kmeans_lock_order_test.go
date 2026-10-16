package gpu

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Remove and UpdateCentroidsBatch take clusterMu and the embedding index's mu
// in one order, so running them together, with centroid updates queued, never
// deadlocks. (Remove used to hold clusterMu while waiting for mu, and the
// batch held mu while waiting for clusterMu.)
func TestClusterRemoveAndCentroidBatchDoNotDeadlock(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for round := 0; round < 400; round++ {
			ci := twoGroupClusterIndex(t)
			if err := ci.ClusterWithContext(context.Background()); err != nil {
				t.Error(err)
				return
			}
			ci.clusterMu.Lock()
			for i := 0; i < 8; i++ {
				ci.pendingUpdates = append(ci.pendingUpdates, nodeUpdate{idx: i, oldCluster: 0, newCluster: 1})
			}
			ci.clusterMu.Unlock()
			finished := make(chan struct{}, 2)
			go func() { ci.UpdateCentroidsBatch(); finished <- struct{}{} }()
			go func() { ci.Remove(fmt.Sprintf("a%d", round%4)); finished <- struct{}{} }()
			<-finished
			<-finished
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("Remove and UpdateCentroidsBatch deadlocked")
	}
}
