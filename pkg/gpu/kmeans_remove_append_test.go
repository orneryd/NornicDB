package gpu

import (
	"context"
	"sync"
	"testing"
	"time"
)

// An append racing a cluster removal must not change which slot the removal
// repairs: every surviving vector stays in exactly one cluster, its own group.
func TestClusterRemovalRacingAppendKeepsMembership(t *testing.T) {
	for round := 0; round < 200; round++ {
		ci := twoGroupClusterIndex(t)
		if err := ci.ClusterWithContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		// Hold readers so the removal and the append both queue on mu; the old
		// removal sampled its slots under this read lock before the append ran.
		ci.mu.RLock()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); ci.Remove("a0") }()
		time.Sleep(time.Millisecond)
		go func() { defer wg.Done(); _ = ci.Add("late", []float32{0.02, 1}) }()
		time.Sleep(time.Millisecond)
		ci.mu.RUnlock()
		wg.Wait()

		requireAbsent(t, ci, "a0")
		requireSameCluster(t, ci, "a1", "a2", "a3")
		requireSameCluster(t, ci, "b0", "b1", "b2", "b3")
		if _, count := clusterOf(ci, "late"); count > 1 {
			t.Fatalf("round %d: late is listed in %d clusters", round, count)
		}
		ci.mu.RLock()
		if len(ci.assignments) > len(ci.nodeIDs) {
			t.Fatalf("round %d: %d assignments for %d vectors", round, len(ci.assignments), len(ci.nodeIDs))
		}
		ci.mu.RUnlock()
	}
}
