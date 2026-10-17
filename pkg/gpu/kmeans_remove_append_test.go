package gpu

import (
	"context"
	"testing"
	"time"
)

// An append racing a cluster removal must not change which slot the removal
// repairs: every surviving vector stays in exactly one cluster, its own group.
// The hook runs the append inside Remove; it may only land after Remove has
// finished its membership repair.
func TestClusterRemovalRacingAppendKeepsMembership(t *testing.T) {
	ci := twoGroupClusterIndex(t)
	if err := ci.ClusterWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	appended := make(chan struct{})
	clusterRemoveHook = func() {
		go func() {
			_ = ci.Add("late", []float32{0.02, 1})
			close(appended)
		}()
		select {
		case <-appended:
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Cleanup(func() { clusterRemoveHook = nil })
	if !ci.Remove("a0") {
		t.Fatal("Remove(a0) = false")
	}
	<-appended

	requireAbsent(t, ci, "a0")
	requireSameCluster(t, ci, "a1", "a2", "a3")
	requireSameCluster(t, ci, "b0", "b1", "b2", "b3")
	if _, count := clusterOf(ci, "late"); count > 1 {
		t.Fatalf("late is listed in %d clusters", count)
	}
}
