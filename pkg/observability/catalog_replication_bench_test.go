package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotReplication measures the MET-25 alloc budget for the
// Replication bag's hot-path observation. Per-subsystem suffix matches the
// established BenchmarkObserve_Hot{Cache,HTTP,Bolt,Cypher,Storage,MVCC,
// Embed,Search} cadence.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5):
//
//   - replication_lag_bytes_set: ≤ 2 allocs/op (per-peer GaugeVec — the
//     replicator rebinds via WithLabelValues every observation per
//     Pitfall 3 contract; the alloc budget reflects that re-bind cost).
//   - replication_apply_duration_bound: ≤ 2 allocs/op (pre-bound
//     LatencyHistogram observer cached in replicatorMetrics.applyDur per
//     MET-25 — single Bind() at metrics injection).
//   - replication_role_set: ≤ 2 allocs/op (per-cluster Gauge with no
//     labels — fastest path).
//   - replication_leader_changes_inc: ≤ 2 allocs/op (per-cluster Counter
//     with no labels).
func BenchmarkObserve_HotReplication(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewReplicationMetrics(reg, "raft", false)

	b.Run("replication_lag_bytes_set", func(b *testing.B) {
		// Pitfall 3 contract: WithLabelValues every observation. The
		// per-peer label is stable (PeerConfig.ID), so subsequent calls
		// hit the per-label cache in client_golang.
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bag.LagBytes.WithLabelValues("peer-1").Set(1024)
		}
	})

	b.Run("replication_apply_duration_bound", func(b *testing.B) {
		// MET-25 pre-bound observer. NeverSample default bypasses exemplar.
		bound := bag.ApplyDuration.Bind()
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("replication_role_set", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bag.Role.Set(2) // RoleEnum("leader")
		}
	})

	b.Run("replication_leader_changes_inc", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bag.LeaderChangesTotal.Inc()
		}
	})
}
