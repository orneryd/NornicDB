package storage

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// NOR-210 reproduction-only audit benchmark at d6ec7766 (backend v1.5.1).
//
// Two databases are warm over the actual Badger storage owner. The named
// comparison is the current warm-cache peer read (control) against the cost of
// an unrelated database DROP and the first peer relationship-type read that
// follows it (the eviction cost this audit measured). These names describe the
// compared current workloads; they are not a before/after fix comparison.
// Sizes are 64, 256 and 1024 relationships per database. No timing threshold is
// asserted.

func newDropRelationshipBenchEngine(b *testing.B) *BadgerEngine {
	b.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatalf("open in-memory Badger engine: %v", err)
	}
	b.Cleanup(func() {
		if err := engine.Close(); err != nil {
			b.Errorf("close benchmark engine: %v", err)
		}
	})
	return engine
}

// seedAndWarmDropRelationshipBench writes one victim generation and warms both
// databases' relationship-type entries (two entries), so the next DROP is the
// only invalidator.
func seedAndWarmDropRelationshipBench(b *testing.B, engine *BadgerEngine, edges int) {
	b.Helper()
	seedRelationshipType(b, engine, "victim", "VICTIM_REL", edges)
	warmRelationshipType(b, NewNamespacedEngine(engine, "victim"), "VICTIM_REL", edges)
	warmRelationshipType(b, NewNamespacedEngine(engine, "peer"), "PEER_REL", edges)
}

// BenchmarkRelationshipTypePeerReadWarmTwoDatabaseControl measures the warm
// control: the unrelated database's relationship-type read while both
// relationship-type entries remain cached.
func BenchmarkRelationshipTypePeerReadWarmTwoDatabaseControl(b *testing.B) {
	for _, edges := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("edges=%d", edges), func(b *testing.B) {
			engine := newDropRelationshipBenchEngine(b)
			ctx := context.Background()
			seedRelationshipType(b, engine, "victim", "VICTIM_REL", edges)
			seedRelationshipType(b, engine, "peer", "PEER_REL", edges)
			peerView := NewNamespacedEngine(engine, "peer")

			b.StopTimer()
			warmRelationshipType(b, NewNamespacedEngine(engine, "victim"), "VICTIM_REL", edges)
			warmRelationshipType(b, peerView, "PEER_REL", edges)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				got, err := peerView.GetEdgesByType(ctx, "PEER_REL")
				if err != nil || len(got) != edges {
					b.Fatalf("warm peer read: err=%v rows=%d want=%d", err, len(got), edges)
				}
			}
		})
	}
}

// BenchmarkRelationshipTypePeerReadUnrelatedDropCost measures the measured cost
// of an unrelated database DROP plus the first peer relationship-type read that
// follows it. The DROP evicts both warm entries, so that read is the cold refill
// this audit reports.
func BenchmarkRelationshipTypePeerReadUnrelatedDropCost(b *testing.B) {
	for _, edges := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("edges=%d", edges), func(b *testing.B) {
			engine := newDropRelationshipBenchEngine(b)
			ctx := context.Background()
			seedRelationshipType(b, engine, "peer", "PEER_REL", edges)
			peerView := NewNamespacedEngine(engine, "peer")

			b.StopTimer()
			seedAndWarmDropRelationshipBench(b, engine, edges)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := engine.DeleteByPrefix("victim:"); err != nil {
					b.Fatalf("unrelated DROP: %v", err)
				}
				got, err := peerView.GetEdgesByType(ctx, "PEER_REL")
				if err != nil || len(got) != edges {
					b.Fatalf("peer read after unrelated DROP: err=%v rows=%d want=%d", err, len(got), edges)
				}
				b.StopTimer()
				seedAndWarmDropRelationshipBench(b, engine, edges)
				b.StartTimer()
			}
		})
	}
}

// BenchmarkRelationshipTypeUnrelatedDropOnlyCost attributes the same DROP cost
// without the following peer read, so the read portion can be separated from
// the destructive work.
func BenchmarkRelationshipTypeUnrelatedDropOnlyCost(b *testing.B) {
	for _, edges := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("edges=%d", edges), func(b *testing.B) {
			engine := newDropRelationshipBenchEngine(b)
			seedRelationshipType(b, engine, "peer", "PEER_REL", edges)

			b.StopTimer()
			seedAndWarmDropRelationshipBench(b, engine, edges)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := engine.DeleteByPrefix("victim:"); err != nil {
					b.Fatalf("unrelated DROP: %v", err)
				}
				b.StopTimer()
				seedAndWarmDropRelationshipBench(b, engine, edges)
				b.StartTimer()
			}
		})
	}
}

// BenchmarkRelationshipTypePeerReadAfterUnrelatedDrop isolates only the first
// peer read after the real unrelated DROP. DROP and generation setup are outside
// the measured interval; process-wide CPU profiles still include that setup.
func BenchmarkRelationshipTypePeerReadAfterUnrelatedDrop(b *testing.B) {
	for _, edges := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("edges=%d", edges), func(b *testing.B) {
			b.StopTimer()
			engine := newDropRelationshipBenchEngine(b)
			seedRelationshipType(b, engine, "peer", "PEER_REL", edges)
			peer := NewNamespacedEngine(engine, "peer")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seedAndWarmDropRelationshipBench(b, engine, edges)
				if _, _, err := engine.DeleteByPrefix("victim:"); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				rows, err := peer.GetEdgesByType(context.Background(), "PEER_REL")
				b.StopTimer()
				if err != nil || len(rows) != edges {
					b.Fatalf("peer read: rows=%d error=%v", len(rows), err)
				}
			}
		})
	}
}
