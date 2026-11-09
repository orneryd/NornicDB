package storage

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// Cost of the prune's reader protection: each batch snapshots the registered
// readers once (mvccPruneReaderFloors), and each candidate key decides which
// records they see (keepFrom). Compare with a batch's commits, about 64 keys
// at tens of microseconds each.

import (
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

type benchReaderRegistry struct{ readers []SnapshotReaderInfo }

func (r *benchReaderRegistry) Register(SnapshotReaderInfo) (string, func()) { return "", func() {} }

func (r *benchReaderRegistry) ActiveCount() int64 { return int64(len(r.readers)) }

// Snapshot copies, as the lifecycle registry does.
func (r *benchReaderRegistry) Snapshot() []SnapshotReaderInfo {
	return append([]SnapshotReaderInfo(nil), r.readers...)
}

type benchReaderController struct {
	*testLifecycleController
	registry SnapshotReaderRegistry
}

func (c *benchReaderController) ReaderRegistry() SnapshotReaderRegistry { return c.registry }

func newReaderCostEngine(b *testing.B, readers int) (*BadgerEngine, []byte, []MVCCVersion) {
	b.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	if _, err := engine.CreateNode(b.Context(), &Node{ID: "ns0:n"}); err != nil {
		b.Fatal(err)
	}
	num, ok := engine.idDict.lookupNodeNumID("ns0:n")
	if !ok {
		b.Fatal("no numeric ID")
	}
	logical := append([]byte{prefixMVCCNode}, encodeNumID(num)...)
	start := time.Unix(1_800_000_000, 0).UTC()
	records := make([]MVCCVersion, 8)
	for i := range records {
		records[i] = MVCCVersion{CommitTimestamp: start.Add(time.Duration(i+10) * time.Second), CommitSequence: uint64(i + 10)}
	}
	infos := make([]SnapshotReaderInfo, readers)
	for i := range infos {
		infos[i] = SnapshotReaderInfo{
			ReaderID:        fmt.Sprintf("r%d", i),
			Namespace:       fmt.Sprintf("ns%d", i%10),
			SnapshotVersion: MVCCVersion{CommitTimestamp: start.Add(time.Duration(i%20) * time.Second), CommitSequence: uint64(i)},
		}
	}
	engine.SetLifecycleController(&benchReaderController{
		testLifecycleController: &testLifecycleController{registry: &testSnapshotRegistry{}},
		registry:                &benchReaderRegistry{readers: infos},
	})
	return engine, logical, records
}

// BenchmarkMVCCPruneReaderFloors is the once-per-batch reader snapshot.
func BenchmarkMVCCPruneReaderFloors(b *testing.B) {
	for _, readers := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			engine, _, _ := newReaderCostEngine(b, readers)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if floors := engine.mvccPruneReaderFloors(); !floors.any {
					b.Fatal("no readers")
				}
			}
		})
	}
}

// BenchmarkMVCCPruneKeepFrom is the per-candidate-key decision.
func BenchmarkMVCCPruneKeepFrom(b *testing.B) {
	for _, readers := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			engine, logical, records := newReaderCostEngine(b, readers)
			floors := engine.mvccPruneReaderFloors()
			head := records[len(records)-1]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, applies := floors.keepFrom(engine, logical, records, head); !applies {
					b.Fatal("no reader applies")
				}
			}
		})
	}
}
