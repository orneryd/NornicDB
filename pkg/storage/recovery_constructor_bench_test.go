package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// BenchmarkRecoveryConstructors measures the public recovery operation itself.
// Fixture creation, result validation and closing the recovered store are outside
// the timer. The same benchmark source is used on the base and corrected trees.
func BenchmarkRecoveryConstructors(b *testing.B) {
	for _, count := range []int{0, 64} {
		b.Run(fmt.Sprintf("nodes_%d", count), func(b *testing.B) {
			dir := b.TempDir()
			snapshot := filepath.Join(dir, "snapshot.json")
			data := &Snapshot{Sequence: 1, Version: "1.0"}
			for index := 0; index < count; index++ {
				data.Nodes = append(data.Nodes, &Node{ID: NodeID(fmt.Sprintf("n%d", index)), Labels: []string{"Fixture"}, Properties: map[string]any{"value": index}})
			}
			if err := SaveSnapshot(data, snapshot); err != nil {
				b.Fatal(err)
			}
			for _, api := range []string{"plain", "logger", "with_result", "transactions"} {
				b.Run(api, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						var engine *MemoryEngine
						var err error
						switch api {
						case "plain":
							engine, err = fixtureowner.OwnResult[*MemoryEngine](b, ErrStorageClosed)(RecoverFromWAL(filepath.Join(dir, "wal"), snapshot))
						case "logger":
							engine, err = fixtureowner.OwnResult[*MemoryEngine](b, ErrStorageClosed)(RecoverFromWALWithLogger(filepath.Join(dir, "wal"), snapshot, nil))
						case "with_result":
							engine, _, err = fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](b, ErrStorageClosed)(RecoverFromWALWithResult(filepath.Join(dir, "wal"), snapshot))
						case "transactions":
							engine, _, err = fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](b, ErrStorageClosed)(RecoverWithTransactions(filepath.Join(dir, "wal"), snapshot))
						}
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						actual, err := engine.NodeCount()
						if err != nil || actual != int64(count) {
							b.Fatalf("recovered nodes=%d want=%d err=%v", actual, count, err)
						}
						if err := engine.Close(); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
				})
			}
		})
	}
}
