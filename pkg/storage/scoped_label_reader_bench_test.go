package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkScopedLabelReaders compares the same label query with independent
// namespace populations, including sparse labels and first-result consumers.
func BenchmarkScopedLabelReaders(b *testing.B) {
	for _, fixture := range []struct {
		name              string
		matching, foreign int
	}{{"own1000", 1000, 0}, {"own1000_foreign50000", 1000, 50000}, {"own1000_sparse", 1, 0}} {
		b.Run(fixture.name, func(b *testing.B) {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(b, err)
			b.Cleanup(func() { _ = engine.Close() })
			for _, population := range []struct {
				namespace string
				count     int
			}{{"a", 1000}, {"b", fixture.foreign}} {
				nodes := make([]*Node, 0, population.count)
				for index := 0; index < population.count; index++ {
					label := "Person"
					if population.namespace == "a" && index >= fixture.matching {
						label = "Other"
					}
					nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("%s:n%05d", population.namespace, index)), Labels: []string{label}, Properties: map[string]any{"age": int64(index)}})
				}
				require.NoError(b, engine.BulkCreateNodes(nodes))
			}
			for _, mode := range []string{"slice", "projected", "first", "snapshot_first"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						switch mode {
						case "slice":
							nodes, err := engine.GetNodesByLabelInScope("a:", "Person")
							require.NoError(b, err)
							require.Len(b, nodes, fixture.matching)
						case "projected":
							seen := 0
							require.NoError(b, engine.StreamNodesByLabelProjectedInScope("a:", "Person", []string{"age"}, func(*Node) error { seen++; return nil }))
							require.Equal(b, fixture.matching, seen)
						case "first":
							node, err := engine.GetFirstNodeByLabelInScope("a:", "Person")
							require.NoError(b, err)
							require.Equal(b, NodeID("a:n00000"), node.ID)
						case "snapshot_first":
							tx, err := engine.BeginTransaction()
							require.NoError(b, err)
							require.NoError(b, tx.SetNamespace("a"))
							err = tx.StreamNodesByLabelProjected("Person", []string{"age"}, func(*Node) error { return ErrIterationStopped })
							require.ErrorIs(b, err, ErrIterationStopped)
							require.NoError(b, tx.Rollback())
						}
					}
				})
			}
		})
	}
}
