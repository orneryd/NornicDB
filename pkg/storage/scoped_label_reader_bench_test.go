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
		foreignFirst      bool
	}{{"own1000", 1000, 0, false}, {"own1000_foreign50000", 1000, 50000, false}, {"foreign50000_before_own1000", 1000, 50000, true}, {"own1000_sparse", 1, 0, false}} {
		b.Run(fixture.name, func(b *testing.B) {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(b, err)
			b.Cleanup(func() { _ = engine.Close() })
			populations := []struct {
				namespace string
				count     int
			}{{"a", 1000}, {"b", fixture.foreign}}
			if fixture.foreignFirst {
				populations[0], populations[1] = populations[1], populations[0]
			}
			for _, population := range populations {
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
			namespace := NewNamespacedEngine(engine, "a")
			for _, mode := range []string{"slice", "projected", "first", "snapshot_first", "id_first", "absent"} {
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
						case "id_first":
							seen := 0
							require.NoError(b, namespace.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
								seen++
								require.Equal(b, NodeID("n00000"), id)
								return false
							}))
							require.Equal(b, 1, seen)
						case "absent":
							node, err := engine.GetFirstNodeByLabelInScope("a:", "Missing")
							require.NoError(b, err)
							require.Nil(b, node)
						}
					}
				})
			}
		})
	}
}
