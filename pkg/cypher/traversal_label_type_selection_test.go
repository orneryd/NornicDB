package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const labelSeededTypedStep = "MATCH (d:PDDocument)-[:MEMBER_OF]->(c) RETURN d.id AS id, c.archived AS archived"

func TestTraversalLabelScanKeepsTheRequestedRelationshipTypes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newNOR160TypedAdjacencyFixture(t, nor160TypedAdjacencyPopulation{documents: 2, unrelatedFanout: 8})
		fixture.spy.reset()
		result, err := fixture.exec.Execute(t.Context(), labelSeededTypedStep, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, fixture.expectedRows(), result.Rows)
		require.Equal(t, 2, fixture.spy.calls)
		require.Equal(t, 2, fixture.spy.entries, "the label-seeded one-hop plan must not enumerate the sixteen unrelated relationships")
	})
}

func BenchmarkLabelSeededTypedStep(b *testing.B) {
	for _, fanout := range []int{8, 64} {
		b.Run(fmt.Sprintf("unrelated=%d", fanout), func(b *testing.B) {
			fixture := newNOR160TypedAdjacencyFixture(b, nor160TypedAdjacencyPopulation{documents: 2, unrelatedFanout: fanout})
			b.ReportAllocs()
			b.ResetTimer()
			var result *ExecuteResult
			for i := 0; i < b.N; i++ {
				var err error
				result, err = fixture.exec.Execute(b.Context(), labelSeededTypedStep, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			require.ElementsMatch(b, fixture.expectedRows(), result.Rows)
		})
	}
}
