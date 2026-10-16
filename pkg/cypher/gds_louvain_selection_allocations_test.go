package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var gdsLouvainAllocationSelection *gdsSubgraph
var gdsLouvainAllocationStream *ExecuteResult

// The catalog graph is prepared outside the measurement. The budget includes
// selection, traversal ordering, optimization and emitted rows, and preserves
// the pre-regression sparse workload allocation bound.
func TestGDSLouvainSingleTypeStreamAllocationBound(t *testing.T) {
	for _, size := range []int{1000, 3000} {
		for _, degree := range []int{8} {
			t.Run(fmt.Sprintf("n%d_d%d", size, degree), func(t *testing.T) {
				gdsFollowUpCatalog(t)
				e, _ := newGDSEngineGraph(t, "independentr3allocation")
				p := newGraphProjection("alloc", e.databaseName())
				p.Labels = []string{"N"}
				p.Types = []string{"R"}
				p.RelationshipPropertyKeys["R"] = []string{"w"}
				for i := 0; i < size; i++ {
					p.NodeIDs = append(p.NodeIDs, fmt.Sprintf("n%06d", i))
				}
				p.indexNodes(true)
				weight := map[string]float64{"w": 1}
				for i := 0; i < size; i++ {
					for j := 1; j <= degree; j++ {
						p.Relationships = append(p.Relationships, ProjectedRelationship{Source: p.NodeIDs[i], Target: p.NodeIDs[(i+j)%size], Type: "R", Properties: weight})
					}
				}
				p.finishCounts()
				require.NoError(t, registerGraphProjection(p, false))
				selectAllocs := func(typed bool) float64 {
					return testing.AllocsPerRun(3, func() {
						var err error
						gdsLouvainAllocationSelection, err = selectGDSSubgraph(p, nil, nil, "w", gdsSubgraphOptions{includeTypes: typed})
						if err != nil {
							t.Fatal(err)
						}
					})
				}
				plain, typed := selectAllocs(false), selectAllocs(true)
				stream := testing.AllocsPerRun(3, func() {
					var err error
					gdsLouvainAllocationStream, err = e.callGdsLouvainStream([]interface{}{"alloc", map[string]interface{}{"concurrency": int64(1), "relationshipWeightProperty": "w", "maxLevels": int64(1), "maxIterations": int64(1), "consecutiveIds": true}})
					if err != nil {
						t.Fatal(err)
					}
				})
				require.Len(t, gdsLouvainAllocationStream.Rows, size)
				budget := float64(41896)
				if size == 1000 {
					budget = 13863
				}
				require.LessOrEqual(t, stream, budget)
				t.Logf("nodes=%d arcs=%d selector_plain_allocs=%.0f selector_typed_allocs=%.0f stream_allocs=%.0f", size, size*degree, plain, typed, stream)
			})
		}
	}
}
