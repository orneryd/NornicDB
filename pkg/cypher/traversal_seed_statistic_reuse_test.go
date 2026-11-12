package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// One MATCH operator sees the same label/type populations for every UNWIND
// input. Statistics guide orientation only; they never substitute for rows.
func TestTraversalSeedStatisticsAreReusedAcrossInputRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) {
				fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
					collections: 1, documents: 12, unrelatedFanout: 64,
				})
				counter := &seedCountStoreSpy{Engine: fixture.spy}
				exec := NewStorageExecutor(counter, QueryCachePolicy{MaxEntries: 0})
				if explicit {
					_, err := exec.Execute(t.Context(), "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, err := exec.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
				}
				query := "UNWIND $ids AS each WITH DISTINCT each MATCH (d:PDDocument {id: each})-[:MEMBER_OF]->(:PDCollection {id: 'col-0'}) RETURN d.id AS id"
				for repeat := 0; repeat < 2; repeat++ {
					counter.requests = nil
					result, err := exec.Execute(t.Context(), query, fixture.idParams())
					require.NoError(t, err)
					require.Equal(t, seedRowKeys(traversalSeedReuseRows(fixture.documents)), seedRowKeys(result.Rows))
					require.NotEmpty(t, counter.requests, "each execution reads its own statistics")
					for _, want := range []storage.CountStoreReadRequest{
						{Kind: storage.CountStoreNodesByLabel, Name: "PDDocument"},
						{Kind: storage.CountStoreNodesByLabel, Name: "PDCollection"},
						{Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument", Types: []string{"MEMBER_OF"}},
						{Kind: storage.CountStoreRelationshipsByEndLabel, Name: "PDCollection", Types: []string{"MEMBER_OF"}},
					} {
						if explicit {
							want.Namespace = "seed_cost"
						}
						requireSeedRequestOnce(t, counter.requests, want)
					}
					require.LessOrEqual(t, len(counter.requests), 4,
						"the same node/typed-endpoint populations must not be reread per input row: %v", counter.requests)
				}
			})
		}
	})
}
