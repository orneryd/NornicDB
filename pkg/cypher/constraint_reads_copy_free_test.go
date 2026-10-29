package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Statements read installed constraints without copying them. A label change
// checks relationship policies and an UNWIND MERGE plan looks for a unique
// anchor; with 40 more constraints installed on other labels, each with a
// property list, neither allocates more than it does without them. A copy
// per constraint would add at least 40 allocations to each.
func TestHotStatementsCopyNoConstraints(t *testing.T) {
	ctx := context.Background()
	measure := func(extra int) (labelChange, merge float64) {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
		run := func(query string) {
			_, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}
		run("CREATE CONSTRAINT person_id FOR (n:Person) REQUIRE n.id IS UNIQUE")
		run("CREATE CONSTRAINT link_policy FOR (:Person)-[r:LINK]->(:Other) REQUIRE ALLOWED")
		for index := 0; index < extra; index++ {
			run(fmt.Sprintf("CREATE CONSTRAINT unrelated_%d FOR (n:Unrelated%d) REQUIRE (n.a, n.b) IS NODE KEY", index, index))
		}
		run("CREATE (:Person {id: 1})")
		labelChange = testing.AllocsPerRun(20, func() {
			run("MATCH (n:Person {id: 1}) SET n:Tagged")
			run("MATCH (n:Person {id: 1}) REMOVE n:Tagged")
		})
		merge = testing.AllocsPerRun(20, func() {
			run("UNWIND [{id: 1, v: 2}] AS row MERGE (n:Person {id: row.id}) SET n.v = row.v")
		})
		return labelChange, merge
	}
	fewLabel, fewMerge := measure(0)
	manyLabel, manyMerge := measure(40)
	t.Logf("label change allocations %.0f -> %.0f; UNWIND MERGE allocations %.0f -> %.0f", fewLabel, manyLabel, fewMerge, manyMerge)
	require.Less(t, manyLabel-fewLabel, float64(10), "a label change copied installed constraints")
	require.Less(t, manyMerge-fewMerge, float64(10), "an UNWIND MERGE plan copied installed constraints")
}
