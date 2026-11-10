package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConcurrentMergeWritersConverge races identical MERGE statements on a
// uniquely constrained key through the database's executor, in every
// storage mode, auto-commit and in explicit transactions (NOR-117). As in
// Neo4j 5.26, every statement succeeds and one node exists, including those
// that write the node they match; before, a later writer's commit failed
// with a UNIQUE violation. The mode setup, per-writer statement execution and
// failure collection are owned by openWriteRaceDB, runWriteRaceStatement and
// writeRaceFailures (entity_write_race_test.go); this test owns the MERGE
// convergence contract.
func TestConcurrentMergeWritersConverge(t *testing.T) {
	shapes := []string{
		"MERGE (n:Doc {id: $k})",
		"MERGE (n:Doc {id: $k}) RETURN n.id",
		"MERGE (n:Doc {id: $k}) ON CREATE SET n.c = 1 ON MATCH SET n.m = 1",
		"MERGE (n:Doc {id: $k}) SET n.t = randomUUID()",
		"MERGE (n:Doc {id: $k}) ON MATCH SET n.t = randomUUID()",
		"MERGE (n:Doc {id: $k}) SET n += $p",
		"UNWIND [$k] AS k MERGE (n:Doc {id: k}) ON MATCH SET n.m = 1",
	}
	for _, mode := range writeRaceModes() {
		t.Run(mode, func(t *testing.T) {
			db := openWriteRaceDB(t, mode)
			defer db.Close()
			ctx := context.Background()
			_, err := db.ExecuteCypher(ctx, "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
			require.NoError(t, err)
			var failures writeRaceFailures
			for s, shape := range shapes {
				for _, explicit := range []bool{false, true} {
					for _, workers := range []int{2, 8} {
						for round := 0; round < 8; round++ {
							key := fmt.Sprintf("%d-%v-%d-%d", s, explicit, workers, round)
							var start, done sync.WaitGroup
							start.Add(1)
							for w := 0; w < workers; w++ {
								done.Add(1)
								go func() {
									defer done.Done()
									start.Wait()
									params := map[string]interface{}{"k": key, "p": map[string]interface{}{"w": int64(w)}}
									if err := runWriteRaceStatement(ctx, db, explicit, shape, params); err != nil {
										failures.addf("%s explicit=%v workers=%d: %v", shape, explicit, workers, err)
									}
								}()
							}
							start.Done()
							done.Wait()
							result, err := db.ExecuteCypher(ctx, "MATCH (n:Doc {id: $k}) RETURN count(n)", map[string]interface{}{"k": key})
							require.NoError(t, err)
							if result.Rows[0][0] != int64(1) {
								failures.addf("%s explicit=%v workers=%d: %v nodes", shape, explicit, workers, result.Rows[0][0])
							}
						}
					}
				}
			}
			failures.requireEmpty(t)
		})
	}
}
