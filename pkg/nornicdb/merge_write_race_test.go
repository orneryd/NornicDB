package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// TestConcurrentMergeWritersConverge races identical MERGE statements on a
// uniquely constrained key through the database's executor, in every
// storage mode, auto-commit and in explicit transactions (NOR-117). As in
// Neo4j 5.26, every statement succeeds and one node exists, including those
// that write the node they match; before, a later writer's commit failed
// with a UNIQUE violation.
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
	for _, mode := range []string{"memory", "durable", "async"} {
		t.Run(mode, func(t *testing.T) {
			config := DefaultConfig()
			config.Memory.DecayEnabled = false
			config.Memory.AutoLinksEnabled = false
			config.Database.AsyncWritesEnabled = mode == "async"
			dir := t.TempDir()
			if mode == "memory" {
				dir = ""
			}
			db, err := Open(dir, config)
			require.NoError(t, err)
			defer db.Close()
			ctx := context.Background()
			_, err = db.ExecuteCypher(ctx, "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
			require.NoError(t, err)
			var failures []string
			var mu sync.Mutex
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
									var err error
									if !explicit {
										_, err = db.ExecuteCypher(ctx, shape, params)
									} else {
										session := cypher.NewStorageExecutor(db.GetStorage())
										if _, err = session.Execute(ctx, "BEGIN", nil); err == nil {
											if _, err = session.Execute(ctx, shape, params); err != nil {
												_, _ = session.Execute(ctx, "ROLLBACK", nil)
											} else {
												_, err = session.Execute(ctx, "COMMIT", nil)
											}
										}
									}
									if err != nil {
										mu.Lock()
										failures = append(failures, fmt.Sprintf("%s explicit=%v workers=%d: %v", shape, explicit, workers, err))
										mu.Unlock()
									}
								}()
							}
							start.Done()
							done.Wait()
							result, err := db.ExecuteCypher(ctx, "MATCH (n:Doc {id: $k}) RETURN count(n)", map[string]interface{}{"k": key})
							require.NoError(t, err)
							if result.Rows[0][0] != int64(1) {
								failures = append(failures, fmt.Sprintf("%s explicit=%v workers=%d: %v nodes", shape, explicit, workers, result.Rows[0][0]))
							}
						}
					}
				}
			}
			require.Empty(t, failures)
		})
	}
}
