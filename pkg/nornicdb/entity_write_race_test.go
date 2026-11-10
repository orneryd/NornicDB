package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// TestConcurrentEntityWritersSerialize races writers of one existing node
// and of one existing relationship through the database's executor, in every
// storage mode, auto-commit and in explicit transactions (NOR-213). As in
// Neo4j, each writer waits for the one before it and applies its write on top
// of that commit: every statement succeeds and no increment is lost. Before,
// a later writer's commit failed with a transaction conflict.
func TestConcurrentEntityWritersSerialize(t *testing.T) {
	shapes := []string{
		"MATCH (n:Doc {id: $k}) SET n.c = n.c + 1",
		"MATCH (n:Doc {id: $k}) SET n += {c: n.c + 1}",
		"MATCH (:Doc {id: $k})-[r:HAS]->() SET r.c = r.c + 1",
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
			var failures []string
			var mu sync.Mutex
			for s, shape := range shapes {
				for _, explicit := range []bool{false, true} {
					const workers, rounds = 8, 6
					key := fmt.Sprintf("%d-%v", s, explicit)
					params := map[string]interface{}{"k": key}
					_, err := db.ExecuteCypher(ctx, "CREATE (:Doc {id: $k, c: 0})-[:HAS {c: 0}]->(:Part)", params)
					require.NoError(t, err)
					var start, done sync.WaitGroup
					start.Add(1)
					for w := 0; w < workers; w++ {
						done.Add(1)
						go func() {
							defer done.Done()
							start.Wait()
							for round := 0; round < rounds; round++ {
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
									failures = append(failures, fmt.Sprintf("%s explicit=%v: %v", shape, explicit, err))
									mu.Unlock()
								}
							}
						}()
					}
					start.Done()
					done.Wait()
					result, err := db.ExecuteCypher(ctx, "MATCH (n:Doc {id: $k})-[r:HAS]->() RETURN n.c + r.c", params)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(workers * rounds)}}, result.Rows, "%s explicit=%v: %v", shape, explicit, failures)
				}
			}
			require.Empty(t, failures)
		})
	}
}
