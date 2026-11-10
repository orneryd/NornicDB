package nornicdb

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// writeRaceModes are the storage modes the concurrent-write regressions in
// this package race through: the in-memory engine, the durable store, and the
// write-behind (async) store.
func writeRaceModes() []string { return []string{"memory", "durable", "async"} }

// openWriteRaceDB is the one owner of the concurrent-writer database setup:
// decay and auto-links are off so the raced writers are the only writers of
// the raced properties, and write-behind is enabled only for the async mode.
func openWriteRaceDB(t *testing.T, mode string) *DB {
	t.Helper()
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
	return db
}

// runWriteRaceStatement runs one concurrent writer's statement the way that
// writer would: auto-commit through the database's executor, or in its own
// explicit transaction, which is rolled back when the statement fails. It
// returns the writer's error; the caller owns the failure report.
func runWriteRaceStatement(ctx context.Context, db *DB, explicit bool, statement string, params map[string]interface{}) error {
	if !explicit {
		_, err := db.ExecuteCypher(ctx, statement, params)
		return err
	}
	session := cypher.NewStorageExecutor(db.GetStorage())
	_, err := session.Execute(ctx, "BEGIN", nil)
	if err != nil {
		return err
	}
	if _, err = session.Execute(ctx, statement, params); err != nil {
		_, _ = session.Execute(ctx, "ROLLBACK", nil)
		return err
	}
	_, err = session.Execute(ctx, "COMMIT", nil)
	return err
}

// writeRaceFailures collects the concurrent writers' and the outcome checks'
// failures under one lock, so a race reports every failure instead of the
// first one a goroutine happened to reach.
type writeRaceFailures struct {
	mu        sync.Mutex
	collected []string
}

// addf records one failure; it is safe for concurrent use.
func (f *writeRaceFailures) addf(format string, args ...interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.collected = append(f.collected, fmt.Sprintf(format, args...))
}

// all returns the failures recorded so far.
func (f *writeRaceFailures) all() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.collected)
}

// requireEmpty fails the test with every collected failure.
func (f *writeRaceFailures) requireEmpty(t *testing.T) {
	t.Helper()
	require.Empty(t, f.all())
}

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
	for _, mode := range writeRaceModes() {
		t.Run(mode, func(t *testing.T) {
			db := openWriteRaceDB(t, mode)
			defer db.Close()
			ctx := context.Background()
			var failures writeRaceFailures
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
								if err := runWriteRaceStatement(ctx, db, explicit, shape, params); err != nil {
									failures.addf("%s explicit=%v: %v", shape, explicit, err)
								}
							}
						}()
					}
					start.Done()
					done.Wait()
					result, err := db.ExecuteCypher(ctx, "MATCH (n:Doc {id: $k})-[r:HAS]->() RETURN n.c + r.c", params)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(workers * rounds)}}, result.Rows, "%s explicit=%v: %v", shape, explicit, failures.all())
				}
			}
			failures.requireEmpty(t)
		})
	}
}
