package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestConcurrentMergeOnUniqueKeyMatchesNeo4j races two sessions running the
// same MERGE on a uniquely constrained key, as auto-commit statements and
// in explicit transactions. As in Neo4j 5.26, every statement succeeds on
// its first attempt: the second waits for the first, then matches its nodes
// and relationships (#961).
func TestConcurrentMergeOnUniqueKeyMatchesNeo4j(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	setup := NewStorageExecutor(store)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE CONSTRAINT o_hash FOR (o:O) REQUIRE o.hash IS UNIQUE",
		"CREATE CONSTRAINT v_id FOR (v:V) REQUIRE v.id IS UNIQUE",
		"CREATE CONSTRAINT s_id FOR (s:S) REQUIRE s.id IS UNIQUE",
	} {
		_, err := setup.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	const multi = "MERGE (o:O {hash: $hash}) ON CREATE SET o.id = 'sha256:' + $hash " +
		"MERGE (v:V {id: $version}) ON CREATE SET v.original_id = o.id " +
		"MERGE (v)-[:HAS_ORIGINAL]->(o) " +
		"MERGE (u:U {id: $upload}) MERGE (u)-[:OF_VERSION]->(v) RETURN v.id"
	const single = "MERGE (s:S {id: $version}) RETURN s.id"

	sessions := [2]*StorageExecutor{NewStorageExecutor(store), NewStorageExecutor(store)}
	for _, explicit := range []bool{false, true} {
		for round := 0; round < 20; round++ {
			key := fmt.Sprintf("%v-%d", explicit, round)
			var start, done sync.WaitGroup
			start.Add(1)
			errs := make([]error, 4)
			for i := range sessions {
				for j, query := range []string{multi, single} {
					done.Add(1)
					go func(slot int, exec *StorageExecutor, query string, upload string) {
						defer done.Done()
						start.Wait()
						params := map[string]interface{}{"hash": "h" + key, "version": "v" + key, "upload": upload}
						if !explicit {
							_, errs[slot] = exec.Execute(ctx, query, params)
							return
						}
						session := NewStorageExecutor(store)
						if _, errs[slot] = session.Execute(ctx, "BEGIN", nil); errs[slot] != nil {
							return
						}
						if _, errs[slot] = session.Execute(ctx, query, params); errs[slot] != nil {
							_, _ = session.Execute(ctx, "ROLLBACK", nil)
							return
						}
						_, errs[slot] = session.Execute(ctx, "COMMIT", nil)
					}(i*2+j, sessions[i], query, fmt.Sprintf("u%s-%d", key, i))
				}
			}
			start.Done()
			done.Wait()
			for slot, err := range errs {
				require.NoError(t, err, "explicit=%v round %d slot %d", explicit, round, slot)
			}
		}
	}

	result, err := setup.Execute(ctx, "MATCH (v:V) OPTIONAL MATCH (v)-[h:HAS_ORIGINAL]->(:O) WITH v, count(h) AS h "+
		"OPTIONAL MATCH (:U)-[f:OF_VERSION]->(v) RETURN count(DISTINCT v), collect(DISTINCT h), count(f)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(40), []interface{}{int64(1)}, int64(80)}}, result.Rows)
	result, err = setup.Execute(ctx, "MATCH (o:O) WITH count(o) AS o MATCH (s:S) RETURN o, count(s)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(40), int64(40)}}, result.Rows)
}

// TestMergeWaitForUniqueKeyEndsWithStatement: a MERGE waiting for a key
// another transaction holds stops when its statement's context ends, in the
// single-statement and the pipeline (UNWIND … MERGE) routes.
func TestMergeWaitForUniqueKeyEndsWithStatement(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	setup := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := setup.Execute(ctx, "CREATE CONSTRAINT w_k FOR (w:W) REQUIRE w.k IS UNIQUE", nil)
	require.NoError(t, err)

	holder := NewStorageExecutor(store)
	_, err = holder.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = holder.Execute(ctx, "MERGE (w:W {k: 1}) RETURN w.k", nil)
	require.NoError(t, err)

	for _, query := range []string{
		"MERGE (w:W {k: 1}) RETURN w.k",
		"UNWIND [1] AS i MERGE (w:W {k: i}) RETURN w.k",
	} {
		waiting, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err = NewStorageExecutor(store).Execute(waiting, query, nil)
		cancel()
		require.Error(t, err, query)
	}
	_, err = holder.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	result, err := setup.Execute(ctx, "UNWIND [1] AS i MERGE (w:W {k: i}) RETURN w.k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}
