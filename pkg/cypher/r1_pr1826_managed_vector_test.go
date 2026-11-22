package cypher

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826. The PR makes the managed-database wrapper (the
// store the server gives every executor) a projected point reader. The vector
// similarity shortcut already asked any projected point reader for only the
// properties a loose text search finds, so the PR switches that on for every
// managed database. The control is the same database and the same search
// service with whole-node reads.
func TestR1PR1826ManagedVectorSimilarityReads(t *testing.T) {
	ctx := t.Context()
	manager, err := multidb.NewDatabaseManager(ctx, newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	_, projected := store.(storage.ProjectedNodeReader)
	t.Logf("R1MANAGED store %T implements ProjectedNodeReader=%t", store, projected)
	seed := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	_, err = seed.Execute(ctx, "CREATE VECTOR INDEX r1_vec FOR (n:Entity) ON (n.vec) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	svc := search.NewService(store, 4, nil)
	seed.SetSearchService(svc)
	for i, vec := range [][]float64{{0.91, 0.13, 0.27, 0.05}, {0.11, 0.83, 0.31, 0.02}, {0.21, 0.17, 0.77, 0.41}, {0.37, 0.29, 0.11, 0.59}} {
		_, err = seed.Execute(ctx, "CREATE (:Entity {uuid:$u, body:$b, extra:$e, vec:$v})-[:REL]->(:Leaf {of:$u})", map[string]any{"u": fmt.Sprintf("e%d", i), "b": fmt.Sprintf("body %d", i), "e": int64(i + 10), "v": vec})
		require.NoError(t, err)
	}
	require.NoError(t, svc.BuildIndexes(ctx))
	require.True(t, svc.IsReady())
	params := map[string]any{"q": []float64{0.9, 0.2, 0.3, 0.1}}
	executor := func(engine storage.Engine) *StorageExecutor {
		exec := NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0})
		exec.SetSearchService(svc)
		return exec
	}
	direct := func(items string) string {
		return "MATCH (n:Entity) RETURN " + items + ", vector.similarity.cosine(n.vec, $q) AS score ORDER BY score DESC LIMIT 3"
	}
	with := func(items string) string {
		return "MATCH (n:Entity) WITH n, vector.similarity.cosine(n.vec, $q) AS score RETURN " + items + ", score ORDER BY score DESC LIMIT 3"
	}
	for _, shape := range []struct {
		name  string
		build func(string) string
	}{{"direct", direct}, {"with", with}} {
		for _, items := range []string{
			"n.uuid AS uuid",
			"n.uuid AS uuid, n.body AS body",
			"n AS x",
			"properties(n) AS x",
			"n {.*} AS x",
			"n {.uuid, .body} AS x",
			"keys(n) AS x",
			"{node: n} AS x",
			"[n] AS x",
			"[n, n] AS x",
			"coalesce(n, null) AS x",
			"CASE WHEN true THEN n END AS x",
			"[y IN [n] | y.body] AS x",
			"head([n]).body AS x",
			"{node: n}.node.body AS x",
			"n.uuid + '/' + coalesce(n, null).body AS x",
			"labels(n) AS x",
			"EXISTS { (n)-[:REL]->(l) WHERE l.of = n.uuid } AS x",
			"[(n)-[:REL]->(l) | l.of] AS x",
			"n['body'] AS x",
			"n [ 'body' ] AS x",
			"size(keys( n )) AS x",
			"N.body AS x",
		} {
			t.Run(shape.name+"/"+items, func(t *testing.T) {
				query := shape.build(items)
				for _, stage := range []string{"first", "after a whole-node read", "again"} {
					want := r1Rows(executor(r1FullReads{store, "nornic"}).Execute(ctx, query, params))
					if stage == "after a whole-node read" {
						_, err := executor(store).Execute(ctx, "MATCH (n:Entity) RETURN n", nil)
						require.NoError(t, err)
					}
					exec := executor(store)
					got := r1Rows(exec.Execute(ctx, query, params))
					if !reflect.DeepEqual(want, got) {
						t.Errorf("%s: %s\n  whole-node answer: %q\n  answer:            %q\n  shortcut used: %t", stage, query, want, got, exec.LastHotPathTrace().CosineVectorIndexFastPath)
					}
				}
			})
		}
	}
}
