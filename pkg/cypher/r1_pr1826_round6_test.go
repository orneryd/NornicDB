package cypher

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826, round 6: the same write-back as round 4 from a
// label scan (which the base narrows too), from the vector similarity shortcut
// of a managed database, and with the write-behind buffer on.

func r1StoredProperties(t *testing.T, store storage.Engine, name, match string) []string {
	t.Helper()
	return r1Rows(NewStorageExecutor(r1FullReads{store, name}, QueryCachePolicy{MaxEntries: 0}).Execute(t.Context(), match, nil))
}

func TestR1PR1826Round6ScanStoredPropertiesSurviveSet(t *testing.T) {
	const want = "{body:'body one',extra:7,flag:1,key:'one',list:[1,2,3],path:'guide',status:'current'}"
	for _, kind := range []string{"namespaced", "managed"} {
		for _, indexed := range []bool{true, false} {
			for _, test := range []struct{ name, query string }{
				{"path/where", "CALL () { MATCH p=(n:Doc) WHERE n.key = 'one' RETURN nodes(p)[0] AS x } SET x.flag = 1"},
				{"path/where-unindexed-property", "CALL () { MATCH p=(n:Doc) WHERE n.status = 'current' RETURN nodes(p)[0] AS x } SET x.flag = 1"},
				{"keyword/where", "CALL () { MATCH (end:Doc) WHERE end.key = 'one' RETURN coalesce(end, null) AS x } SET x.flag = 1"},
				{"keyword/where-unindexed-property", "CALL () { MATCH (end:Doc) WHERE end.status = 'current' RETURN coalesce(end, null) AS x } SET x.flag = 1"},
				{"varlength/where", "CALL () { MATCH (n:Doc) WHERE n.key = 'one' MATCH (n:Doc)-[:REL*0..0]->(m) RETURN m AS x } SET x.flag = 1"},
				{"varlength/where-unindexed-property", "CALL () { MATCH (n:Doc) WHERE n.status = 'current' MATCH (n:Doc)-[:REL*0..0]->(m) RETURN m AS x } SET x.flag = 1"},
				{"endpoint/keyword", "CALL () { MATCH (start)-[:REL]->(end) RETURN coalesce(start, null) AS q, coalesce(end, null) AS z } WITH z MATCH (n:Doc {key:'one'}) SET n.flag = 1"},
				{"endpoint/keyword-set", "CALL () { MATCH (end)-[:REL]->(m) RETURN coalesce(end, null) AS x } SET x.flag = 1"},
			} {
				t.Run(fmt.Sprintf("%s/indexed=%t/%s", kind, indexed, test.name), func(t *testing.T) {
					store := r1Round4Store(t, kind, indexed)
					result, err := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}).Execute(t.Context(), test.query, nil)
					t.Logf("R1SCAN %s: rows %q", test.name, r1Rows(result, err))
					got := r1StoredProperties(t, store, r1Round4Name(kind), "MATCH (n:Doc) WHERE n.key = 'one' RETURN properties(n) AS p")
					require.Equal(t, []string{want}, got, test.query)
				})
			}
		}
	}
}

func TestR1PR1826Round6VectorShortcutStoredPropertiesSurviveSet(t *testing.T) {
	for _, test := range []struct{ name, query string }{
		{"control/node", "CALL () { MATCH (n:Entity) RETURN n AS x, vector.similarity.cosine(n.vec, $q) AS score ORDER BY score DESC LIMIT 1 } SET x.flag = 1"},
		{"coalesce", "CALL () { MATCH (n:Entity) RETURN coalesce(n, null) AS x, vector.similarity.cosine(n.vec, $q) AS score ORDER BY score DESC LIMIT 1 } SET x.flag = 1"},
		{"list", "CALL () { MATCH (n:Entity) RETURN [n] AS xs, vector.similarity.cosine(n.vec, $q) AS score ORDER BY score DESC LIMIT 1 } UNWIND xs AS x SET x.flag = 1"},
		{"case", "CALL () { MATCH (n:Entity) RETURN CASE WHEN true THEN n END AS x, vector.similarity.cosine(n.vec, $q) AS score ORDER BY score DESC LIMIT 1 } SET x.flag = 1"},
		{"with", "CALL () { MATCH (n:Entity) WITH n, vector.similarity.cosine(n.vec, $q) AS score RETURN coalesce(n, null) AS x, score ORDER BY score DESC LIMIT 1 } SET x.flag = 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			manager, err := multidb.NewDatabaseManager(ctx, newTestMemoryEngine(t), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			store, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			_, err = exec.Execute(ctx, "CREATE VECTOR INDEX r1_vec FOR (n:Entity) ON (n.vec) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
			require.NoError(t, err)
			svc := search.NewService(store, 4, nil)
			exec.SetSearchService(svc)
			for i, vec := range [][]float64{{0.91, 0.13, 0.27, 0.05}, {0.11, 0.83, 0.31, 0.02}, {0.21, 0.17, 0.77, 0.41}} {
				_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:$u, body:$b, extra:$e, vec:$v})", map[string]any{"u": fmt.Sprintf("e%d", i), "b": fmt.Sprintf("body %d", i), "e": int64(i + 10), "v": vec})
				require.NoError(t, err)
			}
			require.NoError(t, svc.BuildIndexes(ctx))
			require.True(t, svc.IsReady())
			result, err := exec.Execute(ctx, test.query, map[string]any{"q": []float64{0.9, 0.2, 0.3, 0.1}})
			t.Logf("R1VECSET %s: rows %q shortcut=%t", test.name, r1Rows(result, err), exec.LastHotPathTrace().CosineVectorIndexFastPath)
			got := r1StoredProperties(t, store, "nornic", "MATCH (n:Entity) RETURN n.uuid, n.body, n.extra, size(n.vec), n.flag")
			require.Equal(t, []string{"'e0' | 'body 0' | 10 | 4 | 1", "'e1' | 'body 1' | 11 | 4 | null", "'e2' | 'body 2' | 12 | 4 | null"}, got, test.query)
		})
	}
}

func TestR1PR1826Round6WriteBehindStoredPropertiesSurviveSet(t *testing.T) {
	ctx := t.Context()
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: 50 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "r1")
	r1SeedRound2(t, store, true)
	time.Sleep(300 * time.Millisecond)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	result, err := exec.Execute(ctx, "CALL () { MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x } SET x.flag = 1", nil)
	t.Logf("R1WB rows %q", r1Rows(result, err))
	time.Sleep(300 * time.Millisecond)
	got := r1StoredProperties(t, store, "r1", "MATCH (n:Doc) WHERE n.key = 'one' RETURN properties(n) AS p")
	require.Equal(t, []string{"{body:'body one',extra:7,flag:1,key:'one',list:[1,2,3],path:'guide',status:'current'}"}, got)
}
