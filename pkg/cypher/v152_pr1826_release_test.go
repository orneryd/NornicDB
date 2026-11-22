package cypher

// v1.5.2 verification (scratch, test-only): parts of the PR #1826 review
// defects that only the embedded executor can show.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// p26Scribble changes the first element of every list reachable in v.
func p26Scribble(v any) {
	switch x := v.(type) {
	case []string:
		x[0] = "CHANGED"
	case []int64:
		x[0] = 999
	case []float64:
		x[0] = 999
	case []any:
		for _, item := range x {
			p26Scribble(item)
		}
		switch x[0].(type) {
		case string:
			x[0] = "CHANGED"
		case int64:
			x[0] = int64(999)
		case float64:
			x[0] = float64(999)
		}
	case map[string]any:
		for _, item := range x {
			p26Scribble(item)
		}
	}
}

// F5 through the executor: a caller changes a list it received inside an
// explicit transaction, then runs the statement again; and list functions that
// could change their argument.
func TestV152P26ReturnedListInTransaction(t *testing.T) {
	ctx := context.Background()
	for _, query := range []string{"MATCH (n:Doc) RETURN n.tags", "MATCH (n:Doc) RETURN n.nums", "MATCH (n:Doc) RETURN n.tags, n.mixed", "MATCH (n:Doc) RETURN n{.tags}", "MATCH (n:Doc) RETURN n.vec", "MATCH (n:Doc) RETURN [n.mixed, n.tags] AS l", "MATCH (n:Doc) RETURN collect(n.tags) AS l"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, query), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "p26")
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
				_, err := exec.Execute(ctx, "CREATE (:Doc {tags:['a','b'], nums:[3,1,2], mixed:['x','y'], vec:[0.1,0.2,0.3], body:'b'})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				run := func() ([][]any, string) {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					return result.Rows, fmt.Sprintf("%v", result.Rows)
				}
				rows, first := run()
				for _, row := range rows {
					for _, cell := range row {
						p26Scribble(cell)
					}
				}
				_, second := run()
				t.Logf("P26ROW F5C explicit=%t %q first=%s second=%s", explicit, query, first, second)
				if first != second {
					t.Errorf("P26 F5C explicit=%t %q: the caller's change to its result is returned by the next statement: %s then %s", explicit, query, first, second)
				}
			})
		}
	}
	// Statements alone.
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "p26")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	_, err := exec.Execute(ctx, "CREATE (:Doc {tags:['c','a','b'], nums:[3,1,2], vec:[0.3,0.1,0.2]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	show := func() string {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) RETURN n.tags, n.nums, n.vec", nil)
		require.NoError(t, err)
		return fmt.Sprintf("%v", result.Rows)
	}
	before := show()
	for _, f := range []string{"apoc.coll.sort(n.tags)", "apoc.coll.sort(n.nums)", "apoc.coll.reverse(n.tags)", "apoc.coll.shuffle(n.nums)", "reverse(n.tags)", "tail(n.nums)", "apoc.coll.remove(n.tags, 0)", "apoc.coll.set(n.tags, 0, 'z')", "apoc.coll.insert(n.nums, 0, 9)", "apoc.coll.sumLongs(n.nums)", "apoc.agg.median(n.vec)", "n.tags + ['q']", "reduce(a = [], x IN n.nums | a + x)"} {
		result, err := exec.Execute(ctx, "MATCH (n:Doc) RETURN "+f+" AS v", nil)
		rows := "ERR"
		if err == nil {
			rows = fmt.Sprintf("%v", result.Rows)
		} else {
			t.Logf("P26ROW F5C %s error %v", f, err)
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		after := show()
		t.Logf("P26ROW F5C %s -> %s, stored lists then %s", f, rows, after)
		if after != before {
			t.Errorf("P26 F5C statement %s changes what the transaction reads afterwards: %s then %s", f, before, after)
			before = after
		}
	}
}

// R1 #9 and R1 #6 on an executor over a namespaced store (the embedded API):
// the vector similarity read asks for a projected node.
func TestV152P26EmbeddedVectorRead(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "p26")
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) {
			label := fmt.Sprintf("Ent%t", explicit)
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			_, err := exec.Execute(ctx, "CREATE VECTOR INDEX vec_"+label+" FOR (n:"+label+") ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
			require.NoError(t, err)
			for i, emb := range [][]float64{{1, 0, 0}, {0, 1, 0}} {
				_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("%s%d", label, i)), Labels: []string{label}, Properties: map[string]any{"key": fmt.Sprint(i), "body": "b", "emb": emb}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
				require.NoError(t, err)
			}
			service := search.NewService(store, 3, nil)
			t.Cleanup(func() { _ = service.Close() })
			require.NoError(t, service.BuildIndexes(ctx))
			exec.SetSearchService(service)
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, c := range []struct{ name, item, want string }{
				{"R9", "n.has_embedding", "true"},
				{"R6", "size(keys({node:n}.node))", "3"},
				{"R6", "size(keys(coalesce(n, null)))", "3"},
				{"R6", "n.key", "0"},
			} {
				query := "MATCH (n:" + label + ") RETURN " + c.item + " AS v, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT 1"
				result, err := exec.Execute(ctx, query, map[string]any{"q": []float64{1, 0, 0}})
				if err != nil {
					t.Errorf("P26 %s-VECTOR explicit=%t %s: %v", c.name, explicit, query, err)
					continue
				}
				got := "no row"
				if len(result.Rows) > 0 {
					got = fmt.Sprint(result.Rows[0][0])
				}
				t.Logf("P26ROW %s-VECTOR explicit=%t fastpath=%t %q -> %v", c.name, explicit, exec.LastHotPathTrace().CosineVectorIndexFastPath, query, result.Rows)
				if got != c.want {
					t.Errorf("P26 %s-VECTOR explicit=%t %s: expected %s, got %s", c.name, explicit, query, c.want, got)
				}
			}
		})
	}
}
