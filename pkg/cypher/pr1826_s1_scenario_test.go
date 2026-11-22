package cypher

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestPR1826S1PropertyReadStatementCorpus is run as separate default and ANTLR
// lab jobs on both revisions. It covers the proof boundaries that decide
// whether a node may be projected; every expected result is independent of the
// physical read path.
func TestPR1826S1PropertyReadStatementCorpus(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	store := storage.NewNamespacedEngine(engine, "s1cypher")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE INDEX s1_doc_key IF NOT EXISTS FOR (n:S1Doc) ON (n.key)",
		"CREATE INDEX s1_doc_category_rank IF NOT EXISTS FOR (n:S1Doc) ON (n.category, n.rank)",
	} {
		_, err = exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	for _, params := range []map[string]any{
		{"key": "a", "name": "Alpha", "category": "x", "rank": int64(1), "body": strings.Repeat("A", 32768), "bytes": []byte{0, 1, 255}},
		{"key": "b", "name": "Beta", "category": "x", "rank": int64(2), "body": strings.Repeat("B", 32768), "bytes": []byte{}},
		{"key": "c", "name": "Gamma", "category": "y", "rank": int64(3), "body": "", "bytes": []byte{3, 2, 1}},
	} {
		_, err = exec.Execute(ctx, "CREATE (:S1Doc {key:$key,name:$name,category:$category,rank:$rank,body:$body,bytes:$bytes})", params)
		require.NoError(t, err)
	}

	assertRows := func(name, query string, params map[string]any, want [][]any) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			result, queryErr := exec.Execute(ctx, query, params)
			require.NoError(t, queryErr)
			require.Equal(t, want, result.Rows)
		})
	}
	assertRows("indexed parameter and where", "MATCH (n:S1Doc {key:$key}) WHERE n.rank >= $min RETURN n.name,n.rank", map[string]any{"key": "b", "min": int64(2)}, [][]any{{"Beta", int64(2)}})
	assertRows("residual where and order", "MATCH (n:S1Doc) WHERE n.rank >= $min RETURN n.name,n.rank ORDER BY n.rank DESC", map[string]any{"min": int64(2)}, [][]any{{"Gamma", int64(3)}, {"Beta", int64(2)}})
	assertRows("with aggregation", "MATCH (n:S1Doc) WITH n.category AS category, count(*) AS total RETURN category,total ORDER BY category", nil, [][]any{{"x", int64(2)}, {"y", int64(1)}})
	assertRows("map selected projection", "MATCH (n:S1Doc {key:'a'}) RETURN n{.name,.rank}", nil, [][]any{{map[string]any{"name": "Alpha", "rank": int64(1)}}})
	assertRows("map star projection", "MATCH (n:S1Doc {key:'c'}) RETURN n{.*}", nil, [][]any{{map[string]any{"key": "c", "name": "Gamma", "category": "y", "rank": int64(3), "body": "", "bytes": []byte{3, 2, 1}}}})
	assertRows("properties requires whole map", "MATCH (n:S1Doc {key:'b'}) RETURN properties(n)", nil, [][]any{{map[string]any{"key": "b", "name": "Beta", "category": "x", "rank": int64(2), "body": strings.Repeat("B", 32768), "bytes": []byte{}}}})
	assertRows("keys requires whole map", "MATCH (n:S1Doc {key:'a'}) RETURN keys(n)", nil, [][]any{{[]any{"body", "bytes", "category", "key", "name", "rank"}}})
	assertRows("optional missing", "OPTIONAL MATCH (n:S1Doc {key:'missing'}) RETURN n.name", nil, [][]any{{nil}})
	assertRows("correlated subquery", "MATCH (n:S1Doc {key:'a'}) CALL (n) { RETURN n.name AS inner } RETURN inner", nil, [][]any{{"Alpha"}})

	union, err := exec.Execute(ctx, "MATCH (n:S1Doc {category:'x'}) RETURN n.name AS name UNION MATCH (n:S1Doc {key:'c'}) RETURN n.name AS name", nil)
	require.NoError(t, err)
	names := make([]string, 0, len(union.Rows))
	for _, row := range union.Rows {
		names = append(names, row[0].(string))
	}
	sort.Strings(names)
	require.Equal(t, []string{"Alpha", "Beta", "Gamma"}, names)

	whole, err := exec.Execute(ctx, "MATCH (n:S1Doc {key:'a'}) RETURN n", nil)
	require.NoError(t, err)
	require.Len(t, whole.Rows, 1)
	node, ok := whole.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, "Alpha", node.Properties["name"])
	require.Equal(t, strings.Repeat("A", 32768), node.Properties["body"])
}
