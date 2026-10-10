package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A quantified group's own WHERE (its elements' inline predicates included)
// is an expression over one iteration: its variables are one node or
// relationship each, a parameter or a function call there is not a pattern
// element, and a {m,n} after the group quantifies it in every selector
// (SHORTEST k, ANY SHORTEST). Recorded on Neo4j 5.26.30 and 2026.09.
func TestQuantifiedGroupPredicates(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quantified_group_predicates"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:G {id:'a'})-[:R {kind:'x'}]->(:G {id:'b'})-[:R {kind:'y'}]->(:G {id:'c'})", nil)
	require.NoError(t, err)
	params := map[string]interface{}{"start": "a", "kinds": []interface{}{"x"}}
	for query, want := range map[string][][]interface{}{
		"MATCH (a:G) WHERE a.id = 'a' MATCH p = SHORTEST 1 (a)(()-[r:R]-()){1,6}(t:G) RETURN t.id, length(p) ORDER BY t.id":                       {{"b", int64(1)}, {"c", int64(2)}},
		"MATCH (a:G {id:'a'}) MATCH p = SHORTEST 2 (a)(()-[r:R]-()){1,6}(t:G) RETURN count(p)":                                                    {{int64(2)}},
		"MATCH p = ANY SHORTEST (a:G {id:'a'})(()-[r:R]-()){1,6}(t:G) RETURN count(p)":                                                            {{int64(2)}},
		"MATCH (s:G {id: $start}) MATCH p = SHORTEST 1 (s)(()-[r:R WHERE size($kinds) = 0 OR r.kind IN $kinds]-())+(t) RETURN t.id ORDER BY t.id": {{"b"}},
		"MATCH (s:G {id:'a'}) WITH s, [] AS kinds MATCH p = SHORTEST 1 (s)(()-[r:R WHERE size(kinds) = 0]-())+(t:G) RETURN t.id ORDER BY t.id":    {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = SHORTEST 1 (s)(()-[r:R WHERE type(r) = 'R']-())+(t:G) RETURN t.id ORDER BY t.id":                          {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x WHERE size($kinds) > 0)-[r:R]-())+(t) RETURN t.id ORDER BY t.id":                                   {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-(y) WHERE x.id <> y.id AND r.kind IS NOT NULL)+(t) RETURN t.id ORDER BY t.id":               {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-(y))+(t) WHERE size(r) >= 1 RETURN t.id ORDER BY t.id":                                      {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)(()-[r:R WHERE r.kind + 1]-())+(t) RETURN t.id":                                                        {},
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
	for _, query := range []string{
		"MATCH (s:G {id:'a'}) MATCH p = (s)(()-[r:R WHERE zz = 1]-())+(t) RETURN t.id",
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-() WHERE type(x) = 'A')+(t) RETURN t.id",
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-(y) WHERE size(r) = 1)+(t) RETURN t.id",
	} {
		_, err := exec.Execute(ctx, query, params)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
}
