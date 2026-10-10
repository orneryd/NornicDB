package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An AND / OR operand is a relationship pattern only when it is one whole
// relationship chain: a parenthesised expression whose subqueries hold
// patterns is evaluated as the expression it is. Recorded on Neo4j 5.26.30;
// the first two are from a user's report (reduced).
func TestPatternOperandIsAWholeChain(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_operand"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:ZD {id: 'a'})-[:ZR {kind: 'same'}]->(b:ZD {id: 'b'})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (a:ZD {id: 'a'})-[r:ZR]->(b) WHERE false OR (NOT EXISTS { MATCH (h:ZPC)-[:C]->(x) } AND NOT EXISTS { MATCH (o:ZPC)-[:C]->(y) }) RETURN b.id AS v":                               {{"b"}},
		"MATCH (a:ZD {id: 'a'})-[r:ZR]->(b) WHERE type(r) <> 'ZR' OR (r.kind = 'same' AND NOT EXISTS { MATCH (h:ZPC)-[:C]->(x) } AND NOT EXISTS { MATCH (o:ZPC)-[:C]->(y) }) RETURN b.id AS v": {{"b"}},
		"MATCH (a:ZD {id: 'a'}), (b:ZD {id: 'b'}) WHERE false OR ((a)-[:ZR]->(b)) RETURN b.id AS v":                                                                                            {{"b"}},
		"MATCH (a:ZD {id: 'a'}), (b:ZD {id: 'b'}) WHERE false OR (a)-[:ZR]->(b) RETURN b.id AS v":                                                                                              {{"b"}},
		"MATCH (a:ZD {id: 'a'}), (b:ZD {id: 'b'}) WHERE false OR (b)-[:ZR]->(a) RETURN b.id AS v":                                                                                              {},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}
