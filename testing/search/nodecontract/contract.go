// Package nodecontract exercises the logical node values returned by search
// procedures through an injected public query route.
package nodecontract

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Vector is a deterministic, valid 2048-dimensional query vector.
func Vector() []float64 { vector := make([]float64, 2048); vector[0] = 1; return vector }

// Run checks ordinary rows, graph rows, continuation pages, grouped passages,
// and the Neo4j-compatible index procedures with the same property assertions.
// The fixture contains source-1 and source-2, one bucket per source, and the
// retrieval_vector / retrieval_text schema indexes.
func Run(t *testing.T, run func(string) ([][]any, error)) {
	t.Helper()
	request := "{query:'source', embedding:$vector, types:['RetrievalSource'], minSimilarity:0.01, limit:10}"
	readback := " RETURN node.id AS id, rank, labels(node), node.ordinary, node.embedding, properties(node)"
	cases := []struct{ name, query string }{}
	for _, procedure := range []string{"db.retrieve", "db.rretrieve", "db.retrieveGraph", "db.rretrieveGraph"} {
		cases = append(cases, struct{ name, query string }{procedure, "CALL " + procedure + "(" + request + ") YIELD node, vector_rank WITH node, vector_rank AS rank" + readback})
		pageRequest := request[:len(request)-1] + ", n:10, max_results:10}"
		cases = append(cases, struct{ name, query string }{procedure + "/page", "CALL " + procedure + "(" + pageRequest + ") YIELD page UNWIND page.results AS hit WITH hit.node AS node, hit.vector_rank AS rank" + readback})
		if procedure == "db.retrieve" || procedure == "db.rretrieve" {
			grouped := pageRequest[:len(pageRequest)-1] + ", mode:'ranked_then_id', ranked_limit:2, group_by:'bucket'}"
			cases = append(cases, struct{ name, query string }{procedure + "/passages", "CALL " + procedure + "(" + grouped + ") YIELD page UNWIND page.results AS hit UNWIND hit.passages AS passage WITH passage.node AS node, passage.vector_rank AS rank" + readback})
		}
	}
	for _, call := range []struct{ name, call string }{
		{"vector.queryNodes", "db.index.vector.queryNodes('retrieval_vector',10,$vector)"},
		{"fulltext.queryNodes", "db.index.fulltext.queryNodes('retrieval_text','source')"},
		{"fulltext.queryNodes/wildcard", "db.index.fulltext.queryNodes('retrieval_text','*')"},
	} {
		cases = append(cases, struct{ name, query string }{call.name, "CALL " + call.call + " YIELD node, score WITH node, score AS rank" + readback})
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var rows [][]any
			var err error
			require.Eventually(t, func() bool { rows, err = run(test.query); return err != nil || len(rows) == 2 }, 15*time.Second, 20*time.Millisecond)
			require.NoError(t, err, test.query)
			ids := make([]any, 0, 2)
			for _, row := range rows {
				require.Len(t, row, 6)
				require.Contains(t, []string{"source-1", "source-2"}, row[0], "node properties missing: %#v", row)
				ids = append(ids, row[0])
				require.NotZero(t, row[1])
				require.Equal(t, []any{"RetrievalSource"}, row[2])
				require.EqualValues(t, 42, row[3])
				require.Len(t, row[4], 2048, "a stored vector property is part of the logical node")
				properties, ok := row[5].(map[string]any)
				require.True(t, ok, "properties(node): %T", row[5])
				require.Equal(t, row[0], properties["id"])
				require.Len(t, properties["embedding"], 2048)
			}
			require.ElementsMatch(t, []any{"source-1", "source-2"}, ids)
		})
	}
}
