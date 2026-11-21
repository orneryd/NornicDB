package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCloseoutL5Cypher(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "closeout_l5"))
	query := func(t *testing.T, stmt string) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(t.Context(), stmt, nil)
		require.NoError(t, err)
		return result
	}
	t.Run("control", func(t *testing.T) {
		require.Equal(t, [][]any{{int64(1)}}, query(t, "RETURN 1").Rows)
	})
	procedures := query(t, "SHOW PROCEDURES YIELD name, signature RETURN name, signature ORDER BY name")
	functions := query(t, "SHOW FUNCTIONS YIELD name, signature RETURN name, signature ORDER BY name")
	catalog := func(result *ExecuteResult) map[string]string {
		out := make(map[string]string)
		for _, row := range result.Rows {
			out[strings.ToLower(row[0].(string))] = row[1].(string)
		}
		return out
	}
	proc, fn := catalog(procedures), catalog(functions)
	for _, control := range []string{"gds.graph.project", "gds.fastrp.stream", "gds.linkprediction.resourceallocation.stream", "apoc.algo.louvain", "apoc.algo.wcc", "db.temporal.asof"} {
		require.Contains(t, proc, control)
	}
	require.Contains(t, fn, "abs")
	type absence struct {
		id         string
		reference  string
		procedure  bool
		candidates []string
	}
	for _, tc := range []absence{
		{"P-N152-156", "gds.linkprediction.resourceallocation.stream", true, []string{"nornicdb.similarity.stream", "nornicdb.similarity.pairs", "nornicdb.similarity.project"}},
		{"H-184", "gds.linkprediction.resourceallocation.stream", true, []string{"nornicdb.similarity.stream"}},
		{"H-255", "gds.linkprediction.resourceallocation.stream", true, []string{"nornicdb.similarity.holdings"}},
		{"H-256", "gds.linkprediction.resourceallocation.stream", true, []string{"nornicdb.similarity.affinities"}},
		{"H-215", "apoc.algo.louvain", true, []string{"gds.louvain.stream"}},
		{"H-253", "apoc.algo.wcc", true, []string{"gds.wcc.stream"}},
		{"H-177", "db.index.fulltext.querynodes", true, []string{"db.index.containedin"}},
		{"H-185", "abs", false, []string{"text.key"}},
		
		{"P-N152-110", "gds.graph.project", true, []string{"gds.graph.project.cypher", "gds.louvain.stream"}},
	} {
		t.Run(tc.id, func(t *testing.T) {
			current := fn
			if tc.procedure {
				current = proc
			}
			require.Contains(t, current, tc.reference)
			for _, name := range tc.candidates {
				require.NotContains(t, current, name, "public registration now exists; qualify its behavior")
			}
			t.Logf("L5 %s catalog-absence missing=%v control=%q", tc.id, tc.candidates, current[tc.reference])
		})
	}
	t.Run("P-N152-107", func(t *testing.T) {
		query(t, "CREATE (:CloseoutList {id: 1, values: [1, 2]})")
		require.Equal(t, [][]any{{int64(2)}}, query(t, "MATCH (n:CloseoutList) RETURN n.values[1]").Rows)
		for _, value := range []any{[]any{map[string]any{"key": "value"}}, []any{int64(1), nil}} {
			_, err := exec.Execute(t.Context(), "MATCH (n:CloseoutList) SET n.values = $values", map[string]any{"values": value})
			require.Error(t, err, "property acceptance changed for %#v", value)
			t.Logf("L5 P-N152-107 rejected %T item list: %v", value.([]any)[len(value.([]any))-1], err)
		}
		require.Equal(t, [][]any{{int64(2)}}, query(t, "MATCH (n:CloseoutList) RETURN n.values[1]").Rows)
	})
}
