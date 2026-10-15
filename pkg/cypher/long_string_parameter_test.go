package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A long string parameter kept as a reference reads as its exact value
// wherever a parameter can stand: projections, WITH, WHERE, a MATCH or MERGE
// property map, CREATE and SET values, CASE, map and list literals, and a
// function's argument on every row.
func TestLongStringParameterInEveryPosition(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "longparam"))
	ctx := context.Background()
	text := strings.Repeat("lorem ipsum ", 200) + "needle, it's a \\ back $x // end"
	needle := int64(strings.Index(text, "needle"))
	params := map[string]interface{}{"text": text, "copy": strings.Clone(text)}
	run := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{{text}}, run("RETURN $text AS v"))
	require.Equal(t, [][]interface{}{{true}}, run("RETURN $text = $copy AS v"))
	require.Equal(t, [][]interface{}{{int64(len(text) + 1)}, {int64(len(text) + 2)}}, run("UNWIND [1, 2] AS c RETURN size($text) + c AS v"))
	require.Equal(t, [][]interface{}{{true}}, run("WITH $text AS t RETURN t = $text AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("UNWIND [1] AS c WITH c WHERE $text CONTAINS 'needle' RETURN count(*) AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("RETURN CASE WHEN $text ENDS WITH '// end' THEN 1 ELSE 0 END AS v"))
	require.Equal(t, [][]interface{}{{true, true}}, run("RETURN {t: $text}.t = $text AS m, [$text, 'b'][0] = $text AS l"))
	span := map[string]interface{}{"start": needle, "end": needle + 6}
	require.Equal(t, [][]interface{}{{span}, {span}}, run("UNWIND [1, 2] AS c RETURN text.locate($text, 'needle') AS v"))

	require.Equal(t, [][]interface{}{{true}}, run("CREATE (n:LongDoc {id: 1, body: $text}) RETURN n.body = $text AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (n:LongDoc {body: $text}) RETURN n.id AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (n:LongDoc) WHERE n.body = $text RETURN n.id AS v"))
	require.Equal(t, [][]interface{}{{true}}, run("MATCH (n:LongDoc {id: 1}) SET n.copy = $text RETURN n.copy = n.body AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MERGE (n:LongDoc {body: $text}) RETURN n.id AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (n:LongDoc) RETURN count(n) AS v"))
}

// BenchmarkLongStringParameterPerRow is #1094's statement: a 200,000-character
// string parameter used by every one of 20 rows, against the same text bound
// once with WITH. On 16 CPUs, without the query cache, 3 runs of 5: on main
// the parameter form took 311–317 ms and 2.05 MB, the bound form 174–179 ms;
// with the parameter kept as a reference both take 166–174 ms and 0.21 MB,
// the time text.locate itself spends on the text.
func BenchmarkLongStringParameterPerRow(b *testing.B) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "longparam"), 0, 0)
	ctx := context.Background()
	rows := make([]interface{}, 20)
	for index := range rows {
		rows[index] = map[string]interface{}{"q": "needle"}
	}
	params := map[string]interface{}{"text": strings.Repeat("lorem ipsum ", 16666) + "needle", "rows": rows}
	for _, bench := range []struct{ name, query string }{
		{"parameter", "UNWIND $rows AS c RETURN text.locate($text, c.q) AS span"},
		{"bound once", "WITH $text AS t UNWIND $rows AS c RETURN text.locate(t, c.q) AS span"},
	} {
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, bench.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
