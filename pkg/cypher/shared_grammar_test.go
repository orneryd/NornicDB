package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSharedCypherGrammar(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, prefix := range []string{"", "CYPHER 5 ", "CYPHER 25 "} {
				t.Run(prefix, func(t *testing.T) {
					for _, test := range []struct {
						query string
						rows  [][]interface{}
					}{
						{"RETURN 1 AS x", [][]interface{}{{int64(1)}}},
						{"FOR x IN [1,2,3] LET scaled = x * 10 FILTER scaled > 10 RETURN x, scaled ORDER BY x", [][]interface{}{{int64(2), int64(20)}, {int64(3), int64(30)}}},
						{"LET x = 2, y = 3 LET z = x + y RETURN x, y, z", [][]interface{}{{int64(2), int64(3), int64(5)}}},
						{"FOR x IN $items FILTER WHERE x IS NOT NULL RETURN x", [][]interface{}{{int64(2)}, {int64(3)}}},
						{"FOR x IN [] LET y = x + 1 FILTER y > 0 RETURN y", [][]interface{}{}},
						{"FOR x IN null RETURN x", [][]interface{}{}},
						{"LET filter = 'FOR LET FILTER', let = 4 RETURN filter, let", [][]interface{}{{"FOR LET FILTER", int64(4)}}},
						{"WITH 2 AS x CALL { RETURN x * 2 AS y } RETURN x, y", [][]interface{}{{int64(2), int64(4)}}},
						{"WITH 2 AS x CALL { RETURN x AS y UNION RETURN x + 1 AS y } RETURN x, y ORDER BY y", [][]interface{}{{int64(2), int64(2)}, {int64(2), int64(3)}}},
						{"WITH 2 AS x CALL { WITH 3 AS y RETURN y } RETURN x, y", [][]interface{}{{int64(2), int64(3)}}},
					} {
						t.Run(test.query, func(t *testing.T) {
							additive := startsWithKeywordFold(test.query, "FOR") || startsWithKeywordFold(test.query, "LET")
							if parser == "antlr" && additive && prefix != "CYPHER 25 " {
								return
							}
							exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "shared"))
							result, err := exec.Execute(context.Background(), prefix+test.query, map[string]interface{}{"items": []interface{}{nil, int64(2), int64(3)}})
							require.NoError(t, err)
							require.Equal(t, test.rows, result.Rows)
						})
					}
				})
			}
		})
	}
}

func TestSharedCypherGrammarErrorsBeforeWrites(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{
				"CREATE (:Shared) LET x = missing RETURN x",
				"CREATE (:Shared) FILTER missing > 0 RETURN 1",
				"CREATE (:Shared) FOR x IN missing RETURN x",
				"CREATE (:Shared) LET x = RETURN x",
				"CREATE (:Shared) LET x = count(*) RETURN x",
				"CREATE (:Shared) FOR x [1,2] RETURN x",
				"CREATE (:Shared) FILTER RETURN 1",
				"CREATE (:Shared) CALL () { RETURN missing AS x } RETURN x",
				"FOR x IN [1,2,3]",
				"LET x = 1",
				"FILTER true",
			} {
				t.Run(query, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "errors"))
					_, err := exec.Execute(context.Background(), query, nil)
					require.Error(t, err)
					result, err := exec.Execute(context.Background(), "MATCH (n:Shared) RETURN count(n)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
				})
			}
		})
	}
}

func TestSharedCypherGrammarWrites(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "writes")
			exec := NewStorageExecutor(store)
			prefix := ""
			if parser == "antlr" {
				prefix = "CYPHER 25 "
			}
			_, err := exec.Execute(context.Background(), prefix+"FOR x IN [1,2,3] LET value = x * 2 FILTER value > 2 CREATE (:Shared {value: value})", nil)
			require.NoError(t, err)
			fresh := NewStorageExecutor(store)
			result, err := fresh.Execute(context.Background(), "MATCH (n:Shared) RETURN n.value ORDER BY n.value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(4)}, {int64(6)}}, result.Rows)
			result, err = fresh.Execute(context.Background(), "MATCH (n:Shared) CALL (n) { SET n.copied = n.value } RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
		})
	}
}

func BenchmarkSharedCypherGrammar(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "benchmark"))
	for _, query := range []string{
		"UNWIND [1,2,3] AS x WITH x, x * 10 AS scaled WHERE scaled > 10 RETURN x, scaled",
		"FOR x IN [1,2,3] LET scaled = x * 10 FILTER scaled > 10 RETURN x, scaled",
	} {
		b.Run(query, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				result, err := exec.Execute(context.Background(), query, nil)
				if err != nil || len(result.Rows) != 2 {
					b.Fatalf("result=%v err=%v", result, err)
				}
			}
		})
	}
}
