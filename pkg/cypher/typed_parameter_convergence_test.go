package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTypedParameterMutationConvergence(t *testing.T) {
	for name, value := range map[string]any{"bytes": []byte{0, 127, 128, 255}, "booleans": []bool{true, false}, "point": CypherPoint{SRID: 7203, X: 1.25, Y: 2.5}} {
		for _, rel := range []bool{false, true} {
			for _, route := range []string{"pattern", "direct", "map", "replace", "on-create", "on-match", "predicate", "named-path", "case"} {
				t.Run(fmt.Sprintf("%s/rel=%v/%s", name, rel, route), func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "typed"))
					ctx := context.Background()
					pattern, prefix, read := "(n:Typed)", "", "MATCH (n:Typed)"
					if rel {
						prefix = "MATCH (a:Start), (b:End) "
						pattern = "(a)-[n:TYPED]->(b)"
						read = "MATCH (:Start)-[n:TYPED]->(:End)"
						_, err := exec.Execute(ctx, "CREATE (:Start), (:End)", nil)
						require.NoError(t, err)
					}
					query := "MERGE " + pattern
					switch route {
					case "pattern":
						query = "MERGE " + pattern[:len(pattern)-1] + " {v:$v}" + pattern[len(pattern)-1:]
						if rel {
							query = "MERGE (a)-[n:TYPED {v:$v}]->(b)"
						}
					case "direct":
						query += " SET n.v=$v"
					case "map":
						query += " SET n += $m"
					case "replace":
						query += " SET n=$m"
					case "on-create":
						query += " ON CREATE SET n += $m"
					case "on-match":
						_, err := exec.Execute(ctx, prefix+"CREATE "+pattern, nil)
						require.NoError(t, err)
						query += " ON MATCH SET n += $m"
					case "predicate", "named-path":
						_, err := exec.Execute(ctx, prefix+"CREATE "+pattern+" SET n.v=$v", map[string]any{"v": value})
						require.NoError(t, err)
						query = "WITH $v AS p " + read + " WHERE n.v=p MERGE (hit:Hit) RETURN count(hit)"
						if route == "named-path" {
							query = "WITH $v AS p MATCH path=(n:Typed {v:p}) MERGE (hit:Hit) RETURN count(hit)"
							if rel {
								query = "WITH $v AS p MATCH path=(:Start)-[n:TYPED {v:p}]->(:End) MERGE (hit:Hit) RETURN count(hit)"
							}
						}
						prefix = ""
					case "case":
						query += " SET n.v=CASE WHEN true THEN $v ELSE null END"
					}
					params := map[string]any{"v": value, "m": map[string]any{"v": value}}
					if route == "predicate" || route == "named-path" {
						r, err := exec.Execute(ctx, query, params)
						require.NoError(t, err)
						require.Equal(t, [][]any{{int64(1)}}, r.Rows)
						return
					}
					r, err := exec.Execute(ctx, prefix+query+" RETURN n.v", params)
					require.NoError(t, err)
					require.Equal(t, [][]any{{value}}, r.Rows)
					r, err = exec.Execute(ctx, read+" RETURN n.v", nil)
					require.NoError(t, err)
					require.Equal(t, [][]any{{value}}, r.Rows)
				})
			}
		}
	}
}

func TestTypedParameterNullNormalization(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nulls"))
	for _, value := range []any{(*time.Time)(nil), (*CypherDateTime)(nil), (*CypherPoint)(nil)} {
		r, err := exec.Execute(context.Background(), "RETURN $v IS NULL", map[string]any{"v": value})
		require.NoError(t, err)
		require.Equal(t, [][]any{{true}}, r.Rows)
		for _, child := range []any{map[any]any{"v": value}, map[string]*time.Time{"v": nil}, map[int]any{1: value}} {
			if typed, ok := child.(map[int]any); ok {
				normalized, changed := normalizeParameterValue(typed)
				require.True(t, changed)
				converted := normalized.(map[any]any)
				v, exists := converted[1]
				require.True(t, exists)
				require.Nil(t, v)
				_, stringKey := converted["1"]
				require.False(t, stringKey)
				continue
			}
			key := "v"
			r, err = exec.Execute(context.Background(), "WITH $m AS m MERGE (n:Nested) SET n.flag=m.child['"+key+"'] IS NULL RETURN n.flag", map[string]any{"m": map[string]any{"child": child}})
			require.NoError(t, err)
			require.Equal(t, [][]any{{true}}, r.Rows)
		}
	}
}

func TestEmbeddedTimeRetainsNamedZone(t *testing.T) {
	loc, ok := loadPinnedTemporalLocation("Europe/London")
	require.True(t, ok)
	instant := time.Unix(1792888200, 123456789).In(loc)
	for _, value := range []any{instant, &instant} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zones"))
		r, err := exec.Execute(context.Background(), "MERGE (n:Zoned) SET n += $m RETURN toString(n.v)", map[string]any{"m": map[string]any{"v": value}})
		require.NoError(t, err)
		require.Equal(t, [][]any{{"2026-10-25T01:30:00.123456789+01:00[Europe/London]"}}, r.Rows)
	}
}
