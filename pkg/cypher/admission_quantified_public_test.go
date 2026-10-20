package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// These are public witnesses of the singleton/list boundary. Pattern admission
// checks each iteration once; later function/operator checks see only the
// admitted remainder. The original native intervals still identify every read.
func TestAdmissionQuantifiedOperandPublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "call", "use", "composite"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/norm_%v/tx_%v", route, normalize, explicit), func(t *testing.T) {
					exec := newFabricCallRowsExecutor(t)
					seed := "CREATE (:N {id:'a'})-[:R {id:'r1'}]->(:N {id:'b'})-[:R {id:'r2'}]->(:N {id:'c'})"
					if route == "use" || route == "composite" {
						seed = "USE comp.one " + seed
					}
					_, err := exec.Execute(ctx, seed, nil)
					require.NoError(t, err)
					if route == "use" || route == "composite" {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					for _, sample := range []struct {
						predicate, tail string
						rows            [][]interface{}
					}{
						{"NOT startNode(r).id IN l", "RETURN count(p) AS value", [][]interface{}{{int64(3)}}},
						{"m[startNode(r).id] IS NULL AND endNode(r).id <> 'x'", "RETURN count(p) AS value", [][]interface{}{{int64(3)}}},
						{"[x IN l WHERE x = r.id] = []", "RETURN count(p) AS value", [][]interface{}{{int64(3)}}},
						{"reduce(acc = true, k IN [r.id] | acc AND m[k] IS NULL)", "RETURN count(p) AS value", [][]interface{}{{int64(3)}}},
						{"COUNT { MATCH (u)-[r]->(v) } = 1", "RETURN count(p) AS value", [][]interface{}{{int64(3)}}},
						{"CASE WHEN m[r.id] IS NULL THEN true ELSE false END", "RETURN size(r) AS value ORDER BY value", [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}}},
						{"NOT startNode(r).id IN l", "RETURN [e IN r | e.id] AS value ORDER BY size(value)", [][]interface{}{{[]interface{}{}}, {[]interface{}{"r1"}}, {[]interface{}{"r1", "r2"}}}},
					} {
						query := "WITH {x:true} AS m, ['x'] AS l MATCH (s:N {id:'a'}) MATCH p = (s)(()-[r:R WHERE " + sample.predicate + "]->())*(t) " + sample.tail
						if route == "call" {
							query = "CALL { " + query + " } RETURN value"
						}
						if route == "use" {
							query = "USE comp.one " + query
						}
						if route == "composite" {
							query = "USE comp CALL { USE comp.one " + query + " } RETURN value"
						}
						for repeat := 0; repeat < 2; repeat++ {
							result, err := exec.Execute(ctx, query, nil)
							require.NoError(t, err, query)
							require.Equal(t, []string{"value"}, result.Columns, query)
							require.Equal(t, sample.rows, result.Rows, query)
						}
					}
				})
			}
		}
	}
}

func TestAdmissionSelectedProjectionPublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "call", "use", "composite"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/norm_%v/tx_%v", route, normalize, explicit), func(t *testing.T) {
					exec := newFabricCallRowsExecutor(t)
					query := "UNWIND ['abc','bbbbbbb','x'] AS s RETURN size(s) AS s ORDER BY size(s) DESC"
					if route == "call" {
						query = "CALL { " + query + " } RETURN s AS value"
					}
					if route == "use" || route == "composite" {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					if route == "use" {
						query = "USE comp.one " + query
					}
					if route == "composite" {
						query = "USE comp CALL { USE comp.one " + query + " } RETURN s AS value"
					}
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]interface{}{{int64(7)}, {int64(3)}, {int64(1)}}, result.Rows, query)
				})
			}
		}
	}
}
