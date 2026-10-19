package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cross product of the admitted single-hop scans: direction, label/type
// access path and every row representation consumed after a streamed MATCH.
// The same public statements run with streaming and reference materialization.
func queryMemoryDifferentialQueries() []string {
	var queries []string
	for _, nodes := range [][2]string{{"(a)", "(b)"}, {"(a:Fact)", "(b:Fact)"}} {
		for _, rel := range []string{"[r]", "[r:REL]", "[r:REL|OTHER]"} {
			for _, direction := range [][2]string{{"-", "->"}, {"<-", "-"}, {"-", "-"}} {
				pattern := nodes[0] + direction[0] + rel + direction[1] + nodes[1]
				for _, projection := range []string{
					"RETURN r", "RETURN a, b", "RETURN type(r)",
					"RETURN startNode(r).id, endNode(r).id", "WITH r RETURN count(r)",
				} {
					queries = append(queries, "MATCH "+pattern+" "+projection)
				}
				queries = append(queries, "MATCH p="+pattern+" RETURN p")
			}
		}
	}
	return queries
}

// Stored and in-memory entities can carry different Go time.Location and
// monotonic-clock representations for the same timestamp. Compare their
// serialized values, preserving all entity fields, rather than Go internals.
func queryMemoryCanonicalRows(t *testing.T, rows [][]interface{}) []string {
	t.Helper()
	out := make([]string, len(rows))
	for index, row := range rows {
		values := make([]interface{}, len(row))
		for index, value := range row {
			if path, ok := value.(map[string]interface{}); ok && path["_pathResult"] != nil {
				nodes, relationships, _, _ := pathValueParts(path)
				value = map[string]interface{}{"nodes": nodes, "relationships": relationships, "length": path["length"]}
			}
			values[index] = value
		}
		encoded, err := json.Marshal(values)
		require.NoError(t, err)
		out[index] = string(encoded)
	}
	return out
}
func TestQueryMemoryTraversalDifferential(t *testing.T) {
	for _, mode := range []string{"committed", "write_behind", "promotion", "suppression"} {
		for _, transaction := range []string{"autocommit", "snapshot", "pending"} {
			t.Run(mode+"/"+transaction, func(t *testing.T) {
				ctx := context.Background()
				var exec *StorageExecutor
				var base *storage.BadgerEngine
				if mode == "promotion" || mode == "suppression" {
					exec, base = queryMemoryPolicyFixture(t)
					if mode == "suppression" {
						require.NoError(t, base.DeleteEdge("policy:a-edge"))
					}
					// Internal-node visibility has a separate public matrix; this
					// differential isolates policy and transaction query views.
					require.NoError(t, base.DeleteEdge("policy:internal-edge"))
					if mode == "promotion" {
						require.NoError(t, base.CreateEdge(&storage.Edge{ID: "policy:a-edge-2", StartNode: "policy:a", EndNode: "policy:b", Type: "REL", Properties: map[string]interface{}{"marker": "visible"}}))
					}
					_, err := base.GetNode("policy:a")
					require.NoError(t, err, "promotion control")
					_, err = base.GetNode("policy:expired")
					require.ErrorIs(t, err, storage.ErrNotFound, "suppression control")
				} else {
					var err error
					base, err = storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
						InMemory: true, WriteBehind: mode == "write_behind", WriteBehindInterval: time.Hour,
					})
					require.NoError(t, err)
					t.Cleanup(func() { _ = base.Close() })
					exec = NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(base, "test"), 0, 0)
					_, err = exec.Execute(ctx, "CREATE (:Fact {id:'a'})-[:REL {marker:'visible'}]->(:Fact {id:'b'})", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "MATCH (a:Fact {id:'a'}), (b:Fact {id:'b'}) CREATE (a)-[:REL {marker:'visible'}]->(b)", nil)
					require.NoError(t, err)
				}
				if transaction != "autocommit" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					if transaction == "pending" {
						_, err = exec.Execute(ctx, "CREATE (:Fact {id:'c',promote:true})-[:OTHER {marker:'visible'}]->(:Fact {id:'d',promote:true})", nil)
						require.NoError(t, err)
					}
				}
				materialized := context.WithValue(ctx, traversalMaterializeKey{}, true)
				for _, query := range queryMemoryDifferentialQueries() {
					t.Run(query, func(t *testing.T) {
						control, err := exec.Execute(materialized, query, nil)
						require.NoError(t, err)
						result, err := exec.Execute(ctx, query, nil)
						require.NoError(t, err)
						require.Equal(t, control.Columns, result.Columns)
						require.ElementsMatch(t, queryMemoryCanonicalRows(t, control.Rows), queryMemoryCanonicalRows(t, result.Rows))
						queryMemoryOutputCheck(t, control.Rows)
						// Exercise the PUBLIC stream as well as lazy MATCH rows. The
						// two visible edges cross threshold1 for eligible nonaggregate
						// auto-commit reads. Transactions, aggregates and empty results
						// retain ResultStream's existing materialized contract.
						for _, streamCtx := range []context.Context{ctx, materialized} {
							streamCtx, cancel := context.WithCancel(streamCtx)
							run := executeStreamed(streamCtx, exec, query, 1)
							var streamedRows [][]interface{}
							if run.stream.streamed() {
								for {
									batch, done := run.stream.Next(1)
									streamedRows = append(streamedRows, batch...)
									if done {
										break
									}
								}
							}
							<-run.stream.Done()
							cancel()
							require.NoError(t, run.err)
							if !run.stream.streamed() {
								streamedRows = run.result.Rows
							} else {
								require.Equal(t, control.Columns, run.stream.Columns())
							}
							if transaction == "autocommit" && len(control.Rows) > 1 {
								require.True(t, run.stream.streamed(), "eligible query must actually cross threshold1")
							}
							queryMemoryOutputCheck(t, streamedRows)
							require.ElementsMatch(t, queryMemoryCanonicalRows(t, control.Rows), queryMemoryCanonicalRows(t, streamedRows))
						}
						// Compare public logical operators as well as the physical
						// storage probes in TestQueryMemorySelectiveAccessSkipsDecoyEdges.
						controlPlan, err := exec.Execute(materialized, "EXPLAIN "+query, nil)
						require.NoError(t, err)
						plan, err := exec.Execute(ctx, "EXPLAIN "+query, nil)
						require.NoError(t, err)
						require.Equal(t, collectOperatorTypes(controlPlan.Metadata["plan"].(*ExecutionPlan).Root), collectOperatorTypes(plan.Metadata["plan"].(*ExecutionPlan).Root))
					})
				}
				query := "MATCH (a:Fact)-[r:REL]->(b:Fact) RETURN r"
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				want := 2
				if mode == "suppression" {
					want = 0
				}
				require.Len(t, result.Rows, want, fmt.Sprintf("%s/%s visibility", mode, transaction))
			})
		}
	}
}
