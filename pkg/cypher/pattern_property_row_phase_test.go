package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A property expression belongs to its syntactic occurrence and input row.
// Successive values expose both eager admission and repeated materialization.
func TestPatternPropertiesEvaluateOncePerRow(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "transaction"
		}
		for _, tc := range []struct {
			name  string
			query string
			rows  [][]interface{}
			calls int64
		}{
			{"node", "MATCH (a:CodeIntel {id:inspection.value()}) RETURN a.id", [][]interface{}{{int64(1)}}, 1},
			{"node-count", "MATCH (a:CodeIntel {id:inspection.value()}) RETURN count(a)", [][]interface{}{{int64(1)}}, 1},
			{"relationship", "MATCH (a:CodeIntel {id:inspection.value()})-[r:T]->() RETURN a.id", [][]interface{}{{int64(1)}}, 1},
			{"chain", "MATCH (a:CodeIntel {id:inspection.value()})-[:T]->()-[:T]->() RETURN a.id", [][]interface{}{{int64(1)}}, 1},
			{"incoming", "MATCH ()<-[:T]-(a:CodeIntel {id:inspection.value()}) RETURN a.id", [][]interface{}{{int64(1)}}, 1},
			{"unwind", "UNWIND [1,2] AS wanted MATCH (a:CodeIntel {id:inspection.value()})-[:T]->() RETURN a.id ORDER BY a.id", [][]interface{}{{int64(1)}, {int64(2)}}, 2},
			{"unwind-projection", "UNWIND [1,2] AS wanted MATCH (a:CodeIntel {id:inspection.value()})-[:T]->() RETURN a.id", [][]interface{}{{int64(1)}, {int64(2)}}, 2},
			{"unwind-end-projection", "UNWIND [1,2] AS wanted MATCH ()-[:T]->(a:CodeIntel {id:inspection.value()}) RETURN a.id", [][]interface{}{{int64(1)}, {int64(2)}}, 2},
			{"optional", "UNWIND [1,2] AS wanted OPTIONAL MATCH (a:CodeIntel {id:inspection.value()})-[:T]->() RETURN count(a)", [][]interface{}{{int64(2)}}, 2},
			{"optional-node", "UNWIND [1,2] AS wanted OPTIONAL MATCH (a:CodeIntel {id:inspection.value()}) RETURN count(a)", [][]interface{}{{int64(2)}}, 2},
			{"traversal-predicate", "MATCH (a:CodeIntel)-[r:T]->() WHERE (a)-[:T {w:inspection.value()}]->() RETURN count(r)", [][]interface{}{{int64(3)}}, 3},
			{"delete-bound-relationship", "MATCH (a:CodeIntel {id:inspection.value()}) MATCH (a)-[r:T {w:a.id + 1}]->() DELETE r RETURN count(r)", [][]interface{}{{int64(1)}}, 1},
			{"delete-limit", "MATCH (a:CodeIntel {id:inspection.value()}) WITH a LIMIT 1 DETACH DELETE a RETURN count(a)", [][]interface{}{{int64(1)}}, 1},
			{"merge-existing-relationship", "MATCH (a:CodeIntel {id:0}), (b:CodeIntel {id:1}) MERGE (a)-[r:T {w:inspection.value()}]->(b) RETURN r.w", [][]interface{}{{int64(1)}}, 1},
			{"merge-bound-relationship", "MATCH (a:CodeIntel {id:0}), (b:CodeIntel {id:1}) MERGE (a)-[r:Produced {w:inspection.value()}]->(b) RETURN r.w", [][]interface{}{{int64(1)}}, 1},
			{"merge-bound-relationship-rows", "MATCH (a:CodeIntel {id:0}), (b:CodeIntel {id:1}) UNWIND [1,2] AS wanted MERGE (a)-[r:Produced {w:inspection.value()}]->(b) RETURN r.w ORDER BY r.w", [][]interface{}{{int64(1)}, {int64(2)}}, 2},
			{"merge-unbound-relationship", "MERGE (a:Produced {id:inspection.value()})-[:Produced]->(b:Produced {id:10}) RETURN a.id", [][]interface{}{{int64(1)}}, 1},
			{"merge-chain", "MERGE (a:Produced {id:inspection.value()})-[:Produced]->(:Produced {id:10})-[:Produced]->(:Produced {id:11}) RETURN a.id", [][]interface{}{{int64(1)}}, 1},
			{"subquery", "MATCH (a:CodeIntel {id:inspection.value()}) CALL { WITH a RETURN a.id AS wanted } RETURN wanted", [][]interface{}{{int64(1)}}, 1},
			{"predicate", "MATCH (a:CodeIntel) WHERE (a)-[:T {w:inspection.value()}]->() RETURN count(a)", [][]interface{}{{int64(3)}}, 3},
			{"create", "UNWIND [1,2] AS wanted CREATE (a:Produced {id:inspection.value()}) RETURN a.id ORDER BY a.id", [][]interface{}{{int64(1)}, {int64(2)}}, 2},
			{"merge-create", "UNWIND [1,2] AS wanted MERGE (a:Produced {id:inspection.value()}) RETURN a.id ORDER BY a.id", [][]interface{}{{int64(1)}, {int64(2)}}, 2},
			{"merge-match", "UNWIND [1,2] AS wanted MERGE (a:CodeIntel {id:inspection.value()}) RETURN a.id ORDER BY a.id", [][]interface{}{{int64(1)}, {int64(2)}}, 2},
			{"distinct-positions", "MATCH (a:CodeIntel {id:inspection.value()})-[:T]->(b:CodeIntel {id:inspection.value()}) RETURN a.id,b.id", [][]interface{}{{int64(1), int64(2)}}, 2},
			{"distinct-clauses", "MATCH (a:CodeIntel {id:inspection.value()}) WITH a.id AS first MATCH (a:CodeIntel {id:inspection.value()}) RETURN first,a.id", [][]interface{}{{int64(1), int64(2)}}, 2},
			{"anonymous-product", "MATCH (:CodeIntel {id:inspection.value()}), (:CodeIntel {id:inspection.value()}) RETURN count(*)", [][]interface{}{{int64(1)}}, 2},
			{"bound-list", "MATCH (a:CodeIntel)-[r:T]->() WITH [r] AS relationships MATCH (start:CodeIntel {id:inspection.value()-1})-[relationships*]->() RETURN count(start)", [][]interface{}{{int64(3)}}, 3},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				var calls int64
				withPluginFunctions(t, map[string]interface{}{
					"inspection.value": func() int64 { calls++; return calls },
				})
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_row_phase"))
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (a:CodeIntel {id:0})-[:T {w:1}]->(b:CodeIntel {id:1}), (b)-[:T {w:2}]->(c:CodeIntel {id:2}), (c)-[:T {w:3}]->(a)", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.calls, calls, "each occurrence must evaluate once per input row")
				require.Equal(t, tc.rows, result.Rows, tc.query)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}

// A traversal WHERE sees matched rows, not candidate start nodes. Branching
// and empty traversals distinguish those two populations even when every
// reference in the predicate names the start variable.
func TestPatternPropertyTraversalPredicateRowCardinality(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "transaction"
		}
		for _, fixture := range []struct {
			name  string
			setup string
			rows  int64
		}{
			{"empty", "CREATE (:PredicateSource {id:0})", 0},
			{"fanout", "CREATE (a:PredicateSource {id:0})-[:T {w:1}]->(:PredicateTarget), (a)-[:T {w:1}]->(:PredicateTarget), (a)-[:T {w:1}]->(:PredicateTarget)", 3},
		} {
			for _, predicate := range []string{
				"(a)-[:T {w:inspection.value()}]->()",
				"exists((a)-[:T {w:inspection.value()}]->())",
				"(a)-[:T {w:inspection.value()}]->() AND a.id >= 0",
				"a.id >= 0 AND (a)-[:T {w:inspection.value()}]->()",
			} {
				t.Run(mode+"/"+fixture.name+"/"+predicate, func(t *testing.T) {
					var calls int64
					withPluginFunctions(t, map[string]interface{}{
						"inspection.value": func() int64 { calls++; return 1 },
					})
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "predicate_rows"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, fixture.setup, nil)
					require.NoError(t, err)
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					result, err := exec.Execute(ctx, "MATCH (a:PredicateSource)-[r:T]->() WHERE "+predicate+" RETURN count(r)", nil)
					require.NoError(t, err)
					require.Equal(t, fixture.rows, calls, "property producer runs once per matched row")
					require.Equal(t, [][]interface{}{{fixture.rows}}, result.Rows)
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestPatternPropertiesUseCompleteRow(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "transaction"
		}
		for _, query := range []string{
			"UNWIND [1,2] AS wanted MATCH (a:CodeIntel) WHERE (a)-[:T {w:wanted}]->() RETURN count(a)",
			"MATCH (a:CodeIntel) WHERE (a)-[:T {w:a.id + 1}]->() RETURN count(a)",
			"UNWIND [{wanted:1},{wanted:2}] AS row MATCH (a:CodeIntel) WHERE (a)-[:T {w:row.wanted}]->() RETURN count(a)",
			"UNWIND [1,2] AS wanted MATCH (a:CodeIntel) WHERE exists((a)-[:T {w:wanted}]->()) RETURN count(a)",
			"UNWIND [1,2] AS wanted MATCH (a:CodeIntel) WHERE (a)-[:T {w:wanted}]->() AND a.id >= 0 RETURN count(a)",
		} {
			t.Run(mode+"/"+query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_complete_row"))
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (a:CodeIntel {id:0})-[:T {w:1}]->(b:CodeIntel {id:1}), (b)-[:T {w:2}]->(a)", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, query)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}

// A syntax plan must not capture row values or collide with real parameters,
// including names absent from the pattern but used by its later RETURN.
func TestPatternPropertyPreparedParametersStayPerExecution(t *testing.T) {
	var calls int64
	withPluginFunctions(t, map[string]interface{}{
		"inspection.value": func() int64 { calls++; return calls },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prepared_parameters"))
	ctx := context.Background()
	pattern := "UNWIND [1,2] AS wanted MERGE (n:PreparedProperty {id:inspection.value()})"
	_, err := exec.Execute(ctx, pattern+" RETURN n.id", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), calls)
	for _, markers := range [][2]int64{{7, 8}, {70, 80}} {
		calls = 0
		result, err := exec.Execute(ctx, pattern+" RETURN n.id,$__nornic_pattern_property_0,$__nornic_pattern_property_1", map[string]interface{}{
			"__nornic_pattern_property_0": markers[0],
			"__nornic_pattern_property_1": markers[1],
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), calls)
		require.Equal(t, [][]interface{}{{int64(1), markers[0], markers[1]}, {int64(2), markers[0], markers[1]}}, result.Rows)
		require.Zero(t, result.Stats.NodesCreated)
	}
}
