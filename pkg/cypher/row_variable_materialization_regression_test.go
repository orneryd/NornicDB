package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Shape inspection must not evaluate properties before their input row exists.
// Each case gets its own transaction so an initial failure cannot hide siblings
// behind TransactionMarkedAsFailed.
func TestRowVariableMatchMaterialization(t *testing.T) {
	ids := make([]interface{}, 22)
	ids[1], ids[21] = "b0", "b1"
	params := map[string]interface{}{
		"ids":        ids,
		"node_uuids": []string{"b0", "b1"},
		"rows": []interface{}{
			map[string]interface{}{"src": "b0", "tgt": "b1", "w": int64(1)},
			map[string]interface{}{"src": "b1", "tgt": "b0", "w": int64(21)},
		},
	}
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "transaction"
		}
		for _, tc := range []struct {
			name  string
			query string
			want  int64
		}{
			{"scalar", "UNWIND [0,1] AS i MATCH (a:CodeIntel {id:i})-[r:T]->() RETURN count(r)", 2},
			{"literal-subscript", "UNWIND [0,1] AS i MATCH (a:CodeIntel {uuid:['b0','b1'][i]})-[r:T]->() RETURN count(r)", 2},
			{"parameter-arithmetic", "UNWIND [0,1] AS i MATCH (a:CodeIntel {uuid:$ids[i * 20 + 1]})-[r:T]->() RETURN count(r)", 2},
			{"node-uuid", "UNWIND $node_uuids AS node_uuid MATCH ()-[:T]->(n:CodeIntel {uuid:node_uuid}) RETURN count(n)", 2},
			{"row-map-both-ends", "UNWIND $rows AS row MATCH (a {uuid:row.src})-[r:T]->(b {uuid:row.tgt}) RETURN count(r)", 2},
			{"row-map-and-where", "UNWIND $rows AS row MATCH (a {uuid:row.src})-[r:T]->(b {uuid:row.tgt}) WHERE r.w = row.w RETURN count(r)", 2},
			{"where-only", "UNWIND [0,1] AS i MATCH (a:CodeIntel)-[r:T]->() WHERE a.id = i RETURN count(r)", 2},
			{"where-list", "UNWIND [0,1] AS i MATCH (a:CodeIntel) WHERE a.id IN [i] RETURN count(a)", 2},
			{"where-subquery", "UNWIND [0,1] AS i MATCH (a:CodeIntel) WHERE EXISTS { MATCH (a)-[:T]->(b:CodeIntel {id:i}) } RETURN count(a)", 2},
			{"property-function", "UNWIND [0,1] AS i MATCH (a:CodeIntel {id:toInteger(i)})-[r:T]->() RETURN count(r)", 2},
			{"with-map", "WITH {id:1} AS row MATCH (n:CodeIntel {id:row.id}) RETURN count(n)", 1},
			{"relationship-map", "UNWIND [1,21] AS w MATCH ()-[r:T {w:w}]->() RETURN count(r)", 2},
			{"optional", "UNWIND [0,2] AS i OPTIONAL MATCH (a:CodeIntel {id:i})-[r:T]->() RETURN count(r)", 1},
			{"nested-list", "UNWIND [{ids:['b0','b1']}] AS row MATCH (a:CodeIntel {uuid:row.ids[0]})-[r:T]->() RETURN count(r)", 1},
			{"empty-input", "UNWIND [] AS i MATCH (a:CodeIntel {id:i})-[r:T]->() RETURN count(r)", 0},
			{"call-row", "CALL { RETURN 'b1' AS node_uuid } MATCH (a:CodeIntel {uuid:node_uuid})-[r:T]->() RETURN count(r)", 1},
			{"value-equals-expression", "UNWIND ['row'] AS row MATCH (n:RowText {id:row}) RETURN count(n)", 1},
			{"chained", "UNWIND [0,1] AS i MATCH (a:CodeIntel {id:i})-[:T]->()-[:T]->() RETURN count(*)", 2},
			{"incoming", "UNWIND [0,1] AS i MATCH ()<-[r:T]-(a:CodeIntel {id:i}) RETURN count(r)", 2},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_materialization"))
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (a:CodeIntel {id:0,uuid:'b0'})-[:T {w:1}]->(b:CodeIntel {id:1,uuid:'b1'}), (b)-[:T {w:21}]->(a), (:RowText {id:'row'})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, tc.query, params)
				require.NoError(t, err, tc.query)
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestRowVariableMatchMaterializationErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_materialization_errors"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CodeIntel {id:0})-[:T]->()", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "UNWIND [0] AS i MATCH (a:CodeIntel {id:1 / i})-[r:T]->() RETURN count(r)", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArithmeticError")
	result, err := exec.Execute(ctx, "MATCH (a:CodeIntel)-[r:T]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// A valid but volatile property producer must not run during inspection,
// including when an empty input prevents MATCH execution entirely.
func TestRowVariableMatchMaterializationDefersProducers(t *testing.T) {
	for _, pattern := range []string{
		"(a:CodeIntel {id:inspection.value()})-[r:T]->()",
		"()-[r:T {w:inspection.value()}]->()",
		"(a:CodeIntel {id:inspection.value()})-[:T]->()-[:T]->()",
		"(a:CodeIntel {id:inspection.value()})",
		"(:CodeIntel {id:inspection.value()})",
		"(a:CodeIntel {id:inspection.value()}), (b:CodeIntel {id:0})",
		"()-[r:T*1 {w:inspection.value()}]->()",
	} {
		t.Run(pattern, func(t *testing.T) {
			calls := 0
			withPluginFunctions(t, map[string]interface{}{
				"inspection.value": func() int64 { calls++; return 1 },
			})
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "inspection_producer"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (a:CodeIntel {id:0})-[:T {w:1}]->(b:CodeIntel {id:1}), (b)-[:T {w:2}]->(a)", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "UNWIND [] AS i MATCH "+pattern+" RETURN count(*)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			require.Zero(t, calls, "inspection must not invoke property producers")
			result, err = exec.Execute(ctx, "UNWIND [0] AS i MATCH "+pattern+" RETURN count(*)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			require.Equal(t, 1, calls, "one materialization per live input row")
		})
	}
}

// A bound relationship list admits the pattern by shape, then reads endpoint
// properties from each row. WHERE must see the complete joined row as well.
func TestRowVariableBoundRelationshipListMaterialization(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "transaction"
		}
		t.Run(mode, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "bound_list_materialization"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (a:CodeIntel {id:0})-[:T]->(b:CodeIntel {id:1}), (b)-[:T]->(a)", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, tc := range []struct {
				value string
				where string
				want  int64
			}{
				{"a.id", "", 2},
				{"a.id + 10", "", 0},
				{"a.id", " WHERE start.id = 1", 1},
			} {
				query := "MATCH (a:CodeIntel)-[r:T]->() WITH [r] AS relationships, " + tc.value +
					" AS wanted MATCH (start:CodeIntel {id:wanted})-[relationships*]->()" + tc.where + " RETURN count(start)"
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, query)
			}
			if explicit {
				_, err = exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
