package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func propertyDurableSnapshot(t testing.TB, store *storage.NamespacedEngine) string {
	t.Helper()
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	edges, err := store.AllEdges()
	require.NoError(t, err)
	values := map[string]interface{}{}
	for _, node := range nodes {
		values["n:"+string(node.ID)] = node
	}
	for _, edge := range edges {
		values["r:"+string(edge.ID)] = edge
	}
	data, err := json.Marshal(values)
	require.NoError(t, err)
	return string(data)
}

// Extend the original 25x12x3 public matrix with grammar/template variants,
// recursive results, shared readers, and failures after a valid write prefix.
func TestPropertyMaterializationInventory(t *testing.T) {
	states := []struct {
		name, expr        string
		bad, null, object bool
	}{
		{"unresolved", "properties(v)", true, false, false},
		{"unknown", "missing_function(v)", true, false, false},
		{"missing-path", "missing.value", true, false, false},
		{"error", "1 / 0", true, false, false},
		{"null", "null", false, true, false},
		{"map", "{keep:7}", false, false, true},
		{"entity", "n", false, false, true},
		{"literal", "'properties(v)'", false, false, false},
		{"list-child", "[properties(v)]", true, false, false},
		{"coalesce-child", "coalesce(properties(v),7)", true, false, false},
		{"temporal-child", "date(properties(v))", true, false, false},
		{"registry-child", "trim(properties(v))", true, false, false},
		{"apoc-child", "apoc.map.clean(properties(v),[],[])", true, false, false},
	}
	routes := []struct{ name, query, kind string }{
		{"named-create-template", "CREATE (z:R4Made {x:%s})", "field"},
		{"reverse-rel-template", "CREATE (n)<-[z:R4Made {x:%s}]-(m)", "field"},
		{"rel-chain", "CREATE (n)-[:R4Made {ok:1}]->(:R4Made)-[:R4Made {x:%s}]->(m)", "field"},
		{"create-prefix", "CREATE (:R4Prefix {ok:1}) CREATE (:R4Made {x:%s})", "field"},
		{"merge-whole-path", "MERGE (n)-[:R4Made {x:%s}]->(:R4Made)-[:R4Made]->(m)", "identity"},
		{"merge-created-rel-action", "MERGE (n)-[z:R4Made]->(m) ON CREATE SET z.x=%s", "field"},
		{"later-assignment", "SET n.ok=1, n.x=%s", "field"},
		{"later-row-create", "UNWIND [1,2] AS i CREATE (:R4Made {x:CASE WHEN i=1 THEN 7 ELSE %s END})", "field"},
		{"later-row-merge", "UNWIND [1,2] AS i MERGE (:R4Made {id:i,x:CASE WHEN i=1 THEN 7 ELSE %s END})", "identity"},
		{"with-alias", "WITH n,r,m,%s AS p SET n.x=p", "field"},
		{"with-map-child", "WITH n,{x:%s} AS bag SET n=bag", "field"},
		{"with-projection", "WITH n,%s AS p SET n=p{.*}", "root"},
	}
	for _, route := range routes {
		for _, state := range states {
			t.Run(route.name+"/"+state.name, func(t *testing.T) {
				for round := 0; round < 3; round++ {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4_inventory")
					e := NewStorageExecutor(store)
					ctx := context.Background()
					_, err := e.Execute(ctx, "CREATE (:R4Seed {id:1,keep:7,x:9})-[:R4Seed {keep:8,x:9}]->(:R4Other)", nil)
					require.NoError(t, err)
					before := propertyDurableSnapshot(t, store)
					query := "MATCH (n:R4Seed)-[r:R4Seed]->(m) WITH n,r,m,1 AS v " + fmt.Sprintf(route.query, state.expr)
					_, err = e.Execute(ctx, query, nil)
					wantError := state.bad || route.kind == "identity" && state.null || route.kind != "root" && state.object || route.kind == "root" && !state.object
					if wantError {
						require.Error(t, err, query)
						require.Equal(t, before, propertyDurableSnapshot(t, store), query)
					} else {
						require.NoError(t, err, query)
					}
				}
			})
		}
	}
}

func TestPropertyMaterializationCheckedResults(t *testing.T) {
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, resolved := range []bool{false, true} {
		for _, evalErr := range []error{nil, fmt.Errorf("producer failed")} {
			ctx := withExpressionFailureSlot(context.Background())
			value, err := checkedPropertyResult(ctx, "input", map[string]interface{}{"x": 1}, resolved, evalErr)
			if !resolved || evalErr != nil {
				require.Error(t, err)
				require.Nil(t, value)
			} else {
				require.NoError(t, err)
				require.Equal(t, map[string]interface{}{"x": 1}, value)
			}
		}
	}
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4_results"))
	row := pipelineRow{"v": int64(1), "text": "text", "nil": nil, "bag": map[string]interface{}{}, "n": &storage.Node{ID: "n"}}
	for _, expr := range []string{"properties(v)", "nodes(v)", "unbound.path", "[properties(v)]", "{x:properties(v)}", "coalesce(properties(v),7)", "CASE WHEN true THEN properties(v) ELSE 7 END", "date(properties(v))", "toString(properties(v))", "any(x IN [1] WHERE properties(v))", "[x IN [1] | properties(v)]", "reduce(a=0,x IN [1] | properties(v))"} {
		for _, compiled := range []bool{false, true} {
			rowValueProgramsDisabled.Store(!compiled)
			for i := 0; i < 3; i++ {
				fixtureAdmission1768Context, fixtureAdmission1768 := expressionTestContext(context.Background(), expr)
				_ = fixtureAdmission1768Context
				value, err := e.checkedPropertyExpression(fixtureAdmission1768, fixtureAdmission1768Context, expr, row)
				require.Error(t, err, expr)
				require.Nil(t, value, expr)
			}
		}
	}
	rowValueProgramsDisabled.Store(false)
	nullEntities := entityRow(context.Background(), map[string]*storage.Node{"optionalNode": nil}, map[string]*storage.Edge{"optionalRel": nil})
	for _, expr := range []string{"optionalNode", "optionalNode.x", "optionalRel", "optionalRel.x"} {
		fixtureAdmission1769Context, fixtureAdmission1769 := expressionTestContext(context.Background(), expr)
		_ = fixtureAdmission1769Context
		value, err := e.checkedPropertyExpression(fixtureAdmission1769, fixtureAdmission1769Context, expr, nullEntities)
		require.NoError(t, err, expr)
		require.Nil(t, value, expr)
	}
	for _, tc := range []struct {
		expr string
		want interface{}
	}{
		{"text", "text"}, {"'properties(v)'", "properties(v)"}, {"nil", nil}, {"bag", map[string]interface{}{}},
		{"properties(n)", map[string]interface{}{}}, {"CASE WHEN false THEN properties(v) ELSE 7 END", int64(7)},
		{"coalesce(7, properties(v))", int64(7)},
		{"date(null)", nil}, {"datetime(null)", nil}, {"duration(null)", nil},
	} {
		fixtureAdmission1770Context, fixtureAdmission1770 := expressionTestContext(context.Background(), tc.expr)
		_ = fixtureAdmission1770Context
		value, err := e.checkedPropertyExpression(fixtureAdmission1770, fixtureAdmission1770Context, tc.expr, row)
		require.NoError(t, err, tc.expr)
		require.Equal(t, tc.want, value, tc.expr)
	}
}

func TestPropertyMaterializationSharedReaders(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4_readers"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:Reader {id:1,keep:7})", nil)
	require.NoError(t, err)
	for _, expr := range []string{"7", "$x", "1+6"} {
		result, err := e.Execute(ctx, "MATCH (n:Reader {keep:"+expr+"}) RETURN n.keep", map[string]interface{}{"x": int64(7)})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
	}
	store := e.storage.(*storage.NamespacedEngine)
	for _, expr := range []string{"properties(1)", "nodes(1)", "unknown_reader()", "missing.path", "1/0"} {
		before := propertyDurableSnapshot(t, store)
		_, err := e.Execute(ctx, "MATCH (n:Reader {keep:"+expr+"}) SET n.hit=true", nil)
		require.Error(t, err, expr)
		require.Equal(t, before, propertyDurableSnapshot(t, store), expr)
	}
	for _, expr := range []string{"[1.0,2.0]", "$vector"} {
		evalCtx := withExpressionFailureSlot(withParams(ctx, map[string]interface{}{"vector": []float32{1, 2}}))
		fixtureAdmission1771Context, fixtureAdmission1771 := expressionTestContext(evalCtx, expr)
		_ = fixtureAdmission1771Context
		vector, ok := e.resolveCosineQueryVector(fixtureAdmission1771, fixtureAdmission1771Context, expr)
		require.True(t, ok)
		require.Equal(t, []float32{1, 2}, vector)
		require.NoError(t, getExpressionFailure(evalCtx))
	}
	for _, expr := range []string{"unknown_vector()", "unbound.value", "properties(1)"} {
		evalCtx := withExpressionFailureSlot(ctx)
		fixtureAdmission1772Context, fixtureAdmission1772 := expressionTestContext(evalCtx, expr)
		_ = fixtureAdmission1772Context
		_, ok := e.resolveCosineQueryVector(fixtureAdmission1772, fixtureAdmission1772Context, expr)
		require.False(t, ok)
		require.Error(t, getExpressionFailure(evalCtx), expr)
	}
	inner := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	e.SetDatabaseManager(optionsTestManager{&testDatabaseManagerAdapter{manager: manager}})
	_, err = e.Execute(ctx, "CREATE DATABASE r4options OPTIONS {txLogEnrichment:'FULL'}", nil)
	require.NoError(t, err)
	for _, expr := range []string{"'DIFF'", "$mode"} {
		_, err = e.Execute(ctx, "ALTER DATABASE r4options SET OPTION txLogEnrichment "+expr, map[string]interface{}{"mode": "DIFF"})
		require.NoError(t, err)
		options, err := manager.DatabaseOptions("r4options")
		require.NoError(t, err)
		require.Equal(t, "DIFF", options["txLogEnrichment"])
	}
	for _, expr := range []string{"undefined_mode", "unknown_mode()", "1/0"} {
		before, err := manager.DatabaseOptions("r4options")
		require.NoError(t, err)
		_, err = e.Execute(ctx, "ALTER DATABASE r4options SET OPTION txLogEnrichment "+expr, nil)
		require.Error(t, err, expr)
		after, err := manager.DatabaseOptions("r4options")
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
}

func TestPropertyMaterializationBindingsAndBatch(t *testing.T) {
	for _, query := range []string{
		"CREATE (a:Binding {value:7}), (b:Binding {copy:a.value}) RETURN b.copy",
		"CREATE (a:Binding {value:7}) CREATE (b:Binding {copy:a.value}) RETURN b.copy",
		"CREATE (a:Binding {value:7}) CREATE (a)-[r:BINDING {value:a.value}]->(b:Binding {copy:a.value}) RETURN r.value",
		"WITH {value:7} AS row CREATE (a:Binding {copy:row.value}) RETURN a.copy",
	} {
		e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4_bindings"))
		result, err := e.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, query)
	}
	for _, query := range []string{
		"UNWIND $rows AS row CREATE (:Batch {id:row.id,x:coalesce(properties(row.v),7)})",
		"UNWIND $rows AS row MERGE (:Batch {id:row.id,x:coalesce(properties(row.v),7)})",
		"UNWIND $rows AS row MERGE (n:Batch {id:row.id}) ON CREATE SET n.x=coalesce(properties(row.v),7)",
		"UNWIND $rows AS row MATCH (n:Batch {id:row.id}) SET n.x=coalesce(properties(row.v),7)",
	} {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4_batch")
		e := NewStorageExecutor(store)
		ctx := context.Background()
		setup := "CREATE (:Batch {id:0,x:9})"
		if strings.Contains(query, "MATCH") {
			setup = "CREATE (:Batch {id:1,x:9}), (:Batch {id:2,x:9})"
		}
		_, err := e.Execute(ctx, setup, nil)
		require.NoError(t, err)
		before := propertyDurableSnapshot(t, store)
		_, err = e.Execute(ctx, query, map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": 1, "v": nil}, map[string]interface{}{"id": 2, "v": 1}}})
		require.Error(t, err, query)
		require.Equal(t, before, propertyDurableSnapshot(t, store), query)
	}
}

// Check the positive original matrix cells against durable values too.
func assertMaterializedValue(t *testing.T, store *storage.NamespacedEngine, route, state string) {
	t.Helper()
	var want interface{}
	switch state {
	case "literal":
		want = "properties(1)"
	case "bound-equal-text":
		want = "v"
	case "dotted":
		want = int64(7)
	case "null":
		want = nil
	default:
		return
	}
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	edges, err := store.AllEdges()
	require.NoError(t, err)
	var bags []map[string]interface{}
	rel := strings.Contains(route, "rel") || route == "foreach" || route == "unwind-create" || route == "subquery-create"
	if rel {
		for _, edge := range edges {
			if edge.Type == "R3Made" || strings.HasPrefix(route, "set-rel") {
				bags = append(bags, edge.Properties)
			}
		}
	} else {
		for _, node := range nodes {
			for _, label := range node.Labels {
				if label == "R3Made" && node.Properties["ok"] == nil || label == "R3Seed" && (strings.HasPrefix(route, "set-node") || route == "on-match") {
					if route != "merge-endpoint" || node.Properties["x"] != nil {
						bags = append(bags, node.Properties)
					}
				}
			}
		}
	}
	require.NotEmpty(t, bags, route)
	for _, bag := range bags {
		value, exists := bag["x"]
		if want == nil {
			require.False(t, exists, route)
		} else {
			require.True(t, exists, route)
			require.True(t, reflect.DeepEqual(want, value), "%s: expected %#v, got %#v", route, want, value)
		}
	}
}
