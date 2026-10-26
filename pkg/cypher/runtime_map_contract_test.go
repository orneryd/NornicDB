package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type contractNamedMap map[string]interface{}

// Exercise the public writers, including aliases and batch routes, rather than
// merely checking what a decoder returns. NULL roots are errors for both SET
// operators in Neo4j 5.26.30; NULL fields remove/omit properties. Failed
// expressions are errors everywhere, including scalar and pattern properties.
func TestRuntimeMapContract(t *testing.T) {
	for _, bundled := range []bool{false, true} {
		t.Run(fmt.Sprintf("bundled=%v", bundled), func(t *testing.T) {
			functions := map[string]interface{}{}
			if bundled {
				functions = bundledAPOCPluginFunctions(t)
			}
			functions["contract.nilMap"] = func() map[string]interface{} { return nil }
			functions["contract.namedNil"] = func() contractNamedMap { return nil }
			functions["contract.nested"] = func() interface{} { return map[string]interface{}{"v": contractNamedMap(nil)} }
			functions["contract.failed"] = func() (map[string]interface{}, error) {
				return map[string]interface{}{"bad": 1}, fmt.Errorf("deliberate producer failure")
			}
			withPluginFunctions(t, functions)
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "contract"))
			ctx := context.Background()
			producers := []struct {
				name, expr string
				params     map[string]interface{}
				failed     bool
			}{
				{"literal", "null", nil, false},
				{"nil-map", "$p", map[string]interface{}{"p": map[string]interface{}(nil)}, false},
				{"nil-interface-map", "$p", map[string]interface{}{"p": map[interface{}]interface{}(nil)}, false},
				{"named-map", "$p", map[string]interface{}{"p": contractNamedMap(nil)}, false},
				{"nested-map", "$p.v", map[string]interface{}{"p": map[string]interface{}{"v": contractNamedMap(nil)}}, false},
				{"nested-list", "$p[0]", map[string]interface{}{"p": []interface{}{contractNamedMap(nil)}}, false},
				{"missing", "n.absent", nil, false},
				{"case", "CASE WHEN true THEN apoc.convert.fromJsonMap('null') ELSE {} END", nil, false},
				{"head", "head([apoc.convert.fromJsonMap('null')])", nil, false},
				{"json", "apoc.convert.fromJsonMap('null')", nil, false},
				{"whitespace", "apoc.convert.fromJsonMap(' null ')", nil, false},
				{"empty-text", "apoc.convert.fromJsonMap('')", nil, false},
				{"json-param", "apoc.convert.fromJsonMap($text)", map[string]interface{}{"text": "null"}, false},
				{"stored", "apoc.convert.fromJsonMap(n.json)", nil, false},
				{"roundtrip", "apoc.convert.fromJsonMap(apoc.convert.toJson(null))", nil, false},
				{"list-null", "apoc.convert.fromJsonList('null')", nil, false},
				{"list-null-arg", "apoc.convert.fromJsonList(null)", nil, false},
				{"properties", "properties(apoc.convert.fromJsonMap('null'))", nil, false},
				{"plugin", "contract.nilMap()", nil, false},
				{"plugin-named", "contract.namedNil()", nil, false},
				{"plugin-nested", "contract.nested().v", nil, false},
				{"json-number", "apoc.convert.fromJsonMap('1')", nil, true},
				{"json-list", "apoc.convert.fromJsonMap('[]')", nil, true},
				{"json-string", "apoc.convert.fromJsonMap('\"x\"')", nil, true},
				{"json-malformed", "apoc.convert.fromJsonMap('{')", nil, true},
				{"list-map", "apoc.convert.fromJsonList('{}')", nil, true},
				{"plugin-error", "contract.failed()", nil, true},
			}
			prefix := "MATCH (n:Contract)-[r:CONTRACT]->(m:Other) "
			for _, p := range producers {
				t.Run(p.name, func(t *testing.T) {
					consumers := map[string]string{}
					for _, target := range []string{"n", "r"} {
						for _, op := range []string{"=", "+="} {
							id := target + op
							consumers["direct-"+id] = prefix + "SET " + target + " " + op + " " + p.expr
							consumers["alias-"+id] = prefix + "WITH n,r,m," + p.expr + " AS v SET " + target + " " + op + " v"
							consumers["projection-"+id] = prefix + "WITH n,r,m," + p.expr + " AS v SET " + target + " " + op + " v{.*}"
							consumers["unwind-"+id] = prefix + "UNWIND [{v:" + p.expr + "}] AS row SET " + target + " " + op + " row.v"
							consumers["foreach-"+id] = prefix + "FOREACH (i IN [1] | SET " + target + " " + op + " " + p.expr + ")"
							consumers["on-match-"+id] = prefix + "MERGE (z:Contract {id:1}) ON MATCH SET " + target + " " + op + " " + p.expr
						}
					}
					consumers["create-set"] = prefix + "CREATE (z:Made {keep:9}) SET z = " + p.expr
					consumers["on-create"] = prefix + "MERGE (z:Made {id:9}) ON CREATE SET z = " + p.expr
					consumers["subquery"] = prefix + "CALL { WITH n,r,m SET n = " + p.expr + " } RETURN n"
					consumers["scalar-node"] = prefix + "SET n.x = " + p.expr
					consumers["scalar-rel"] = prefix + "SET r.x = " + p.expr
					consumers["dynamic-node"] = prefix + "SET n['x'] = " + p.expr
					consumers["dynamic-rel"] = prefix + "SET r['x'] = " + p.expr
					consumers["create-field"] = prefix + "WITH n,r,m," + p.expr + " AS fieldValue CREATE (:Made {value:fieldValue})"
					// The existing CREATE validator does not accept a dotted
					// function-result postfix directly; its alias is supported.
					if p.name != "plugin-nested" {
						consumers["create-field-direct"] = prefix + "CREATE (:Made {value:" + p.expr + "})"
					}
					consumers["merge-field"] = prefix + "MERGE (:Made {value:" + p.expr + "})"
					consumers["return"] = prefix + "RETURN " + p.expr + " AS value"
					for name, query := range consumers {
						t.Run(name, func(t *testing.T) {
							// Repeated execution exercises cold and compiled row programs.
							for round := 0; round < 3; round++ {
								_, err := e.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
								require.NoError(t, err)
								_, err = e.Execute(ctx, "CREATE (:Contract {id:1,keep:1,x:7,json:'null'})-[:CONTRACT {keep:2,x:8}]->(:Other)", nil)
								require.NoError(t, err)
								result, err := e.Execute(ctx, query, p.params)
								scalar := strings.HasPrefix(name, "scalar-") || strings.HasPrefix(name, "dynamic-")
								wantError := p.failed || (!scalar && !strings.HasPrefix(name, "create-field") && name != "return")
								if wantError {
									require.Error(t, err, "round %d: %s", round, query)
								} else {
									require.NoError(t, err, query)
									if name == "return" {
										require.Len(t, result.Rows, 1)
										require.Nil(t, result.Rows[0][0])
									}
								}
								state, stateErr := e.Execute(ctx, "MATCH (n:Contract)-[r:CONTRACT]->() RETURN properties(n),properties(r)", nil)
								require.NoError(t, stateErr)
								require.Len(t, state.Rows, 1)
								wantNode := map[string]interface{}{"id": int64(1), "keep": int64(1), "x": int64(7), "json": "null"}
								wantRel := map[string]interface{}{"keep": int64(2), "x": int64(8)}
								if scalar && !p.failed {
									if strings.HasSuffix(name, "node") {
										delete(wantNode, "x")
									} else {
										delete(wantRel, "x")
									}
								}
								require.Equal(t, wantNode, state.Rows[0][0])
								require.Equal(t, wantRel, state.Rows[0][1])
								made, madeErr := e.Execute(ctx, "MATCH (z:Made) RETURN properties(z)", nil)
								require.NoError(t, madeErr)
								if strings.HasPrefix(name, "create-field") && !p.failed {
									require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, made.Rows)
								} else {
									require.Empty(t, made.Rows)
								}
							}
						})
					}
				})
			}
		})
	}
}

func TestRuntimeMapContractControls(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "controls"))
	ctx := context.Background()
	for _, expr := range []string{"{}", "$p", "m", "properties(m)", "m{.*}", "coalesce(apoc.convert.fromJsonMap('null'),{})"} {
		for _, target := range []string{"n", "r"} {
			for _, op := range []string{"=", "+="} {
				_, err := e.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, "CREATE (:Contract {keep:1})-[:CONTRACT {keep:2}]->(:Other)", nil)
				require.NoError(t, err)
				r, err := e.Execute(ctx, "MATCH (n:Contract)-[r:CONTRACT]->(m:Other) SET "+target+" "+op+" "+expr+" RETURN properties("+target+")", map[string]interface{}{"p": map[string]interface{}{}})
				require.NoError(t, err, expr)
				if op == "=" {
					require.Equal(t, map[string]interface{}{}, r.Rows[0][0], expr)
				} else {
					require.NotEmpty(t, r.Rows[0][0], expr)
				}
			}
		}
	}
	for _, value := range []interface{}{nil, map[string]interface{}(nil), map[interface{}]interface{}(nil), contractNamedMap(nil), map[string]interface{}{}, map[string]interface{}{"x": nil}} {
		for _, query := range []string{"CREATE (z:Made $p)", "CREATE (:Made)-[:NEW $p]->(:Made)"} {
			_, err := e.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
			require.NoError(t, err)
			_, err = e.Execute(ctx, query, map[string]interface{}{"p": value})
			empty, ok := value.(map[string]interface{})
			valid := ok && empty != nil
			if valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestRuntimeMapContractEvaluation(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "evaluation"))
	// Supply raw row bindings as well as public parameters: compiled child
	// nodes must normalize their results before coalesce/projection consumes them.
	row := pipelineRow{"v": contractNamedMap(nil), "m": map[string]interface{}{"v": map[string]interface{}(nil)}}
	for _, disabled := range []bool{true, false} {
		rowValueProgramsDisabled.Store(disabled)
		t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
		for round := 0; round < 3; round++ {
			for _, expr := range []string{"v", "m.v", "properties(v)", "v{.*}", "CASE WHEN true THEN v ELSE {} END", "head([v])"} {
				value, resolved, err := e.evaluateRowValue(originalBraceView(expr), row)
				require.NoError(t, err, expr)
				require.True(t, resolved, expr)
				require.Nil(t, value, expr)
			}
			value, resolved, err := e.evaluateRowValue(expressionBraceView("coalesce(m.v,v,{fallback:1})"), row)
			require.NoError(t, err)
			require.True(t, resolved)
			require.Equal(t, map[string]interface{}{"fallback": int64(1)}, value)
		}
	}
	for _, query := range []string{
		"RETURN coalesce({},apoc.convert.fromJsonMap('{')) AS v",
		"RETURN CASE WHEN true THEN {} ELSE apoc.convert.fromJsonMap('{') END AS v",
	} {
		result, err := e.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{}, result.Rows[0][0])
	}
	for _, entity := range []interface{}{&storage.Node{}, &storage.Edge{}, storage.RemoteEntityValue{"elementId": "remote", "properties": map[string]interface{}(nil)}} {
		for _, expr := range []string{"properties($p)", "v{.*}"} {
			result, err := e.Execute(context.Background(), "WITH $p AS v RETURN "+expr+" AS value", map[string]interface{}{"p": entity})
			require.NoError(t, err)
			require.Equal(t, map[string]interface{}{}, result.Rows[0][0])
		}
	}
}

// Dynamic reads must keep the same NULL/list distinction when enclosing forms
// are compiled without redundant result checks. Bindings may shadow even a
// scalar expression, and head/last may expose a raw typed nil inside a list.
func TestRuntimeMapContractDynamicCompiledResults(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "compiled_sources"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, tc := range []struct {
		name string
		raw  interface{}
		want interface{}
	}{
		{"map_null", map[string]interface{}(nil), nil},
		{"named_map_null", contractNamedMap(nil), nil},
		{"node_null", (*storage.Node)(nil), nil},
		{"edge_null", (*storage.Edge)(nil), nil},
		{"nil_list", []string(nil), []string(nil)},
		{"empty_map", map[string]interface{}{}, map[string]interface{}{}},
		{"empty_entity", &storage.Node{}, &storage.Node{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := pipelineRow{"v": tc.raw, "m": map[string]interface{}{"v": tc.raw}, "xs": []interface{}{tc.raw}}
			forms := []string{
				"v", "m.v", "head(xs)", "last(xs)", "xs[0]", "(v)",
				"CASE WHEN true THEN v ELSE 7 END", "coalesce(NULL,v)",
				"reduce(a=v,i IN [] | i)", "[i IN xs | i][0]", "{x:v}.x",
			}
			for _, expr := range []string{"v IS NULL", "1 + 2", "size(xs)", "[7]", "{x:7}"} {
				row[expr] = tc.raw
				forms = append(forms, expr)
			}
			for _, disabled := range []bool{true, false} {
				rowValueProgramsDisabled.Store(disabled)
				for round := 0; round < 3; round++ {
					for _, expr := range forms {
						value, resolved, err := e.evaluateRowValue(originalBraceView(expr), row)
						require.NoError(t, err, "expr=%s text=%v round=%d", expr, disabled, round)
						require.True(t, resolved, "expr=%s text=%v round=%d", expr, disabled, round)
						require.Equal(t, tc.want, value, "expr=%s text=%v round=%d", expr, disabled, round)
					}
				}
			}
		})
	}
}

func TestPropertyMaterializationBindingsStayPerRow(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "property_plan_rows")
	e := NewStorageExecutor(store)
	ctx := context.Background()
	query := "MERGE (n:PropertyPlan {id:$id,x:$v}) RETURN n.x"
	for round := 0; round < 3; round++ {
		for i, value := range []interface{}{int64(1), nil, int64(2), contractNamedMap(nil), int64(3)} {
			before := propertyDurableSnapshot(t, store)
			result, err := e.Execute(ctx, query, map[string]interface{}{"id": int64(round*10 + i), "v": value})
			if value == nil || i == 3 {
				require.Error(t, err)
				require.Equal(t, before, propertyDurableSnapshot(t, store))
				continue
			}
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{value}}, result.Rows)
		}
	}
}
