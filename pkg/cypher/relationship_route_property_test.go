package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipDeclarationExpressionMarkers(t *testing.T) {
	for _, marker := range []string{":", " IS "} {
		for _, expression := range []string{"$({k:'A'}.k)", "$(([{k:'A'}][0]).k)", "$('A')", "$(['A'][0])"} {
			declaration := "r" + marker + expression
			t.Run(declaration, func(t *testing.T) {
				owner := relationshipDeclarationOf(declaration)
				require.Equal(t, "r", owner.variable)
				require.Equal(t, expression, owner.typeText)
				ctx := context.Background()
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
				_, err := e.Execute(ctx, "CREATE ()-[:A]->()", nil)
				require.NoError(t, err)
				result, err := e.Execute(ctx, "MATCH ()-["+declaration+"]->() RETURN count(*)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				for _, clause := range []string{"CREATE", "MERGE"} {
					result, err = e.Execute(ctx, clause+" ()-["+declaration+"]->() RETURN type(r)", nil)
					require.NoError(t, err, clause)
					require.NotEmpty(t, result.Rows, clause)
					for _, row := range result.Rows {
						require.Equal(t, []interface{}{"A"}, row, clause)
					}
				}
			})
		}
	}
}

// The mutual executor's established output is an unordered pair for a scalar
// type. The fallback emits both directions and all alternative-type bindings;
// derive those two cardinalities from the same owner-selected type population.
func TestRelationshipRoutePropertyMutual(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	names := []string{"A", "B", "A|B", "A`B", "Other"}
	_, err := e.Execute(ctx, "CREATE (x:Source {name:'x'}),(y:Target {name:'y'})", nil)
	require.NoError(t, err)
	for _, name := range names {
		quoted := cyphertext.QuoteName(name)
		_, err = e.Execute(ctx, "MATCH (x:Source),(y:Target) CREATE (x)-[:"+quoted+"]->(y)-[:"+quoted+"]->(x)", nil)
		require.NoError(t, err)
	}
	var admitted, declined int
	for _, declaration := range relationshipRouteDeclarations() {
		owner, selected := relationshipRouteExpected(t, declaration, names)
		second := strings.TrimPrefix(declaration, "r")
		query := "MATCH (x)-[" + declaration + "]->(y)-[" + second + "]->(x) RETURN x.name,y.name"
		info := DetectQueryPattern(ctx, query)
		_, singleton := owner.singleLiteralType()
		require.Equal(t, singleton, info.Pattern == PatternMutualRelationship, query)
		result, applied := e.ExecuteOptimized(ctx, query, info)
		require.Equal(t, singleton, applied, query)
		if applied {
			admitted++
			require.Len(t, result.Rows, len(selected), query)
		} else {
			declined++
			result, err = e.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 2*len(selected)*len(selected), query)
		}
	}
	require.Positive(t, admitted)
	require.Positive(t, declined)
	t.Logf("ROUTE mutual applied=%d declined=%d", admitted, declined)
}

func TestRelationshipReviewExactRoutes(t *testing.T) {
	ctx := context.Background()
	for _, query := range []string{
		"MATCH ()-[r IS $({k:'A'}.k)]->() RETURN count(*)",
		"MATCH (x)-[:A|B]->(y) RETURN x.name,count(y)",
		"MATCH (y)<-[:A|B]-(x) RETURN y.name,count(x)",
		"MATCH (x)-[r:A]->(y) RETURN y.name,avg(r.rating),count(r)",
	} {
		t.Run(query, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			seed := "CREATE (x {name:'x'})-[:B {rating:9}]->(y {name:'y'})"
			if strings.Contains(query, " IS ") {
				seed = "CREATE ()-[:A]->()"
			}
			_, err := e.Execute(ctx, seed, nil)
			require.NoError(t, err)
			result, applied := e.ExecuteOptimized(ctx, query, DetectQueryPattern(ctx, query))
			if !applied {
				result, err = e.Execute(ctx, query, nil)
				require.NoError(t, err)
			}
			if strings.Contains(query, " IS ") {
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			} else if strings.Contains(query, "avg(") {
				require.Empty(t, result.Rows, "A must not aggregate B")
			} else {
				require.Len(t, result.Rows, 1)
				require.Equal(t, int64(1), result.Rows[0][1])
			}
		})
	}
	t.Run("duplicate counter", func(t *testing.T) {
		e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		_, err := e.Execute(ctx, "CREATE ()-[:A]->()", nil)
		require.NoError(t, err)
		result, err := e.Execute(ctx, "MATCH ()-[r:A|`A`]->() RETURN count(r)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	})
}

// Generate equivalent spellings and values, then derive each route's expected
// membership from the declaration owner. The fixture includes names whose data
// looks like syntax, duplicates, absent names and alternatives in either order.
func relationshipRouteDeclarations() []string {
	declarations := []string{"r"}
	for _, marker := range []string{":", " IS "} {
		for _, names := range [][]string{{"A"}, {"B"}, {"Missing"}, {"A|B"}, {"A`B"}, {"A", "B"}, {"B", "A"}, {"A", "A"}, {"B", "Missing", "B"}} {
			var quoted, values []string
			for _, name := range names {
				quoted = append(quoted, cyphertext.QuoteName(name))
				values = append(values, "'"+name+"'")
			}
			declarations = append(declarations, "r"+marker+strings.Join(quoted, "|"))
			if len(names) == 1 {
				if isSimpleIdentifier(names[0]) {
					declarations = append(declarations, "r"+marker+names[0])
				}
				for _, expression := range []string{values[0], `"` + names[0] + `"`, "{k:" + values[0] + "}.k", "[" + values[0] + "][0]", "([{" + "k:" + values[0] + "}][0]).k"} {
					declarations = append(declarations, "r"+marker+"$("+expression+")")
				}
			} else {
				declarations = append(declarations, "r"+marker+"$any(["+strings.Join(values, ",")+"])")
			}
		}
		declarations = append(declarations, "r"+marker+"A|`A`")
	}
	return declarations
}

func relationshipRouteExpected(t *testing.T, declaration string, names []string) (relationshipDeclaration, []string) {
	t.Helper()
	owner := relationshipDeclarationOf(declaration)
	if !owner.hasColon {
		return owner, append([]string(nil), names...)
	}
	chain, ok := owner.typeChain()
	require.True(t, ok, declaration)
	expression, constant, err := chain.expr.resolveDynamic(func(expression string) (interface{}, bool, error) {
		e := &StorageExecutor{}
		value, err := e.dynamicTokenValue(context.Background(), expression, nil, nil)
		return value, true, err
	})
	require.NoError(t, err, declaration)
	require.True(t, constant, declaration)
	var selected []string
	for _, name := range names {
		if expression.matches([]string{name}) {
			selected = append(selected, name)
		}
	}
	return owner, selected
}

func TestRelationshipRouteProperty(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	names := []string{"A", "B", "A|B", "A`B", "Other"}
	_, err := e.Execute(ctx, "CREATE (x:Source {name:'x'}), (y:Target {name:'y'})", nil)
	require.NoError(t, err)
	for _, name := range names {
		_, err = e.Execute(ctx, "MATCH (x:Source),(y:Target) CREATE (x)-[:"+cyphertext.QuoteName(name)+" {rating:2}]->(y)", nil)
		require.NoError(t, err)
	}
	populations := map[string]int{}
	for index, declaration := range relationshipRouteDeclarations() {
		t.Run(fmt.Sprintf("%03d/%s", index, declaration), func(t *testing.T) {
			owner, selected := relationshipRouteExpected(t, declaration, names)
			want := int64(len(selected))
			// Syntax readers preserve classification before evaluation.
			pattern := "(x)-[" + declaration + "]->(y)"
			for route, read := range map[string]func() *TraversalMatch{
				"reader":          func() *TraversalMatch { return e.parseTraversalPattern(ctx, pattern) },
				"structure":       func() *TraversalMatch { return e.parseTraversalPatternStructure(ctx, pattern) },
				"bound":           func() *TraversalMatch { m, _ := e.parseBoundRelationshipPattern(ctx, pattern); return m },
				"bound-structure": func() *TraversalMatch { m, _ := e.parseBoundRelationshipPatternStructure(ctx, pattern); return m },
			} {
				match := read()
				require.NotNil(t, match, route)
				require.True(t, owner.types.same(match.Relationship.Types), route)
				populations[route]++
			}
			connection, err := e.parseRelationshipPattern(ctx, "-["+declaration+"]->")
			require.NoError(t, err)
			require.True(t, owner.types.same(connection.Types))
			populations["connection"]++
			rawMatch := e.parseTraversalPattern(ctx, pattern)
			_, rawCounter, err := e.relationshipCountFromStore(ctx, rawMatch, returnItem{expr: "count(r)"})
			require.NoError(t, err)
			require.Equal(t, !owner.dynamic, rawCounter, "counter must decline unevaluated names")
			populations["raw-counter-admission"]++
			literal, literalOK := owner.singleLiteralType()
			vector, vectorOK := e.parseSimpleMatchRelationshipPattern(pattern)
			require.Equal(t, literalOK, vectorOK, "vector singleton admission")
			if vectorOK {
				require.Equal(t, literal.storageName(), vector.relType.storageName())
			}
			populations["vector-admission"]++
			_, batchOwner, _, batchOK := boundOutgoingRelationship(pattern)
			require.True(t, batchOK)
			require.True(t, owner.types.same(batchOwner.types))
			_, staticOK := owner.singleStaticType()
			require.Equal(t, literalOK && isSimpleIdentifier(literal.storageName()), staticOK)
			populations["batch-admission"]++
			degree := parseBoundDegreeShapeText("(x)-[" + declaration + "]->()")
			require.Equal(t, !owner.dynamic, degree.ok, "degree admission")
			if degree.ok {
				require.True(t, owner.types.same(degree.types))
				populations["degree-admitted"]++
			} else {
				populations["degree-declined"]++
			}
			// Resolution is the same owner phase public Execute runs before readers.
			lowered, _, err := desugarLabelExpressions("MATCH "+pattern+" RETURN count(r)", nil, false)
			require.NoError(t, err)
			// Row-dependent expressions retain a predicate. The enclosing public
			// route evaluates it; a bare counter/type cursor is not applicable.
			if !containsKeywordOutsideStrings(lowered, "WHERE") {
				match := e.parseTraversalPattern(ctx, strings.TrimSpace(strings.TrimPrefix(extractMatchClause(lowered), "MATCH")))
				require.NotNil(t, match)
				for _, name := range names {
					require.Equal(t, containsString(selected, name), match.Relationship.Types.matches(name), name)
				}
				populations["resolved-reader"]++
				for route, plan := range map[string]traversalScanPlan{"planner": planTraversalScan(match), "pipeline-planner": planPipelineTraversalScan(match, "", nil)} {
					var got int64
					err := plan.visitEdges(ctx, e, match, func(edge *storage.Edge, _, _ storage.NodeID) error {
						if match.Relationship.Types.matches(edge.Type) {
							got++
						}
						return nil
					})
					require.NoError(t, err)
					require.Equal(t, want, got, route)
					populations[route]++
				}
				for _, endpoints := range [][2]string{{"", ""}, {":Source", ""}, {"", ":Target"}} {
					if !owner.hasColon && (endpoints[0] != "" || endpoints[1] != "") {
						continue
					}
					for _, incoming := range []bool{false, true} {
						left, right, arrow := endpoints[0], endpoints[1], "outgoing"
						if incoming {
							left, right, arrow = right, left, "incoming"
						}
						counterMatch := *match
						counterMatch.StartNode.labels, counterMatch.EndNode.labels = nil, nil
						if left != "" {
							counterMatch.StartNode.labels = []string{strings.TrimPrefix(left, ":")}
						}
						if right != "" {
							counterMatch.EndNode.labels = []string{strings.TrimPrefix(right, ":")}
						}
						counterMatch.Relationship.Direction = arrow
						got, admitted, err := e.relationshipCountFromStore(ctx, &counterMatch, returnItem{expr: "count(r)"})
						require.NoError(t, err)
						require.True(t, admitted)
						require.Equal(t, want, got, "counter "+left+arrow+right)
						populations["counter-"+left+arrow+right]++
					}
				}
				undirected := *match
				undirected.Relationship.Direction = "both"
				got, admitted, err := e.relationshipCountFromStore(ctx, &undirected, returnItem{expr: "count(r)"})
				require.NoError(t, err)
				require.True(t, admitted)
				require.Equal(t, 2*want, got)
				populations["counter-undirected"]++
			}
			for route, query := range map[string]string{
				"public-count":       "MATCH " + pattern + " RETURN count(r)",
				"public-traversal":   "MATCH " + pattern + " RETURN r",
				"public-large":       "MATCH " + pattern + " RETURN r LIMIT 101",
				"public-pipeline":    "MATCH " + pattern + " WITH r RETURN count(r)",
				"public-degree":      "MATCH (x:Source) RETURN COUNT { (x)-[" + declaration + "]->() }",
				"outgoing-optimizer": "MATCH " + pattern + " RETURN x.name,count(y)",
				"incoming-optimizer": "MATCH (y)<-[" + declaration + "]-(x) RETURN y.name,count(x)",
				"edge-optimizer":     "MATCH " + pattern + " RETURN y.name,avg(r.rating),count(r)",
			} {
				info := DetectQueryPattern(ctx, query)
				if strings.Contains(route, "optimizer") {
					_, singleton := owner.singleLiteralType()
					require.Equal(t, singleton || !owner.hasColon, info.IsOptimizable(), "detector admission: "+query)
				}
				result, applied := e.ExecuteOptimized(ctx, query, info)
				if applied {
					populations[route+"-applied"]++
				} else {
					populations[route+"-declined"]++
					result, err = e.Execute(ctx, query, nil)
					require.NoError(t, err, query)
				}
				var got int64
				switch route {
				case "public-traversal", "public-large":
					got = int64(len(result.Rows))
				case "public-count", "public-pipeline", "public-degree":
					require.Len(t, result.Rows, 1)
					got = result.Rows[0][0].(int64)
				default:
					if len(result.Rows) > 0 {
						value := result.Rows[0][len(result.Rows[0])-1]
						require.IsType(t, int64(0), value, route+" count projection")
						got = value.(int64)
					}
					if route == "edge-optimizer" && want > 0 {
						require.Equal(t, float64(2), result.Rows[0][1])
					}
				}
				require.Equal(t, want, got, route)
				populations[route]++
			}
		})
	}
	for _, route := range []string{"reader", "structure", "bound", "bound-structure", "connection", "raw-counter-admission", "counter-undirected", "vector-admission", "batch-admission", "degree-admitted", "degree-declined", "resolved-reader", "planner", "pipeline-planner", "public-count", "public-traversal", "public-large", "public-pipeline", "public-degree", "outgoing-optimizer-applied", "outgoing-optimizer-declined", "incoming-optimizer-applied", "incoming-optimizer-declined", "edge-optimizer-applied", "edge-optimizer-declined", "counter-outgoing", "counter-:Sourceoutgoing", "counter-outgoing:Target", "counter-incoming", "counter-incoming:Source", "counter-:Targetincoming"} {
		require.Positive(t, populations[route], "empty route population: "+route)
	}
	keys := make([]string, 0, len(populations))
	for route := range populations {
		keys = append(keys, route)
	}
	sort.Strings(keys)
	for _, route := range keys {
		t.Logf("ROUTE %s population=%d", route, populations[route])
	}
}

func TestRelationshipOptimizedBoundaryAdmission(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := e.Execute(ctx, "CREATE (x {name:'x'})-[:B {rating:2}]->(y {name:'y'})-[:B]->(x)", nil)
	require.NoError(t, err)
	for _, declaration := range []string{"r:A|B", "r IS $({k:'B'}.k)", "r:B*1..2", "r:B {rating:9}", "r:B WHERE r.rating=9"} {
		for _, query := range []string{
			"MATCH (x)-[" + declaration + "]->(y) RETURN x.name,count(y)",
			"MATCH (y)<-[" + declaration + "]-(x) RETURN y.name,count(x)",
			"MATCH (x)-[" + declaration + "]->(y) RETURN y.name,avg(r.rating),count(r)",
			"MATCH (x)-[" + declaration + "]->(y)-[:B]->(x) RETURN x.name,y.name",
		} {
			for _, kind := range []QueryPattern{PatternIncomingCountAgg, PatternOutgoingCountAgg, PatternEdgePropertyAgg, PatternMutualRelationship} {
				_, applied := e.ExecuteOptimized(ctx, query, PatternInfo{Pattern: kind, RelType: "B", AggProperty: "rating", AggFunctions: []string{"avg", "count"}})
				require.False(t, applied, query)
			}
			_, err := e.executeRelationshipCountOptimized(ctx, query, PatternInfo{Pattern: PatternOutgoingCountAgg, RelType: "B"})
			require.Error(t, err, "direct count admission: "+query)
			_, err = e.executeEdgePropertyAggOptimized(ctx, query, PatternInfo{Pattern: PatternEdgePropertyAgg, RelType: "B", AggProperty: "rating"})
			require.Error(t, err, "direct property admission: "+query)
		}
	}
	for _, kind := range []QueryPattern{PatternIncomingCountAgg, PatternOutgoingCountAgg, PatternEdgePropertyAgg, PatternMutualRelationship} {
		query := "MATCH (x)-[:B]->(y) RETURN x.name,count(y)"
		if kind == PatternIncomingCountAgg {
			query = "MATCH (y)<-[:B]-(x) RETURN y.name,count(x)"
		}
		if kind == PatternEdgePropertyAgg {
			query = "MATCH (x)-[r:B]->(y) RETURN y.name,avg(r.rating),count(r)"
		}
		if kind == PatternMutualRelationship {
			query = "MATCH (x)-[:B]->(y)-[:B]->(x) RETURN x.name,y.name"
		}
		result, applied := e.ExecuteOptimized(ctx, query, PatternInfo{Pattern: kind, RelType: "A", StartVar: "wrong", AggProperty: "wrong", AggFunctions: []string{"sum"}})
		require.True(t, applied, query)
		require.NotEmpty(t, result.Rows, "public scalar hints must be replaced by source admission")
	}
}

// Causal negative control: the rejected first-type physical executor disagrees
// with the same generated owner expectations. This must remain non-vacuous.
func TestRelationshipRoutePropertyRejectsSingleTypeOptimizer(t *testing.T) {
	var killed int
	for _, declaration := range relationshipRouteDeclarations() {
		owner, selected := relationshipRouteExpected(t, declaration, []string{"A", "B", "A|B", "A`B"})
		if owner.dynamic {
			continue
		}
		var actual int
		for _, name := range []string{"A", "B", "A|B", "A`B"} {
			if owner.types.len() > 0 && owner.types.at(0).storageName() == name {
				actual++
			}
		}
		if actual != len(selected) {
			killed++
		}
	}
	require.Positive(t, killed, "the planted first-type optimizer survived")
	t.Logf("planted first-type optimizer killed by %d generated declarations", killed)
}
