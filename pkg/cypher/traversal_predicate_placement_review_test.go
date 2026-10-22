package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Public regressions retained from the independent predicate-placement review.
type traversalPredicatePlacementReviewCase struct {
	name, query string
	want        [][]interface{}
}

func traversalPredicatePlacementReviewCases() []traversalPredicatePlacementReviewCase {
	return []traversalPredicatePlacementReviewCase{
		{"rel_row_null", "UNWIND [null] AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"rel_literal_null", "MATCH (p:P)-[:R {value:null}]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"rel_mixed_rows", "UNWIND [1,null,3] AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN q.id", [][]interface{}{{int64(1)}, {int64(3)}}},
		{"rel_mixed_map", "UNWIND [1,null,3] AS x MATCH (p:P)-[:R {value:x, absent:null}]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"rel_map_row", "UNWIND [{v:1},{v:null},{v:3}] AS row MATCH (p:P)-[:R {value:row.v}]->(q) RETURN q.id", [][]interface{}{{int64(1)}, {int64(3)}}},
		{"rel_with_null", "WITH null AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"call_scoped_null", "UNWIND [null] AS x CALL (x) { MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q) AS c } RETURN c", [][]interface{}{{int64(0)}}},
		{"call_import_null", "UNWIND [null] AS x CALL { WITH x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q) AS c } RETURN c", [][]interface{}{{int64(0)}}},
		{"rel_reverse_null", "UNWIND [null] AS x MATCH (q:Q)<-[:R {value:x}]-(p:P) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"rel_undirected_null", "MATCH (p:P)-[:R {value:null}]-(q:Q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"rel_variable_length_null", "UNWIND [null] AS x MATCH (p:P)-[:R*1..2 {value:x}]->(q:Q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"chain_first_null", "UNWIND [null] AS x MATCH (p:P)-[:R {value:x}]->(q)-[:S]->(s) RETURN count(s)", [][]interface{}{{int64(0)}}},
		{"chain_second_null", "UNWIND [null] AS x MATCH (p:P)-[:R]->(q)-[:S {value:x}]->(s) RETURN count(s)", [][]interface{}{{int64(0)}}},
		{"quantified_rel_null", "UNWIND [null] AS x MATCH (p:P)((a)-[:R {value:x}]->(b))+(q:Q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"selected_shortest_null", "UNWIND [null] AS x MATCH path = SHORTEST 1 (p:P)-[:R {value:x}]->(q:Q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"optional_literal_null", "OPTIONAL MATCH (p:P)-[:R {value:null}]->(q) RETURN p.id,q.id", [][]interface{}{{nil, nil}}},
		{"optional_mixed_rows", "UNWIND [1,null,3] AS x OPTIONAL MATCH (p:P)-[:R {value:x}]->(q) RETURN x,q.id", [][]interface{}{{int64(1), int64(1)}, {nil, nil}, {int64(3), int64(3)}}},
		{"optional_mixed_map", "UNWIND [1,null] AS x OPTIONAL MATCH (p:P)-[:R {value:1, absent:x}]->(q) RETURN x,q.id", [][]interface{}{{int64(1), nil}, {nil, nil}}},
		{"optional_bound_start", "MATCH (p:P) WITH p, null AS x OPTIONAL MATCH (p)-[:R {value:x}]->(q) RETURN p.id,q.id", [][]interface{}{{int64(1), nil}, {int64(2), nil}, {int64(3), nil}}},
		{"start_null", "UNWIND [null] AS x MATCH (p:P {id:x})-[:R]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"end_null", "UNWIND [null] AS x MATCH (p:P)-[:R]->(q:Q {id:x}) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"end_literal_null", "MATCH (p:P)-[:R]->(q:Q {id:null}) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"nodes_and_rel_mixed", "UNWIND [1,null,3] AS x MATCH (p:P {id:x})-[:R {value:x}]->(q:Q {id:x}) RETURN q.id", [][]interface{}{{int64(1)}, {int64(3)}}},
		{"late_endpoint_where", "MATCH (p:P)-[:R]->(q:Q) WHERE q.id > 1 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(2)}}},
		{"late_rel_where", "MATCH (p:P)-[r:R]->(q:Q) WHERE r.value > 1 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(2)}}},
		{"late_mixed_where", "MATCH (p:P)-[r:R]->(q:Q) WHERE p.id > 0 AND q.id > 1 AND r.value > 1 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(2)}}},
		{"late_null_where", "MATCH (p:P)-[r:R]->(q:Q) WHERE p.id > 0 AND r.value = null RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{}},
		{"late_rel_map_literal", "MATCH (p:P)-[:R {value:3}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(3)}}},
		{"late_rel_map_row", "WITH 3 AS x MATCH (p:P)-[:R {value:x}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(3)}}},
		{"late_end_map_literal", "MATCH (p:P)-[:R]->(q:Q {id:3}) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(3)}}},
		{"late_end_map_row", "WITH 3 AS x MATCH (p:P)-[:R]->(q:Q {id:x}) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(3)}}},
		{"ordered_rel_null", "WITH null AS x MATCH (p:P)-[:R {value:x}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{}},
		{"ordered_optional_null", "WITH null AS x OPTIONAL MATCH (p:P)-[:R {value:x}]->(q:Q) RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{nil}}},
		{"ordered_start_only_control", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id > 1 RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(2)}}},
		{"ordered_row_where", "UNWIND [1] AS x MATCH (p:P)-[:R]->(q:Q) WHERE q.id > x RETURN p.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(2)}}},
		{"bound_end_null", "MATCH (q:Q) WITH q,null AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(p)", [][]interface{}{{int64(0)}}},
		{"plain_empty_maps", "MATCH (p:P {})-[:R {}]->(q:Q {}) RETURN count(q)", [][]interface{}{{int64(3)}}},
		{"plain_row_control", "UNWIND [1,2,3] AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN q.id", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
	}
}
func TestTraversalPredicatePlacementReviewPredicates(t *testing.T) {
	tests := traversalPredicatePlacementReviewCases()
	for _, indexed := range []bool{false, true} {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "review1621b054")
		for n := int64(1); n <= 3; n++ {
			p, q, s := storage.NodeID(fmt.Sprintf("p%d", n)), storage.NodeID(fmt.Sprintf("q%d", n)), storage.NodeID(fmt.Sprintf("s%d", n))
			for _, node := range []*storage.Node{{ID: p, Labels: []string{"P"}, Properties: map[string]interface{}{"id": n}}, {ID: q, Labels: []string{"Q"}, Properties: map[string]interface{}{"id": n}}, {ID: s, Labels: []string{"End"}}} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", n)), StartNode: p, EndNode: q, Type: "R", Properties: map[string]interface{}{"value": n}}))
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("s%d", n)), StartNode: q, EndNode: s, Type: "S"}))
		}
		if indexed {
			_, err := NewStorageExecutor(store).Execute(context.Background(), "CREATE INDEX review1621_id FOR (p:P) ON (p.id)", nil)
			require.NoError(t, err)
		}
		for _, explicit := range []bool{false, true} {
			for _, profile := range []bool{false, true} {
				for _, tc := range tests {
					t.Run(fmt.Sprintf("index=%t/tx=%t/profile=%t/%s", indexed, explicit, profile, tc.name), func(t *testing.T) {
						e := NewStorageExecutor(store)
						ctx := context.Background()
						if explicit {
							_, err := e.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() { _, _ = e.Execute(ctx, "ROLLBACK", nil) }()
						}
						query := tc.query
						if profile {
							query = "PROFILE " + query
						}
						got, err := e.Execute(ctx, query, nil)
						require.NoError(t, err, query)
						if len(tc.want) == 0 {
							require.Empty(t, got.Rows, query)
						} else {
							require.ElementsMatch(t, tc.want, got.Rows, query)
						}
					})
				}
			}
		}
	}
}

func TestTraversalPredicatePlacementReviewMutationNull(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, profile := range []bool{false, true} {
			t.Run(fmt.Sprintf("tx=%t/profile=%t", explicit, profile), func(t *testing.T) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review1621mutation"))
				ctx := context.Background()
				if explicit {
					_, err := e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = e.Execute(ctx, "ROLLBACK", nil) }()
				}
				for _, query := range []string{
					"CREATE (p:P {missing:null})-[r:R {missing:null}]->(q:Q {missing:null}) RETURN properties(p),properties(r),properties(q)",
					"MATCH (p:P)-[r:R]->(q:Q) SET p.missing=1,r.missing=1,q.missing=1 SET p.missing=null,r.missing=null,q.missing=null RETURN properties(p),properties(r),properties(q)",
				} {
					if profile {
						query = "PROFILE " + query
					}
					got, err := e.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{map[string]interface{}{}, map[string]interface{}{}, map[string]interface{}{}}}, got.Rows)
				}
			})
		}
	}
}
