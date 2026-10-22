package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type traversalLimitPlacementCase struct {
	name, query string
	want        [][]interface{}
}

func traversalLimitRows(values ...int64) [][]interface{} {
	rows := make([][]interface{}, 0, len(values))
	for _, value := range values {
		rows = append(rows, []interface{}{value})
	}
	return rows
}

func traversalLimitPlacementCases() []traversalLimitPlacementCase {
	return []traversalLimitPlacementCase{
		{"exhausted_indexed_seed", "MATCH (p:P {id:999})-[:R]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 2", nil},
		{"with_own_limit", "MATCH (p:P)-[:R]->(q:Q) WITH p,q LIMIT 2 RETURN count(p)", traversalLimitRows(2)},
		{"repeated_endpoint", "MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 1", traversalLimitRows(3)},
		{"plain_limit_after_map", "MATCH (p:P)-[:R {value:3}]->(q:Q) RETURN p.id LIMIT 1", traversalLimitRows(3)},
		{"review_relationship_literal", "MATCH (p:P)-[:R {value:3}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"review_endpoint_row", "WITH 3 AS x MATCH (p:P)-[:R]->(q:Q {id:x}) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"relationship_row", "WITH 3 AS x MATCH (p:P)-[:R {value:x}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"endpoint_literal", "MATCH (p:P)-[:R]->(q:Q {id:3}) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"later_endpoint_label", "MATCH (p:P)-[:R]->(q:Last) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"later_chain_label", "MATCH (p:P)-[:R]->(q)-[:S]->(s:LastEnd) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"relationship_type", "MATCH (p:P)-[:ONLY]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"relationship_direction", "MATCH (p:P)<-[:REV]-(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"relationship_length", "MATCH (p:P)-[:ONLY*1..2]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"exact_two_hops", "MATCH (p:P)-[:R|T*2..2]->(s:End) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"missing_first_seed", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id >= 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(1)},
		{"partial_seed_rows", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id >= 0 RETURN p.id ORDER BY p.id LIMIT 2", traversalLimitRows(1, 2)},
		{"start_window_exhaustion", "MATCH (p:HoleStart)-[:TAIL]->(q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(250)},
		{"end_window_exhaustion", "MATCH (p:HoleStart)-[:TAIL]->(q:HoleEnd) WHERE q.id > 0 RETURN q.id ORDER BY q.id LIMIT 1", traversalLimitRows(250)},
		{"descending_map", "MATCH (p:P)-[:R {value:1}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id DESC LIMIT 1", traversalLimitRows(1)},
		{"multiple_seed_order_keys", "MATCH (p:P)-[:R {value:3}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id,p.id DESC LIMIT 1", traversalLimitRows(3)},
		{"other_binding_order", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY q.id DESC LIMIT 2", traversalLimitRows(3, 2)},
		{"where_after_match", "MATCH (p:P)-[r:R]->(q:Q) WHERE p.id > 0 AND r.value > 1 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(2)},
		{"skip_where", "MATCH (p:P)-[r:R]->(q:Q) WHERE p.id > 0 AND r.value > 1 RETURN p.id ORDER BY p.id SKIP 1 LIMIT 1", traversalLimitRows(3)},
		{"later_match_filter", "MATCH (p:P)-[:R]->(q:Q) MATCH (q)-[:S]->(s:End {id:3}) RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"later_with_filter", "MATCH (p:P)-[:R]->(q:Q) WITH p,q WHERE q.id=3 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"later_projected_filter", "MATCH (p:P)-[:R]->(q:Q) WITH p.id AS id WHERE id=3 RETURN id ORDER BY id LIMIT 1", traversalLimitRows(3)},
		{"distinct", "MATCH (p:P)-[:F]->(q) WHERE p.id > 0 RETURN DISTINCT p.id ORDER BY p.id LIMIT 2", traversalLimitRows(1, 2)},
		{"aggregation", "MATCH (p:P)-[:F]->(q) WHERE p.id > 0 RETURN count(q) AS total ORDER BY total LIMIT 1", traversalLimitRows(3)},
		{"optional", "OPTIONAL MATCH (p:P)-[:ONLY]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1", traversalLimitRows(3)},
		{"optional_bound", "MATCH (p:P) WHERE p.id > 0 WITH p OPTIONAL MATCH (p)-[:ONLY]->(q:Q) RETURN p.id,q.id ORDER BY p.id LIMIT 1", [][]interface{}{{int64(1), nil}}},
		{"with_limit_boundary", "MATCH (p:P)-[:R]->(q:Q) WITH p,q ORDER BY p.id LIMIT 2 MATCH (q)-[:S]->(s:End {id:3}) RETURN p.id ORDER BY p.id LIMIT 1", nil},
		{"eligible_start_only", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 2", traversalLimitRows(1, 2)},
		{"limit_zero", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 0", nil},
	}
}

func traversalLimitPlacementFixture(t *testing.T, indexed bool) *storage.NamespacedEngine {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "limitplacement")
	create := func(node *storage.Node) { _, err := store.CreateNode(node); require.NoError(t, err) }
	edge := func(id, start, end, kind string, value int64) {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), StartNode: storage.NodeID(start), EndNode: storage.NodeID(end), Type: kind, Properties: map[string]interface{}{"value": value}}))
	}
	for n := int64(0); n <= 3; n++ {
		create(&storage.Node{ID: storage.NodeID(fmt.Sprintf("p%d", n)), Labels: []string{"P"}, Properties: map[string]interface{}{"id": n}})
		if n == 0 {
			continue
		}
		qLabels, sLabels := []string{"Q"}, []string{"End"}
		if n == 3 {
			qLabels = append(qLabels, "Last")
			sLabels = append(sLabels, "LastEnd")
		}
		create(&storage.Node{ID: storage.NodeID(fmt.Sprintf("q%d", n)), Labels: qLabels, Properties: map[string]interface{}{"id": n}})
		create(&storage.Node{ID: storage.NodeID(fmt.Sprintf("s%d", n)), Labels: sLabels, Properties: map[string]interface{}{"id": n}})
		edge(fmt.Sprintf("r%d", n), fmt.Sprintf("p%d", n), fmt.Sprintf("q%d", n), "R", n)
		edge(fmt.Sprintf("s%d", n), fmt.Sprintf("q%d", n), fmt.Sprintf("s%d", n), "S", n)
	}
	for _, e := range [][4]string{{"only", "p3", "q3", "ONLY"}, {"rev", "q3", "p3", "REV"}, {"two", "q3", "s3", "T"}, {"f1", "p1", "q1", "F"}, {"f2", "p1", "q2", "F"}, {"f3", "p2", "q3", "F"}} {
		edge(e[0], e[1], e[2], e[3], 3)
	}
	edge("self-reject", "p1", "p2", "SELF", 1)
	edge("self-accept", "p3", "p3", "SELF", 3)
	for n := 1; n <= 250; n++ {
		create(&storage.Node{ID: storage.NodeID(fmt.Sprintf("hp%d", n)), Labels: []string{"HoleStart"}, Properties: map[string]interface{}{"id": int64(n)}})
		create(&storage.Node{ID: storage.NodeID(fmt.Sprintf("hq%d", n)), Labels: []string{"HoleEnd"}, Properties: map[string]interface{}{"id": int64(n)}})
	}
	edge("tail", "hp250", "hq250", "TAIL", 250)
	if indexed {
		e := NewStorageExecutor(store)
		for _, label := range []string{"P", "HoleStart", "HoleEnd"} {
			_, err := e.Execute(context.Background(), fmt.Sprintf("CREATE INDEX limit_placement_%s FOR (n:%s) ON (n.id)", label, label), nil)
			require.NoError(t, err)
		}
	}
	return store
}

func TestTraversalLimitPlacement(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		store := traversalLimitPlacementFixture(t, indexed)
		for _, explicit := range []bool{false, true} {
			for _, profile := range []bool{false, true} {
				for _, tc := range traversalLimitPlacementCases() {
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
							require.Equal(t, tc.want, got.Rows, query)
						}
					})
				}
			}
		}
	}
}
