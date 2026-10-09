package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGh713OptionalCountGroupsProjectedNamesAndFiltersLabels(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			for _, products := range []int{1, 2} {
				t.Run(fmt.Sprintf("products_%d", products), func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh713"))
					ctx := context.Background()
					_, err := executor.Execute(ctx, "CREATE (:Product {productName: 'B'})", nil)
					require.NoError(t, err)
					for product := 0; product < products; product++ {
						_, err = executor.Execute(ctx, "CREATE (p:Product {productName: 'A'}), (o:Order), (c:Customer), (o)-[:ORDERS]->(p), (c)-[:ORDERS]->(p)", nil)
						require.NoError(t, err)
					}
					result, err := executor.Execute(ctx, "MATCH (p:Product) OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order) RETURN p.productName, count(o) AS orderCount ORDER BY orderCount DESC", nil)
					require.NoError(t, err)
					require.Equal(t, []string{"p.productName", "orderCount"}, result.Columns)
					require.Equal(t, [][]interface{}{{"A", int64(products)}, {"B", int64(0)}}, result.Rows)
				})
			}
		})
	}
}

func TestGh713OptionalProjectionExpressionPagination(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			executor, ctx := newUnitExecutor(t)
			_, err := executor.Execute(ctx, "CREATE (a:Anchor), (p1:Product {productName: 'A'}), (p2:Product {productName: 'B'}), (p3:Product {productName: 'C'}), (a)-[:HAS]->(p1), (a)-[:HAS]->(p2), (a)-[:HAS]->(p3)", nil)
			require.NoError(t, err)
			for _, test := range []struct {
				name string
				tail string
				want [][]interface{}
			}{
				{"aggregate parameters", "RETURN p.productName AS name, count(o) AS c ORDER BY name SKIP $skip LIMIT $take", [][]interface{}{{"B", int64(0)}}},
				{"aggregate arithmetic", "RETURN p.productName AS name, count(o) AS c ORDER BY name SKIP 0 + 1 LIMIT 0 + 1", [][]interface{}{{"B", int64(0)}}},
				{"missing relationship count", "RETURN p.productName AS name, count(r) AS c ORDER BY name SKIP $skip LIMIT $take", [][]interface{}{{"B", int64(0)}}},
				{"missing entity nulls", "RETURN p.productName AS name, o IS NULL AS nodeMissing, r IS NULL AS relationshipMissing ORDER BY name SKIP $skip LIMIT $take", [][]interface{}{{"B", true, true}}},
				{"plain parameters", "RETURN 'hit' AS value SKIP $skip LIMIT $take", [][]interface{}{{"hit"}}},
				{"plain arithmetic", "RETURN 'hit' AS value SKIP 0 + 1 LIMIT 0 + 1", [][]interface{}{{"hit"}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					query := "MATCH (a:Anchor)-[:HAS]->(p:Product) OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order) " + test.tail
					params := map[string]interface{}{"skip": int64(1), "take": int64(1)}
					result, err := executor.Execute(ctx, query, params)
					require.NoError(t, err)
					require.Equal(t, test.want, result.Rows)
				})
			}
			t.Run("matched entity counts", func(t *testing.T) {
				_, err := executor.Execute(ctx, "MATCH (p:Product {productName: 'B'}) CREATE (:Order)-[:ORDERS]->(p)", nil)
				require.NoError(t, err)
				tail := "RETURN p.productName AS name, count(o) AS nodes, count(r) AS relationships ORDER BY name SKIP $skip LIMIT $take"
				params := map[string]interface{}{"skip": int64(1), "take": int64(1)}
				result, err := executor.Execute(ctx, "MATCH (a:Anchor)-[:HAS]->(p:Product) OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order) "+tail, params)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"B", int64(1), int64(1)}}, result.Rows)
			})
		})
	}
}

// TestCompoundMatchOptionalMatch_OrderStatusPharmacy tests the exact query shape used by
// FormatOrderStatusContext: MATCH (n:OrderStatus) WHERE n.userId = $ AND NOT (n)-[:SUPERSEDED_BY]->()
// OPTIONAL MATCH (n)-[:FILLED_AT]->(ph:Pharmacy) RETURN n.orderId, ..., ph.id, ph.name ORDER BY n.orderId
func TestCompoundMatchOptionalMatch_OrderStatusPharmacy(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "db")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// OrderStatus head (no SUPERSEDED_BY)
	o := &storage.Node{
		ID:     "ord-1",
		Labels: []string{"OrderStatus"},
		Properties: map[string]interface{}{
			"userId":          "user-1",
			"orderId":         "ord-1",
			"orderDate":       "2026-02-03",
			"fillType":        "RETAIL",
			"fulFillmentType": "PICK",
			"details":         "[]",
		},
	}
	_, err := store.CreateNode(o)
	require.NoError(t, err)

	// Pharmacy
	ph := &storage.Node{
		ID:     "ph-001",
		Labels: []string{"Pharmacy"},
		Properties: map[string]interface{}{
			"id":   "ph-001",
			"name": "Store 4521",
		},
	}
	_, err = store.CreateNode(ph)
	require.NoError(t, err)

	// FILLED_AT edge
	edge := &storage.Edge{
		ID:         "e1",
		Type:       "FILLED_AT",
		StartNode:  o.ID,
		EndNode:    ph.ID,
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateEdge(edge))

	query := `MATCH (n:OrderStatus) WHERE n.userId = 'user-1' AND NOT (n)-[:SUPERSEDED_BY]->()
OPTIONAL MATCH (n)-[:FILLED_AT]->(ph:Pharmacy)
RETURN n.orderId AS orderId, n.orderDate AS orderDate, ph.id AS pharmacyId, ph.name AS pharmacyName
ORDER BY n.orderId`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "should return one row for the order")
	require.Len(t, result.Columns, 4)
	require.Equal(t, "orderId", result.Columns[0])
	require.Equal(t, "pharmacyId", result.Columns[2])
	require.Equal(t, "pharmacyName", result.Columns[3])

	row := result.Rows[0]
	require.Equal(t, "ord-1", rowStringAny(row[0]), "orderId")
	require.Equal(t, "2026-02-03", rowStringAny(row[1]), "orderDate")
	require.NotNil(t, row[2], "pharmacyId must be non-null when FILLED_AT exists")
	require.NotNil(t, row[3], "pharmacyName must be non-null when FILLED_AT exists")
	require.Equal(t, "ph-001", rowStringAny(row[2]))
	require.Equal(t, "Store 4521", rowStringAny(row[3]))
}

// TestCompoundMatchOptionalMatch_OrderStatusNoPharmacy tests that when there is no FILLED_AT
// edge, OPTIONAL MATCH yields null for ph columns but the row is still returned.
func TestCompoundMatchOptionalMatch_OrderStatusNoPharmacy(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "db")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	o := &storage.Node{
		ID:     "ord-2",
		Labels: []string{"OrderStatus"},
		Properties: map[string]interface{}{
			"userId":    "user-1",
			"orderId":   "ord-2",
			"orderDate": "2026-02-04",
		},
	}
	_, err := store.CreateNode(o)
	require.NoError(t, err)

	query := `MATCH (n:OrderStatus) WHERE n.userId = 'user-1' AND NOT (n)-[:SUPERSEDED_BY]->()
OPTIONAL MATCH (n)-[:FILLED_AT]->(ph:Pharmacy)
RETURN n.orderId AS orderId, ph.id AS pharmacyId
ORDER BY n.orderId`

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "ord-2", rowStringAny(result.Rows[0][0]))
	require.Nil(t, result.Rows[0][1], "pharmacyId should be null when no FILLED_AT")
}

func rowStringAny(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case float64:
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%v", t)
	case int64:
		return fmt.Sprintf("%d", t)
	default:
		return fmt.Sprint(v)
	}
}
