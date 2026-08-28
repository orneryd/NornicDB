package cypher_test

// Deterministic hot-path assertions for the exact Cypher shapes emitted by
// testing/benchmarks/northwind_power. Each test:
//   1. Seeds the required base nodes.
//   2. Runs one batch of the seeder query.
//   3. Asserts the UnwindMultiMatchCreateBatch hot-path flag is set.
//   4. Asserts the side-effects (nodes / edges created) match expectation.
//
// When these go red, the Northwind benchmark has dropped off the batched
// fast path and will crawl at scale.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func seedCategoriesAndSuppliers(t *testing.T, exec *cypher.StorageExecutor, catN, supN int) {
	t.Helper()
	ctx := context.Background()
	// The UnwindMultiMatchCreateBatch fast path is deterministic: it
	// REQUIRES a schema property index for every MATCH-side (label, prop)
	// pair. Declaring indexes up front matches the production seeder's
	// DDL-first ordering and keeps the test aligned with the one contract
	// the fast path relies on.
	for _, ddl := range []string{
		"CREATE INDEX category_id IF NOT EXISTS FOR (n:Category) ON (n.categoryID)",
		"CREATE INDEX supplier_id IF NOT EXISTS FOR (n:Supplier) ON (n.supplierID)",
		"CREATE INDEX customer_id IF NOT EXISTS FOR (n:Customer) ON (n.customerID)",
		"CREATE INDEX product_id IF NOT EXISTS FOR (n:Product) ON (n.productID)",
		"CREATE INDEX order_id IF NOT EXISTS FOR (n:Order) ON (n.orderID)",
	} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err, "seed DDL: %s", ddl)
	}
	for i := 1; i <= catN; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(
			`CREATE (:Category {categoryID: %d, categoryName: 'C%d', description: 'desc'})`, i, i), nil)
		require.NoError(t, err)
	}
	for i := 1; i <= supN; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(
			`CREATE (:Supplier {supplierID: %d, companyName: 'S%d', contactName: 'x', country: 'US', region: 'NA-West'})`, i, i), nil)
		require.NoError(t, err)
	}
}

// TestNorthwindSeeder_ProductsHitsUnwindMultiMatchCreate verifies the
// products seed query hits UnwindMultiMatchCreateBatch.
func TestNorthwindSeeder_ProductsHitsUnwindMultiMatchCreate(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	seedCategoriesAndSuppliers(t, exec, 8, 8)
	ctx := context.Background()

	rows := make([]interface{}, 0, 50)
	for i := 0; i < 50; i++ {
		rows = append(rows, map[string]interface{}{
			"productID":    int64(i + 1),
			"productName":  fmt.Sprintf("P%d", i+1),
			"sku":          fmt.Sprintf("SKU-%05d", i+1),
			"unitPrice":    float64(i%20+1) * 1.25,
			"unitsInStock": int64(i % 500),
			"discontinued": false,
			"description":  "mid-size description",
			"categoryID":   int64((i % 8) + 1),
			"supplierID":   int64((i % 8) + 1),
		})
	}

	query := `
UNWIND $rows AS row
MATCH (c:Category {categoryID: row.categoryID})
MATCH (s:Supplier {supplierID: row.supplierID})
CREATE (p:Product {productID: row.productID, productName: row.productName, sku: row.sku, unitPrice: row.unitPrice, unitsInStock: row.unitsInStock, discontinued: row.discontinued, description: row.description})
CREATE (p)-[:PART_OF]->(c)
CREATE (s)-[:SUPPLIES]->(p)`

	_, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMultiMatchCreateBatch,
		"products seed must hit UnwindMultiMatchCreateBatch; got trace=%+v", trace)

	count, err := exec.Execute(ctx, `MATCH (p:Product) RETURN count(p) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(50), count.Rows[0][0])

	partOf, err := exec.Execute(ctx, `MATCH ()-[r:PART_OF]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(50), partOf.Rows[0][0])

	supplies, err := exec.Execute(ctx, `MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(50), supplies.Rows[0][0])
}

func TestUnwindMultiMatchCreate_ElementIDEndpointsHitsBatchPath(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := testutil.SetupTestExecutorWithStore(t, store)
	ctx := context.Background()

	leftID, err := store.CreateNode(&storage.Node{ID: "left", Labels: []string{"Entity"}})
	require.NoError(t, err)
	rightID, err := store.CreateNode(&storage.Node{ID: "right", Labels: []string{"Entity"}})
	require.NoError(t, err)

	query := `
UNWIND $rows AS row
MATCH (source) WHERE elementId(source) = row.sourceID
MATCH (target) WHERE elementId(target) = row.targetID
CREATE (source)-[:BENCH_ENTITY_LINK]->(target)
RETURN count(source) AS created`
	rows := []any{
		map[string]any{"sourceID": string(leftID), "targetID": string(rightID)},
		map[string]any{"sourceID": string(rightID), "targetID": string(leftID)},
	}
	result, err := exec.Execute(ctx, query, map[string]any{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(2)}}, result.Rows)
	require.True(t, exec.LastHotPathTrace().UnwindMultiMatchCreateBatch)

	count, err := exec.Execute(ctx, "MATCH ()-[r:BENCH_ENTITY_LINK]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), count.Rows[0][0])
}

func TestNorthwindSeeder_ProductsIncompleteIndexedMatchBucketKeepsFastPath(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := testutil.SetupTestExecutorWithStore(t, store)
	seedCategoriesAndSuppliers(t, exec, 4, 4)
	ctx := context.Background()

	schema := store.GetSchema()
	require.NotNil(t, schema)
	ids := schema.PropertyIndexLookup("Supplier", "supplierID", int64(2))
	require.NotEmpty(t, ids)
	for _, id := range ids {
		require.NoError(t, schema.PropertyIndexDelete("Supplier", "supplierID", id, int64(2)))
	}
	require.Empty(t, schema.PropertyIndexLookup("Supplier", "supplierID", int64(2)))

	rows := []interface{}{
		map[string]interface{}{"productID": int64(101), "productName": "P101", "sku": "SKU-00101", "unitPrice": 1.25, "unitsInStock": int64(10), "discontinued": false, "description": "indexed supplier", "categoryID": int64(1), "supplierID": int64(1)},
		map[string]interface{}{"productID": int64(102), "productName": "P102", "sku": "SKU-00102", "unitPrice": 2.50, "unitsInStock": int64(11), "discontinued": false, "description": "stale supplier", "categoryID": int64(2), "supplierID": int64(2)},
		map[string]interface{}{"productID": int64(103), "productName": "P103", "sku": "SKU-00103", "unitPrice": 3.75, "unitsInStock": int64(12), "discontinued": false, "description": "indexed supplier again", "categoryID": int64(3), "supplierID": int64(3)},
	}

	query := `
UNWIND $rows AS row
MATCH (c:Category {categoryID: row.categoryID})
MATCH (s:Supplier {supplierID: row.supplierID})
CREATE (p:Product {productID: row.productID, productName: row.productName, sku: row.sku, unitPrice: row.unitPrice, unitsInStock: row.unitsInStock, discontinued: row.discontinued, description: row.description})
CREATE (p)-[:PART_OF]->(c)
CREATE (s)-[:SUPPLIES]->(p)`

	_, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMultiMatchCreateBatch,
		"products seed with one stale supplier index entry must keep UnwindMultiMatchCreateBatch; got trace=%+v", trace)

	products, err := exec.Execute(ctx, `MATCH (p:Product) RETURN count(p) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(rows)), products.Rows[0][0])

	supplies, err := exec.Execute(ctx, `MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(rows)), supplies.Rows[0][0])
}

// TestNorthwindSeeder_OrdersPass1HitsUnwindMultiMatchCreate verifies Pass 1
// of the order seed (Order node + PURCHASED edge).
func TestNorthwindSeeder_OrdersPass1HitsUnwindMultiMatchCreate(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	ctx := context.Background()
	// The fast path REQUIRES an index on the MATCH-side property.
	_, err := exec.Execute(ctx,
		"CREATE INDEX customer_id IF NOT EXISTS FOR (n:Customer) ON (n.customerID)", nil)
	require.NoError(t, err)
	for i := 1; i <= 8; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(
			`CREATE (:Customer {customerID: %d, companyName: 'Cust%d'})`, i, i), nil)
		require.NoError(t, err)
	}

	rows := make([]interface{}, 0, 30)
	for i := 0; i < 30; i++ {
		rows = append(rows, map[string]interface{}{
			"orderID":     int64(10000 + i),
			"customerID":  int64((i % 8) + 1),
			"shipCity":    "TestCity",
			"shipCountry": "US",
			"orderDate":   int64(1700000000 + i*3600),
			"notes":       "notes",
		})
	}

	query := `
UNWIND $rows AS row
MATCH (c:Customer {customerID: row.customerID})
CREATE (o:Order {orderID: row.orderID, shipCity: row.shipCity, shipCountry: row.shipCountry, orderDate: row.orderDate, notes: row.notes})
CREATE (c)-[:PURCHASED]->(o)`

	_, err = exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMultiMatchCreateBatch,
		"order pass-1 must hit UnwindMultiMatchCreateBatch; got trace=%+v", trace)

	orders, err := exec.Execute(ctx, `MATCH (o:Order) RETURN count(o) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(30), orders.Rows[0][0])

	purchased, err := exec.Execute(ctx, `MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(30), purchased.Rows[0][0])
}

// TestNorthwindSeeder_OrdersPass1WithTrailingWithReturnCountHitsFastPath
// verifies the same Pass 1 shape still hits the fast path when the query
// carries a simple trailing WITH projection and count RETURN.
func TestNorthwindSeeder_OrdersPass1WithTrailingWithReturnCountHitsFastPath(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx,
		"CREATE INDEX customer_id IF NOT EXISTS FOR (n:Customer) ON (n.customerID)", nil)
	require.NoError(t, err)
	for i := 1; i <= 8; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(
			`CREATE (:Customer {customerID: %d, companyName: 'Cust%d'})`, i, i), nil)
		require.NoError(t, err)
	}

	rows := make([]interface{}, 0, 30)
	for i := 0; i < 30; i++ {
		rows = append(rows, map[string]interface{}{
			"orderID":     int64(10000 + i),
			"customerID":  int64((i % 8) + 1),
			"shipCity":    "TestCity",
			"shipCountry": "US",
			"orderDate":   int64(1700000000 + i*3600),
			"notes":       "notes",
		})
	}

	query := `
UNWIND $rows AS row
MATCH (c:Customer {customerID: row.customerID})
CREATE (o:Order {orderID: row.orderID, shipCity: row.shipCity, shipCountry: row.shipCountry, orderDate: row.orderDate, notes: row.notes})
CREATE (c)-[:PURCHASED]->(o)
WITH o, row
RETURN count(o) AS n`

	res, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(30), res.Rows[0][0])

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMultiMatchCreateBatch,
		"order pass-1 WITH/RETURN shape must hit UnwindMultiMatchCreateBatch; got trace=%+v", trace)

	orders, err := exec.Execute(ctx, `MATCH (o:Order) RETURN count(o) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(30), orders.Rows[0][0])

	purchased, err := exec.Execute(ctx, `MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(30), purchased.Rows[0][0])
}

// TestNorthwindSeeder_OrdersPass2HitsUnwindMultiMatchCreate verifies Pass 2
// of the order seed (ORDERS edges via two independent MATCHes).
func TestNorthwindSeeder_OrdersPass2HitsUnwindMultiMatchCreate(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	ctx := context.Background()
	// The fast path REQUIRES indexes on both MATCH-side properties.
	for _, ddl := range []string{
		"CREATE INDEX order_id IF NOT EXISTS FOR (n:Order) ON (n.orderID)",
		"CREATE INDEX product_id IF NOT EXISTS FOR (n:Product) ON (n.productID)",
	} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err, "seed DDL: %s", ddl)
	}
	for i := 1; i <= 10; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(
			`CREATE (:Order {orderID: %d, shipCity: 'x', shipCountry: 'US', orderDate: 0, notes: 'n'})`, 10000+i), nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, fmt.Sprintf(
			`CREATE (:Product {productID: %d, productName: 'P%d', unitPrice: 1.0})`, i, i), nil)
		require.NoError(t, err)
	}

	rows := make([]interface{}, 0, 20)
	for i := 0; i < 20; i++ {
		rows = append(rows, map[string]interface{}{
			"orderID":   int64(10001 + i%10),
			"productID": int64(((i + 1) % 10) + 1),
			"quantity":  int64(1 + i%5),
			"discount":  0.1,
		})
	}

	query := `
UNWIND $rows AS row
MATCH (o:Order {orderID: row.orderID})
MATCH (p:Product {productID: row.productID})
CREATE (o)-[:ORDERS {quantity: row.quantity, discount: row.discount}]->(p)`

	_, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMultiMatchCreateBatch,
		"order pass-2 must hit UnwindMultiMatchCreateBatch; got trace=%+v", trace)

	ordersEdges, err := exec.Execute(ctx, `MATCH ()-[r:ORDERS]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(20), ordersEdges.Rows[0][0])
}
