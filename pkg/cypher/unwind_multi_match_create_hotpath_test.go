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
	"github.com/stretchr/testify/require"
)

func seedCategoriesAndSuppliers(t *testing.T, exec *cypher.StorageExecutor, catN, supN int) {
	t.Helper()
	ctx := context.Background()
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

// TestNorthwindSeeder_OrdersPass1HitsUnwindMultiMatchCreate verifies Pass 1
// of the order seed (Order node + PURCHASED edge).
func TestNorthwindSeeder_OrdersPass1HitsUnwindMultiMatchCreate(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	ctx := context.Background()
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

	_, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
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

// TestNorthwindSeeder_OrdersPass2HitsUnwindMultiMatchCreate verifies Pass 2
// of the order seed (ORDERS edges via two independent MATCHes).
func TestNorthwindSeeder_OrdersPass2HitsUnwindMultiMatchCreate(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	ctx := context.Background()
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
