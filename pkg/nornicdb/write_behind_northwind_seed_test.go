package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// These tests pin the Northwind seed shape against the write-behind buffer:
// buffered-but-unflushed writes must be visible to every read path (scans,
// streams, counters, point reads) and cross-statement endpoints must resolve
// while the flusher drains generations underneath the seeder.

func newAsyncSeedDB(t *testing.T) (*DB, context.Context) {
	t.Helper()
	config := &Config{
		Database: nornicConfig.DatabaseConfig{
			AsyncWritesEnabled: true,
		},
	}
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, context.Background()
}

// TestAsyncWrites_NorthwindProductsSeesBufferedWrites reproduces the products
// phase: MATCH by indexed property, CREATE node + two edges in one UNWIND
// batch, then immediately count via the derived-counter fast paths.
func TestAsyncWrites_NorthwindProductsSeesBufferedWrites(t *testing.T) {
	db, ctx := newAsyncSeedDB(t)

	for _, q := range []string{
		"CREATE INDEX category_id IF NOT EXISTS FOR (n:Category) ON (n.categoryID)",
		"CREATE INDEX supplier_id IF NOT EXISTS FOR (n:Supplier) ON (n.supplierID)",
	} {
		_, err := db.ExecuteCypher(ctx, q, nil)
		require.NoError(t, err)
	}
	for i := 0; i < 8; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (:Category {categoryID: $id})",
			map[string]interface{}{"id": int64(i + 1)})
		require.NoError(t, err)
	}
	for i := 0; i < 8; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (:Supplier {supplierID: $id})",
			map[string]interface{}{"id": int64(i + 1)})
		require.NoError(t, err)
	}

	rows := make([]map[string]interface{}, 0, 1000)
	for i := 0; i < 1000; i++ {
		rows = append(rows, map[string]interface{}{
			"productID":   int64(i + 1),
			"productName": fmt.Sprintf("p%d", i),
			"categoryID":  int64((i % 8) + 1),
			"supplierID":  int64((i % 8) + 1),
		})
	}
	cypher := `UNWIND $rows AS row
 MATCH (c:Category {categoryID: row.categoryID})
 MATCH (s:Supplier {supplierID: row.supplierID})
 CREATE (p:Product {productID: row.productID, productName: row.productName})
 CREATE (p)-[:PART_OF]->(c)
 CREATE (s)-[:SUPPLIES]->(p)`
	_, err := db.ExecuteCypher(ctx, cypher, map[string]interface{}{"rows": rows})
	require.NoError(t, err)

	result, err := db.ExecuteCypher(ctx, "MATCH (n:Product) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1000), result.Rows[0][0], "product count")
	result, err = db.ExecuteCypher(ctx, "MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1000), result.Rows[0][0], "supplies count")
}

// TestAsyncWrites_NorthwindOrdersSeesBufferedCustomers reproduces the orders
// phase: customers may still be buffered when orders MATCH them by indexed
// property and create PURCHASED edges to fresh Order nodes.
func TestAsyncWrites_NorthwindOrdersSeesBufferedCustomers(t *testing.T) {
	db, ctx := newAsyncSeedDB(t)

	_, err := db.ExecuteCypher(ctx,
		"CREATE INDEX customer_id IF NOT EXISTS FOR (n:Customer) ON (n.customerID)", nil)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (:Customer {customerID: $id})",
			map[string]interface{}{"id": int64(i + 1)})
		require.NoError(t, err)
	}

	mkRows := func(offset, n int) []map[string]interface{} {
		rows := make([]map[string]interface{}, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, map[string]interface{}{
				"orderID":    int64(10000 + offset + i),
				"customerID": int64((offset+i)%50 + 1),
			})
		}
		return rows
	}
	cypher := `UNWIND $rows AS row
 MATCH (c:Customer {customerID: row.customerID})
 CREATE (o:Order {orderID: row.orderID})
 CREATE (c)-[:PURCHASED]->(o)`

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rows := mkRows(w*1000, 1000)
			_, err := db.ExecuteCypher(ctx, cypher, map[string]interface{}{"rows": rows})
			if err != nil {
				errs <- fmt.Errorf("worker %d: %w", w, err)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	result, err := db.ExecuteCypher(ctx, "MATCH (n:Order) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, int64(4000), result.Rows[0][0], "order count")
	result, err = db.ExecuteCypher(ctx, "MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, int64(4000), result.Rows[0][0], "purchased count")
}
