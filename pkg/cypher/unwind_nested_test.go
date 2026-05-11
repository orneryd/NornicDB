package cypher_test

// Deterministic, in-process unit tests for nested UNWIND support.
//
// The seeder at testing/benchmarks/northwind_power needs this shape:
//
//	UNWIND $rows AS row
//	MATCH (c:Customer {customerID: row.customerID})
//	CREATE (o:Order {...})
//	CREATE (c)-[:PURCHASED]->(o)
//	WITH o, row
//	UNWIND row.products AS prodRef
//	MATCH (p:Product {productID: prodRef.productID})
//	CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)
//
// Each progressive test isolates a single Cypher feature added on top of the
// previous, so when one goes red we see exactly where in the pipeline the
// regression lives.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/stretchr/testify/require"
)

func seedNestedUnwindBase(t *testing.T) (*cypher.StorageExecutor, context.Context) {
	t.Helper()
	exec := testutil.SetupTestExecutor(t)
	ctx := context.Background()
	// Customers and products to match against.
	for i := 1; i <= 3; i++ {
		_, err := exec.Execute(ctx,
			`CREATE (:Customer {customerID: $id, companyName: 'Cust'})`,
			map[string]interface{}{"id": int64(i)})
		require.NoError(t, err)
	}
	for i := 1; i <= 5; i++ {
		_, err := exec.Execute(ctx,
			`CREATE (:Product {productID: $id, productName: 'P'})`,
			map[string]interface{}{"id": int64(i)})
		require.NoError(t, err)
	}
	return exec, ctx
}

// level 1: flat UNWIND + single MATCH + CREATE. Already passes.
func TestNestedUnwind_01_FlatMatchCreate(t *testing.T) {
	exec, ctx := seedNestedUnwindBase(t)
	rows := []interface{}{
		map[string]interface{}{"customerID": int64(1), "orderID": int64(9001)},
		map[string]interface{}{"customerID": int64(2), "orderID": int64(9002)},
	}
	_, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		MATCH (c:Customer {customerID: row.customerID})
		CREATE (o:Order {orderID: row.orderID})
		CREATE (c)-[:PURCHASED]->(o)`,
		map[string]interface{}{"rows": rows})
	require.NoError(t, err)

	cnt, err := exec.Execute(ctx, `MATCH (o:Order) RETURN count(o) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt.Rows[0][0])
}

// level 2: adds trailing `WITH o, row`. Without fix, substituting bare `row`
// with the full map literal corrupts the Order property map.
func TestNestedUnwind_02_AddsTrailingWithRow(t *testing.T) {
	exec, ctx := seedNestedUnwindBase(t)
	rows := []interface{}{
		map[string]interface{}{
			"customerID": int64(1),
			"orderID":    int64(9001),
			"products":   []interface{}{map[string]interface{}{"productID": int64(1), "quantity": int64(3)}},
		},
	}
	_, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		MATCH (c:Customer {customerID: row.customerID})
		CREATE (o:Order {orderID: row.orderID})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, row
		RETURN count(o) AS n`,
		map[string]interface{}{"rows": rows})
	require.NoError(t, err, "WITH o, row should not corrupt the Order CREATE property map")

	cnt, err := exec.Execute(ctx, `MATCH (o:Order) RETURN count(o) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt.Rows[0][0])
}

// level 3: adds `UNWIND row.products AS prodRef`. This is the nested UNWIND
// that the seeder needs. `row.products` must be substituted to the concrete
// list value while preserving any downstream references to prodRef.
func TestNestedUnwind_03_NestedUnwindRowProducts(t *testing.T) {
	exec, ctx := seedNestedUnwindBase(t)
	rows := []interface{}{
		map[string]interface{}{
			"customerID": int64(1),
			"orderID":    int64(9001),
			"products": []interface{}{
				map[string]interface{}{"productID": int64(1), "quantity": int64(3)},
				map[string]interface{}{"productID": int64(2), "quantity": int64(5)},
			},
		},
	}
	_, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		MATCH (c:Customer {customerID: row.customerID})
		CREATE (o:Order {orderID: row.orderID})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, row
		UNWIND row.products AS prodRef
		RETURN count(prodRef) AS n`,
		map[string]interface{}{"rows": rows})
	require.NoError(t, err, "nested UNWIND row.products must not corrupt the outer CREATE")
}

// level 4: adds inner `MATCH (p:Product {productID: prodRef.productID})`.
// After `row.products` is substituted to `[{productID: N, quantity: M}, ...]`,
// the inner UNWIND binds prodRef per iteration and the inner MATCH must find
// the existing Product.
func TestNestedUnwind_04_InnerMatchOnProdRef(t *testing.T) {
	exec, ctx := seedNestedUnwindBase(t)
	rows := []interface{}{
		map[string]interface{}{
			"customerID": int64(1),
			"orderID":    int64(9001),
			"products": []interface{}{
				map[string]interface{}{"productID": int64(1), "quantity": int64(3)},
				map[string]interface{}{"productID": int64(2), "quantity": int64(5)},
			},
		},
	}
	_, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		MATCH (c:Customer {customerID: row.customerID})
		CREATE (o:Order {orderID: row.orderID})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, row
		UNWIND row.products AS prodRef
		MATCH (p:Product {productID: prodRef.productID})
		RETURN count(p) AS n`,
		map[string]interface{}{"rows": rows})
	require.NoError(t, err, "inner MATCH on prodRef.productID must find existing Products")
}

// level 5: full seeder shape — inner MATCH + CREATE edge with prodRef props.
func TestNestedUnwind_05_FullSeederShape(t *testing.T) {
	exec, ctx := seedNestedUnwindBase(t)
	rows := []interface{}{
		map[string]interface{}{
			"customerID": int64(1),
			"orderID":    int64(9001),
			"products": []interface{}{
				map[string]interface{}{"productID": int64(1), "quantity": int64(3)},
				map[string]interface{}{"productID": int64(2), "quantity": int64(5)},
			},
		},
		map[string]interface{}{
			"customerID": int64(2),
			"orderID":    int64(9002),
			"products": []interface{}{
				map[string]interface{}{"productID": int64(3), "quantity": int64(1)},
			},
		},
	}
	_, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		MATCH (c:Customer {customerID: row.customerID})
		CREATE (o:Order {orderID: row.orderID})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, row
		UNWIND row.products AS prodRef
		MATCH (p:Product {productID: prodRef.productID})
		CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)`,
		map[string]interface{}{"rows": rows})
	require.NoError(t, err, "full seeder shape must succeed")

	ords, err := exec.Execute(ctx, `MATCH ()-[r:ORDERS]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), ords.Rows[0][0], "expected 3 ORDERS edges (2 + 1)")

	purch, err := exec.Execute(ctx, `MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), purch.Rows[0][0], "expected 2 PURCHASED edges")
}
