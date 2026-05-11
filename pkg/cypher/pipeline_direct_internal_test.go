package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestExecutePipeline_DirectInvocation calls executePipeline directly with the
// exact post-substitution query that trips the legacy router. If this passes,
// only the router wiring is wrong.
func TestExecutePipeline_DirectInvocation(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Seed Customer and Product.
	_, err := exec.Execute(ctx, `CREATE (:Customer {customerID: 1, companyName: 'C1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Product {productID: 1, productName: 'P1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Product {productID: 2, productName: 'P2'})`, nil)
	require.NoError(t, err)

	q := `
		MATCH (c:Customer {customerID: 1})
		CREATE (o:Order {orderID: 9001})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, {}
		UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef
		MATCH (p:Product {productID: prodRef.productID})
		CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)`

	result, ok, err := exec.executePipeline(ctx, q)
	require.True(t, ok, "executePipeline must accept this shape")
	require.NoError(t, err, "executePipeline must not error")
	require.NotNil(t, result)

	// Also run via the full Execute path (which goes through the router) to
	// confirm the router delegates to the pipeline executor.
	_, err = exec.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Customer {customerID: 1, companyName: 'C1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Product {productID: 1, productName: 'P1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Product {productID: 2, productName: 'P2'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, q, nil)
	require.NoError(t, err, "Execute (router) must delegate to pipeline executor and succeed")

	// Verify side-effects.
	ords, err := exec.Execute(ctx, `MATCH ()-[r:ORDERS]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), ords.Rows[0][0], "expected 2 ORDERS edges")

	purch, err := exec.Execute(ctx, `MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), purch.Rows[0][0], "expected 1 PURCHASED edge")
}
