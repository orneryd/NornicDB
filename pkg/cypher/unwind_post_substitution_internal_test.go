package cypher

// Internal unit tests that execute the *already-substituted* form of the
// nested-UNWIND seeder query. Previous tests showed replaceVariableInQuery
// produces clean Cypher; the error arises only when the executor re-parses
// that text. These tests pin which downstream clause triggers the
// "unsupported property value type" corruption.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func buildPostSubstExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
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
	return exec
}

func TestPostSubst_01_WithEmptyMapOnly(t *testing.T) {
	exec := buildPostSubstExecutor(t)
	_, err := exec.Execute(context.Background(), `
		MATCH (c:Customer {customerID: 1})
		CREATE (o:Order {orderID: 9001})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, {}
		RETURN count(o) AS n`, nil)
	require.NoError(t, err, "WITH o, {} should not corrupt the pipeline")
}

func TestPostSubst_02_UnwindListLiteral(t *testing.T) {
	exec := buildPostSubstExecutor(t)
	_, err := exec.Execute(context.Background(), `
		UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef
		RETURN count(prodRef) AS n`, nil)
	require.NoError(t, err, "UNWIND of an inline list of maps must parse")
}

func TestPostSubst_03_WithEmptyMapThenUnwindListLiteral(t *testing.T) {
	exec := buildPostSubstExecutor(t)
	_, err := exec.Execute(context.Background(), `
		MATCH (c:Customer {customerID: 1})
		CREATE (o:Order {orderID: 9001})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, {}
		UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef
		RETURN count(prodRef) AS n`, nil)
	require.NoError(t, err, "WITH o, {} followed by UNWIND list-of-maps must parse")
}

func TestPostSubst_04_FullForm(t *testing.T) {
	exec := buildPostSubstExecutor(t)
	_, err := exec.Execute(context.Background(), `
		MATCH (c:Customer {customerID: 1})
		CREATE (o:Order {orderID: 9001})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, {}
		UNWIND [{productID: 1, quantity: 3}, {productID: 2, quantity: 5}] AS prodRef
		MATCH (p:Product {productID: prodRef.productID})
		CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)`, nil)
	require.NoError(t, err, "full substituted form must parse and run")
}
