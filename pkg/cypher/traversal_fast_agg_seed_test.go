package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFastAgg_WithCypherSeed_CategoryNamePresent(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seed := []string{
		`CREATE (:Category {categoryID: 1, categoryName: 'Beverages'})`,
		`CREATE (:Category {categoryID: 2, categoryName: 'Condiments'})`,
		`MATCH (c:Category {categoryID: 1}) CREATE (p:Product {productID: 1, productName: 'Chai', unitPrice: 18.0})-[:PART_OF]->(c)`,
		`MATCH (c:Category {categoryID: 2}) CREATE (p:Product {productID: 2, productName: 'Aniseed Syrup', unitPrice: 10.0})-[:PART_OF]->(c)`,
	}
	for _, q := range seed {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}

	edges, err := exec.storage.GetEdgesByType("PART_OF")
	require.NoError(t, err)
	require.NotEmpty(t, edges)

	catIDs := make([]storage.NodeID, 0, len(edges))
	seen := make(map[storage.NodeID]struct{})
	for _, e := range edges {
		if _, ok := seen[e.EndNode]; ok {
			continue
		}
		seen[e.EndNode] = struct{}{}
		catIDs = append(catIDs, e.EndNode)
	}

	nodes, err := exec.storage.BatchGetNodes(catIDs)
	require.NoError(t, err)
	for _, id := range catIDs {
		n := nodes[id]
		require.NotNil(t, n)
		require.Contains(t, n.Properties, "categoryName")
		require.NotEqual(t, "", n.Properties["categoryName"])
	}
}

func TestFastAgg_WithCypherSeed_ProductsPerCategoryQueryShape(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seed := []string{
		`CREATE (:Category {categoryID: 1, categoryName: 'Beverages'})`,
		`CREATE (:Category {categoryID: 2, categoryName: 'Condiments'})`,
		`MATCH (c:Category {categoryID: 1}) CREATE (p:Product {productID: 1, productName: 'Chai', unitPrice: 18.0})-[:PART_OF]->(c)`,
		`MATCH (c:Category {categoryID: 1}) CREATE (p:Product {productID: 2, productName: 'Chang', unitPrice: 19.0})-[:PART_OF]->(c)`,
		`MATCH (c:Category {categoryID: 1}) CREATE (p:Product {productID: 3, productName: 'NoOrders', unitPrice: 5.0})-[:PART_OF]->(c)`,
		`MATCH (c:Category {categoryID: 2}) CREATE (p:Product {productID: 4, productName: 'Aniseed Syrup', unitPrice: 10.0})-[:PART_OF]->(c)`,
	}
	for _, q := range seed {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}

	matches := exec.parseTraversalPattern("(c:Category)<-[:PART_OF]-(p:Product)")
	require.NotNil(t, matches)

	returnItems := exec.parseReturnItems("c.categoryName, count(p) as productCount")
	rows, ok, err := exec.tryFastRelationshipAggregations(matches, returnItems)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 2)
}
