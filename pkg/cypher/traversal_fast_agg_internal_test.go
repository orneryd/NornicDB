package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryFastSingleHopAgg_GroupValue(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	c1, err := store.CreateNode(&storage.Node{ID: "c1", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryName": "Beverages"}})
	require.NoError(t, err)
	c2, err := store.CreateNode(&storage.Node{ID: "c2", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryName": "Condiments"}})
	require.NoError(t, err)

	p1, err := store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chai", "unitPrice": 18.0}})
	require.NoError(t, err)
	p2, err := store.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chang", "unitPrice": 19.0}})
	require.NoError(t, err)
	p3, err := store.CreateNode(&storage.Node{ID: "p3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Aniseed Syrup", "unitPrice": 10.0}})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "PART_OF", StartNode: p1, EndNode: c1, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", Type: "PART_OF", StartNode: p2, EndNode: c1, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e3", Type: "PART_OF", StartNode: p3, EndNode: c2, Properties: map[string]interface{}{}}))
	ctx := context.Background()

	matches := exec.parseTraversalPattern(ctx, "(c:Category)<-[:PART_OF]-(p:Product)")
	require.NotNil(t, matches)

	items := []returnItem{
		{expr: "c.categoryName"},
		{expr: "count(p)", alias: "productCount"},
	}

	rows, ok, err := exec.tryFastRelationshipAggregations(matches, items)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 2)
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r[0].(string)] = true
	}
	require.True(t, seen["Beverages"])
}

func TestTryFastRelationshipAggregations_EarlyGuards(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	items := []returnItem{{expr: "c.categoryName"}, {expr: "count(p)"}}
	ctx := context.Background()

	matches := exec.parseTraversalPattern(ctx, "(c:Category)<-[:PART_OF*1..2]-(p:Product)")
	require.NotNil(t, matches)
	_, ok, err := exec.tryFastRelationshipAggregations(matches, items)
	require.NoError(t, err)
	assert.False(t, ok)

	chained := &TraversalMatch{
		IsChained: true,
		Segments:  []TraversalSegment{{}, {}, {}, {}},
		Relationship: RelationshipPattern{
			MinHops: 1,
			MaxHops: 1,
		},
	}
	_, ok, err = exec.tryFastRelationshipAggregations(chained, items)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestTryFastSingleHopAgg_AggregateVariants(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	catID, err := store.CreateNode(&storage.Node{
		ID:     "cat",
		Labels: []string{"Category"},
		Properties: map[string]interface{}{
			"categoryName": "Beverages",
		},
	})
	require.NoError(t, err)
	p1, err := store.CreateNode(&storage.Node{
		ID:     "p1",
		Labels: []string{"Product"},
		Properties: map[string]interface{}{
			"unitPrice": int64(10),
			"name":      "Tea",
		},
	})
	require.NoError(t, err)
	p2, err := store.CreateNode(&storage.Node{
		ID:     "p2",
		Labels: []string{"Product"},
		Properties: map[string]interface{}{
			"unitPrice": float64(20),
			"name":      "Coffee",
		},
	})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "e1",
		Type:      "PART_OF",
		StartNode: p1,
		EndNode:   catID,
		Properties: map[string]interface{}{
			"weight": int64(2),
		},
	}))
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "e2",
		Type:      "PART_OF",
		StartNode: p2,
		EndNode:   catID,
		Properties: map[string]interface{}{
			"weight": float64(3.5),
		},
	}))
	ctx := context.Background()

	matches := exec.parseTraversalPattern(ctx, "(c:Category)<-[r:PART_OF]-(p:Product)")
	require.NotNil(t, matches)

	rows, ok, err := exec.tryFastRelationshipAggregations(matches, []returnItem{
		{expr: "c.categoryName"},
		{expr: "sum(r.weight)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "Beverages", rows[0][0])
	assert.Equal(t, float64(5.5), rows[0][1])

	rows, ok, err = exec.tryFastRelationshipAggregations(matches, []returnItem{
		{expr: "c.categoryName"},
		{expr: "avg(p.unitPrice)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, float64(15), rows[0][1])

	rows, ok, err = exec.tryFastRelationshipAggregations(matches, []returnItem{
		{expr: "c.categoryName"},
		{expr: "collect(p.name)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	names, ok := rows[0][1].([]interface{})
	require.True(t, ok)
	assert.ElementsMatch(t, []interface{}{"Tea", "Coffee"}, names)
}

func TestTryFastChainedAgg_SupplierCategoryAndDistinctOrders(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	customerID, err := store.CreateNode(&storage.Node{ID: "cust-1", Labels: []string{"Customer"}, Properties: map[string]interface{}{"companyName": "CustCo"}})
	require.NoError(t, err)
	orderID, err := store.CreateNode(&storage.Node{ID: "ord-1", Labels: []string{"Order"}, Properties: map[string]interface{}{"orderNo": "001"}})
	require.NoError(t, err)
	supplierID, err := store.CreateNode(&storage.Node{ID: "sup-1", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"companyName": "SupCo"}})
	require.NoError(t, err)
	productID, err := store.CreateNode(&storage.Node{ID: "prod-1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chai"}})
	require.NoError(t, err)
	categoryID, err := store.CreateNode(&storage.Node{ID: "cat-1", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryName": "Beverages"}})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-purchased", Type: "PURCHASED", StartNode: customerID, EndNode: orderID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-orders", Type: "ORDERS", StartNode: orderID, EndNode: productID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-supplies", Type: "SUPPLIES", StartNode: supplierID, EndNode: productID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-partof", Type: "PART_OF", StartNode: productID, EndNode: categoryID, Properties: map[string]interface{}{}}))
	ctx := context.Background()

	supplierCategory := exec.parseTraversalPattern(ctx, "(s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)")
	require.NotNil(t, supplierCategory)
	rows, ok, err := exec.tryFastRelationshipAggregations(supplierCategory, []returnItem{
		{expr: "s.companyName"},
		{expr: "c.categoryName"},
		{expr: "count(p)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "SupCo", rows[0][0])
	assert.Equal(t, "Beverages", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	customerCategory := exec.parseTraversalPattern(ctx, "(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)")
	require.NotNil(t, customerCategory)
	rows, ok, err = exec.tryFastRelationshipAggregations(customerCategory, []returnItem{
		{expr: "c.companyName"},
		{expr: "cat.categoryName"},
		{expr: "count(DISTINCT o)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "CustCo", rows[0][0])
	assert.Equal(t, "Beverages", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	customerSupplier := exec.parseTraversalPattern(ctx, "(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)")
	require.NotNil(t, customerSupplier)
	rows, ok, err = exec.tryFastRelationshipAggregations(customerSupplier, []returnItem{
		{expr: "c.companyName"},
		{expr: "s.companyName"},
		{expr: "count(DISTINCT o)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "CustCo", rows[0][0])
	assert.Equal(t, "SupCo", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	// Shape mismatch guard in chained fast path.
	_, ok, err = exec.tryFastRelationshipAggregations(customerSupplier, []returnItem{
		{expr: "c.companyName"},
		{expr: "s.companyName"},
		{expr: "count(o)"},
	})
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestTryFastChainedAgg_CustomerSupplierDistinctOrders_MultiRow(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	customerID, err := store.CreateNode(&storage.Node{ID: "cust-1", Labels: []string{"Customer"}, Properties: map[string]interface{}{"companyName": "Alfreds Futterkiste"}})
	require.NoError(t, err)
	order1ID, err := store.CreateNode(&storage.Node{ID: "ord-1", Labels: []string{"Order"}, Properties: map[string]interface{}{"orderNo": "10643"}})
	require.NoError(t, err)
	order2ID, err := store.CreateNode(&storage.Node{ID: "ord-2", Labels: []string{"Order"}, Properties: map[string]interface{}{"orderNo": "10308"}})
	require.NoError(t, err)
	supplier1ID, err := store.CreateNode(&storage.Node{ID: "sup-1", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"companyName": "Exotic Liquids"}})
	require.NoError(t, err)
	supplier2ID, err := store.CreateNode(&storage.Node{ID: "sup-2", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"companyName": "New Orleans Cajun Delights"}})
	require.NoError(t, err)
	product1ID, err := store.CreateNode(&storage.Node{ID: "prod-1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chai"}})
	require.NoError(t, err)
	product2ID, err := store.CreateNode(&storage.Node{ID: "prod-2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chang"}})
	require.NoError(t, err)
	product3ID, err := store.CreateNode(&storage.Node{ID: "prod-3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Aniseed Syrup"}})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-purchased-1", Type: "PURCHASED", StartNode: customerID, EndNode: order1ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-purchased-2", Type: "PURCHASED", StartNode: customerID, EndNode: order2ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-orders-1", Type: "ORDERS", StartNode: order1ID, EndNode: product1ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-orders-2", Type: "ORDERS", StartNode: order1ID, EndNode: product2ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-orders-3", Type: "ORDERS", StartNode: order2ID, EndNode: product3ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-supplies-1", Type: "SUPPLIES", StartNode: supplier1ID, EndNode: product1ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-supplies-2", Type: "SUPPLIES", StartNode: supplier2ID, EndNode: product2ID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-supplies-3", Type: "SUPPLIES", StartNode: supplier1ID, EndNode: product3ID, Properties: map[string]interface{}{}}))
	ctx := context.Background()

	matches := exec.parseTraversalPattern(ctx, "(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)")
	require.NotNil(t, matches)
	rows, ok, err := exec.tryFastRelationshipAggregations(matches, []returnItem{
		{expr: "c.companyName"},
		{expr: "s.companyName"},
		{expr: "count(DISTINCT o)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 2)

	got := map[string]int64{}
	for _, row := range rows {
		got[row[1].(string)] = row[2].(int64)
	}
	assert.Equal(t, int64(2), got["Exotic Liquids"])
	assert.Equal(t, int64(1), got["New Orleans Cajun Delights"])
}

func TestTryFastChainedAgg_UsesProjectedBoundaryProperties(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	customerID, err := store.CreateNode(&storage.Node{ID: "cust-1", Labels: []string{"Customer"}, Properties: map[string]interface{}{"buyer": "CustCo"}})
	require.NoError(t, err)
	orderID, err := store.CreateNode(&storage.Node{ID: "ord-1", Labels: []string{"Order"}, Properties: map[string]interface{}{"code": "001"}})
	require.NoError(t, err)
	supplierID, err := store.CreateNode(&storage.Node{ID: "sup-1", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"display": "SupplyCo", "vendor": "SupplyCo"}})
	require.NoError(t, err)
	productID, err := store.CreateNode(&storage.Node{ID: "prod-1", Labels: []string{"Product"}, Properties: map[string]interface{}{"sku": "chai"}})
	require.NoError(t, err)
	categoryID, err := store.CreateNode(&storage.Node{ID: "cat-1", Labels: []string{"Category"}, Properties: map[string]interface{}{"bucket": "Beverages"}})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-purchased", Type: "PURCHASED", StartNode: customerID, EndNode: orderID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-orders", Type: "ORDERS", StartNode: orderID, EndNode: productID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-supplies", Type: "SUPPLIES", StartNode: supplierID, EndNode: productID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-partof", Type: "PART_OF", StartNode: productID, EndNode: categoryID, Properties: map[string]interface{}{}}))
	ctx := context.Background()

	supplierCategory := exec.parseTraversalPattern(ctx, "(s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)")
	require.NotNil(t, supplierCategory)
	rows, ok, err := exec.tryFastRelationshipAggregations(supplierCategory, []returnItem{
		{expr: "s.display"},
		{expr: "c.bucket"},
		{expr: "count(p)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "SupplyCo", rows[0][0])
	assert.Equal(t, "Beverages", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	customerCategory := exec.parseTraversalPattern(ctx, "(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)")
	require.NotNil(t, customerCategory)
	rows, ok, err = exec.tryFastRelationshipAggregations(customerCategory, []returnItem{
		{expr: "c.buyer"},
		{expr: "cat.bucket"},
		{expr: "count(DISTINCT o)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "CustCo", rows[0][0])
	assert.Equal(t, "Beverages", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	customerSupplier := exec.parseTraversalPattern(ctx, "(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)")
	require.NotNil(t, customerSupplier)
	rows, ok, err = exec.tryFastRelationshipAggregations(customerSupplier, []returnItem{
		{expr: "c.buyer"},
		{expr: "s.vendor"},
		{expr: "count(DISTINCT o)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "CustCo", rows[0][0])
	assert.Equal(t, "SupplyCo", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])
}
