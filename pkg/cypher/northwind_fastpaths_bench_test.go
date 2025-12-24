package cypher_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupNorthwindBenchExecutor(b *testing.B, products int, orders int) *cypher.StorageExecutor {
	b.Helper()

	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "bench")

	// Categories
	catIDs := make([]storage.NodeID, 0, 8)
	for i := 0; i < 8; i++ {
		id, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("cat-" + string(rune('a'+i))),
			Labels: []string{"Category"},
			Properties: map[string]interface{}{
				"categoryID":   int64(i + 1),
				"categoryName": "Category" + string(rune('A'+i)),
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		catIDs = append(catIDs, id)
	}

	// Suppliers
	supIDs := make([]storage.NodeID, 0, 8)
	for i := 0; i < 8; i++ {
		id, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("sup-" + string(rune('a'+i))),
			Labels: []string{"Supplier"},
			Properties: map[string]interface{}{
				"supplierID":  int64(i + 1),
				"companyName": "Supplier" + string(rune('A'+i)),
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		supIDs = append(supIDs, id)
	}

	// Customers
	custIDs := make([]storage.NodeID, 0, 8)
	for i := 0; i < 8; i++ {
		id, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("cust-" + string(rune('a'+i))),
			Labels: []string{"Customer"},
			Properties: map[string]interface{}{
				"companyName": "Customer" + string(rune('A'+i)),
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		custIDs = append(custIDs, id)
	}

	// Products
	prodIDs := make([]storage.NodeID, 0, products)
	for i := 0; i < products; i++ {
		id, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("prod-" + strconv.Itoa(i+1)),
			Labels: []string{"Product"},
			Properties: map[string]interface{}{
				"productID":   int64(i + 1),
				"productName": "Product" + string(rune('A'+(i%26))),
				"unitPrice":   float64((i%50)+1) * 1.25,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		prodIDs = append(prodIDs, id)

		// PART_OF
		cat := catIDs[i%len(catIDs)]
		if err := store.CreateEdge(&storage.Edge{
			ID:         storage.EdgeID("e-partof-" + strconv.Itoa(i+1)),
			Type:       "PART_OF",
			StartNode:  id,
			EndNode:    cat,
			Properties: map[string]interface{}{},
		}); err != nil {
			b.Fatal(err)
		}

		// SUPPLIES
		sup := supIDs[i%len(supIDs)]
		if err := store.CreateEdge(&storage.Edge{
			ID:         storage.EdgeID("e-supplies-" + strconv.Itoa(i+1)),
			Type:       "SUPPLIES",
			StartNode:  sup,
			EndNode:    id,
			Properties: map[string]interface{}{},
		}); err != nil {
			b.Fatal(err)
		}
	}

	// Orders
	orderIDs := make([]storage.NodeID, 0, orders)
	for i := 0; i < orders; i++ {
		id, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID("order-" + strconv.Itoa(i+1)),
			Labels: []string{"Order"},
			Properties: map[string]interface{}{
				"orderID": int64(10000 + i),
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		orderIDs = append(orderIDs, id)

		// PURCHASED
		cust := custIDs[i%len(custIDs)]
		if err := store.CreateEdge(&storage.Edge{
			ID:         storage.EdgeID("e-purchased-" + strconv.Itoa(i+1)),
			Type:       "PURCHASED",
			StartNode:  cust,
			EndNode:    id,
			Properties: map[string]interface{}{},
		}); err != nil {
			b.Fatal(err)
		}

		// ORDERS: 3 products per order
		for j := 0; j < 3; j++ {
			prod := prodIDs[(i*3+j)%len(prodIDs)]
			if err := store.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID("e-orders-" + strconv.Itoa(i+1) + "-" + strconv.Itoa(j+1)),
				Type:      "ORDERS",
				StartNode: id,
				EndNode:   prod,
				Properties: map[string]interface{}{
					"quantity": int64((j % 5) + 1),
				},
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	return cypher.NewStorageExecutor(store)
}

func BenchmarkNorthwindFastPath_ProductsPerCategory(b *testing.B) {
	exec := setupNorthwindBenchExecutor(b, 2000, 2000)
	ctx := context.Background()
	q := `
		MATCH (c:Category)<-[:PART_OF]-(p:Product)
		RETURN c.categoryName, count(p) as productCount
		ORDER BY productCount DESC
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNorthwindFastPath_CustomerCategoryDistinctOrders(b *testing.B) {
	exec := setupNorthwindBenchExecutor(b, 2000, 2000)
	ctx := context.Background()
	q := `
		MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)
		RETURN c.companyName, cat.categoryName, count(DISTINCT o) as orders
		ORDER BY orders DESC
		LIMIT 10
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNorthwindFastPath_OptionalMatchOrdersCount(b *testing.B) {
	exec := setupNorthwindBenchExecutor(b, 2000, 2000)
	ctx := context.Background()
	q := `
		MATCH (p:Product)
		OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
		RETURN p.productName, count(o) as orderCount
		ORDER BY orderCount DESC
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNorthwindFastPath_RevenueByProduct(b *testing.B) {
	exec := setupNorthwindBenchExecutor(b, 2000, 2000)
	ctx := context.Background()
	q := `
		MATCH (p:Product)<-[r:ORDERS]-(:Order)
		WITH p, sum(p.unitPrice * r.quantity) as revenue
		RETURN p.productName, revenue
		ORDER BY revenue DESC
		LIMIT 10
	`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			b.Fatal(err)
		}
	}
}
