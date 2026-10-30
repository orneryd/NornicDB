package cypher_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
)

// BenchmarkSeedProductUnwindMatchCreate mirrors the exact product UNWIND
// shape used by testing/benchmarks/northwind_power at production batch size
// (500 rows). It exists to give -cpuprofile a large enough sample to
// attribute per-row cost inside the implicit-transaction commit path; keep
// it synthetic (single bench, no assertions beyond error-free execution) so
// the profile is dominated by hot-path code, not test scaffolding.
func BenchmarkSeedProductUnwindMatchCreate(b *testing.B) {
	exec := testutil.SetupTestExecutorBench(b)

	// Mirror the real northwind_power seeder: declare property indexes
	// BEFORE seeding. Without these, the UnwindMultiMatchCreate fast path
	// refuses and each batch falls through to a per-row GetNodesByLabel +
	// full-body decode scan (quadratic in label population). Indexes are
	// the single biggest seed-speed knob.
	for _, ddl := range []string{
		`CREATE INDEX category_id IF NOT EXISTS FOR (n:Category) ON (n.categoryID)`,
		`CREATE INDEX supplier_id IF NOT EXISTS FOR (n:Supplier) ON (n.supplierID)`,
	} {
		if _, err := exec.Execute(context.Background(), ddl, nil); err != nil {
			b.Fatalf("index ddl: %v", err)
		}
	}

	// Seed the referential targets once — MATCH needs Category/Supplier nodes
	// to resolve, and seeding them inside the measured loop would swamp the
	// product-create cost.
	const base = 8
	for i := 0; i < base; i++ {
		if _, err := exec.Execute(context.Background(),
			fmt.Sprintf(`CREATE (:Category {categoryID: %d, categoryName: 'Cat%d'})`, i+1, i+1), nil); err != nil {
			b.Fatal(err)
		}
		if _, err := exec.Execute(context.Background(),
			fmt.Sprintf(`CREATE (:Supplier {supplierID: %d, companyName: 'Sup%d'})`, i+1, i+1), nil); err != nil {
			b.Fatal(err)
		}
	}

	rows := make([]interface{}, 0, 500)
	for i := 0; i < 500; i++ {
		rows = append(rows, map[string]interface{}{
			"productID":   int64(i + 1),
			"productName": fmt.Sprintf("Prod%d", i+1),
			"unitPrice":   float64((i%20)+1) * 1.25,
			"description": "A mid-size description that exercises the property write path with enough bytes to cross the inline threshold.",
			"categoryID":  int64((i % base) + 1),
			"supplierID":  int64((i % base) + 1),
		})
	}

	// The exact shape used by the northwind_power seeder. Each Execute is
	// one implicit transaction committing 500 nodes + 1000 edges.
	query := `
		UNWIND $rows AS row
		MATCH (c:Category {categoryID: row.categoryID})
		MATCH (s:Supplier {supplierID: row.supplierID})
		CREATE (p:Product {productID: row.productID, productName: row.productName,
		                   unitPrice: row.unitPrice, description: row.description})
		CREATE (p)-[:PART_OF]->(c)
		CREATE (s)-[:SUPPLIES]->(p)`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Fresh product IDs every iteration so we're measuring pure-create
		// cost and not MERGE / duplicate-check overhead.
		for j := range rows {
			m := rows[j].(map[string]interface{})
			m["productID"] = int64(i*len(rows) + j + 1)
			m["productName"] = fmt.Sprintf("Prod%d_%d", i, j)
		}
		if _, err := exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSeedOrderLinesFlat mirrors the second-pass shape used by the
// northwind_power seeder: UNWIND → two MATCH-by-prop clauses → one CREATE
// edge. The Order and Product populations are deliberately sized so that
// a slow label-scan fallback would be obvious in the profile.
func BenchmarkSeedOrderLinesFlat(b *testing.B) {
	exec := testutil.SetupTestExecutorBench(b)

	for _, ddl := range []string{
		`CREATE INDEX order_id IF NOT EXISTS FOR (n:Order) ON (n.orderID)`,
		`CREATE INDEX product_id IF NOT EXISTS FOR (n:Product) ON (n.productID)`,
	} {
		if _, err := exec.Execute(context.Background(), ddl, nil); err != nil {
			b.Fatalf("index ddl: %v", err)
		}
	}

	const orderPop = 2000
	const productPop = 2000
	for i := 0; i < orderPop; i++ {
		if _, err := exec.Execute(context.Background(),
			fmt.Sprintf(`CREATE (:Order {orderID: %d, shipCity: 'C%d'})`, i+1, i+1), nil); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < productPop; i++ {
		if _, err := exec.Execute(context.Background(),
			fmt.Sprintf(`CREATE (:Product {productID: %d, productName: 'P%d'})`, i+1, i+1), nil); err != nil {
			b.Fatal(err)
		}
	}

	rows := make([]interface{}, 0, 500)
	for i := 0; i < 500; i++ {
		rows = append(rows, map[string]interface{}{
			"orderID":   int64((i % orderPop) + 1),
			"productID": int64((i % productPop) + 1),
			"quantity":  int64(1 + i%25),
			"discount":  float64(i%10) * 0.05,
		})
	}

	query := `
		UNWIND $rows AS row
		MATCH (o:Order {orderID: row.orderID})
		MATCH (p:Product {productID: row.productID})
		CREATE (o)-[:ORDERS {quantity: row.quantity, discount: row.discount}]->(p)`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSeedBareCreate isolates node-create cost from MATCH resolution.
// If this is materially cheaper per row than the MATCH+CREATE variant above,
// the delta is MATCH overhead; if it's similar, the bottleneck is in the
// CREATE/commit path itself.
func BenchmarkSeedBareCreate(b *testing.B) {
	exec := testutil.SetupTestExecutorBench(b)

	rows := make([]interface{}, 0, 500)
	for i := 0; i < 500; i++ {
		rows = append(rows, map[string]interface{}{
			"productID":   int64(i + 1),
			"productName": fmt.Sprintf("Prod%d", i+1),
			"unitPrice":   float64((i%20)+1) * 1.25,
			"description": "A mid-size description that exercises the property write path with enough bytes to cross the inline threshold.",
		})
	}

	query := `
		UNWIND $rows AS row
		CREATE (p:Product {productID: row.productID, productName: row.productName,
		                   unitPrice: row.unitPrice, description: row.description})`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range rows {
			m := rows[j].(map[string]interface{})
			m["productID"] = int64(i*len(rows) + j + 1)
			m["productName"] = fmt.Sprintf("Prod%d_%d", i, j)
		}
		if _, err := exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows}); err != nil {
			b.Fatal(err)
		}
	}
}
