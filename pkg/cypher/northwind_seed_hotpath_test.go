package cypher_test

// Deterministic hot-path assertions for the query shapes used by
// testing/benchmarks/northwind_power (the NornicDB vs Neo4j benchmark
// orchestrator). Each test sends the exact Cypher that the seeder sends,
// and asserts:
//   1. the query succeeds (no parser / evaluator errors)
//   2. the executor records the expected hot-path flag so we catch
//      silent regressions back onto the slow per-row path.
//
// When these tests go red, fix the executor or seeder before shipping — do
// NOT mask the failure, because a cold-path UNWIND+MATCH+CREATE will stall
// the benchmark harness at medium scales.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertNoHotPathRegression fails the test if the trace indicates a known
// scan fallback. Presence of a "used" hot-path flag is asserted per-case by
// the caller, because different queries legitimately land on different
// specialised paths.
func assertNoScanFallback(t *testing.T, trace cypher.HotPathTrace) {
	t.Helper()
	if trace.MergeScanFallbackUsed {
		t.Errorf("hot path regression: MergeScanFallbackUsed=true — should use schema-indexed MERGE")
	}
	if trace.OuterScanFallbackUsed {
		t.Errorf("hot path regression: OuterScanFallbackUsed=true — should use OuterIndexTopK")
	}
}

// seedBaseGraph writes the Category / Supplier / Customer nodes used by the
// product + order UNWIND batches. Kept tiny and deterministic — this is about
// exercising the executor paths, not throughput.
func seedBaseGraph(t *testing.T, exec *cypher.StorageExecutor) {
	t.Helper()
	const N = 8
	for i := 0; i < N; i++ {
		testutil.MustExecute(t, exec,
			fmt.Sprintf(`CREATE (:Category {categoryID: %d, categoryName: 'Cat%d'})`, i+1, i+1))
		testutil.MustExecute(t, exec,
			fmt.Sprintf(`CREATE (:Supplier {supplierID: %d, companyName: 'Sup%d'})`, i+1, i+1))
		testutil.MustExecute(t, exec,
			fmt.Sprintf(`CREATE (:Customer {customerID: %d, companyName: 'Cust%d'})`, i+1, i+1))
	}
}

// TestSeeder_ProductUnwindMatchCreate covers the exact product UNWIND shape
// used by testing/benchmarks/northwind_power:
//
//	UNWIND $rows AS row
//	MATCH (c:Category {categoryID: row.categoryID})
//	MATCH (s:Supplier {supplierID: row.supplierID})
//	CREATE (p:Product {...})
//	CREATE (p)-[:PART_OF]->(c)
//	CREATE (s)-[:SUPPLIES]->(p)
//
// The batched form MUST hit UnwindFixedChainLinkBatch (or another batch hot
// path) — per-row execution takes ~5ms per row and blows up at scale.
func TestSeeder_ProductUnwindMatchCreate_HitsBatchPath(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	seedBaseGraph(t, exec)

	rows := make([]interface{}, 0, 50)
	for i := 0; i < 50; i++ {
		rows = append(rows, map[string]interface{}{
			"productID":   int64(i + 1),
			"productName": fmt.Sprintf("Prod%d", i+1),
			"unitPrice":   float64((i%20)+1) * 1.25,
			"description": "A mid-size description that exercises the property write path with enough bytes to cross the inline threshold.",
			"categoryID":  int64((i % 8) + 1),
			"supplierID":  int64((i % 8) + 1),
		})
	}

	query := `
		UNWIND $rows AS row
		MATCH (c:Category {categoryID: row.categoryID})
		MATCH (s:Supplier {supplierID: row.supplierID})
		CREATE (p:Product {productID: row.productID, productName: row.productName,
		                   unitPrice: row.unitPrice, description: row.description})
		CREATE (p)-[:PART_OF]->(c)
		CREATE (s)-[:SUPPLIES]->(p)`

	start := time.Now()
	_, err := exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows})
	elapsed := time.Since(start)
	require.NoError(t, err, "product UNWIND+MATCH+CREATE must not error")

	// Verify side-effects.
	countRes, err := exec.Execute(context.Background(), `MATCH (p:Product) RETURN count(p) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(50), countRes.Rows[0][0], "expected 50 Product nodes")

	partOfRes, err := exec.Execute(context.Background(), `MATCH ()-[r:PART_OF]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(50), partOfRes.Rows[0][0], "expected 50 PART_OF edges")

	suppliesRes, err := exec.Execute(context.Background(), `MATCH ()-[r:SUPPLIES]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(50), suppliesRes.Rows[0][0], "expected 50 SUPPLIES edges")

	// Hot-path assertion. The trace records the path hit for the Execute call
	// at the top of this test — we do the assert AFTER the counts checks so
	// we get useful error messages if the main query failed.
	trace := exec.LastHotPathTrace()
	// NOTE: we re-run the same Execute to capture the trace (count queries
	// overwrote it above). This is the actual shape under test.
	_, err = exec.Execute(context.Background(), `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)
	seedBaseGraph(t, exec)
	start = time.Now()
	_, err = exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows})
	elapsed = time.Since(start)
	require.NoError(t, err)
	trace = exec.LastHotPathTrace()

	t.Logf("product UNWIND+MATCH+CREATE 50 rows: %s, trace=%+v", elapsed, trace)
	assertNoScanFallback(t, trace)

	// This is the assertion that flags when we're on the slow path:
	if !trace.UnwindFixedChainLinkBatch && !trace.UnwindMergeChainBatch && !trace.FabricBatchedApplyRows && !trace.UnwindMultiMatchCreateBatch {
		t.Errorf("HOT PATH MISS: product UNWIND+MATCH+CREATE did not hit any batch path.\n"+
			"  trace: %+v\n"+
			"  elapsed: %s (>5ms per row indicates per-row fallback)\n"+
			"  fix: extend executeUnwindFixedChainLinkBatch/executeUnwindMergeChainBatch to cover this shape in pkg/cypher/clauses.go",
			trace, elapsed)
	}

	// Soft perf guard: 50 rows should take well under 250ms on the batch path.
	// This is deliberately loose to avoid flakiness in CI; the hot-path flag
	// is the authoritative signal. If we blow through 2s, something is very
	// wrong even on the batch path.
	if elapsed > 2*time.Second {
		t.Errorf("perf regression: 50-row UNWIND+MATCH+CREATE took %s (expected <250ms)", elapsed)
	}
}

// TestSeeder_OrdersUnwindNestedUnwind covers the exact order UNWIND shape:
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
// This is the query that currently errors with
//
//	"unsupported property value type" / "{customerID" tokenization
//
// on NornicDB. The test pins the expected behaviour and surfaces the failure
// deterministically in the test suite.
func TestSeeder_OrdersUnwindNestedUnwind_Succeeds(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	seedBaseGraph(t, exec)
	// Also seed a few products so the nested UNWIND has targets.
	for i := 0; i < 8; i++ {
		testutil.MustExecute(t, exec, fmt.Sprintf(
			`CREATE (:Product {productID: %d, productName: 'P%d', unitPrice: 1.0})`, i+1, i+1))
	}

	rows := make([]interface{}, 0, 20)
	for i := 0; i < 20; i++ {
		products := []interface{}{
			map[string]interface{}{"productID": int64((i % 8) + 1), "quantity": int64(1 + i%5), "discount": 0.1},
			map[string]interface{}{"productID": int64(((i + 1) % 8) + 1), "quantity": int64(1 + (i+2)%5), "discount": 0.0},
		}
		rows = append(rows, map[string]interface{}{
			"orderID":    int64(10000 + i),
			"customerID": int64((i % 8) + 1),
			"shipCity":   "TestCity",
			"orderDate":  int64(1700000000 + i*3600),
			"notes":      "test order",
			"products":   products,
		})
	}

	query := `
		UNWIND $rows AS row
		MATCH (c:Customer {customerID: row.customerID})
		CREATE (o:Order {orderID: row.orderID, shipCity: row.shipCity,
		                 orderDate: row.orderDate, notes: row.notes})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, row
		UNWIND row.products AS prodRef
		MATCH (p:Product {productID: prodRef.productID})
		CREATE (o)-[:ORDERS {quantity: prodRef.quantity, discount: prodRef.discount}]->(p)`

	start := time.Now()
	_, err := exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows})
	elapsed := time.Since(start)
	trace := exec.LastHotPathTrace()

	t.Logf("nested-UNWIND order seed 20 rows: %s, trace=%+v", elapsed, trace)

	// The known failure mode is a parser/evaluator error — assert it doesn't.
	require.NoError(t, err, "nested-UNWIND order seed must not error")

	// Verify side-effects.
	orderRes, err := exec.Execute(context.Background(), `MATCH (o:Order) RETURN count(o) AS n`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(20), orderRes.Rows[0][0], "expected 20 Order nodes")

	purchRes, err := exec.Execute(context.Background(), `MATCH ()-[r:PURCHASED]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(20), purchRes.Rows[0][0], "expected 20 PURCHASED edges")

	ordersRes, err := exec.Execute(context.Background(), `MATCH ()-[r:ORDERS]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(40), ordersRes.Rows[0][0], "expected 40 ORDERS edges (2 per order)")
}

// TestSeeder_OrderLineEdgesFlat covers the fallback flat-pass shape we use
// when nested UNWIND isn't available — each edge is one flat row.
//
//	UNWIND $rows AS row
//	MATCH (o:Order {orderID: row.orderID})
//	MATCH (p:Product {productID: row.productID})
//	CREATE (o)-[:ORDERS {quantity: row.quantity}]->(p)
func TestSeeder_OrderLineEdgesFlat_HitsBatchPath(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)
	seedBaseGraph(t, exec)
	for i := 0; i < 8; i++ {
		testutil.MustExecute(t, exec, fmt.Sprintf(
			`CREATE (:Product {productID: %d, productName: 'P%d', unitPrice: 1.0})`, i+1, i+1))
	}
	// Pre-create orders.
	for i := 0; i < 20; i++ {
		testutil.MustExecute(t, exec, fmt.Sprintf(
			`CREATE (:Order {orderID: %d, shipCity: 'X', notes: 'n'})`, 10000+i))
	}

	rows := make([]interface{}, 0, 40)
	for i := 0; i < 20; i++ {
		for j := 0; j < 2; j++ {
			rows = append(rows, map[string]interface{}{
				"orderID":   int64(10000 + i),
				"productID": int64(((i + j) % 8) + 1),
				"quantity":  int64(1 + (i+j)%5),
				"discount":  0.1,
			})
		}
	}

	query := `
		UNWIND $rows AS row
		MATCH (o:Order {orderID: row.orderID})
		MATCH (p:Product {productID: row.productID})
		CREATE (o)-[:ORDERS {quantity: row.quantity, discount: row.discount}]->(p)`

	start := time.Now()
	_, err := exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows})
	elapsed := time.Since(start)
	require.NoError(t, err, "order-line flat UNWIND+MATCH+CREATE must not error")

	res, err := exec.Execute(context.Background(), `MATCH ()-[r:ORDERS]->() RETURN count(r) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(40), res.Rows[0][0], "expected 40 ORDERS edges")

	// Re-run to capture the trace without count queries overwriting it.
	_, err = exec.Execute(context.Background(), `MATCH ()-[r:ORDERS]->() DELETE r`, nil)
	require.NoError(t, err)
	start = time.Now()
	_, err = exec.Execute(context.Background(), query, map[string]interface{}{"rows": rows})
	elapsed = time.Since(start)
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()

	t.Logf("order-line flat UNWIND+MATCH+CREATE 40 rows: %s, trace=%+v", elapsed, trace)
	assertNoScanFallback(t, trace)

	if !trace.UnwindFixedChainLinkBatch && !trace.UnwindMergeChainBatch && !trace.FabricBatchedApplyRows && !trace.UnwindMultiMatchCreateBatch {
		t.Errorf("HOT PATH MISS: order-line flat UNWIND+MATCH+CREATE did not hit any batch path.\n"+
			"  trace: %+v\n"+
			"  elapsed: %s\n"+
			"  fix: extend executeUnwindFixedChainLinkBatch in pkg/cypher/clauses.go to cover\n"+
			"       the two-MATCH edge-creation shape",
			trace, elapsed)
	}

	if elapsed > 2*time.Second {
		t.Errorf("perf regression: 40-row order-line UNWIND took %s", elapsed)
	}
}

// Quick smoke: BareUnwindCreate should never miss the batch path.
// If even this test fails, we have a much deeper regression and the richer
// tests above won't tell us much.
func TestSeeder_BareUnwindCreate_HitsBatchPath(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)

	rows := make([]interface{}, 0, 100)
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]interface{}{
			"id":          int64(i),
			"description": "mid-size prop",
		})
	}
	_, err := exec.Execute(context.Background(),
		`UNWIND $rows AS row CREATE (:Widget {id: row.id, description: row.description})`,
		map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	t.Logf("bare UNWIND+CREATE trace: %+v", trace)

	// We don't assert a specific flag here — bare CREATE can legitimately go
	// through the generic single-mutation path without triggering a batch
	// flag. We only care that there's no scan fallback and no error.
	assertNoScanFallback(t, trace)

	// Verify the count.
	cnt, err := exec.Execute(context.Background(), `MATCH (w:Widget) RETURN count(w) AS n`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(100), cnt.Rows[0][0])

	// Silence unused-import guard on storage import when the build tag filters
	// out other tests.
	_ = storage.NodeID("unused")
}
