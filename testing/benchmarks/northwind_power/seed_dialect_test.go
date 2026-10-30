package main

import (
	"strings"
	"testing"
)

func TestFoldChainedCreates(t *testing.T) {
	products := foldChainedCreates("products", productsCreateCypher)
	if got := strings.Count(products, "CREATE"); got != 1 {
		t.Errorf("expected exactly one CREATE keyword, got %d:\n%s", got, products)
	}
	if strings.Contains(products, "CREATE (p)-[:PART_OF]") || strings.Contains(products, "CREATE (s)-[:SUPPLIES]") {
		t.Errorf("products still contains chained CREATE clauses:\n%s", products)
	}
	for _, want := range []string{
		"CREATE (p:Product",
		"(p)-[:PART_OF]->(c)",
		"(s)-[:SUPPLIES]->(p)",
	} {
		if !strings.Contains(products, want) {
			t.Errorf("products missing %q:\n%s", want, products)
		}
	}

	orders := foldChainedCreates("orders", ordersCreateCypher)
	if got := strings.Count(orders, "CREATE"); got != 1 {
		t.Errorf("expected exactly one CREATE keyword, got %d:\n%s", got, orders)
	}
	if strings.Contains(orders, "CREATE (c)-[:PURCHASED]") {
		t.Fatalf("orders still contains chained CREATE clauses:\n%s", orders)
	}
	if !strings.Contains(orders, "CREATE (o:Order") || !strings.Contains(orders, "(c)-[:PURCHASED]->(o)") {
		t.Fatalf("orders malformed:\n%s", orders)
	}

	// Phases without chained CREATE clauses pass through unchanged.
	if got := foldChainedCreates("categories", categoriesCreateCypher); got != categoriesCreateCypher {
		t.Fatalf("categories phase should pass through unchanged, got:\n%s", got)
	}
	if got := foldChainedCreates("order-lines", orderLinesCreateCypher); got != orderLinesCreateCypher {
		t.Fatalf("order-lines phase should pass through unchanged, got:\n%s", got)
	}

	// FalkorDB index rewrite: name and IF NOT EXISTS are stripped, the
	// FOR/ON clause is preserved.
	rewritten, ok := falkorIndexQuery("CREATE INDEX category_id IF NOT EXISTS FOR (n:Category) ON (n.categoryID)")
	if !ok || rewritten != "CREATE INDEX FOR (n:Category) ON (n.categoryID)" {
		t.Fatalf("unexpected index rewrite: %q (matched=%v)", rewritten, ok)
	}
	if _, ok := falkorIndexQuery("CREATE INDEX FOR (n:Category) ON (n.categoryID)"); ok {
		t.Fatal("already-falkor statement should not match the Neo4j pattern")
	}
	for _, q := range ensureIndexQueries {
		if _, ok := falkorIndexQuery(q); !ok {
			t.Errorf("ensureIndexQueries entry does not match the rewrite pattern: %q", q)
		}
	}

	// The Bolt seed statements themselves must keep their Neo4j flavor.
	if !strings.Contains(productsCreateCypher, "CREATE (p)-[:PART_OF]->(c)") ||
		!strings.Contains(productsCreateCypher, "CREATE (s)-[:SUPPLIES]->(p)") {
		t.Fatal("bolt products seed statement was modified")
	}
	if !strings.Contains(ordersCreateCypher, "CREATE (c)-[:PURCHASED]->(o)") {
		t.Fatal("bolt orders seed statement was modified")
	}
}

func TestLadybugDialectQuotesTheOrderLabelOnly(t *testing.T) {
	got := ladybugDialect("MATCH (c:Customer)-[:PURCHASED]->(o:Order) MATCH (x:OrderLine) WHERE o.orderID = 1 RETURN o ORDER BY o.orderID")
	want := "MATCH (c:Customer)-[:PURCHASED]->(o:`Order`) MATCH (x:OrderLine) WHERE o.orderID = 1 RETURN o ORDER BY o.orderID"
	if got != want {
		t.Fatalf("ladybugDialect:\n got  %s\n want %s", got, want)
	}
	if got := ladybugDialect(categoriesCreateCypher); got != categoriesCreateCypher {
		t.Fatalf("statements without the Order label must be unchanged, got %q", got)
	}
	if got := ladybugDialect(ordersCreateCypher); !strings.Contains(got, "(o:`Order` {") {
		t.Fatalf("the orders seed must quote the label, got %q", got)
	}
}
