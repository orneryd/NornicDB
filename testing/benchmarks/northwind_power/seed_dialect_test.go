package main

import (
	"strings"
	"testing"
)

func TestLadybugCypherForPhaseFoldsChainedCreates(t *testing.T) {
	products := ladybugCypherForPhase("products", productsCreateCypher)
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

	orders := ladybugCypherForPhase("orders", ordersCreateCypher)
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
	if got := ladybugCypherForPhase("categories", categoriesCreateCypher); got != categoriesCreateCypher {
		t.Fatalf("categories phase should pass through unchanged, got:\n%s", got)
	}
	if got := ladybugCypherForPhase("order-lines", orderLinesCreateCypher); got != orderLinesCreateCypher {
		t.Fatalf("order-lines phase should pass through unchanged, got:\n%s", got)
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
