package cypher

import "testing"

// TestPipelineRejectsCommaMatchThenCreate pins the pipeline splitter's
// behaviour on the setup-style `MATCH (a), (b) CREATE ...` query used by
// TestCallLeadingVectorYieldTailClausePermutations. This shape has MATCH +
// CREATE but no WITH/UNWIND, so the pipeline splitter MUST reject it (ok=false)
// — otherwise the router will divert the query from executeCompoundMatchCreate
// and break the comma-split MATCH binding logic.
func TestPipelineRejectsCommaMatchThenCreate(t *testing.T) {
	q := `MATCH (o:OriginalText {id:'o1'}), (t:TranslatedText {id:'t1'}) CREATE (o)-[:TRANSLATES_TO]->(t)`
	_, ok := canExecuteAsPipeline(q)
	if ok {
		t.Fatalf("pipeline splitter must reject comma-MATCH+CREATE (no WITH/UNWIND) — got ok=true")
	}
}

func TestPipelineAcceptsSeederShape(t *testing.T) {
	q := `MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS]->(p)`
	_, ok := canExecuteAsPipeline(q)
	if !ok {
		t.Fatalf("pipeline splitter must accept full seeder shape")
	}
}
