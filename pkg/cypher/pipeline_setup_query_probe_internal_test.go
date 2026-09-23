package cypher

import "testing"

func TestPipelineAcceptsCommaMatchThenCreate(t *testing.T) {
	q := `MATCH (o:OriginalText {id:'o1'}), (t:TranslatedText {id:'t1'}) CREATE (o)-[:TRANSLATES_TO]->(t)`
	_, ok := canExecuteAsPipeline(q)
	if !ok {
		t.Fatalf("pipeline splitter must accept comma-MATCH+CREATE")
	}
}

func TestPipelineLeavesStandaloneCreateReturnAtomic(t *testing.T) {
	q := `CREATE (node:Item {num: 1}) RETURN node`
	_, ok := canExecuteAsPipeline(q)
	if ok {
		t.Fatalf("standalone CREATE ... RETURN must remain one atomic write operator")
	}
}

func TestPipelineAppliesStandaloneCreateReturnWindow(t *testing.T) {
	q := `CREATE (node:Item {num: 1}) RETURN node LIMIT 0`
	_, ok := canExecuteAsPipeline(q)
	if !ok {
		t.Fatalf("CREATE ... RETURN with a result window must use the row pipeline")
	}
}

func TestPipelineAcceptsSeederShape(t *testing.T) {
	q := `MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS]->(p)`
	_, ok := canExecuteAsPipeline(q)
	if !ok {
		t.Fatalf("pipeline splitter must accept full seeder shape")
	}
}
