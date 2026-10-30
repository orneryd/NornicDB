package cypher

import "testing"

func TestKeywordScan_Allocs(t *testing.T) {
	query := "MATCH (n:Person {name: 'Alice'})-[r:KNOWS]->(m:Person) WHERE m.age > 21 WITH m, COUNT(r) AS cnt ORDER BY cnt DESC LIMIT 10 RETURN m.name, cnt"

	allocs := testing.AllocsPerRun(1000, func() {
		_ = findKeywordIndex(query, "MATCH")
		_ = findKeywordIndex(query, "WHERE")
		_ = findKeywordIndex(query, "WITH")
		_ = findKeywordIndex(query, "ORDER BY")
		_ = findKeywordIndex(query, "LIMIT")
		_ = findKeywordIndex(query, "RETURN")
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/run, got %f", allocs)
	}
}
