package cypher

import "testing"

func TestKeywordScan_Allocs(t *testing.T) {
	query := "MATCH (n:Person {name: 'Alice'})-[r:KNOWS]->(m:Person) WHERE m.age > 21 WITH m, COUNT(r) AS cnt ORDER BY cnt DESC LIMIT 10 RETURN m.name, cnt"

	allocs := testing.AllocsPerRun(1000, func() {
		_ = findKeywordIndex(originalBraceView(query), "MATCH")
		_ = findKeywordIndex(originalBraceView(query), "WHERE")
		_ = findKeywordIndex(originalBraceView(query), "WITH")
		_ = findKeywordIndex(originalBraceView(query), "ORDER BY")
		_ = findKeywordIndex(originalBraceView(query), "LIMIT")
		_ = findKeywordIndex(originalBraceView(query), "RETURN")
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/run, got %f", allocs)
	}
}
