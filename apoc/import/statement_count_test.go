package apocimport

import "testing"

// CypherData reports CREATE statement counts without a backing executor.
func TestCypherDataStatementFraming(t *testing.T) {
	input := "CREATE (:N {text:'a;CREATE (fake)'}); /* ; CREATE */ CREATE (:N); CREATEVALUE(); // CREATE (:Fake);\n"
	result, err := CypherData(input, nil)
	if err != nil || result.Nodes != 2 {
		t.Fatalf("CREATE statement count = %v, %v; want 2", result, err)
	}
}
