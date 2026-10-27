package cypher

import "testing"

// Run's standalone implementation has no backing executor. This proves only
// the public container's statement count, not execution or mutation outcomes.
func TestRunManyStatementContainers(t *testing.T) {
	for _, query := range []string{
		"RETURN 'a;b'; RETURN `c;d`;",
		"RETURN [1, ';']; /* ; */ RETURN 2; // only a comment ;\n",
	} {
		results, err := RunMany(query, nil)
		if err != nil || len(results) != 2 {
			t.Fatalf("RunMany(%q) returned %d containers, %v; want 2", query, len(results), err)
		}
	}
}
