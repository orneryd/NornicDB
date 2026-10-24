package fabric

import (
	"os"
	"strings"
	"testing"
)

// TestMain installs testAccessMode: the Cypher executor installs the real
// access-mode owner, which these package tests do not load.
func TestMain(m *testing.M) {
	SetAccessMode(testAccessMode)
	os.Exit(m.Run())
}

// testAccessMode stands in for the Cypher executor's access-mode owner: a
// fragment writes when its text has a write clause.
func testAccessMode(query string, _ bool) bool {
	upper := strings.ToUpper(query)
	for _, clause := range []string{"CREATE", "MERGE", "DELETE", "SET ", "REMOVE "} {
		if strings.Contains(upper, clause) {
			return true
		}
	}
	return false
}

// withAccessMode installs mode for the rest of the test.
func withAccessMode(t *testing.T, mode AccessMode) {
	t.Helper()
	SetAccessMode(mode)
	t.Cleanup(func() { SetAccessMode(testAccessMode) })
}
