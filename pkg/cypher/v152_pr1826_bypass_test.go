package cypher

import "testing"

// Diagnostic branch only: each reproduction with the named step bypassed.
func TestV152P26Bypass(t *testing.T) {
	for _, bypass := range []string{"none", "ST", "R6"} {
		t.Run(bypass, func(t *testing.T) {
			t.Setenv("P26_BYPASS", bypass)
			t.Run("ReturnedListInTransaction", TestV152P26ReturnedListInTransaction)
			t.Run("EmbeddedVectorRead", TestV152P26EmbeddedVectorRead)
		})
	}
}
