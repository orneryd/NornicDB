package server

import "testing"

// Diagnostic branch only: each reproduction with the named step bypassed.
func TestV152P26Bypass(t *testing.T) {
	for _, c := range []struct {
		bypass, name string
		run          func(*testing.T)
	}{
		{"F1", "PolicyVisibility", TestV152P26PolicyVisibility},
		{"F2", "NamedNodePaths", TestV152P26NamedNodePaths},
		{"F2", "TrimmedNodes", TestV152P26TrimmedNodes},
		{"R11", "TrimmedNodes", TestV152P26TrimmedNodes},
		{"R11", "NamedNodePaths", TestV152P26NamedNodePaths},
		{"R11", "TrimmedWrites", TestV152P26TrimmedWrites},
		{"F3", "OlderTransaction", TestV152P26OlderTransaction},
		{"R6", "VectorInTransaction", TestV152P26VectorInTransaction},
	} {
		t.Run(c.bypass+"/"+c.name, func(t *testing.T) {
			t.Setenv("P26_BYPASS", c.bypass)
			c.run(t)
		})
	}
}

func TestV152P26MemoryBypass(t *testing.T) {
	t.Setenv("P26_BYPASS", "ST")
	t.Run("FlippedBit", TestV152P26FlippedBit)
	t.Run("NumericListCache", TestV152P26NumericListCache)
}
