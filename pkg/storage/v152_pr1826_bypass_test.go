package storage

import "testing"

// Diagnostic branch only: each reproduction with the named step bypassed.
func TestV152P26Bypass(t *testing.T) {
	t.Setenv("P26_BYPASS", "ST")
	t.Run("F5/StreamSharesImage", TestV152P26StreamSharesImage)
	t.Run("F6/OneByteAllocation", TestV152P26OneByteAllocation)
	t.Run("R7/NumericListEstimate", TestV152P26NumericListEstimate)
	t.Run("R9/ProjectedHasEmbedding", TestV152P26ProjectedHasEmbedding)
}
