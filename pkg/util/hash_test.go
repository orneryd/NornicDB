package util

import "testing"

func TestHashStringToInt64_DeterministicAndNonNegative(t *testing.T) {
	input := "user-123"
	first := HashStringToInt64(input)
	second := HashStringToInt64(input)

	if first != second {
		t.Fatalf("expected deterministic hash, got %d and %d", first, second)
	}
	if first < 0 {
		t.Fatalf("expected non-negative hash, got %d", first)
	}
}

func TestHashStringToInt64_DifferentInputs(t *testing.T) {
	a := HashStringToInt64("alpha")
	b := HashStringToInt64("beta")
	if a == b {
		t.Fatalf("expected different hashes for different inputs, both were %d", a)
	}
}
