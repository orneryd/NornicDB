package bolt

import (
	"hash/fnv"
	"testing"
)

// TestHashStringToInt64_FNV1aCorrectness verifies that hashStringToInt64
// correctly implements FNV-1a hash algorithm with forward byte iteration.
//
// This test ensures:
// 1. The hash matches Go's standard library FNV-1a implementation (masked to positive)
// 2. Byte order matters (forward iteration, not reverse)
// 3. Same input produces same output (deterministic)
func TestHashStringToInt64_FNV1aCorrectness(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "single character",
			input: "a",
		},
		{
			name:  "hello",
			input: "hello",
		},
		{
			name:  "user-123",
			input: "user-123",
		},
		{
			name:  "uuid",
			input: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:  "multi-byte utf8",
			input: "café",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate expected using Go's standard library FNV-1a
			h := fnv.New64a()
			h.Write([]byte(tt.input))
			expectedUint64 := h.Sum64()

			// Calculate using our implementation
			got := hashStringToInt64(tt.input)

			// Our implementation masks the high bit to ensure positive int64
			// So we compare the lower 63 bits (the actual hash computation)
			expectedMasked := expectedUint64 & 0x7FFFFFFFFFFFFFFF
			gotUint64 := uint64(got)
			if got < 0 {
				gotUint64 = uint64(got) & 0x7FFFFFFFFFFFFFFF
			}

			if gotUint64 != expectedMasked {
				t.Errorf("hashStringToInt64(%q) = %d (masked: %d), expected masked %d (original: %d)",
					tt.input, got, gotUint64, expectedMasked, expectedUint64)
			}

			// Verify result is always positive (Bolt protocol requirement)
			if got < 0 {
				t.Errorf("hashStringToInt64(%q) returned negative value: %d", tt.input, got)
			}
		})
	}
}

// TestHashStringToInt64_ByteOrderMatters verifies that byte order is significant.
// This test demonstrates that reversing the iteration would produce different hash values.
func TestHashStringToInt64_ByteOrderMatters(t *testing.T) {
	// These strings have different byte orders
	testCases := []struct {
		name  string
		input string
	}{
		{name: "forward", input: "abc"},
		{name: "different", input: "cba"},
	}

	hashes := make(map[string]int64)
	for _, tc := range testCases {
		hashes[tc.name] = hashStringToInt64(tc.input)
	}

	// Different strings should produce different hashes
	if hashes["forward"] == hashes["different"] {
		t.Errorf("Different inputs produced same hash: %d", hashes["forward"])
	}

	// Same input should produce same hash (deterministic)
	hash1 := hashStringToInt64("abc")
	hash2 := hashStringToInt64("abc")
	if hash1 != hash2 {
		t.Errorf("Same input produced different hashes: %d != %d", hash1, hash2)
	}
}

// TestHashStringToInt64_Deterministic verifies that the hash is deterministic.
func TestHashStringToInt64_Deterministic(t *testing.T) {
	inputs := []string{
		"",
		"a",
		"hello",
		"user-123",
		"550e8400-e29b-41d4-a716-446655440000",
		"café",
	}

	for _, input := range inputs {
		hash1 := hashStringToInt64(input)
		hash2 := hashStringToInt64(input)
		hash3 := hashStringToInt64(input)

		if hash1 != hash2 || hash2 != hash3 {
			t.Errorf("hashStringToInt64(%q) is not deterministic: %d, %d, %d", input, hash1, hash2, hash3)
		}
	}
}

