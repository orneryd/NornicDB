// Package util provides shared utility functions used across NornicDB packages.
package util

// HashStringToInt64 converts a string ID to an int64 for Neo4j Bolt protocol compatibility.
// NornicDB uses string IDs (UUIDs or custom strings), but Bolt protocol expects int64 IDs.
// This function uses FNV-1a hash algorithm for deterministic conversion with good distribution.
//
// FNV-1a (Fowler-Noll-Vo) is a fast, non-cryptographic hash function that:
//   - Produces deterministic results (same input = same output)
//   - Has good distribution (fewer collisions than simple multiplicative hashing)
//   - Is fast and suitable for high-frequency operations
//
// The hash uses FNV-1a 64-bit variant with offset basis 14695981039346656037.
//
// Example:
//
//	HashStringToInt64("user-123") => 1234567890123456789
//	HashStringToInt64("user-123") => 1234567890123456789 (same result)
func HashStringToInt64(s string) int64 {
	const offsetBasis uint64 = 14695981039346656037
	const prime uint64 = 1099511628211

	hash := offsetBasis
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime
	}

	result := int64(hash)
	if result < 0 {
		result = result & 0x7FFFFFFFFFFFFFFF
	}

	return result
}
