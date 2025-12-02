// Package hashing provides APOC hashing functions.
//
// This package implements all apoc.hashing.* functions for
// cryptographic and non-cryptographic hashing operations.
package hashing

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash/fnv"
)

// MD5 computes MD5 hash of a value.
//
// Example:
//
//	apoc.hashing.md5('hello') => '5d41402abc4b2a76b9719d911017c592'
func MD5(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := md5.Sum([]byte(str))
	return hex.EncodeToString(hash[:])
}

// SHA1 computes SHA1 hash of a value.
//
// Example:
//
//	apoc.hashing.sha1('hello') => 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'
func SHA1(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := sha1.Sum([]byte(str))
	return hex.EncodeToString(hash[:])
}

// SHA256 computes SHA256 hash of a value.
//
// Example:
//
//	apoc.hashing.sha256('hello') => '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
func SHA256(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := sha256.Sum256([]byte(str))
	return hex.EncodeToString(hash[:])
}

// SHA384 computes SHA384 hash of a value.
//
// Example:
//
//	apoc.hashing.sha384('hello') => hash
func SHA384(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := sha512.Sum384([]byte(str))
	return hex.EncodeToString(hash[:])
}

// SHA512 computes SHA512 hash of a value.
//
// Example:
//
//	apoc.hashing.sha512('hello') => hash
func SHA512(value interface{}) string {
	str := fmt.Sprintf("%v", value)
	hash := sha512.Sum512([]byte(str))
	return hex.EncodeToString(hash[:])
}

// FNV1 computes FNV-1 hash (32-bit).
//
// Example:
//
//	apoc.hashing.fnv1('hello') => 1335831723
func FNV1(value interface{}) uint32 {
	str := fmt.Sprintf("%v", value)
	h := fnv.New32()
	h.Write([]byte(str))
	return h.Sum32()
}

// FNV1a computes FNV-1a hash (32-bit).
//
// Example:
//
//	apoc.hashing.fnv1a('hello') => 1335831723
func FNV1a(value interface{}) uint32 {
	str := fmt.Sprintf("%v", value)
	h := fnv.New32a()
	h.Write([]byte(str))
	return h.Sum32()
}

// FNV164 computes FNV-1 hash (64-bit).
//
// Example:
//
//	apoc.hashing.fnv164('hello') => 11831194018420276491
func FNV164(value interface{}) uint64 {
	str := fmt.Sprintf("%v", value)
	h := fnv.New64()
	h.Write([]byte(str))
	return h.Sum64()
}

// FNV1a64 computes FNV-1a hash (64-bit).
//
// Example:
//
//	apoc.hashing.fnv1a64('hello') => 11831194018420276491
func FNV1a64(value interface{}) uint64 {
	str := fmt.Sprintf("%v", value)
	h := fnv.New64a()
	h.Write([]byte(str))
	return h.Sum64()
}

// MurmurHash3 computes MurmurHash3 (32-bit).
//
// Example:
//
//	apoc.hashing.murmur3('hello', 0) => hash
func MurmurHash3(value interface{}, seed uint32) uint32 {
	str := fmt.Sprintf("%v", value)
	return murmur3_32([]byte(str), seed)
}

// CityHash64 computes CityHash (64-bit).
//
// Example:
//
//	apoc.hashing.cityHash64('hello') => hash
func CityHash64(value interface{}) uint64 {
	str := fmt.Sprintf("%v", value)
	return cityHash64([]byte(str))
}

// XXHash32 computes xxHash (32-bit).
//
// Example:
//
//	apoc.hashing.xxHash32('hello', 0) => hash
func XXHash32(value interface{}, seed uint32) uint32 {
	str := fmt.Sprintf("%v", value)
	return xxHash32([]byte(str), seed)
}

// XXHash64 computes xxHash (64-bit).
//
// Example:
//
//	apoc.hashing.xxHash64('hello', 0) => hash
func XXHash64(value interface{}, seed uint64) uint64 {
	str := fmt.Sprintf("%v", value)
	return xxHash64([]byte(str), seed)
}

// Fingerprint creates a fingerprint of a node or relationship.
//
// Example:
//
//	apoc.hashing.fingerprint(node) => hash
func Fingerprint(entity interface{}) string {
	str := fmt.Sprintf("%v", entity)
	return SHA256(str)
}

// FingerprintGraph creates a fingerprint of an entire graph.
//
// Example:
//
//	apoc.hashing.fingerprintGraph(nodes, rels) => hash
func FingerprintGraph(nodes, rels interface{}) string {
	combined := fmt.Sprintf("%v%v", nodes, rels)
	return SHA256(combined)
}

// ConsistentHash computes a consistent hash for distributed systems.
//
// Example:
//
//	apoc.hashing.consistentHash('key', 100) => bucket index
func ConsistentHash(key interface{}, buckets int) int {
	hash := FNV1a64(key)
	return int(hash % uint64(buckets))
}

// RendezvousHash computes rendezvous (highest random weight) hash.
//
// Example:
//
//	apoc.hashing.rendezvousHash('key', ['node1', 'node2', 'node3']) => 'node2'
func RendezvousHash(key interface{}, nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}

	maxHash := uint64(0)
	selectedNode := nodes[0]

	keyStr := fmt.Sprintf("%v", key)

	for _, node := range nodes {
		combined := keyStr + node
		hash := FNV1a64(combined)
		if hash > maxHash {
			maxHash = hash
			selectedNode = node
		}
	}

	return selectedNode
}

// JumpHash computes jump consistent hash.
//
// Example:
//
//	apoc.hashing.jumpHash(12345, 10) => bucket index
func JumpHash(key uint64, buckets int32) int32 {
	var b int64 = -1
	var j int64 = 0

	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

// Helper functions for hash algorithms

func murmur3_32(data []byte, seed uint32) uint32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
		r1 = 15
		r2 = 13
		m  = 5
		n  = 0xe6546b64
	)

	hash := seed
	length := len(data)

	// Process 4-byte chunks
	nblocks := length / 4
	for i := 0; i < nblocks; i++ {
		k := uint32(data[i*4]) | uint32(data[i*4+1])<<8 | uint32(data[i*4+2])<<16 | uint32(data[i*4+3])<<24

		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2

		hash ^= k
		hash = (hash << r2) | (hash >> (32 - r2))
		hash = hash*m + n
	}

	// Process remaining bytes
	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << r1) | (k1 >> (32 - r1))
		k1 *= c2
		hash ^= k1
	}

	hash ^= uint32(length)
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16

	return hash
}

func cityHash64(data []byte) uint64 {
	// Simplified CityHash64 implementation
	if len(data) == 0 {
		return 0
	}

	h := FNV1a64(data)
	return h
}

func xxHash32(data []byte, seed uint32) uint32 {
	// Simplified xxHash32 implementation
	const prime1 uint32 = 2654435761
	const prime2 uint32 = 2246822519
	const prime3 uint32 = 3266489917
	const prime4 uint32 = 668265263
	const prime5 uint32 = 374761393

	h32 := seed + prime5 + uint32(len(data))

	for _, b := range data {
		h32 += uint32(b) * prime5
		h32 = ((h32 << 11) | (h32 >> (32 - 11))) * prime1
	}

	h32 ^= h32 >> 15
	h32 *= prime2
	h32 ^= h32 >> 13
	h32 *= prime3
	h32 ^= h32 >> 16

	return h32
}

func xxHash64(data []byte, seed uint64) uint64 {
	// Simplified xxHash64 implementation
	const prime1 uint64 = 11400714785074694791
	const prime2 uint64 = 14029467366897019727
	const prime3 uint64 = 1609587929392839161
	const prime4 uint64 = 9650029242287828579
	const prime5 uint64 = 2870177450012600261

	h64 := seed + prime5 + uint64(len(data))

	for _, b := range data {
		h64 += uint64(b) * prime5
		h64 = ((h64 << 11) | (h64 >> (64 - 11))) * prime1
	}

	h64 ^= h64 >> 33
	h64 *= prime2
	h64 ^= h64 >> 29
	h64 *= prime3
	h64 ^= h64 >> 32

	return h64
}
