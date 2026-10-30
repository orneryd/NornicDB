package hashing

import "testing"

func TestCryptoAndFNVHashesDeterministic(t *testing.T) {
	if MD5("hello") != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatalf("md5 mismatch")
	}
	if SHA1("hello") != "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d" {
		t.Fatalf("sha1 mismatch")
	}
	if len(SHA256("hello")) != 64 || len(SHA384("hello")) != 96 || len(SHA512("hello")) != 128 {
		t.Fatalf("sha length mismatch")
	}
	if FNV1a("hello") == 0 || FNV164("hello") == 0 || FNV1a64("hello") == 0 {
		t.Fatalf("fnv hash should be non-zero")
	}
}

func TestNonCryptoHashesAndFingerprints(t *testing.T) {
	if MurmurHash3("hello", 0) == MurmurHash3("hello", 1) {
		t.Fatalf("seed should affect murmur")
	}
	if CityHash64("hello") == 0 {
		t.Fatalf("city hash should be non-zero")
	}
	if XXHash32("hello", 0) == XXHash32("hello", 1) {
		t.Fatalf("seed should affect xxhash32")
	}
	if XXHash64("hello", 0) == XXHash64("hello", 1) {
		t.Fatalf("seed should affect xxhash64")
	}

	if len(Fingerprint(map[string]interface{}{"a": 1})) != 64 {
		t.Fatalf("fingerprint should be sha256 hex")
	}
	if len(FingerprintGraph([]string{"n1"}, []string{"r1"})) != 64 {
		t.Fatalf("graph fingerprint should be sha256 hex")
	}
}

func TestConsistentRendezvousAndJumpHash(t *testing.T) {
	bucket := ConsistentHash("key", 10)
	if bucket < 0 || bucket >= 10 {
		t.Fatalf("consistent hash out of range: %d", bucket)
	}

	node := RendezvousHash("key", []string{"node1", "node2", "node3"})
	if node == "" {
		t.Fatalf("rendezvous should choose a node")
	}
	if got := RendezvousHash("key", nil); got != "" {
		t.Fatalf("empty node list should return empty")
	}

	j := JumpHash(12345, 10)
	if j < 0 || j >= 10 {
		t.Fatalf("jump hash out of range: %d", j)
	}
}
