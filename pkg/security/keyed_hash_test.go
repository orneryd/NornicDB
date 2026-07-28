package security

import "testing"

func TestKeyedDigest_DeterministicWithinNamespace(t *testing.T) {
	left := KeyedDigest("auth.basic.header", "Basic YWxpY2U6cGFzcw==")
	right := KeyedDigest("auth.basic.header", "Basic YWxpY2U6cGFzcw==")

	if left != right {
		t.Fatal("expected identical digests for identical namespace and input")
	}
}

func TestKeyedDigest_DomainSeparated(t *testing.T) {
	authDigest := KeyedDigest("auth.basic.credentials", "alice", "password123")
	schemaDigest := KeyedDigest("storage.composite_key", "alice", "password123")

	if authDigest == schemaDigest {
		t.Fatal("expected different digests across namespaces")
	}
}

func TestKeyedDigest_DistinguishesPartBoundaries(t *testing.T) {
	joined := KeyedDigest("boundary", "ab", "c")
	split := KeyedDigest("boundary", "a", "bc")

	if joined == split {
		t.Fatal("expected length-prefixed encoding to distinguish different part boundaries")
	}
}
