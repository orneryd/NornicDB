package storage

import "encoding/binary"

// Legacy vectors and metadata use a base suffix; shards add a part suffix.
// Decode from those fixed widths so binary IDs cannot be truncated at a NUL.
// A physical key may have two candidates; callers must retain that ambiguity.
type embeddingKeyOwner struct {
	id    NodeID
	index uint32
	part  bool
}

func embeddingKeyOwners(key []byte) []embeddingKeyOwner {
	var candidates [2]embeddingKeyOwner
	n := embeddingKeyOwnersInto(key, "", &candidates)
	if n == 0 {
		return nil
	}
	return append([]embeddingKeyOwner(nil), candidates[:n]...)
}

// The durable scope borrows immutable raw-map keys. Other codec callers keep
// independent owner strings, as before; both routes share this decoder.
func embeddingOwnerID(key []byte, source string, start, end int) NodeID {
	if source != "" {
		return NodeID(source[start:end])
	}
	return NodeID(key[start:end])
}
func embeddingKeyOwnersInto(key []byte, source string, owners *[2]embeddingKeyOwner) int {
	if len(key) == 0 || key[0] != prefixEmbedding {
		return 0
	}
	n := 0
	if len(key) >= 6 && key[len(key)-5] == 0 {
		owners[n] = embeddingKeyOwner{embeddingOwnerID(key, source, 1, len(key)-5), binary.BigEndian.Uint32(key[len(key)-4:]), false}
		n++
	}
	if len(key) >= 9 && key[len(key)-8] == 0 && key[len(key)-3] == 0 {
		owners[n] = embeddingKeyOwner{embeddingOwnerID(key, source, 1, len(key)-8), binary.BigEndian.Uint32(key[len(key)-7:]), true}
		n++
	}
	return n
}

func nodeCacheEmbeddingPrefix(prefix byte) bool { return prefix == prefixEmbedding }
