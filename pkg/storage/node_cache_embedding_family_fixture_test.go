package storage

import "encoding/binary"

func nodeCacheEmbeddingKeyFixtures(id NodeID) []nodeCacheEmbeddingKeyFixture {
	var fixtures []nodeCacheEmbeddingKeyFixture
	for _, family := range []struct {
		name   string
		prefix byte
		key    func(NodeID, int) []byte
	}{
		{"legacy", prefixEmbedding, legacyEmbeddingKey},
		{"compatibility", prefixEmbeddingBinary, func(id NodeID, index int) []byte {
			return binary.BigEndian.AppendUint32(binaryEmbeddingPrefix(id), uint32(index))
		}},
		{"current", prefixEmbeddingCurrent, embeddingKey},
	} {
		fixtures = append(fixtures,
			nodeCacheEmbeddingKeyFixture{name: family.name + "-base", key: family.key(id, 0)},
			nodeCacheEmbeddingKeyFixture{name: family.name + "-meta", key: family.key(id, reservedEmbeddingMetaChunkIndex)},
			nodeCacheEmbeddingKeyFixture{name: family.name + "-part", key: embeddingPartKey(family.key(id, 0), 1), ambiguous: family.prefix == prefixEmbedding},
			nodeCacheEmbeddingKeyFixture{name: family.name + "-malformed", key: []byte{family.prefix, 0xff}, unknown: true},
		)
	}
	return fixtures
}
