package storage

func nodeCacheEmbeddingKeyFixtures(id NodeID) []nodeCacheEmbeddingKeyFixture {
	return []nodeCacheEmbeddingKeyFixture{
		{name: "legacy-base", key: embeddingKey(id, 0)},
		{name: "legacy-meta", key: embeddingMetaKey(id)},
		// A legacy part can also be a base for a binary ID. Do not guess ownership.
		{name: "legacy-part-ambiguous", key: embeddingChunkPartKey(id, 0, 1), ambiguous: true},
		{name: "malformed-legacy", key: []byte{prefixEmbedding, 0xff}, unknown: true},
	}
}
