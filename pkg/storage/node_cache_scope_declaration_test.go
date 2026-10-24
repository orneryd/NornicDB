package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCacheScopeRequiresDeclaration(t *testing.T) {
	require.ErrorIs(t, admitDurableWrite(nodeCacheWriteScope{}), ErrInvalidData)
	require.Panics(t, func() { unknownNodeCacheWrites("unlisted writer") })
	for _, entrance := range []nodeCacheUnknownEntrance{nodeCacheUnknownManaged, nodeCacheUnknownMaintenance, nodeCacheUnknownRollback, nodeCacheUnknownSerializer} {
		require.NoError(t, admitDurableWrite(unknownNodeCacheWrites(entrance)))
	}
}

func TestNodeCacheEmbeddingScopeOwnerDecodingAllocatesNothing(t *testing.T) {
	for _, fixture := range nodeCacheEmbeddingKeyFixtures("test:scope-codec") {
		if fixture.unknown {
			continue
		}
		t.Run(fixture.name, func(t *testing.T) {
			key := string(fixture.key)
			expected := embeddingKeyOwners(fixture.key)
			var candidates [2]embeddingKeyOwner
			n := embeddingKeyOwnersInto([]byte(key), key, &candidates)
			require.Equal(t, expected, candidates[:n])
			allocations := testing.AllocsPerRun(100, func() {
				var scratch [2]embeddingKeyOwner
				count := embeddingKeyOwnersInto([]byte(key), key, &scratch)
				if count != n {
					panic("owner count changed")
				}
			})
			require.Zero(t, allocations, "durable raw-key classification must borrow its existing key")
		})
	}
}
