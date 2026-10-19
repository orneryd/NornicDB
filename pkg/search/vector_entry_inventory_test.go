package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorEntryInventoryUsesEscapedMain(t *testing.T) {
	for _, alias := range []string{nodeNamedVectorKey("other", "v"), nodePropertyVectorKey("other", "p"), nodeChunkVectorKey("other", 1)} {
		t.Run(alias, func(t *testing.T) {
			svc := NewServiceWithDimensions(newNamespacedEngine(t), vectorOwnerDims)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			mainKey := nodeMainVectorKey(alias)
			chunkKey := nodeChunkVectorKey(alias, 1)
			namedKey := nodeNamedVectorKey(alias, "v")
			propertyKey := nodePropertyVectorKey(alias, "p")
			svc.indexMu.Lock()
			svc.nodeChunkVectors[alias] = []string{mainKey, chunkKey}
			svc.nodeNamedVector[alias] = map[string]string{"v": namedKey}
			svc.nodePropVector[alias] = map[string]string{"p": propertyKey}
			var keys []string
			svc.nodeVectorIDsLocked(alias, func(key string) { keys = append(keys, key) })
			svc.indexMu.Unlock()
			require.ElementsMatch(t, []string{mainKey, chunkKey, namedKey, propertyKey}, keys, "main/chunk zero must appear once; another node's encoded entry must never enter this inventory")
			for _, key := range keys {
				owner, ok := vectorEntryOwner(key)
				require.True(t, ok)
				require.Equal(t, alias, owner, "every inventory key must decode to the original marker-prefixed node")
			}
		})
	}
}
