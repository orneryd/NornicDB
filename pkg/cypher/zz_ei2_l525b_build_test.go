package cypher

// Final-build adapters for zz_ei2_l525b_test.go (research only, untracked).

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzei2L525Fixture(t *testing.T) (*storage.BadgerEngine, storage.Engine, storage.Engine, uint64) {
	engine := newTestMemoryEngine(t).BadgerEngine
	home := storage.NewNamespacedEngine(engine, "nornic")
	other := storage.NewNamespacedEngine(engine, "other")
	var vb uint64
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("src-%d", i)
		_, err := other.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source"}, Properties: map[string]any{"id": id, "title": "source " + id}})
		require.NoError(t, err)
		chunks := make([][]float32, 64)
		for c := range chunks {
			v := make([]float32, nor413Dims)
			v[c%nor413Dims] = 1
			chunks[c] = v
		}
		writeManagedTestChunks(t, engine, storage.NodeID("other:"+id), chunks)
		vb += uint64(64 * nor413Dims * 4)
	}
	return engine, home, other, vb
}

func zzei2L525Exec(store storage.Engine) *StorageExecutor {
	return NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

func zzei2L525PropertyView(exec *StorageExecutor, q string) bool {
	return exec.withStatementPropertyViews(context.Background(), q) != nil
}
