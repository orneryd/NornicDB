package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A Fabric path has a Go slice carrier but remains a graph scalar. Broad list
// conversion must preserve it when flattening a list of paths.
func TestListInputPreservesRemotePath(t *testing.T) {
	path := storage.RemotePathValue{{"elementId": "remote:n", "labels": []string{"N"}}}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "path_lists"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	check := func(t *testing.T) {
		t.Helper()
		result, err := exec.Execute(context.Background(), "RETURN apoc.coll.flatten([$p],true) AS v", map[string]interface{}{"p": path})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{path}}}, result.Rows)
	}
	t.Run("builtin", check)
	t.Run("plugin", func(t *testing.T) {
		withPluginFunctions(t, map[string]interface{}{"apoc.coll.flatten": coll.Flatten})
		check(t)
	})
}
