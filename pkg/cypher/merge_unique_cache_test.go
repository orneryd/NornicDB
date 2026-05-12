package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestMergeUniqueConstraintNonAuthoritativeCacheFallsBackToScan verifies that
// MERGE does not trust a missing UNIQUE-cache entry unless that cache has been
// rebuilt or maintained authoritatively. Commit-time validation already falls
// back to a label scan in this state; MERGE planning must do the same so it can
// MATCH the committed node instead of choosing CREATE and failing at commit.
func TestMergeUniqueConstraintNonAuthoritativeCacheFallsBackToScan(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	t.Cleanup(func() {
		_ = baseStore.Close()
	})
	store := storage.NewNamespacedEngine(baseStore, "nornic")
	_, err := store.CreateNode(&storage.Node{
		ID:     "existing",
		Labels: []string{"TerraformResource"},
		Properties: map[string]interface{}{
			"uid":  "cache-miss",
			"name": "before",
		},
	})
	require.NoError(t, err)
	require.NoError(t, store.GetSchema().AddUniqueConstraint("tr_uid", "TerraformResource", "uid"))

	exec := NewStorageExecutor(store)
	_, err = exec.Execute(context.Background(),
		"MERGE (r:TerraformResource {uid: $uid}) SET r.name = $name",
		map[string]interface{}{
			"uid":  "cache-miss",
			"name": "after",
		},
	)
	require.NoError(t, err)

	result, err := exec.Execute(context.Background(),
		"MATCH (r:TerraformResource {uid: $uid}) RETURN r.name, count(r)",
		map[string]interface{}{"uid": "cache-miss"},
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "after", result.Rows[0][0])
	require.EqualValues(t, 1, result.Rows[0][1])
}
