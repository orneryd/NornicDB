package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericWireImportMetadataCompatibility(t *testing.T) {
	for _, lines := range []bool{false, true} {
		t.Run(map[bool]string{false: "combined", true: "json-lines"}[lines], func(t *testing.T) {
			store := NewNamespacedEngine(newTestMemoryEngine(t), "test")
			t.Cleanup(func() { _ = store.Close() })
			const node = `{"id":"a","labels":["OldExport"],"properties":{"v":7,"_createdAt":1600000000,"_updatedAt":1600000100}}`
			const edge = `{"id":"r","type":"OLD","start":{"id":"a"},"end":{"id":"a"},"properties":{"_createdAt":1600000200,"_confidence":1}}`
			dir := t.TempDir()
			if lines {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(node+"\n"), 0600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "relationships.json"), []byte(edge+"\n"), 0600))
				require.NoError(t, LoadFromNeo4jJSON(store, dir))
			} else {
				path := filepath.Join(dir, "old-export.json")
				require.NoError(t, os.WriteFile(path, []byte(`{"nodes":[`+node+`],"relationships":[`+edge+`]}`), 0600))
				require.NoError(t, LoadFromNeo4jExport(store, path))
			}
			n, err := store.GetNode("a")
			require.NoError(t, err)
			r, err := store.GetEdge("r")
			require.NoError(t, err)
			t.Logf("node created=%v updated=%v props=%#v; edge confidence=%v created=%v props=%#v", n.CreatedAt, n.UpdatedAt, n.Properties, r.Confidence, r.CreatedAt, r.Properties)
			require.Equal(t, int64(1600000000), n.CreatedAt.Unix())
			require.Equal(t, int64(1600000100), n.UpdatedAt.Unix())
			require.NotContains(t, n.Properties, "_createdAt")
			require.Equal(t, float64(1), r.Confidence)
			require.Equal(t, int64(1600000200), r.CreatedAt.Unix())
		})
	}
}
