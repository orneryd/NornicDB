package storage

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONIngestionNeo4jImports(t *testing.T) {
	const nodes = `{"id":"a","labels":["Exact"],"properties":{"v":9007199254740993,"unsigned":18446744073709551615,"decimal":1.25,"nested":[-9007199254740993,2.0]}}`
	const edge = `{"id":"r","type":"EXACT","start":{"id":"a"},"end":{"id":"a"},"properties":{"v":-9007199254740993,"unsigned":18446744073709551615,"decimal":2.5}}`
	for _, lines := range []bool{false, true} {
		t.Run(map[bool]string{false: "combined", true: "json-lines"}[lines], func(t *testing.T) {
			base := newTestMemoryEngine(t)
			store := NewNamespacedEngine(base, "test")
			dir := t.TempDir()
			if lines {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(nodes+"\n"), 0600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "relationships.json"), []byte(edge+"\n"), 0600))
				require.NoError(t, LoadFromNeo4jJSON(store, dir))
			} else {
				path := filepath.Join(dir, "export.json")
				require.NoError(t, os.WriteFile(path, []byte(`{"nodes":[`+nodes+`],"relationships":[`+edge+`]}`), 0600))
				require.NoError(t, LoadFromNeo4jExport(store, path))
			}
			node, err := store.GetNode("a")
			require.NoError(t, err)
			require.Equal(t, int64(9007199254740993), node.Properties["v"])
			require.Equal(t, uint64(math.MaxUint64), node.Properties["unsigned"])
			require.Equal(t, 1.25, node.Properties["decimal"])
			require.Equal(t, []interface{}{int64(-9007199254740993), float64(2)}, node.Properties["nested"])
			rel, err := store.GetEdge("r")
			require.NoError(t, err)
			require.Equal(t, int64(-9007199254740993), rel.Properties["v"])
			require.Equal(t, uint64(math.MaxUint64), rel.Properties["unsigned"])
			require.Equal(t, 2.5, rel.Properties["decimal"])
		})
	}
}

func TestJSONIngestionNeo4jExportRoundTrip(t *testing.T) {
	base := newTestMemoryEngine(t)
	source := NewNamespacedEngine(base, "test")
	_, err := source.CreateNode(&Node{ID: "a", Labels: []string{"Exact"}, Properties: map[string]interface{}{"v": int64(9007199254740993), "unsigned": uint64(math.MaxUint64), "whole": float64(2), "nested": []interface{}{float64(4), int64(5)}}})
	require.NoError(t, err)
	require.NoError(t, source.CreateEdge(&Edge{ID: "r", StartNode: "a", EndNode: "a", Type: "EXACT", Properties: map[string]interface{}{"v": int64(-9007199254740993), "whole": float64(3)}}))
	for _, save := range []func(string) error{
		func(path string) error { return SaveToNeo4jExport(source, path) },
		func(path string) error { return GenericSaveToNeo4jExport(source, path) },
	} {
		path := filepath.Join(t.TempDir(), "export.json")
		require.NoError(t, save(path))
		other := newTestMemoryEngine(t)
		restored := NewNamespacedEngine(other, "test")
		require.NoError(t, LoadFromNeo4jExport(restored, path))
		node, err := restored.GetNode("a")
		require.NoError(t, err)
		require.Equal(t, int64(9007199254740993), node.Properties["v"])
		require.Equal(t, uint64(math.MaxUint64), node.Properties["unsigned"])
		require.Equal(t, float64(2), node.Properties["whole"])
		require.Equal(t, []interface{}{float64(4), int64(5)}, node.Properties["nested"])
		edge, err := restored.GetEdge("r")
		require.NoError(t, err)
		require.Equal(t, int64(-9007199254740993), edge.Properties["v"])
		require.Equal(t, float64(3), edge.Properties["whole"])
	}
}
