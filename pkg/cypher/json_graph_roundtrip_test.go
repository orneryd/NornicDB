package cypher

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestJSONIngestionAPOCGraphRoundTrip(t *testing.T) {
	source := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := source.CreateNode(&storage.Node{ID: "a", Labels: []string{"ExactGraph"}, Properties: map[string]interface{}{"v": int64(9007199254740993), "unsigned": uint64(math.MaxUint64), "whole": float64(2)}})
	require.NoError(t, err)
	require.NoError(t, source.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "a", Type: "EXACT", Properties: map[string]interface{}{"v": int64(-9007199254740993), "unsigned": uint64(math.MaxUint64), "whole": float64(3)}}))
	exporter := NewStorageExecutor(source)
	exporter.SetAllowLocalAPOCFileAccess(true)
	path := filepath.Join(t.TempDir(), "graph.json")
	_, err = exporter.Execute(context.Background(), "CALL apoc.export.json.all($file) YIELD file", map[string]interface{}{"file": path})
	require.NoError(t, err)
	document, err := os.ReadFile(path)
	require.NoError(t, err)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(document) }))
	defer server.Close()
	for _, location := range []string{path, "https://example.com/graph.json"} {
		target := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		importer := NewStorageExecutor(target)
		importer.SetAllowLocalAPOCFileAccess(true)
		importer.allowRemoteAPOCURLAccess = true
		importer.apocRemoteURLAllowlist = []string{"example.com"}
		allowPublicAPOCTestHost(importer)
		importer.apocRemoteHTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			request = request.Clone(request.Context())
			request.URL.Host = server.Listener.Addr().String()
			return server.Client().Transport.RoundTrip(request)
		})}
		_, err := importer.Execute(context.Background(), "CALL apoc.import.json($file) YIELD nodes, relationships", map[string]interface{}{"file": location})
		require.NoError(t, err)
		node, err := target.GetNode("a")
		require.NoError(t, err)
		require.Equal(t, int64(9007199254740993), node.Properties["v"])
		require.Equal(t, uint64(math.MaxUint64), node.Properties["unsigned"])
		require.Equal(t, float64(2), node.Properties["whole"])
		edge, err := target.GetEdge("r")
		require.NoError(t, err)
		require.Equal(t, int64(-9007199254740993), edge.Properties["v"])
		require.Equal(t, uint64(math.MaxUint64), edge.Properties["unsigned"])
		require.Equal(t, float64(3), edge.Properties["whole"])
	}
}
