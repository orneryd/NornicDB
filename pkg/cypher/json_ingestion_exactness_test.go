// Author: L6 / #1665 lane (Codex gpt-6.1-sol).
package cypher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestJSONIngestionLoadExactNumbers(t *testing.T) {
	const document = `{"v":9007199254740993,"decimal":1.25,"whole":2.0,"exponent":2e0,"nested":[{"v":-9007199254740993},true,null,"9007199254740993"]}`
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(document)) }))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "numbers.json")
	require.NoError(t, os.WriteFile(path, []byte(document), 0600))
	for _, source := range []string{path, "file://" + path, "https://example.com/numbers.json"} {
		t.Run(source, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			exec.SetAllowLocalAPOCFileAccess(true)
			exec.SetAllowRemoteAPOCURLAccess(true)
			exec.SetAPOCRemoteURLAllowlist([]string{"example.com"})
			allowPublicAPOCTestHost(exec)
			exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				request = request.Clone(request.Context())
				request.URL.Host = server.Listener.Addr().String()
				return server.Client().Transport.RoundTrip(request)
			})})
			result, err := exec.Execute(context.Background(), "CALL apoc.load.json($url) YIELD value RETURN value.v, value", map[string]interface{}{"url": source})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, int64(9007199254740993), result.Rows[0][0])
			value := result.Rows[0][1].(map[string]interface{})
			require.Equal(t, 1.25, value["decimal"])
			require.Equal(t, float64(2), value["whole"])
			require.Equal(t, float64(2), value["exponent"])
			require.Equal(t, []interface{}{map[string]interface{}{"v": int64(-9007199254740993)}, true, nil, "9007199254740993"}, value["nested"])
		})
	}
}
