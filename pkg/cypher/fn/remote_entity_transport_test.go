package fn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteHTTPGraphFunctionTransportControls(t *testing.T) {
	wire, err := os.ReadFile("../../storage/testdata/remote_entity_response.json")
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(wire)
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	columns, rows, err := remote.QueryCypher(context.Background(), "RETURN graph values", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"node", "relationship", "ordinary"}, columns)
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 3)
	node, relationship, ordinary := rows[0][0], rows[0][1], rows[0][2]
	for _, tt := range []struct {
		name  string
		value any
		want  any
	}{
		{"labels", node, []any{"Doc"}},
		{"elementid", node, "4:remote:node"},
		{"properties", node, map[string]any{"id": "local-node", "name": "Remote"}},
		{"keys", node, []any{"id", "name"}},
		{"type", relationship, "LINKS"},
		{"elementid", relationship, "5:remote:rel"},
		{"properties", relationship, map[string]any{"id": "local-rel", "weight": float64(2)}},
		{"keys", relationship, []any{"id", "weight"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, found, err := EvaluateFunction(tt.name, []string{"v"}, Context{Eval: func(string) (any, error) { return tt.value, nil }})
			require.True(t, found)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
	// Public JSON (also used by GraphQL/HTTP scalar output) retains the remote
	// element IDs and property nesting; metadata-shaped ordinary maps stay maps.
	raw, err := json.Marshal(node)
	require.NoError(t, err)
	assert.JSONEq(t, `{"elementId":"4:remote:node","labels":["Doc"],"properties":{"id":"local-node","name":"Remote"}}`, string(raw))
	_, ok := ordinary.(map[string]any)
	require.True(t, ok)
	raw, err = json.Marshal(ordinary)
	require.NoError(t, err)
	assert.JSONEq(t, `{"elementId":"forged","labels":["Fake"],"properties":{"name":"ordinary"}}`, string(raw))
}
