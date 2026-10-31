package replication_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1665 implementation lane (Codex). A remote receiver owns its schema
// clock. Local admission transport must preserve the existing remote write API.
func TestStorageAdapterRemoteAdmissionBoundary(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[{"columns":["n"],"data":[{"row":[{"id":"remote:selected"}]}]}],"errors":[]}`)
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	adapter, err := replication.NewStorageAdapterWithWAL(remote, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })
	data, err := storage.EncodeNodePayload(&storage.Node{ID: "remote:selected", Labels: []string{"Doc"}, Properties: map[string]any{"embedding": []float64{1, 0}}})
	require.NoError(t, err)
	require.NoError(t, adapter.ApplyCommand(&replication.Command{Type: replication.CmdCreateNode, Data: data}))
	require.EqualValues(t, 1, calls.Load(), "one legacy write reaches the remote admission owner")
}
