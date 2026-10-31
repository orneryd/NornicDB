// Package remotegraph provides an authenticated HTTP peer backed by real Cypher
// and native graph storage for coordinator protocol regression tests.
package remotegraph

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	CatalogName = "protocol_descriptor"
	Query       = "USE protocol_descriptor.r MATCH (n:DescriptorPeer) RETURN n.marker AS marker"
	Show        = "SHOW CONSTITUENTS FOR COMPOSITE DATABASE protocol_descriptor"
	Password    = " synthetic-adapter-password "
)

// Peer executes received statements against native stored data. Its handler
// checks exact Basic credentials before dispatch; it never echoes a fixture row.
type Peer struct {
	URL   string
	store storage.Engine
	calls atomic.Int64
}

// New constructs a keyed coordinator catalog and an authenticated remote graph.
// Both lifetimes belong to t; callers select their actual coordinator protocol.
//
//	manager, peer := remotegraph.New(t)
//	peer.SetMarker(t, "updated")
func New(t *testing.T) (*multidb.DatabaseManager, *Peer) {
	t.Helper()
	remote := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	peer := &Peer{store: storage.NewNamespacedEngine(remote, "peer")}
	_, err := peer.store.CreateNode(&storage.Node{ID: "marker", Labels: []string{"DescriptorPeer"}, Properties: map[string]interface{}{"marker": "first-remote-row"}})
	require.NoError(t, err)
	executor := cypher.NewStorageExecutor(peer.store)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "adapter-user" || password != Password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// The coordinator owns a read-only remote transaction for each statement.
		// Its open/close endpoints carry no graph mutation; statement execution
		// below still reads the real peer store through Cypher.
		if r.Method == http.MethodDelete && r.URL.Path == "/db/peer/tx/1" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}})
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/db/peer/tx" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"commit": peer.URL + "/db/peer/tx/1/commit", "results": []interface{}{}, "errors": []interface{}{}})
			return
		}
		if r.Method != http.MethodPost || (r.URL.Path != "/db/peer/tx/commit" && r.URL.Path != "/db/peer/tx/1" && r.URL.Path != "/db/peer/tx/1/commit") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request struct {
			Statements []struct {
				Statement  string                 `json:"statement"`
				Parameters map[string]interface{} `json:"parameters"`
			} `json:"statements"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		results := make([]map[string]interface{}, 0, len(request.Statements))
		for _, statement := range request.Statements {
			result, err := executor.Execute(cypher.WithClientStatement(r.Context()), statement.Statement, statement.Parameters)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			rows := make([]map[string]interface{}, len(result.Rows))
			for i, row := range result.Rows {
				rows[i] = map[string]interface{}{"row": row}
			}
			results = append(results, map[string]interface{}{"columns": result.Columns, "data": rows})
		}
		peer.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results, "errors": []interface{}{}})
	}))
	t.Cleanup(endpoint.Close)
	peer.URL = endpoint.URL
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	cfg := multidb.DefaultConfig()
	cfg.RemoteCredentialEncryptionKey = "synthetic-protocol-catalog-key"
	cfg.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
		engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, User: ref.User, Password: ref.Password, AuthToken: token})
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		return engine, err
	}
	manager, err := multidb.NewDatabaseManager(t.Context(), base, cfg)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), CatalogName, []multidb.ConstituentRef{{Alias: "r", DatabaseName: "peer", Type: "remote", AccessMode: "read", URI: peer.URL, AuthMode: "user_password", User: "adapter-user", Password: Password}}))
	refs, err := manager.GetCompositeConstituents(CatalogName)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.NotEmpty(t, refs[0].ConnectionV1)
	require.Empty(t, refs[0].URI)
	require.Empty(t, refs[0].Password)
	return manager, peer
}

// SetMarker changes the real remote body, so repeated coordinator reads prove
// the peer's execution and storage path rather than a cached constant response.
func (p *Peer) SetMarker(t *testing.T, marker string) {
	t.Helper()
	node, err := p.store.GetNode("marker")
	require.NoError(t, err)
	before := *node
	before.Properties = maps.Clone(node.Properties)
	node.Properties["marker"] = marker
	require.NoError(t, p.store.UpdateNode(storage.NodeUpdateRequest{Before: &before, After: node}))
}

// Calls counts authenticated statement executions against the remote graph.
func (p *Peer) Calls() int64 { return p.calls.Load() }

// CheckPublicRead verifies display redaction and two authenticated Fabric reads
// through a caller's actual protocol. The second read observes changed storage.
func CheckPublicRead(t *testing.T, peer *Peer, run func(string) [][]interface{}) {
	t.Helper()
	rows := run(Show)
	require.Len(t, rows, 1)
	require.Contains(t, fmt.Sprint(rows), peer.URL)
	require.NotContains(t, fmt.Sprint(rows), Password)
	for _, marker := range []string{"first-remote-row", "updated-remote-row"} {
		peer.SetMarker(t, marker)
		require.Equal(t, [][]interface{}{{marker}}, run(Query))
	}
	require.GreaterOrEqual(t, peer.Calls(), int64(2))
}
