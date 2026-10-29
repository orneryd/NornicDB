package multidb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestR73ReviewHTTPQueryCredentials(t *testing.T) {
	const rawQuery = "api_key=first%2Bvalue&region=eu&api_key=second%2Bvalue"
	var mu sync.Mutex
	seen := map[string]int{}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != rawQuery {
			t.Error("remote request did not preserve the ordered query")
		}
		mu.Lock()
		seen[r.URL.EscapedPath()]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.EscapedPath() == "/proxy/db/remote/tx" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"commit":  server.URL + "/db/remote/tx/7/commit",
				"results": []any{},
				"errors":  []any{},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	if err != nil {
		t.Fatal("storage setup failed")
	}
	config := DefaultConfig()
	config.RemoteCredentialEncryptionKey = "route-test-encryption-key"
	// The manager's Close releases only the inner store through
	// NamespacedEngine.Close, so the factory return's lifetime belongs to
	// this test. A remote engine has no closed-state probe; it can only close
	// an already-completed transport, so the test registers and closes that
	// exact return explicitly when it stops naming the engine.
	var factoryEngines []*storage.RemoteEngine
	config.RemoteEngineFactory = func(ref ConstituentRef, _ string) (storage.Engine, error) {
		remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName})
		if err != nil {
			return nil, err
		}
		factoryEngines = append(factoryEngines, remote)
		return remote, nil
	}
	manager, err := NewDatabaseManager(t.Context(), inner, config)
	if err != nil {
		t.Fatal("manager setup failed")
	}
	ref := ConstituentRef{
		Alias: "remote", DatabaseName: "remote", Type: "remote", AccessMode: "read_write",
		URI: server.URL + "/proxy?" + rawQuery,
	}
	if err := manager.CreateCompositeDatabase(t.Context(), "route_lifecycle", []ConstituentRef{ref}); err != nil {
		t.Fatal("remote metadata setup failed")
	}
	node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
	if err != nil {
		t.Fatal("metadata read failed")
	}
	persisted := node.Properties["data"].(string)
	if strings.Contains(persisted, "first+value") || strings.Contains(persisted, "first%2Bvalue") {
		t.Fatal("configured query credential remained in plaintext metadata")
	}
	if err := manager.Close(); err != nil {
		t.Fatal("manager close failed")
	}

	inner, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	if err != nil {
		t.Fatal("storage reopen failed")
	}
	manager, err = NewDatabaseManager(t.Context(), inner, config)
	if err != nil {
		t.Fatal("manager reopen failed")
	}
	t.Cleanup(func() {
		_ = manager.Close()
		for _, remote := range factoryEngines {
			_ = remote.Close()
		}
	})
	engine, err := manager.GetStorage("route_lifecycle.remote")
	if err != nil {
		t.Fatal("restored remote engine lookup failed")
	}
	remote, ok := engine.(*storage.RemoteEngine)
	if !ok {
		t.Fatal("restored engine was not remote")
	}
	if _, _, err := remote.QueryCypher(context.Background(), "RETURN 1", nil, storage.RemoteAccessRead); err != nil {
		t.Fatal("implicit remote request failed")
	}
	tx, err := remote.BeginCypherTx(context.Background(), storage.RemoteAccessWrite)
	if err != nil {
		t.Fatal("explicit remote transaction open failed")
	}
	if _, _, err := tx.QueryCypher(context.Background(), "RETURN 1", nil); err != nil {
		t.Fatal("explicit remote transaction request failed")
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal("explicit remote transaction commit failed")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, path := range []string{
		"/proxy/db/remote/tx/commit",
		"/proxy/db/remote/tx",
		"/db/remote/tx/7",
		"/db/remote/tx/7/commit",
	} {
		if seen[path] == 0 {
			t.Error("remote lifecycle request path was not reached")
		}
	}
}
