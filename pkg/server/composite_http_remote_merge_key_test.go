package server

import (
	"context"
	"encoding/json"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A composite explicit transaction that fails past its deadline, while its
// HTTP remote participant holds a MERGE key, still rolls that participant
// back: an HTTP transaction outlives its connection, so its rollback runs
// detached from the expired statement, and the remote releases the key when
// it answers. The failed statement returns at once with its own error, and
// the later ROLLBACK waits for that rollback without sending another.
func TestCompositeExplicitTx_ExpiredFailureRollsBackHTTPParticipant(t *testing.T) {
	remote, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	var stall atomic.Bool
	var deletes atomic.Int32
	router := remote.buildRouter()
	httpRemote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			if stall.Load() {
				time.Sleep(2 * time.Second) // it answers, after the statement's deadline
			}
		}
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(httpRemote.Close)
	autocommit := func(statement string) TransactionResponse {
		rec := makeRequest(t, remote, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": statement}}}, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return resp
	}
	require.Empty(t, autocommit("CREATE CONSTRAINT hk_u FOR (u:U) REQUIRE u.k IS UNIQUE").Errors)

	mgr, err := multidb.NewDatabaseManager(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: authToken})
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateCompositeDatabase("cmp_h", []multidb.ConstituentRef{
		{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: httpRemote.URL, AuthMode: "oidc_forwarding"},
	}))
	cmpStore, err := mgr.GetStorageWithAuth("cmp_h", token)
	require.NoError(t, err)
	exec := cypher.NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&databaseManagerAdapter{manager: mgr})
	ctx := cypher.WithAuthToken(context.Background(), token)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CALL { USE cmp_h.r MERGE (u:U {k: 1}) RETURN u.k AS k } RETURN k", nil)
	require.NoError(t, err)

	stall.Store(true)
	merged := make(chan TransactionResponse, 1)
	go func() { merged <- autocommit("MERGE (u:U {k: 1}) RETURN u.k AS k") }()
	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	start := time.Now()
	_, err = exec.Execute(expired, "RETURN 1", nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "sub-transaction", "the statement reports its own failure")
	require.Less(t, time.Since(start), time.Second, "the failed statement doesn't wait for the remote rollback")

	select {
	case resp := <-merged:
		require.Empty(t, resp.Errors)
	case <-time.After(15 * time.Second):
		t.Fatal("the remote keeps the failed transaction's MERGE key: its rollback never reached the server")
	}
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	require.Equal(t, int32(1), deletes.Load(), "the remote transaction is rolled back once")
	resp := autocommit("MATCH (u:U {k: 1}) RETURN count(u) AS c")
	require.Empty(t, resp.Errors)
	require.Equal(t, float64(1), resp.Results[0].Data[0].Row[0])
}
