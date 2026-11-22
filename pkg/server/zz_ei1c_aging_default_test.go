package server

// Untracked probe (batch 1b, third request, outcome 1): the repeated
// create-and-delete cycle over HTTP and over Bolt with the database's history
// setting (Database.MVCCRetentionMaxVersions) left at nornicdb.DefaultConfig's
// value. The server is built like the package's test fixture
// (setupTestServerWithConfig) except that the fixture's history value of 100
// is not applied. One fresh server per route; 300 cycles each of: seed 2,000
// nodes over Bolt, a timed unconstrained delete over the route, clean-up.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzEI1cServer(t *testing.T, retention int) (*httptest.Server, string, neo4j.DriverWithContext) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "zz-ei1c-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.SearchCursorMax = 128
	config.Database.AsyncWritesEnabled = false
	config.Database.MVCCRetentionMaxVersions = retention
	t.Logf("ZZEI1C MVCCRetentionMaxVersions=%d", retention)
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	db, err := nornicdb.Open(tmpDir, config)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
		storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	require.NoError(t, err)
	_, err = authenticator.CreateUser(t.Context(), "admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	srv, err := New(db, authenticator, serverConfig)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, srv) })
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	token := getAuthToken(t, authenticator, "admin")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, srv.GetDatabaseManager())
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	t.Cleanup(func() { _ = bs.Close(); <-done })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return peer, token, driver
}

func zzEI1cBolt(t *testing.T, driver neo4j.DriverWithContext, q string, p map[string]any) []*neo4j.Record {
	t.Helper()
	ctx := context.Background()
	s := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	r, err := s.Run(ctx, q, p)
	require.NoError(t, err)
	recs, err := r.Collect(ctx)
	require.NoError(t, err)
	return recs
}

func zzEI1cHTTP(t *testing.T, peer *httptest.Server, token, q string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": q}}})
	req, _ := http.NewRequest(http.MethodPost, peer.URL+"/db/nornic/tx/commit", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := peer.Client().Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	var out struct {
		Errors  []struct{ Code, Message string }
		Results []struct{ Data []struct{ Row []any } }
	}
	require.NoError(t, json.NewDecoder(res.Body).Decode(&out))
	require.Equal(t, 200, res.StatusCode)
	require.Empty(t, out.Errors)
	return fmt.Sprint(out.Results[0].Data[0].Row)
}

func zzEI1cMedian(v []int64) int64 {
	s := append([]int64(nil), v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func TestZZEI1cAgingDefaultHistory(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	const nodes, cycles = 2000, 300
	for _, route := range []string{"HTTP", "Bolt"} {
		t.Run(route, func(t *testing.T) {
			peer, token, driver := zzEI1cServer(t, 0)
			var all []int64
			for i := 1; i <= cycles; i++ {
				zzEI1cBolt(t, driver, "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
				start := time.Now()
				var got string
				if route == "HTTP" {
					got = zzEI1cHTTP(t, peer, token, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted")
				} else {
					recs := zzEI1cBolt(t, driver, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted", nil)
					got = fmt.Sprint(recs[0].Values)
				}
				d := time.Since(start).Milliseconds()
				if got != "[2000]" {
					t.Logf("ZZEI1C %s cycle %d delete returned %s", route, i, got)
				}
				zzEI1cBolt(t, driver, "MATCH (n:ZZDel) DETACH DELETE n", nil)
				all = append(all, d)
				if i%15 == 0 {
					t.Logf("ZZEI1C %s cycles %d-%d ms=%v median=%d", route, i-14, i, all[i-15:i], zzEI1cMedian(all[i-15:i]))
				}
			}
			left := zzEI1cBolt(t, driver, "MATCH (n:ZZDel) RETURN count(n) AS c", nil)
			t.Logf("ZZEI1C %s done: %d cycles, nodes left %v", route, cycles, left[0].Values)
		})
	}
}
