package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Section 38.15. The historical neutral query and seed run through the HTTP
// transaction endpoint. Measure the two reported sizes after checking counts.
func TestV152PortCorrelatedCallCost(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "50m")
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.WALAutoCompactionEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	system := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b"),
	}, system)
	require.NoError(t, err)
	_, err = authenticator.CreateUser(t.Context(), "admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	bearer := "Bearer " + getAuthToken(t, authenticator, "admin")
	serverCfg := DefaultConfig()
	serverCfg.Port = 0
	serverCfg.EmbeddingEnabled = false
	srv, err := New(db, authenticator, serverCfg)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, srv) })
	router := srv.buildRouter()

	query := func(statement string, params map[string]any) ([][]any, time.Duration) {
		t.Helper()
		payload, err := json.Marshal(map[string]any{"statements": []any{
			map[string]any{"statement": statement, "parameters": params},
		}})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", bearer)
		response := httptest.NewRecorder()
		start := time.Now()
		router.ServeHTTP(response, req)
		elapsed := time.Since(start)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var decoded struct {
			Errors  []json.RawMessage
			Results []struct {
				Data []struct{ Row []any }
			}
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
		require.Empty(t, decoded.Errors, response.Body.String())
		var rows [][]any
		for _, result := range decoded.Results {
			for _, row := range result.Data {
				rows = append(rows, row.Row)
			}
		}
		return rows, elapsed
	}

	query("CREATE INDEX item_id FOR (n:Item) ON (n.id)", nil)
	query("CREATE INDEX item_group FOR (n:Item) ON (n.group)", nil)
	const statement = `MATCH (:Item {group: $group})-[r:LINK]->()
WITH DISTINCT r
CALL (r) {
  WITH r, startNode(r) AS s, endNode(r) AS e
  OPTIONAL MATCH (s)<-[:TAGS]-(t:Tag)
  WITH s, e, collect(t.name) AS tags
  CALL (s) {
    OPTIONAL MATCH (s)-[:LINK]->(n:Item)
    RETURN count(n) AS out
  }
  RETURN s.id + '|' + e.id AS key, size(tags) + out AS score
}
RETURN count(key) AS rows, sum(score) AS total`
	for _, count := range []int{8000, 32000} {
		query("MATCH (x:Item) DETACH DELETE x", nil)
		query("MATCH (x:Tag) DETACH DELETE x", nil)
		for from := 0; from < count; from += 1000 {
			query("UNWIND range($from, $to - 1) AS k CREATE (:Item {id:'i' + k, group:'g'})", map[string]any{"from": from, "to": from + 1000})
		}
		for from := 0; from < count; from += 1000 {
			query(`UNWIND range($from, $to - 1) AS k
MATCH (a:Item {id:'i' + k}), (b:Item {id:'i' + ((k + 1) % $n)})
CREATE (a)-[:LINK {id:'l' + k}]->(b)`, map[string]any{"from": from, "to": from + 1000, "n": count})
		}
		query("UNWIND range(0,9) AS k MATCH (a:Item {id:'i' + k}) CREATE (:Tag {name:'t' + k})-[:TAGS]->(a)", nil)
		for sample := 1; sample <= 2; sample++ {
			query("MATCH (x:Item {id:'i0'}) SET x.touch=$v", map[string]any{"v": sample})
			rows, elapsed := query(statement, map[string]any{"group": "g"})
			t.Logf("PORT_CORRELATED_CALL input=%d sample=%d elapsed=%s result=%v", count, sample, elapsed, rows)
			require.Equal(t, [][]any{{float64(count), float64(count + 10)}}, rows)
		}
	}
}
