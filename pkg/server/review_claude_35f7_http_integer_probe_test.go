package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review probe (test-only, r73 35f7/5d36): HTTP rows and valueType() for
// integers and floats written over HTTP are identical before and after reopen.
func TestReviewClaude35f7HTTPIntegerWarmCold(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	keys := "n.a, n.b, n.c, n.d, n.e, n.f, n.l, n.p, n.pl, n.pf, n.pn"
	types := "valueType(n.a), valueType(n.b), valueType(n.c), valueType(n.d), valueType(n.e), valueType(n.f), valueType(n.l), valueType(n.p), valueType(n.pl), valueType(n.pf), valueType(n.pn)"
	warm := map[string]string{}
	for _, phase := range []string{"warm", "cold"} {
		config := nornicdb.DefaultConfig()
		config.Memory.DecayEnabled = false
		config.Memory.AutoLinksEnabled = false
		config.Database.AsyncWritesEnabled = false
		db, err := nornicdb.Open(dir, config)
		require.NoError(t, err)
		authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")}, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
		require.NoError(t, err)
		_, _ = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
		serverConfig := DefaultConfig()
		serverConfig.Port = 0
		serverConfig.EmbeddingEnabled = false
		server, err := New(db, authenticator, serverConfig)
		require.NoError(t, err)
		token := "Bearer " + getAuthToken(t, authenticator, "admin")
		run := func(statement string, params json.RawMessage) string {
			body := map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}
			resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", body, token)
			require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
			var result TransactionResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Empty(t, result.Errors, statement)
			if len(result.Results) == 0 {
				return ""
			}
			data, _ := json.Marshal(result.Results[0].Data)
			return string(data)
		}
		if phase == "warm" {
			run("CREATE (n:R73H {key:'h', a:127, b:128, c:-129, d:32768, e:9223372036854775807, f:3.0, l:[128,3,-129]}) SET n.p = $p, n.pl = $pl, n.pf = $pf, n.pn = $pn",
				json.RawMessage(`{"p":128,"pl":[128,3],"pf":3.0,"pn":-129}`))
		}
		for name, query := range map[string]string{"values": "MATCH (n:R73H {key:'h'}) RETURN " + keys, "types": "MATCH (n:R73H {key:'h'}) RETURN " + types} {
			got := run(query, json.RawMessage(`{}`))
			t.Logf("%s %s: %s", phase, name, got)
			if phase == "warm" {
				warm[name] = got
			} else if warm[name] != got {
				t.Errorf("HTTP %s COLD != WARM\n warm=%s\n cold=%s", name, warm[name], got)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = server.Stop(ctx)
		cancel()
		require.NoError(t, db.Close())
	}
}
