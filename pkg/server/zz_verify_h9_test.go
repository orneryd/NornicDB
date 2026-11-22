// Verification tests for hunt leads L-H9-002, L-H9-003 and L-H9-009 (v1.5.2
// verification campaign, H9 verifier lane, 2026-11-21). Test-only, not for
// merge. Each lead test fails exactly at the disagreement the lead states; each
// control next to it is expected to pass.
package server

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type verifyH9Node struct {
	t      *testing.T
	db     *nornicdb.DB
	auth   *auth.Authenticator
	srv    *Server
	closed bool
}

var verifyH9AuthConfig = auth.AuthConfig{
	SecurityEnabled: true,
	JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
}

// verifyH9Open opens a whole server (database, authenticator, router) over dir,
// configured as setupTestServer does. createAdmin creates the admin account.
func verifyH9Open(t *testing.T, dir string, createAdmin bool) (*verifyH9Node, error) {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.SearchCursorMax = 128
	config.Database.AsyncWritesEnabled = false
	config.Database.MVCCRetentionMaxVersions = 100
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	db, err := nornicdb.Open(dir, config)
	if err != nil {
		return nil, err
	}
	node := &verifyH9Node{t: t, db: db}
	t.Cleanup(node.close)
	node.auth, err = auth.NewAuthenticator(verifyH9AuthConfig, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	require.NoError(t, err)
	if createAdmin {
		_, err = node.auth.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin})
		require.NoError(t, err)
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	node.srv, err = New(db, node.auth, serverConfig)
	require.NoError(t, err)
	return node, nil
}

func (n *verifyH9Node) close() {
	if n.closed {
		return
	}
	n.closed = true
	if n.srv != nil {
		stopTestServer(n.t, n.srv)
	}
	require.NoError(n.t, n.db.Close())
}

func (n *verifyH9Node) exec(query string) *nornicdb.CypherResult {
	n.t.Helper()
	result, err := n.db.ExecuteCypher(context.Background(), query, nil)
	require.NoError(n.t, err, query)
	return result
}

func (n *verifyH9Node) count(label string) interface{} {
	n.t.Helper()
	result := n.exec("MATCH (n:" + label + ") RETURN count(n) AS c")
	require.Len(n.t, result.Rows, 1)
	n.t.Logf("VERIFY_H9 count(:%s) = %#v", label, result.Rows[0][0])
	return result.Rows[0][0]
}

func (n *verifyH9Node) admin(method, path string, body interface{}) int {
	n.t.Helper()
	token := getAuthToken(n.t, n.auth, "admin")
	resp := makeRequest(n.t, n.srv, method, path, body, "Bearer "+token)
	n.t.Logf("VERIFY_H9 %s %s -> %d %s", method, path, resp.Code, resp.Body.String())
	return resp.Code
}

// verifyH9SourceBackup writes 50 :FR nodes (and, when account != "", one
// editor account) on a new store under base/src, backs it up with POST
// /admin/backup and closes the store. It returns the backup path.
func verifyH9SourceBackup(t *testing.T, base, account string) string {
	t.Helper()
	src, err := verifyH9Open(t, filepath.Join(base, "src"), true)
	require.NoError(t, err)
	src.exec("UNWIND range(1, 50) AS i CREATE (:FR {id: i})")
	if account != "" {
		_, err = src.auth.CreateUser(context.Background(), account, "Fu-acct-pass-1", []auth.Role{auth.RoleEditor})
		require.NoError(t, err)
	}
	backup := filepath.Join(base, "b.backup")
	require.Equal(t, http.StatusOK, src.admin("POST", "/admin/backup", map[string]interface{}{"path": backup}))
	src.close()
	return backup
}

// L-H9-002: a store restored with POST /admin/restore and closed cleanly opens
// again (backup-restore.md: "Exercise writes/reads, reopen the DB").
func TestVerifyH9_002_RestoredStoreReopens(t *testing.T) {
	for _, writeAfter := range []bool{false, true} {
		t.Run(fmt.Sprintf("write=%v", writeAfter), func(t *testing.T) {
			base := t.TempDir()
			backup := verifyH9SourceBackup(t, base, "")
			target := filepath.Join(base, "target")
			tgt, err := verifyH9Open(t, target, true)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, tgt.admin("POST", "/admin/restore", map[string]interface{}{"path": backup}))
			require.Equal(t, int64(50), tgt.count("FR"), "restored nodes")
			want := int64(50)
			if writeAfter {
				tgt.exec("CREATE (:FR {id: 51})")
				want = 51
			}
			tgt.close()

			again, err := verifyH9Open(t, target, false)
			t.Logf("VERIFY_H9 reopen restored store: err=%v", err)
			require.NoError(t, err, "reopen restored store")
			assert.Equal(t, want, again.count("FR"))
		})
	}
}

// Control: the source store, closed after the same writes and backup, reopens.
func TestVerifyH9_002_Control_SourceStoreReopens(t *testing.T) {
	base := t.TempDir()
	verifyH9SourceBackup(t, base, "")
	again, err := verifyH9Open(t, filepath.Join(base, "src"), false)
	t.Logf("VERIFY_H9 reopen source store: err=%v", err)
	require.NoError(t, err)
	assert.Equal(t, int64(50), again.count("FR"))
}

// L-H9-003: an account in a native backup can log in after restore
// (backup-restore.md:14 and :90). Checked on the restoring server's own
// authenticator and on a fresh authenticator over the restored store.
func TestVerifyH9_003_RestoredAccountLogsIn(t *testing.T) {
	base := t.TempDir()
	backup := verifyH9SourceBackup(t, base, "fuacct")
	tgt, err := verifyH9Open(t, filepath.Join(base, "target"), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, tgt.admin("POST", "/admin/restore", map[string]interface{}{"path": backup}))
	require.Equal(t, int64(50), tgt.count("FR"), "restored nodes")

	_, _, sameErr := tgt.auth.Authenticate(context.Background(), "fuacct", "Fu-acct-pass-1", "127.0.0.1", "verify-h9")
	t.Logf("VERIFY_H9 restoring server's authenticator: fuacct login err=%v", sameErr)
	fresh, err := auth.NewAuthenticator(verifyH9AuthConfig, storage.NewNamespacedEngine(tgt.db.GetBaseStorageForManager(), "system"))
	require.NoError(t, err)
	_, _, freshErr := fresh.Authenticate(context.Background(), "fuacct", "Fu-acct-pass-1", "127.0.0.1", "verify-h9")
	t.Logf("VERIFY_H9 fresh authenticator over restored store: fuacct login err=%v", freshErr)
	var names []string
	for _, u := range fresh.ListUsers() {
		names = append(names, u.Username)
	}
	t.Logf("VERIFY_H9 fresh authenticator users after restore: %v", names)
	assert.NoError(t, sameErr, "restoring server's authenticator")
	assert.NoError(t, freshErr, "fresh authenticator over the restored store")
}

// Control: the same account logs in on the source store, through its own
// authenticator and through a fresh authenticator over the store.
func TestVerifyH9_003_Control_AccountOnSourceLogsIn(t *testing.T) {
	src, err := verifyH9Open(t, filepath.Join(t.TempDir(), "src"), true)
	require.NoError(t, err)
	_, err = src.auth.CreateUser(context.Background(), "fuacct", "Fu-acct-pass-1", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	_, _, err = src.auth.Authenticate(context.Background(), "fuacct", "Fu-acct-pass-1", "127.0.0.1", "verify-h9")
	assert.NoError(t, err)
	fresh, err := auth.NewAuthenticator(verifyH9AuthConfig, storage.NewNamespacedEngine(src.db.GetBaseStorageForManager(), "system"))
	require.NoError(t, err)
	_, _, err = fresh.Authenticate(context.Background(), "fuacct", "Fu-acct-pass-1", "127.0.0.1", "verify-h9")
	assert.NoError(t, err)
}

// L-H9-009: GET /nornicdb/health, which the configuration guide tells probes
// to use, is routed on a default server.
func TestVerifyH9_009_NornicdbHealthIsRouted(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	resp := makeRequest(t, server, "GET", "/nornicdb/health", nil, "Bearer "+token)
	t.Logf("VERIFY_H9 GET /nornicdb/health -> %d %s", resp.Code, resp.Body.String())
	assert.Equal(t, http.StatusOK, resp.Code)
}

// Control: GET /health on the same server.
func TestVerifyH9_009_Control_Health(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	resp := makeRequest(t, server, "GET", "/health", nil, "Bearer "+token)
	t.Logf("VERIFY_H9 GET /health -> %d %s", resp.Code, resp.Body.String())
	assert.Equal(t, http.StatusOK, resp.Code)
}
