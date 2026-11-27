// Regression coverage for native restore, close, and reopen (N152-265).
package server

import (
	"context"
	"encoding/base64"
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

// M06: a successful public restore must make the restored account usable on
// the server that performed it, without requiring a process restart.
func TestRestoreReloadsLiveAccountDirectory(t *testing.T) {
	base := t.TempDir()
	backup := verifyH9SourceBackup(t, base, "restored-editor")
	target, err := verifyH9Open(t, filepath.Join(base, "target"), true)
	require.NoError(t, err)
	_, err = target.auth.CreateUser(t.Context(), "target-only", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	targetOnlyToken := getAuthToken(t, target.auth, "target-only")
	targetOnlyBasic := "Basic " + base64.StdEncoding.EncodeToString([]byte("target-only:password123"))
	resp := makeRequest(t, target.srv, http.MethodGet, "/auth/me", nil, targetOnlyBasic)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	_, _, err = target.auth.Authenticate(t.Context(), "restored-editor", "Fu-acct-pass-1", "", "")
	require.Error(t, err, "account must not exist before restore")
	require.Equal(t, http.StatusOK, target.admin(http.MethodPost, "/admin/restore", map[string]any{"path": backup}))
	_, user, err := target.auth.Authenticate(t.Context(), "restored-editor", "Fu-acct-pass-1", "", "")
	require.NoError(t, err)
	require.Equal(t, "restored-editor", user.Username)
	login := makeRequest(t, target.srv, http.MethodPost, "/auth/token", map[string]string{
		"username": "restored-editor", "password": "Fu-acct-pass-1",
	}, "")
	require.Equal(t, http.StatusOK, login.Code, login.Body.String())
	_, _, err = target.auth.Authenticate(t.Context(), "target-only", "password123", "", "")
	require.Error(t, err, "an account absent from the restored store must lose access")
	login = makeRequest(t, target.srv, http.MethodPost, "/auth/token", map[string]string{
		"username": "target-only", "password": "password123",
	}, "")
	require.Equal(t, http.StatusUnauthorized, login.Code, login.Body.String())
	resp = makeRequest(t, target.srv, http.MethodGet, "/auth/me", nil, "Bearer "+targetOnlyToken)
	require.Equal(t, http.StatusUnauthorized, resp.Code, "pre-restore bearer token must lose access")
	resp = makeRequest(t, target.srv, http.MethodGet, "/auth/me", nil, targetOnlyBasic)
	require.Equal(t, http.StatusUnauthorized, resp.Code, "cached pre-restore Basic credentials must lose access")
}

// M06: the restored catalog's CDC setting must take effect in the same server.
func TestRestoreReloadsLiveChangeCapture(t *testing.T) {
	base := t.TempDir()
	source, err := verifyH9Open(t, filepath.Join(base, "source"), true)
	require.NoError(t, err)
	admin := getAuthToken(t, source.auth, "admin")
	require.Empty(t, txErrorCode(t, source.srv, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))
	backup := filepath.Join(base, "cdc.backup")
	require.Equal(t, http.StatusOK, source.admin(http.MethodPost, "/admin/backup", map[string]any{"path": backup}))
	source.close()

	target, err := verifyH9Open(t, filepath.Join(base, "target"), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, target.admin(http.MethodPost, "/admin/restore", map[string]any{"path": backup}))
	admin = getAuthToken(t, target.auth, "admin")
	options, err := target.srv.dbManager.DatabaseOptions("nornic")
	require.NoError(t, err)
	require.Equal(t, "FULL", options["txLogEnrichment"])
	rows := httpRows(t, target.srv, "Bearer "+admin, "/db/nornic/tx/commit", "CALL db.cdc.current() YIELD id RETURN id", nil)
	require.Len(t, rows, 1)
}

func TestRestoreDisablesPreRestoreChangeCapture(t *testing.T) {
	base := t.TempDir()
	source, err := verifyH9Open(t, filepath.Join(base, "source"), true)
	require.NoError(t, err)
	backup := filepath.Join(base, "cdc-off.backup")
	require.Equal(t, http.StatusOK, source.admin(http.MethodPost, "/admin/backup", map[string]any{"path": backup}))
	source.close()

	target, err := verifyH9Open(t, filepath.Join(base, "target"), true)
	require.NoError(t, err)
	admin := getAuthToken(t, target.auth, "admin")
	require.Empty(t, txErrorCode(t, target.srv, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))
	require.Equal(t, http.StatusOK, target.admin(http.MethodPost, "/admin/restore", map[string]any{"path": backup}))
	options, err := target.srv.dbManager.DatabaseOptions("nornic")
	require.NoError(t, err)
	require.Empty(t, options["txLogEnrichment"])
	admin = getAuthToken(t, target.auth, "admin")
	require.Equal(t, "Neo.DatabaseError.ChangeDataCapture.Disabled",
		cdcWireErrorCode(t, target.srv, "Bearer "+admin, "/db/nornic/tx/commit", "CALL db.cdc.current()", nil))
}
