package server

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func setupAsyncUnwindServer(t *testing.T) *Server {
	t.Helper()

	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = true

	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	authStorage := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = authStorage.Close() })
	authn, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, authStorage)
	require.NoError(t, err)
	_, _ = authn.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, authn, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, srv) })
	return srv
}

func setupAsyncUnwindServerWithDir(t *testing.T, dir string) (*Server, *nornicdb.DB) {
	t.Helper()

	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = true

	db, err := nornicdb.Open(dir, dbCfg)
	require.NoError(t, err)

	authStorage := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = authStorage.Close() })
	authn, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, authStorage)
	require.NoError(t, err)
	_, _ = authn.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, authn, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, srv) })
	return srv, db
}

func execImplicitTxAsAdmin(t *testing.T, srv *Server, dbName string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	rec := httptest.NewRecorder()
	srv.handleImplicitTransaction(rec, req, dbName)
	return rec
}

func txErrorCount(t *testing.T, rec *httptest.ResponseRecorder) int {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	errs, _ := payload["errors"].([]any)
	return len(errs)
}

func pollLabelCount(t *testing.T, srv *Server, label string, want int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for {
		q := fmt.Sprintf(`{"statements":[{"statement":"/* probe:%d */ MATCH (n:%s) RETURN count(n) AS c"}]}`, time.Now().UnixNano(), label)
		rec := execImplicitTxAsAdmin(t, srv, "nornic", q)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var payload map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
		results, _ := payload["results"].([]any)
		if len(results) > 0 {
			first, _ := results[0].(map[string]any)
			data, _ := first["data"].([]any)
			if len(data) > 0 {
				rowObj, _ := data[0].(map[string]any)
				row, _ := rowObj["row"].([]any)
				if len(row) > 0 {
					switch v := row[0].(type) {
					case float64:
						last = int64(v)
					case int64:
						last = v
					}
				}
			}
		}
		if last == want {
			return last
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestImplicitTx_UnwindBulkCreate_AsyncEventualVisibility(t *testing.T) {
	srv := setupAsyncUnwindServer(t)

	const total = 1500
	rows := make([]map[string]any, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, map[string]any{
			"id":      fmt.Sprintf("nornic-bulk-%d", i),
			"payload": fmt.Sprintf("nornic-small-%d", i),
		})
	}

	stmt := `{"statements":[{"statement":"UNWIND $rows AS row CREATE (n:NornicBulk) SET n = row","parameters":{"rows":%s}}]}`
	rowsJSON, err := json.Marshal(rows)
	require.NoError(t, err)
	rec := execImplicitTxAsAdmin(t, srv, "nornic", fmt.Sprintf(stmt, string(rowsJSON)))

	// Async path may return 202; both 200 and 202 are acceptable transport statuses.
	require.Contains(t, []int{http.StatusOK, http.StatusAccepted}, rec.Code, rec.Body.String())
	require.Equal(t, 0, txErrorCount(t, rec), rec.Body.String())

	got := pollLabelCount(t, srv, "NornicBulk", total, 20*time.Second)
	require.Equal(t, int64(total), got, "UNWIND async writes did not reach expected visibility")
}

func TestImplicitTx_UnwindBulkCreate_LargePayloadRows_AsyncEventualVisibility(t *testing.T) {
	srv := setupAsyncUnwindServer(t)

	const total = 220
	largeText := strings.Repeat("nornic-payload-", 1024) // ~15KB
	rows := make([]map[string]any, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, map[string]any{
			"id":      fmt.Sprintf("nornic-large-%d", i),
			"content": largeText,
			"kind":    "nornic-large-row",
		})
	}

	stmt := `{"statements":[{"statement":"UNWIND $rows AS row CREATE (n:NornicLargeBulk) SET n = row","parameters":{"rows":%s}}]}`
	rowsJSON, err := json.Marshal(rows)
	require.NoError(t, err)
	rec := execImplicitTxAsAdmin(t, srv, "nornic", fmt.Sprintf(stmt, string(rowsJSON)))

	require.Contains(t, []int{http.StatusOK, http.StatusAccepted}, rec.Code, rec.Body.String())
	require.Equal(t, 0, txErrorCount(t, rec), rec.Body.String())

	got := pollLabelCount(t, srv, "NornicLargeBulk", total, 30*time.Second)
	require.Equal(t, int64(total), got, "UNWIND large-row async writes did not reach expected visibility")
}

func TestImplicitTx_UnwindBulkCreate_Stress_NoSilentDrop(t *testing.T) {
	srv := setupAsyncUnwindServer(t)

	const total = 6000
	rows := make([]map[string]any, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, map[string]any{
			"id":       fmt.Sprintf("nornic-stress-%d", i),
			"category": "nornic-stress",
			"v":        i,
		})
	}

	stmt := `{"statements":[{"statement":"UNWIND $rows AS row CREATE (n:NornicStressBulk) SET n = row","parameters":{"rows":%s}}]}`
	rowsJSON, err := json.Marshal(rows)
	require.NoError(t, err)
	rec := execImplicitTxAsAdmin(t, srv, "nornic", fmt.Sprintf(stmt, string(rowsJSON)))

	require.Contains(t, []int{http.StatusOK, http.StatusAccepted}, rec.Code, rec.Body.String())
	require.Equal(t, 0, txErrorCount(t, rec), rec.Body.String())

	// Immediate visibility can lag with async writes; eventual visibility must not drop rows.
	got := pollLabelCount(t, srv, "NornicStressBulk", total, 45*time.Second)
	require.Equal(t, int64(total), got, "UNWIND stress write dropped rows or failed to fully flush")
}

func TestImplicitTx_UnwindBulkCreate_ArchitecturePayload_PersistsAcrossRestart(t *testing.T) {
	rootDir := t.TempDir()
	srv, db := setupAsyncUnwindServerWithDir(t, rootDir)

	docCandidates := []string{
		filepath.Join("docs", "architecture", "cognitive-slm-architecture.md"),
		filepath.Join("..", "..", "docs", "architecture", "cognitive-slm-architecture.md"),
	}
	var raw []byte
	var err error
	for _, p := range docCandidates {
		raw, err = os.ReadFile(p)
		if err == nil {
			break
		}
	}
	require.NoError(t, err)
	// Keep payload heavy but bounded for CI runtime stability.
	payload := string(raw)
	if len(payload) > 8192 {
		payload = payload[:8192]
	}

	const total = 3000
	const batch = 100

	for start := 0; start < total; start += batch {
		end := start + batch
		if end > total {
			end = total
		}
		rows := make([]map[string]any, 0, end-start)
		for i := start; i < end; i++ {
			rows = append(rows, map[string]any{
				"id":      fmt.Sprintf("nornic-arch-%d", i),
				"kind":    "nornic-architecture-stress",
				"content": payload,
				"seq":     i,
			})
		}

		rowsJSON, err := json.Marshal(rows)
		require.NoError(t, err)
		body := fmt.Sprintf(
			`{"statements":[{"statement":"UNWIND $rows AS row CREATE (n:NornicArchitectureBulk) SET n = row","parameters":{"rows":%s}}]}`,
			string(rowsJSON),
		)

		rec := execImplicitTxAsAdmin(t, srv, "nornic", body)
		require.Contains(t, []int{http.StatusOK, http.StatusAccepted}, rec.Code, rec.Body.String())
		require.Equal(t, 0, txErrorCount(t, rec), rec.Body.String())
	}

	got := pollLabelCount(t, srv, "NornicArchitectureBulk", total, 90*time.Second)
	require.Equal(t, int64(total), got, "pre-restart count mismatch")

	stopTestServer(t, srv)
	require.NoError(t, db.Close())

	// Re-open from the same on-disk path and verify rows persisted to disk.
	srv2, db2 := setupAsyncUnwindServerWithDir(t, rootDir)
	defer func() { _ = db2.Close() }()

	gotAfterRestart := pollLabelCount(t, srv2, "NornicArchitectureBulk", total, 20*time.Second)
	require.Equal(t, int64(total), gotAfterRestart, "post-restart count mismatch (expected durable writes)")
}
