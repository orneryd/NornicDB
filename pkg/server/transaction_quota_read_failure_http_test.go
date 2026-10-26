package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPTransactionQuotaReadFailureRetiresTransactionAndKeepsConnection(t *testing.T) {
	for _, mode := range []storagefault.ReadOperation{storagefault.NodeRead, storagefault.EdgeRead, storagefault.NodeCountRead, storagefault.EdgeCountRead, storagefault.NodeStreamRead} {
		t.Run(string(mode), func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			real := server.db.GetBaseStorageForManager()
			stored, ok := storage.StoredReader(real)
			require.True(t, ok)
			injected := errors.New("injected HTTP quota admission read failure")
			faults := storagefault.NewReadFailureEngine(real.(storagefault.ReadEngine), stored, injected)
			manager, err := multidb.NewDatabaseManager(faults, nil)
			require.NoError(t, err)
			require.NoError(t, manager.CreateDatabase("quota"))
			limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100, MaxEdges: 100}}
			if mode == storagefault.NodeStreamRead {
				limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: 100000}}
			}
			require.NoError(t, manager.SetDatabaseLimits("quota", limits))
			server.dbManager = manager
			t.Cleanup(func() { stopTestServer(t, server); _ = manager.Close() })
			endpoint := httptest.NewServer(server.buildRouter())
			t.Cleanup(endpoint.Close)
			transport := &http.Transport{MaxConnsPerHost: 1}
			t.Cleanup(transport.CloseIdleConnections)
			client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			request := func(target string, body any) (int, TransactionResponse, bool) {
				t.Helper()
				encoded, err := json.Marshal(body)
				require.NoError(t, err)
				if !strings.HasPrefix(target, "http") {
					target = endpoint.URL + target
				}
				req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(encoded))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", token)
				reused := false
				req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
				response, err := client.Do(req)
				require.NoError(t, err)
				data, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(data, &result), string(data))
				return response.StatusCode, result, reused
			}
			query := "CREATE (:Quota)"
			if mode == storagefault.EdgeRead || mode == storagefault.EdgeCountRead {
				query = "CREATE (:Quota)-[:LINK]->(:Quota)"
			}
			status, opened, _ := request("/db/quota/tx", map[string]any{"statements": []map[string]any{{"statement": query}}})
			require.Equal(t, http.StatusCreated, status)
			require.Empty(t, opened.Errors)
			require.NotEmpty(t, opened.Commit)
			faults.SetFailure(mode)
			status, failed, reused := request(opened.Commit, map[string]any{"statements": []any{}})
			require.Equal(t, http.StatusOK, status)
			require.True(t, reused)
			require.Len(t, failed.Errors, 1)
			require.Contains(t, failed.Errors[0].Message, injected.Error())
			require.Nil(t, failed.Receipt)
			faults.SetFailure("")
			status, retired, reused := request(opened.Commit, map[string]any{"statements": []any{}})
			require.Equal(t, http.StatusNotFound, status)
			require.True(t, reused)
			require.Len(t, retired.Errors, 1)
			require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", retired.Errors[0].Code)
			status, storedResult, reused := request("/db/quota/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "MATCH (n:Quota) RETURN count(n)"}}})
			require.Equal(t, http.StatusOK, status)
			require.True(t, reused, "known rollback must leave this HTTP connection reusable")
			require.Empty(t, storedResult.Errors)
			require.Len(t, storedResult.Results, 1)
			require.Len(t, storedResult.Results[0].Data, 1)
			require.Equal(t, []any{float64(0)}, storedResult.Results[0].Data[0].Row)
		})
	}
}
