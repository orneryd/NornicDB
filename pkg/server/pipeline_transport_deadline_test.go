package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPPipelineTransportQueryDeadline(t *testing.T) {
	server, authenticator := setupTestServer(t)
	limits := multidb.DefaultLimits()
	limits.Query.MaxQueryTime = 200 * time.Millisecond
	require.NoError(t, server.dbManager.SetDatabaseLimits("nornic", limits))
	engine, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	for {
		wrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	readers, ok := engine.(interface{ ActiveReaders() int64 })
	require.True(t, ok)
	host := httptest.NewServer(server.buildRouter())
	defer host.Close()
	client := &http.Client{Timeout: 2 * time.Second}
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	request := func(t *testing.T, method, path, statement string) (TransactionResponse, int) {
		t.Helper()
		items := []StatementRequest{}
		if statement != "" {
			items = append(items, StatementRequest{Statement: statement})
		}
		payload, err := json.Marshal(TransactionRequest{Statements: items})
		require.NoError(t, err)
		req, err := http.NewRequest(method, host.URL+path, bytes.NewReader(payload))
		require.NoError(t, err)
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var body TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
		return body, response.StatusCode
	}
	for _, explicit := range []bool{false, true} {
		for _, tail := range []string{
			"WITH DISTINCT x RETURN count(*)", "WITH x ORDER BY x DESC RETURN count(*)", "RETURN collect(x+1)", "WITH x+1 AS y RETURN sum(y)",
		} {
			t.Run(strconv.FormatBool(explicit)+"/"+tail, func(t *testing.T) {
				baseline := readers.ActiveReaders()
				path := "/db/nornic/tx/commit"
				if explicit {
					opened, status := request(t, http.MethodPost, "/db/nornic/tx", "")
					require.Equal(t, http.StatusCreated, status)
					require.Empty(t, opened.Errors)
					commit, err := url.Parse(opened.Commit)
					require.NoError(t, err)
					path = strings.TrimSuffix(commit.Path, "/commit")
				}
				started := time.Now()
				body, status := request(t, http.MethodPost, path, "UNWIND range(1,10000000) AS x "+tail)
				elapsed := time.Since(started)
				require.Equal(t, http.StatusOK, status)
				require.NotEmpty(t, body.Errors)
				standard, _ := nornicerrors.Neo4jStatus(context.DeadlineExceeded)
				require.Equal(t, standard, body.Errors[0].Code)
				require.Less(t, elapsed, 450*time.Millisecond)
				require.Zero(t, server.activeTxReqs.Load(), "router returned while its transaction handler was active")
				if explicit {
					rolled, status := request(t, http.MethodDelete, path, "")
					if status == http.StatusNotFound {
						// A timed-out HTTP transaction is already aborted and removed.
						require.Len(t, rolled.Errors, 1)
						require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", rolled.Errors[0].Code)
					} else {
						require.Equal(t, http.StatusOK, status)
						require.Empty(t, rolled.Errors)
					}
				}
				require.Equal(t, baseline, readers.ActiveReaders(), "deadline/rollback left a snapshot reader")
				t.Logf("configured_query_timeout=200ms response=%s excess_over_configured_budget=%s explicit=%t", elapsed, elapsed-200*time.Millisecond, explicit)
			})
		}
	}
	// Healthy recovery is independent of the short-deadline matrix, which
	// must finish first without any parser warmup from a control statement.
	require.NoError(t, server.dbManager.SetDatabaseLimits("nornic", multidb.DefaultLimits()))
	healthy, status := request(t, http.MethodPost, "/db/nornic/tx/commit", "RETURN 1 AS healthy")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, healthy.Errors)
	require.Len(t, healthy.Results, 1)
	require.Equal(t, []string{"healthy"}, healthy.Results[0].Columns)
}
