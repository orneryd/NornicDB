package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestSplitMCPPath(t *testing.T) {
	for _, tc := range []struct {
		path     string
		endpoint string
		database string
	}{
		{"/mcp", "/mcp", ""},
		{"/mcp/initialize", "/mcp/initialize", ""},
		{"/mcp/tools/list", "/mcp/tools/list", ""},
		{"/mcp/tools/call", "/mcp/tools/call", ""},
		{"/mcp/health", "/mcp/health", ""},
		{"/mcp/", "/mcp", ""},
		{"/mcp/tenant_a", "/mcp", "tenant_a"},
		{"/mcp/tenant_a/initialize", "/mcp/initialize", "tenant_a"},
		{"/mcp/tenant_a/tools/list", "/mcp/tools/list", "tenant_a"},
		{"/mcp/tenant_a/tools/call", "/mcp/tools/call", "tenant_a"},
		{"/mcp/tenant_a/unknown", "/mcp/unknown", "tenant_a"},
		{"/mcp/ tenant_a /tools/call", "/mcp/tools/call", "tenant_a"},
		{"/other", "/other", ""},
	} {
		t.Run(tc.path, func(t *testing.T) {
			endpoint, database := splitMCPPath(tc.path)
			require.Equal(t, tc.endpoint, endpoint)
			require.Equal(t, tc.database, database)
		})
	}
}

// urlPinSpyServer returns a server whose scoped executor records every database
// it is asked for. A nil executor makes handlers fail after resolution, which
// is enough to observe which database the call targeted.
func urlPinSpyServer(t *testing.T) (*Server, *[]string) {
	t.Helper()
	server := NewServer(nil, nil)
	var resolved []string
	server.SetDatabaseScopedExecutor(func(dbName string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
		resolved = append(resolved, dbName)
		return nil, nil, nil
	})
	return server, &resolved
}

func TestServeHTTP_URLPinOverridesPayloadDatabase(t *testing.T) {
	server, resolved := urlPinSpyServer(t)

	body := `{"name":"recall","arguments":{"id":"node-1","database":"other","db":"third"}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp/tenant_a/tools/call", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"isError":true`)
	require.Equal(t, []string{"tenant_a"}, *resolved,
		"the URL-pinned database must replace any payload database")
}

func TestServeHTTP_JSONRPCURLPinOverridesPayloadDatabase(t *testing.T) {
	server, resolved := urlPinSpyServer(t)

	body := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"recall","arguments":{"id":"node-1","database":"other"}}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp/tenant_b", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"tenant_b"}, *resolved)
}

func TestServeHTTP_UnpinnedPayloadDatabaseStillWorks(t *testing.T) {
	server, resolved := urlPinSpyServer(t)

	body := `{"name":"recall","arguments":{"id":"node-1","database":"tenant_c"}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"tenant_c"}, *resolved)
}

func TestHandleListTools_URLPinAdvertisesPinnedDefault(t *testing.T) {
	server := NewServer(nil, nil)
	server.SetDefaultDatabase("fallback")

	req := httptest.NewRequest(http.MethodGet, "/mcp/tenant_a/tools/list", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp ListToolsResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Len(t, resp.Tools, 5)

	var storeTool Tool
	for _, tool := range resp.Tools {
		if tool.Name == ToolStore {
			storeTool = tool
			break
		}
	}
	require.NotEmpty(t, storeTool.InputSchema)
	var schema struct {
		Properties struct {
			Database struct {
				Default string `json:"default"`
			} `json:"database"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(storeTool.InputSchema, &schema))
	require.Equal(t, "tenant_a", schema.Properties.Database.Default,
		"pinned tools/list must advertise the pinned database as the default")
}

func TestRegisterRoutes_URLPinnedPathDispatches(t *testing.T) {
	server, resolved := urlPinSpyServer(t)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	body := `{"name":"recall","arguments":{"id":"node-1","database":"other"}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp/tenant_a/tools/call", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"tenant_a"}, *resolved)
}

func TestServeHTTP_UnknownPinnedEndpointIsNotFound(t *testing.T) {
	server := NewServer(nil, nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp/tenant_a/unknown", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}
