package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPluginToolsPublicProtocolFIFO(t *testing.T) {
	s := NewServer(nil, nil)
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)
	before := s.ToolDefinitions()
	handler := func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
		require.Equal(t, "tenant", DatabaseFromContext(ctx))
		return args, nil
	}
	tool := Tool{Name: "plugin_z", Description: "first", InputSchema: json.RawMessage(`{"type":"object"}`)}
	require.NoError(t, s.RegisterTool(tool, handler))
	require.NoError(t, s.RegisterTool(Tool{Name: "plugin_a", InputSchema: tool.InputSchema}, func(context.Context, map[string]interface{}) (interface{}, error) {
		return nil, fmt.Errorf("inference failed")
	}))
	require.EqualError(t, s.RegisterTool(Tool{Name: "plugin_z"}, nil), "MCP tool registration failed: name collision: plugin_z")
	require.Error(t, s.RegisterTool(Tool{Name: ToolStore}, nil))
	require.Contains(t, logs.String(), "plugin_z")
	require.Contains(t, logs.String(), "kept first registration")
	defs := s.ToolDefinitions()
	require.Equal(t, before, defs[:len(before)])
	require.Equal(t, tool, defs[len(before)])
	require.Equal(t, "plugin_a", defs[len(before)+1].Name)
	for _, path := range []string{"/mcp/tenant", "/mcp/tenant/tools/list"} {
		body := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), `"name":"plugin_z"`)
		require.Less(t, strings.Index(w.Body.String(), `"name":"plugin_z"`), strings.Index(w.Body.String(), `"name":"plugin_a"`))
	}
	for _, path := range []string{"/mcp/tenant", "/mcp/tenant/tools/call"} {
		body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"plugin_z","arguments":{"value":42}}}`
		if strings.HasSuffix(path, "/call") {
			body = `{"name":"plugin_z","arguments":{"value":42}}`
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
		require.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
		if strings.HasSuffix(path, "/call") {
			require.Nil(t, response["result"])
		} else {
			response = response["result"].(map[string]interface{})
		}
		require.NotEqual(t, true, response["isError"])
		content := response["content"].([]interface{})[0].(map[string]interface{})["text"].(string)
		var args map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(content), &args))
		require.Equal(t, map[string]interface{}{"database": "tenant", "value": float64(42)}, args)
	}
	_, err := s.CallTool(context.Background(), "plugin_a", nil)
	require.EqualError(t, err, "inference failed")
	_, err = s.CallTool(context.Background(), "internal", nil)
	require.Error(t, err)
}

func TestPluginToolsConcurrentRegistrationAndDiscovery(t *testing.T) {
	s := NewServer(nil, nil)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("plugin_%d", i)
			if err := s.RegisterTool(Tool{Name: name}, func(context.Context, map[string]interface{}) (interface{}, error) { return name, nil }); err != nil {
				results <- err
				return
			}
			s.ToolDefinitions()
			result, err := s.CallTool(context.Background(), name, nil)
			if err != nil {
				results <- err
				return
			}
			if result != name {
				results <- fmt.Errorf("tool %s returned %v", name, result)
			}
		}(i)
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Len(t, s.ToolDefinitions(), len(GetToolDefinitions())+8)
}
