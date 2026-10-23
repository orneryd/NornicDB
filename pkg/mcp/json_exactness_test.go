package mcp

import (
	"context"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJSONIngestionMCPToolArguments(t *testing.T) {
	server := NewServer(nil, nil)
	var arguments map[string]interface{}
	server.handlers["exact"] = func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
		arguments = args
		return args, nil
	}
	request := httptest.NewRequest("POST", "/mcp/tools/call", strings.NewReader(`{"name":"exact","arguments":{"v":9007199254740993,"d":2.0}}`))
	response := httptest.NewRecorder()
	server.handleCallTool(response, request)
	require.Equal(t, 200, response.Code)
	require.Equal(t, int64(9007199254740993), arguments["v"])
	require.Equal(t, float64(2), arguments["d"])
	request = httptest.NewRequest("POST", "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":9007199254740993,"method":"tools/call","params":{"name":"exact","arguments":{"v":-9007199254740993,"d":2.0}}}`))
	response = httptest.NewRecorder()
	server.handleMCP(response, request)
	require.Equal(t, 200, response.Code)
	require.Equal(t, int64(-9007199254740993), arguments["v"])
	require.Equal(t, float64(2), arguments["d"])
	require.Contains(t, response.Body.String(), `"id":9007199254740993`)
}
