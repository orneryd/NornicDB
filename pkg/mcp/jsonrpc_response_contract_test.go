package mcp

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// The common envelope owner preserves these independently pinned original
// wire products, including explicit null results and empty error data.
func TestJSONRPCEnvelopeWireContract(t *testing.T) {
	cases := []struct {
		name string
		call func(w http.ResponseWriter)
		body string
	}{
		{
			name: "result nil id",
			call: func(w http.ResponseWriter) {
				(&Server{}).writeJSONRPCResponse(w, nil, "result", nil)
			},
			body: "{\"id\":null,\"jsonrpc\":\"2.0\",\"result\":null}\n",
		},
		{
			name: "result map id 7",
			call: func(w http.ResponseWriter) {
				(&Server{}).writeJSONRPCResponse(w, 7, "result", map[string]interface{}{"ok": true})
			},
			body: "{\"id\":7,\"jsonrpc\":\"2.0\",\"result\":{\"ok\":true}}\n",
		},
		{
			name: "error opaque id",
			call: func(w http.ResponseWriter) {
				(&Server{}).writeJSONRPCResponse(w, "opaque", "error", map[string]interface{}{"code": -32601, "message": "missing", "data": ""})
			},
			body: "{\"error\":{\"code\":-32601,\"data\":\"\",\"message\":\"missing\"},\"id\":\"opaque\",\"jsonrpc\":\"2.0\"}\n",
		},
		{
			name: "error nil id details",
			call: func(w http.ResponseWriter) {
				(&Server{}).writeJSONRPCResponse(w, nil, "error", map[string]interface{}{"code": -32700, "message": "parse", "data": "details"})
			},
			body: "{\"error\":{\"code\":-32700,\"data\":\"details\",\"message\":\"parse\"},\"id\":null,\"jsonrpc\":\"2.0\"}\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.call(rec)
			res := rec.Result()
			require.Equal(t, http.StatusOK, res.StatusCode)
			require.Equal(t, "application/json", res.Header.Get("Content-Type"))
			require.Equal(t, tc.body, rec.Body.String())
		})
	}
}
