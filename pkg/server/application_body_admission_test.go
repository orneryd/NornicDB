package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

type admissionUnreadBody struct{ reads int }

func (b *admissionUnreadBody) Read([]byte) (int, error) { b.reads++; return 0, io.EOF }
func (*admissionUnreadBody) Close() error               { return nil }

func TestHTTPApplicationBodyAdmissionBeforeAllDecoders(t *testing.T) {
	server, client, token := startMemoryHTTPServer(t)
	policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) {
		return memory.Sample{Used: 1 << 20}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	for _, path := range []string{"/db/nornic/tx/commit", "/auth/token", "/auth/users", "/nornicdb/search",
		"/admin/databases/nornic/config", "/nornicdb/embed/retry-failures", "/gdpr/export", "/mcp/tools/call", "/graphql"} {
		t.Run(path, func(t *testing.T) {
			body := &admissionUnreadBody{}
			r := httptest.NewRequest(http.MethodPost, path, body)
			r.Header.Set("Authorization", token)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			server.buildRouter().ServeHTTP(w, r)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), "Neo.TransientError.General.MemoryPoolOutOfMemoryError")
			require.Zero(t, body.reads, "admission must precede the first body read")
			require.Zero(t, policy.Reserved())
		})
	}
	// Exercise the actual HTTP listener as well as the unread-body sentinel.
	r, err := http.NewRequest(http.MethodPost, client.base+"/nornicdb/search", strings.NewReader("{invalid JSON"))
	require.NoError(t, err)
	r.Header.Set("Authorization", token)
	r.Header.Set("Content-Type", "application/json")
	response, err := client.client.Do(r)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
}
