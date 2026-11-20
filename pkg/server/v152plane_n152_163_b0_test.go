package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

// Diagnose the existing lease benchmark's failed calibration without changing
// its assertion or the HTTP contract. Its empty request is distinct from an
// empty statement list in a valid transaction JSON request.
func TestNOR8HTTPLeaseBenchmarkRequestContract(t *testing.T) {
	s, authenticator := setupTestServer(t)
	require.NoError(t, s.txSessions.Close())
	s.txSessions = txsession.NewManager(time.Hour, s.newExecutorForDatabase, t.Context())
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	opened := makeRequest(t, s, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
	require.Equal(t, http.StatusCreated, opened.Code, opened.Body.String())
	path := strings.TrimPrefix(opened.Header().Get("Location"), "http://example.com")
	require.NotEmpty(t, path)
	request := httptest.NewRequest(http.MethodPost, path, http.NoBody)
	request.Header.Set("Authorization", token)
	response := httptest.NewRecorder()
	s.buildRouter().ServeHTTP(response, request)
	t.Logf("existing benchmark request: status=%d body=%s", response.Code, response.Body.String())
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "Neo.ClientError.Request.InvalidFormat")
	require.Contains(t, response.Body.String(), "invalid request body")
	// Invalid transaction requests can retire their session. The healthy
	// control must use an independently opened transaction, not that failed one.
	control := makeRequest(t, s, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
	require.Equal(t, http.StatusCreated, control.Code, control.Body.String())
	path = strings.TrimPrefix(control.Header().Get("Location"), "http://example.com")
	healthy := makeRequest(t, s, http.MethodPost, path, map[string]any{"statements": []any{}}, token)
	t.Logf("valid empty-statement control: status=%d body=%s", healthy.Code, healthy.Body.String())
	require.Equal(t, http.StatusOK, healthy.Code, healthy.Body.String())
	require.NoError(t, s.Stop(context.Background()))
}

// The existing benchmark sends NoBody, so it cannot measure a successful
// request. This separate, uncommitted measurement includes a fresh valid JSON
// body and a cloned request per iteration. It does not alter that benchmark,
// the server, or the lifetime contract.
func BenchmarkNOR8ValidHTTPAdmissionLease(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		b.Run(fmt.Sprintf("parallel%t", parallel), func(b *testing.B) {
			s, authenticator := setupTestServer(b)
			token := "Bearer " + getAuthToken(b, authenticator, "admin")
			opened := makeRequest(b, s, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
			require.Equal(b, http.StatusCreated, opened.Code, opened.Body.String())
			path := strings.TrimPrefix(opened.Header().Get("Location"), "http://example.com")
			require.NotEmpty(b, path)
			payload := []byte(`{"statements":[]}`)
			template := httptest.NewRequest(http.MethodPost, path, nil)
			template.Header.Set("Authorization", token)
			template.Header.Set("Content-Type", "application/json")
			handler := s.buildRouter()
			var failures atomic.Int64
			run := func(writer *r4BenchWriter) {
				request := template.Clone(context.Background())
				request.Body = io.NopCloser(bytes.NewReader(payload))
				request.ContentLength = int64(len(payload))
				writer.status = 0
				handler.ServeHTTP(writer, request)
				if writer.status != http.StatusOK {
					failures.Add(1)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					writer := &r4BenchWriter{header: make(http.Header)}
					for pb.Next() {
						run(writer)
					}
				})
			} else {
				writer := &r4BenchWriter{header: make(http.Header)}
				for i := 0; i < b.N; i++ {
					run(writer)
				}
			}
			b.StopTimer()
			require.Zero(b, failures.Load())
		})
	}
}
