package server

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type r4BenchWriter struct {
	header http.Header
	status int
}

func (w *r4BenchWriter) Header() http.Header         { return w.header }
func (w *r4BenchWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *r4BenchWriter) WriteHeader(status int)      { w.status = status }
func BenchmarkR4Admission(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "Sequential"
		if parallel {
			name = "Parallel"
		}
		b.Run(name, func(b *testing.B) {
			s, _ := setupTestServer(b)
			var handler http.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
			if owner, ok := any(s).(interface {
				workMiddleware(http.Handler) http.Handler
			}); ok {
				handler = owner.workMiddleware(handler)
			}
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			b.ReportAllocs()
			b.ResetTimer()
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						handler.ServeHTTP(nil, request)
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					handler.ServeHTTP(nil, request)
				}
			}
		})
	}
}
func BenchmarkR4HTTPAdmissionAndLease(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "Sequential"
		if parallel {
			name = "ParallelSharedTransaction"
		}
		b.Run(name, func(b *testing.B) {
			s, a := setupTestServer(b)
			s.txSessions = txsession.NewManager(time.Hour, s.newExecutorForDatabase)
			token := "Bearer " + getAuthToken(b, a, "admin")
			opened := makeRequest(b, s, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
			path := strings.TrimPrefix(opened.Header().Get("Location"), "http://example.com")
			if path == "" {
				b.Fatal("opening response omitted transaction Location")
			}
			handler := s.buildRouter()
			request := httptest.NewRequest(http.MethodPost, path, http.NoBody)
			request.Header.Set("Authorization", token)
			var failures atomic.Int64
			run := func(writer *r4BenchWriter) {
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
			if failures.Load() != 0 {
				b.Fatalf("%d failed request(s)", failures.Load())
			}
			s.Stop(context.Background())
		})
	}
}
