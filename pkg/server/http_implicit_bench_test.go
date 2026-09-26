package server

import (
	"net/http"
	"testing"
)

// BenchmarkImplicitTransactionRequests measures one-shot /tx/commit requests:
// a single read, a single write, and a request of several statements.
func BenchmarkImplicitTransactionRequests(b *testing.B) {
	server, authenticator := setupTestServer(b)
	token := "Bearer " + getAuthToken(b, authenticator, "admin")
	statements := func(texts ...string) map[string]any {
		list := make([]map[string]any, len(texts))
		for i, text := range texts {
			list[i] = map[string]any{"statement": text}
		}
		return map[string]any{"statements": list}
	}
	for _, bench := range []struct {
		name string
		body map[string]any
	}{
		{"single_read", statements("MATCH (n:BenchOneShot) RETURN count(n) AS c")},
		{"single_write", statements("CREATE (:BenchOneShot {v: 1})")},
		{"three_statements", statements("CREATE (:BenchOneShot {v: 1})", "MATCH (n:BenchOneShot) RETURN count(n) AS c", "RETURN 1 AS x")},
	} {
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if rec := makeRequest(b, server, http.MethodPost, "/db/nornic/tx/commit", bench.body, token); rec.Code != http.StatusOK && rec.Code != http.StatusAccepted {
					b.Fatalf("%d %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}
