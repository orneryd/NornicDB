package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// BenchmarkExplicitTransactionRequests measures an explicit HTTP transaction:
// open it with a statement, run a statement in it, and commit.
func BenchmarkExplicitTransactionRequests(b *testing.B) {
	server, authenticator := setupTestServer(b)
	token := "Bearer " + getAuthToken(b, authenticator, "admin")
	open := map[string]any{"statements": []map[string]any{{"statement": "CREATE (:BenchTx {v: 1})"}}}
	run := map[string]any{"statements": []map[string]any{{"statement": "MATCH (n:BenchTx) RETURN count(n) AS c"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := makeRequest(b, server, http.MethodPost, "/db/nornic/tx", open, token)
		if rec.Code != http.StatusCreated {
			b.Fatalf("open: %d %s", rec.Code, rec.Body.String())
		}
		var resp TransactionResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			b.Fatal(err)
		}
		txPath := strings.TrimSuffix(resp.Commit, "/commit")
		if rec = makeRequest(b, server, http.MethodPost, txPath, run, token); rec.Code != http.StatusOK {
			b.Fatalf("run: %d", rec.Code)
		}
		if rec = makeRequest(b, server, http.MethodPost, txPath+"/commit", nil, token); rec.Code != http.StatusOK {
			b.Fatalf("commit: %d", rec.Code)
		}
	}
}
