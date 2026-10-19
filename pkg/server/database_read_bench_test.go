package server

import (
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkDirectDatabaseReadRoutes(b *testing.B) {
	server, users := setupTestServer(b)
	token := "Bearer " + getAuthToken(b, users, "admin")
	_, err := server.db.GetStorage().CreateNode(&storage.Node{ID: "read-bench", Labels: []string{"ReadBench"}})
	if err != nil { b.Fatal(err) }
	for _, route := range []struct { name, method, path string; body any }{
		{"database-info", http.MethodGet, "/db/nornic", nil},
		{"status", http.MethodGet, "/status", nil},
		{"decay", http.MethodGet, "/nornicdb/decay", nil},
		{"neighborhood", http.MethodPost, "/nornicdb/graph/nornic/neighborhood", map[string]any{"node_ids": []string{"read-bench"}}},
	} {
		b.Run(route.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				response := makeRequest(b, server, route.method, route.path, route.body, token)
				if response.Code != http.StatusOK { b.Fatalf("%d %s", response.Code, response.Body.String()) }
			}
		})
	}
}
