package server

import (
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
)

// writeSearchNotReady keeps early readiness checks and a search interrupted by
// a concurrent rebuild on the same documented, retryable HTTP response.
func (s *Server) writeSearchNotReady(w http.ResponseWriter, database string, status nornicdb.DatabaseSearchStatus) {
	httpresponse.WriteJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
		"error":          search.ErrSearchIndexBuilding.Error(),
		"database":       database,
		"bm25_enabled":   status.BM25Enabled,
		"vector_enabled": status.VectorEnabled,
		"search_status":  status,
		"retryable":      true,
		"http_code":      http.StatusServiceUnavailable,
		"request_status": "search_not_ready",
	})
}
