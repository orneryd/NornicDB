package server

import (
	"net/http"
	"strings"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// metadataAdmissionMiddleware precedes every application middleware decoder.
// Only canonical bodyless control requests with an empty raw query are exempt.
func (s *Server) metadataAdmissionMiddleware(next http.Handler) http.Handler {
	base := strings.TrimSuffix(s.config.BasePath, "/")
	if base != "" && !strings.HasPrefix(base, "/") {
		base = "/" + base
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admitted, _ := storage.AdmitVectorWork(r.Context())
		r = r.WithContext(admitted)
		if s.memoryPolicy == nil || bodylessControlRequest(r, base) {
			next.ServeHTTP(w, r)
			return
		}
		memory.HandleHTTPRequest(s.memoryPolicy, w, r, memory.HTTPWireOptions{MaxBodyBytes: s.config.MaxRequestSize, LimitBody: true, MetadataOnly: true}, s.writeRequestAdmissionError, next)
	})
}

// Inspect framing and path substrings only: no Query, header, form or body parse.
// The raw-query exemption cap is zero bytes, including for prefixed deployments.
func bodylessControlRequest(r *http.Request, base string) bool {
	if len(r.URL.RawQuery) != 0 || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		return false
	}
	path := r.URL.Path
	if base != "" && strings.HasPrefix(path, base) && (len(path) == len(base) || path[len(base)] == '/') {
		path = strings.TrimPrefix(path, base)
	}
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && path == "/health" {
		return true
	}
	path = strings.TrimSuffix(path, "/")
	path, ok := strings.CutPrefix(path, "/db/")
	if !ok {
		return false
	}
	database, path, ok := strings.Cut(path, "/")
	if !ok || database == "" {
		return false
	}
	path, ok = strings.CutPrefix(path, "tx/")
	if !ok {
		return false
	}
	id, tail, hasTail := strings.Cut(path, "/")
	if id == "" || id == "commit" {
		return false
	}
	return (r.Method == http.MethodDelete && !hasTail) ||
		(r.Method == http.MethodPost && hasTail && tail == "commit")
}
