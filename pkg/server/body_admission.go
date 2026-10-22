package server

import (
	"net/http"

	"github.com/orneryd/nornicdb/pkg/memory"
)

// bodyAdmissionMiddleware is inside timeout handlers so a timed-out worker
// retains its reservation until it actually stops decoding/handling the body.
// It covers every routed body, including delegated MCP, GraphQL and Heimdall
// handlers. Bodyless health, rollback and bare commit requests remain available.
func (s *Server) bodyAdmissionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Base-path rewriting has already run. Reuse the outer owner's exact
		// framing/path classifier: native h2 has a non-nil reader even for an
		// empty body, which must not re-admit an exempt control under pressure.
		if s.memoryPolicy == nil || bodylessControlRequest(r, "") {
			next.ServeHTTP(w, r)
			return
		}
		memory.HandleHTTPRequest(s.memoryPolicy, w, r, memory.HTTPWireOptions{MaxBodyBytes: s.config.MaxRequestSize, LimitBody: true}, s.writeRequestAdmissionError, next)
	})
}
