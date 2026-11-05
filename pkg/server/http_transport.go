package server

import (
	"net/http"

	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// newCleartextHTTPHandler owns protocol selection before application admission.
// h2c buffers an Upgrade's complete initial body before invoking application, so
// only bodyless requests may take that path (a zero-byte upgrade-body cap).
// Body-bearing proposals stay HTTP/1 and reach the same admission/body limits.
// No lease spans ServeConn: each upgraded stream owns its application credit.
func newCleartextHTTPHandler(application http.Handler, config *http2.Server) http.Handler {
	protocol := h2c.NewHandler(application, config)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.ContentLength != 0 || len(r.TransferEncoding) != 0) &&
			httpguts.HeaderValuesContainsToken(r.Header.Values("Upgrade"), "h2c") {
			application.ServeHTTP(w, r)
			return
		}
		protocol.ServeHTTP(w, r)
	})
}
