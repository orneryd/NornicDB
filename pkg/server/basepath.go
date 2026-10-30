package server

import (
	"net/http"
	"net/url"
	"strings"
)

// basePathMiddleware strips the configured base path prefix from incoming requests.
// This enables deployment behind a reverse proxy with a URL prefix.
// Example: with BasePath="/nornicdb", request to /nornicdb/health becomes /health
func (s *Server) basePathMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		basePath := s.config.BasePath
		if basePath == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Normalize base path (ensure it starts with / and doesn't end with /)
		if !strings.HasPrefix(basePath, "/") {
			basePath = "/" + basePath
		}
		basePath = strings.TrimSuffix(basePath, "/")

		// Check if request path starts with base path
		if strings.HasPrefix(r.URL.Path, basePath+"/") || r.URL.Path == basePath {
			// Strip the base path prefix
			newPath := strings.TrimPrefix(r.URL.Path, basePath)
			if newPath == "" {
				newPath = "/"
			}

			// Clone the request with the new path
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = new(url.URL)
			*r2.URL = *r.URL
			r2.URL.Path = newPath
			r2.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, basePath)

			// Store original path and base path in context for handlers that need it
			r2.Header.Set("X-Original-Path", r.URL.Path)
			r2.Header.Set("X-Base-Path", basePath)

			next.ServeHTTP(w, r2)
			return
		}

		// Also check X-Forwarded-Prefix header from reverse proxy
		if fwdPrefix := r.Header.Get("X-Forwarded-Prefix"); fwdPrefix != "" {
			// Store for handlers that need it
			r.Header.Set("X-Base-Path", fwdPrefix)
		}

		next.ServeHTTP(w, r)
	})
}
