package server

// Lane L6b (closeout): P06 L-H9-009, GET /nornicdb/health.
// Default server (no BasePath) versus the base-path variant BasePath=/nornicdb
// (basepath.go:9-11 strips it, so /nornicdb/health reaches /health), with
// /health as control on both. Decides that the route exists only through the
// base-path middleware, not in the router's /nornicdb/... group
// (server_router.go:130, :316-333).

import (
	"net/http"
	"testing"
)

func TestL6bHealthRouteBasePath(t *testing.T) {
	for _, variant := range []struct {
		name     string
		basePath string
	}{{"default", ""}, {"basepath-nornicdb", "/nornicdb"}} {
		t.Run(variant.name, func(t *testing.T) {
			server, authenticator := setupTestServerWithConfig(t, func(c *Config) { c.BasePath = variant.basePath })
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			codes := map[string]int{}
			for _, path := range []string{"/nornicdb/health", "/health", "/nornicdb/status", "/status"} {
				resp := makeRequest(t, server, http.MethodGet, path, nil, token)
				codes[path] = resp.Code
				body := resp.Body.String()
				if len(body) > 160 {
					body = body[:160]
				}
				t.Logf("L6B_P06 basePath=%q GET %s -> %d %s", variant.basePath, path, resp.Code, body)
			}
			if variant.basePath == "" && codes["/nornicdb/health"] == http.StatusNotFound && codes["/health"] == http.StatusOK {
				t.Errorf("CAUSE P06: without BasePath the router has no /nornicdb/health route (404) while /health answers 200")
			}
		})
	}
}
