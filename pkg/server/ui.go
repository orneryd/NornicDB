// Package server provides an HTTP REST API server for NornicDB.
package server

import (
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// UIAssets holds the UI files (set by main package or tests).
var UIAssets fs.FS

// UIEnabled indicates if UI assets are available
var UIEnabled bool

// UIBasePath is a trusted, server-configured UI base path used when rewriting
// static asset references in index.html for reverse-proxy deployments.
var UIBasePath string

// SetUIAssets configures the UI assets.
func SetUIAssets(assets fs.FS) {
	UIAssets = assets
	UIEnabled = true
}

// SetUIBasePath configures the trusted base path used for UI asset rewriting.
func SetUIBasePath(basePath string) {
	UIBasePath = sanitizeUIBasePath(basePath)
}

// uiHandler serves the embedded SPA UI
type uiHandler struct {
	fileServer http.Handler
	indexHTML  []byte
	basePath   string
}

func normalizeUIBasePath(raw string) string {
	base := strings.TrimSpace(raw)
	if base == "" || base == "/" {
		return ""
	}
	// Accept only conservative path characters to avoid reflecting attacker-controlled
	// header content into HTML attributes.
	for _, r := range base {
		if !(r == '/' || r == '-' || r == '_' ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9')) {
			return ""
		}
	}
	if !strings.HasPrefix(base, "/") {
		base = "/" + base
	}
	if strings.Contains(base, "..") {
		return ""
	}
	for strings.Contains(base, "//") {
		base = strings.ReplaceAll(base, "//", "/")
	}
	base = strings.TrimSuffix(base, "/")
	if base == "/" {
		return ""
	}
	return base
}

func rewriteIndexHTMLBasePath(indexHTML []byte, basePath string) []byte {
	base := sanitizeUIBasePath(basePath)
	if base == "" {
		return indexHTML
	}
	s := string(indexHTML)
	repl := []struct {
		from string
		to   string
	}{
		{`src="/assets/`, `src="` + base + `/assets/`},
		{`href="/assets/`, `href="` + base + `/assets/`},
		{`href="/nornicdb.svg"`, `href="` + base + `/nornicdb.svg"`},
		{`href="/favicon.ico"`, `href="` + base + `/favicon.ico"`},
	}
	for _, r := range repl {
		s = strings.ReplaceAll(s, r.from, r.to)
	}
	return []byte(s)
}

func sanitizeUIBasePath(raw string) string {
	base := normalizeUIBasePath(raw)
	if base == "" {
		return ""
	}
	// Reject unexpected characters so untrusted header input cannot escape
	// HTML attribute context when used for asset URL rewriting.
	for i := 0; i < len(base); i++ {
		c := base[i]
		if (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '/' || c == '-' || c == '_' || c == '.' || c == '~' {
			continue
		}
		return ""
	}
	// Disallow traversal-like inputs and normalize path shape.
	if strings.Contains(base, "..") || strings.Contains(base, "//") || strings.Contains(base, "\\") {
		return ""
	}
	clean := path.Clean(base)
	if clean == "." || clean == "/" {
		return ""
	}
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return clean
}

// newUIHandler creates a handler for serving embedded UI assets
func newUIHandler() (*uiHandler, error) {
	if !UIEnabled {
		return nil, nil
	}
	if UIAssets == nil {
		return nil, fmt.Errorf("UI assets not configured")
	}

	// List the embedded files to debug
	entries, err := fs.ReadDir(UIAssets, ".")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded root: %w", err)
	}

	// Find the correct path (might be just "dist" or "ui/dist")
	var distPath string
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() == "ui" {
			distPath = "ui/dist"
			break
		} else if entry.IsDir() && entry.Name() == "dist" {
			distPath = "dist"
			break
		}
	}

	if distPath == "" {
		return nil, fmt.Errorf("UI dist directory not found in embedded assets")
	}

	// Get the dist subdirectory from embedded files
	distFS, err := fs.Sub(UIAssets, distPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get dist subdirectory: %w", err)
	}

	// Read index.html for SPA fallback
	indexHTML, err := fs.ReadFile(distFS, "index.html")
	if err != nil {
		return nil, fmt.Errorf("failed to read index.html: %w", err)
	}

	return &uiHandler{
		fileServer: http.FileServer(http.FS(distFS)),
		indexHTML:  indexHTML,
		basePath:   sanitizeUIBasePath(UIBasePath),
	}, nil
}

// ServeHTTP implements http.Handler for the UI
func (h *uiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqPath := r.URL.Path

	// Validate path to prevent directory traversal attacks. We compare
	// the cleaned path against the request path with a single allowance:
	// a benign trailing slash (e.g. /databases/) is fine — React Router
	// produces those normally and `path.Clean` strips them. Any OTHER
	// difference (`..`, `//`, control chars) is a real traversal attempt.
	cleanPath := path.Clean(reqPath)
	if !strings.HasPrefix(cleanPath, "/") {
		cleanPath = "/" + cleanPath
	}
	normalized := strings.TrimSuffix(reqPath, "/")
	if normalized == "" {
		normalized = "/"
	}
	if strings.Contains(cleanPath, "..") || cleanPath != normalized {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Serve static assets directly
	if strings.HasPrefix(cleanPath, "/assets/") ||
		strings.HasSuffix(cleanPath, ".js") ||
		strings.HasSuffix(cleanPath, ".css") ||
		strings.HasSuffix(cleanPath, ".svg") ||
		strings.HasSuffix(cleanPath, ".png") ||
		strings.HasSuffix(cleanPath, ".ico") ||
		strings.HasSuffix(cleanPath, ".woff") ||
		strings.HasSuffix(cleanPath, ".woff2") {
		h.fileServer.ServeHTTP(w, r)
		return
	}

	// For all other paths, serve index.html (SPA routing).
	// Only use trusted server-configured base path (never request headers).
	basePath := h.basePath
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(rewriteIndexHTMLBasePath(h.indexHTML, basePath))
}

// isUIRequest checks if request is from a browser wanting HTML
func isUIRequest(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	// Browser requests typically accept text/html
	return strings.Contains(accept, "text/html")
}
