package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestUIRouterBasePathIsInstanceScoped(t *testing.T) {
	originalEnabled, originalAssets, originalPath := UIEnabled, UIAssets, UIBasePath
	t.Cleanup(func() { UIEnabled, UIAssets, UIBasePath = originalEnabled, originalAssets, originalPath })
	SetUIAssets(fstest.MapFS{"dist/index.html": &fstest.MapFile{Data: []byte(`<script src="/assets/app.js"></script>`)}})
	SetUIBasePath("/legacy")
	type handlerResult struct {
		handler  *uiHandler
		basePath string
	}
	results := make(chan handlerResult, 8)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			basePath := fmt.Sprintf("/server-%d", worker)
			server := &Server{config: &Config{BasePath: basePath}}
			results <- handlerResult{server.registerUIRoutes(http.NewServeMux()), basePath}
		}(worker)
	}
	workers.Wait()
	close(results)
	for result := range results {
		require.NotNil(t, result.handler)
		require.Equal(t, result.basePath, result.handler.basePath)
	}
	require.Equal(t, "/legacy", UIBasePath)
}

func TestSanitizeUIBasePath(t *testing.T) {
	t.Run("allows normal prefixed path", func(t *testing.T) {
		require.Equal(t, "/nornic-db", sanitizeUIBasePath("/nornic-db/"))
		require.Equal(t, "/nornic-db", sanitizeUIBasePath("nornic-db"))
		require.Equal(t, "/foo_bar-v1", sanitizeUIBasePath("/foo_bar-v1"))
	})

	t.Run("rejects malicious header payload", func(t *testing.T) {
		require.Equal(t, "", sanitizeUIBasePath(""))
		require.Equal(t, "", sanitizeUIBasePath("/"))
		require.Equal(t, "", sanitizeUIBasePath(`/" onload="alert(1)`))
		require.Equal(t, "", sanitizeUIBasePath(`/x"><script>alert(1)</script>`))
		require.Equal(t, "", sanitizeUIBasePath(`/../admin`))
		require.Equal(t, "/foo/bar", sanitizeUIBasePath(`/foo//bar`))
		require.Equal(t, "", sanitizeUIBasePath(`/foo\bar`))
		require.Equal(t, "", sanitizeUIBasePath(`/foo:bar`))
	})
}

func TestUIAssetHelpersAdditionalBranches(t *testing.T) {
	origEnabled := UIEnabled
	origAssets := UIAssets
	origBasePath := UIBasePath
	defer func() {
		UIEnabled = origEnabled
		UIAssets = origAssets
		UIBasePath = origBasePath
	}()

	UIEnabled = true
	UIAssets = nil
	h, err := newUIHandler()
	require.Error(t, err)
	require.Nil(t, h)

	UIAssets = fstest.MapFS{
		"dist/assets/app.js": &fstest.MapFile{Data: []byte("console.log('missing index')")},
	}
	h, err = newUIHandler()
	require.Error(t, err)
	require.Nil(t, h)

	SetUIBasePath("proxy/ui/")
	require.Equal(t, "/proxy/ui", UIBasePath)
	SetUIBasePath(`/bad:path`)
	require.Empty(t, UIBasePath)

	UIAssets = fstest.MapFS{
		"ui/dist/index.html":    &fstest.MapFile{Data: []byte(`<script src="/assets/app.js"></script><link href="/favicon.ico">`)},
		"ui/dist/assets/app.js": &fstest.MapFile{Data: []byte("console.log('ok')")},
	}
	SetUIBasePath("/app")
	h, err = newUIHandler()
	require.NoError(t, err)
	require.NotNil(t, h)
	require.Equal(t, "/app", h.basePath)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "console.log('ok')")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `src="/app/assets/app.js"`)
	require.Contains(t, rec.Body.String(), `href="/app/favicon.ico"`)
}

func TestUIHandler_ServeHTTP_DoesNotReflectMaliciousBasePathHeader(t *testing.T) {
	h := &uiHandler{
		fileServer: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		indexHTML: []byte(`<html><head><link rel="stylesheet" href="/assets/app.css"></head><body><script src="/assets/app.js"></script></body></html>`),
		basePath:  "/trusted",
	}

	req := httptest.NewRequest(http.MethodGet, "/app/route", nil)
	req.Header.Set("X-Base-Path", `/" onload="alert(1)`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.NotContains(t, body, `onload="alert(1)`)
	require.Contains(t, body, `href="/trusted/assets/app.css"`)
	require.Contains(t, body, `src="/trusted/assets/app.js"`)
	require.NotContains(t, body, `src="//assets/`)
}

// TestUIHandler_ServeHTTP_AcceptsTrailingSlash covers the SPA-fallback
// path validator. React Router emits trailing slashes for nested
// routes (e.g. /databases/, /admin/users/); a hard-refresh on those
// URLs reaches the server, so the path validator must NOT reject them.
// path.Clean strips the trailing slash, so a naive cleanPath==reqPath
// equality test 400s — fixed in the handler.
func TestUIHandler_ServeHTTP_AcceptsTrailingSlash(t *testing.T) {
	h := &uiHandler{
		fileServer: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError) // assets path; never reached for /databases/
		}),
		indexHTML: []byte(`<html><body>ok</body></html>`),
		basePath:  "",
	}

	req := httptest.NewRequest(http.MethodGet, "/databases/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "<body>ok</body>")
}

// TestUIHandler_ServeHTTP_RejectsPathTraversal — the relaxed
// trailing-slash allowance must NOT punch a hole through the
// directory-traversal guard.
func TestUIHandler_ServeHTTP_RejectsPathTraversal(t *testing.T) {
	h := &uiHandler{
		fileServer: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		indexHTML: []byte(`<html>ok</html>`),
		basePath:  "",
	}

	for _, badPath := range []string{
		"/../etc/passwd",
		"/foo/../bar",
		"/foo/..",
	} {
		t.Run(badPath, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, badPath, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code, "must reject traversal: %s", badPath)
		})
	}
}
