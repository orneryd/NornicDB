package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeUIBasePath(t *testing.T) {
	t.Run("allows normal prefixed path", func(t *testing.T) {
		require.Equal(t, "/nornic-db", sanitizeUIBasePath("/nornic-db/"))
		require.Equal(t, "/nornic-db", sanitizeUIBasePath("nornic-db"))
	})

	t.Run("rejects malicious header payload", func(t *testing.T) {
		require.Equal(t, "", sanitizeUIBasePath(`/" onload="alert(1)`))
		require.Equal(t, "", sanitizeUIBasePath(`/x"><script>alert(1)</script>`))
		require.Equal(t, "", sanitizeUIBasePath(`/../admin`))
		require.Equal(t, "/foo/bar", sanitizeUIBasePath(`/foo//bar`))
		require.Equal(t, "", sanitizeUIBasePath(`/foo\bar`))
	})
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
