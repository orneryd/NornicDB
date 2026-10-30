package server

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestIsHTTPSRequest(t *testing.T) {
	t.Run("direct tls request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "https://example.com/auth", nil)
		req.TLS = &tls.ConnectionState{}
		if !isHTTPSRequest(req) {
			t.Fatalf("expected HTTPS for direct TLS request")
		}
	})

	t.Run("untrusted forwarded proto is ignored", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/auth", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set("X-Forwarded-Proto", "https")
		if isHTTPSRequest(req) {
			t.Fatalf("expected spoofed forwarded proto to be ignored")
		}
	})

	t.Run("trusted forwarded proto list uses first hop", func(t *testing.T) {
		server := &Server{trustedProxyPrefixes: mustParseTrustedProxyPrefixes(t, []string{"192.0.2.0/24"})}
		req := httptest.NewRequest(http.MethodGet, "http://example.com/auth", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set("X-Forwarded-Proto", "https, http")
		recorder := httptest.NewRecorder()
		server.trustedProxyMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, trustedReq *http.Request) {
			if !isHTTPSRequest(trustedReq) {
				t.Fatalf("expected HTTPS for a configured proxy peer")
			}
		})).ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected trusted proxy middleware to continue request, got %d", recorder.Code)
		}
	})

	t.Run("trusted proxy does not make plain request secure", func(t *testing.T) {
		server := &Server{trustedProxyPrefixes: mustParseTrustedProxyPrefixes(t, []string{"192.0.2.10"})}
		req := httptest.NewRequest(http.MethodGet, "http://example.com/auth", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		server.trustedProxyMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, trustedReq *http.Request) {
			if isHTTPSRequest(trustedReq) {
				t.Fatalf("expected non-HTTPS for trusted proxy request without forwarded proto")
			}
		})).ServeHTTP(httptest.NewRecorder(), req)
	})

	t.Run("forwarded proto without trust context is ignored", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/auth", nil)
		req.Header.Set("X-Forwarded-Proto", "https, http")
		if isHTTPSRequest(req) {
			t.Fatalf("expected forwarded proto without trusted peer context to be ignored")
		}
	})

	t.Run("plain http request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/auth", nil)
		if isHTTPSRequest(req) {
			t.Fatalf("expected non-HTTPS for plain request")
		}
	})
}

func mustParseTrustedProxyPrefixes(t *testing.T, proxies []string) []netip.Prefix {
	t.Helper()
	prefixes, err := parseTrustedProxyPrefixes(proxies)
	if err != nil {
		t.Fatalf("parse trusted proxies: %v", err)
	}
	return prefixes
}
