package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
)

func TestWithAuth_PrefersTokenOverBasic(t *testing.T) {
	server, authenticator := setupTestServer(t)
	handler := server.buildRouter()

	token := getAuthToken(t, authenticator, "admin")

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.AddCookie(&http.Cookie{Name: "nornicdb_token", Value: token})

	// Invalid Basic auth would fail if Basic is incorrectly prioritized.
	invalidBasic := base64.StdEncoding.EncodeToString([]byte("admin:wrongpassword"))
	req.Header.Set("Authorization", "Basic "+invalidBasic)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
}

func TestWithAuth_BasicAuthSetsJWTTokenCookie(t *testing.T) {
	for _, test := range []struct {
		name       string
		forwarded  string
		wantSecure bool
	}{
		{name: "plain HTTP", wantSecure: false},
		{name: "TLS terminated by proxy", forwarded: "https", wantSecure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, _ := setupTestServer(t)
			if test.forwarded != "" {
				server.trustedProxyPrefixes = mustParseTrustedProxyPrefixes(t, []string{"192.0.2.1"})
			}
			handler := server.buildRouter()
			validBasic := base64.StdEncoding.EncodeToString([]byte("admin:password123"))
			req := httptest.NewRequest(http.MethodGet, "/status", nil)
			req.Header.Set("Authorization", "Basic "+validBasic)
			if test.forwarded != "" {
				req.Header.Set("X-Forwarded-Proto", test.forwarded)
			}

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("got status %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
			}

			var tokenCookie *http.Cookie
			for _, cookie := range w.Result().Cookies() {
				if cookie.Name == "nornicdb_token" && cookie.Value != "" {
					tokenCookie = cookie
					break
				}
			}
			if tokenCookie == nil {
				t.Fatal("expected nornicdb_token cookie to be set")
			}
			if tokenCookie.Secure != test.wantSecure {
				t.Fatalf("nornicdb_token Secure = %v, want %v", tokenCookie.Secure, test.wantSecure)
			}
		})
	}
}

func TestAuthenticationE2E_LocalhostAndTLSReverseProxy(t *testing.T) {
	t.Run("localhost HTTP", func(t *testing.T) {
		server, _ := setupTestServerWithConfig(t, func(config *Config) {
			config.EnableCORS = false
		})
		startTestHTTPServer(t, server)

		client := newCookieClient(t, nil)
		baseURL := "http://" + server.Addr()
		exerciseAuthenticationE2E(t, client, baseURL, false, true)
	})

	t.Run("load balancer TLS termination to public HTTP backend", func(t *testing.T) {
		processConfig := nornicConfig.LoadDefaults()
		processConfig.Auth.Enabled = true
		processConfig.Auth.InitialPassword = "operator-supplied-secret"
		processConfig.Server.HTTPAddress = "0.0.0.0"
		processConfig.Server.HTTPTrustedProxies = []string{"127.0.0.1/32", "::1/128"}
		processConfig.Server.EnableCORS = false
		processConfig.Server.BoltEnabled = false
		processConfig.Features.QdrantGRPCEnabled = false
		if err := nornicConfig.ValidateSecurityConfiguration(processConfig); err != nil {
			t.Fatalf("documented reverse-proxy configuration rejected: %v", err)
		}

		server, _ := setupTestServerWithConfig(t, func(config *Config) {
			config.Address = "0.0.0.0"
			config.EnableCORS = false
			config.TrustedProxies = []string{"127.0.0.1/32", "::1/128"}
		})
		startTestHTTPServer(t, server)

		_, backendPort, err := net.SplitHostPort(server.Addr())
		if err != nil {
			t.Fatal(err)
		}
		backendURL, err := url.Parse("http://127.0.0.1:" + backendPort)
		if err != nil {
			t.Fatal(err)
		}
		if backendURL.Scheme != "http" {
			t.Fatalf("backend scheme = %q, want http", backendURL.Scheme)
		}
		proxy := httputil.NewSingleHostReverseProxy(backendURL)
		originalDirector := proxy.Director
		proxy.Director = func(req *http.Request) {
			originalDirector(req)
			req.Header.Set("X-Forwarded-Proto", "https")
			req.Header.Set("X-Forwarded-Host", req.Host)
			req.Header.Set("X-Forwarded-For", "198.51.100.25")
		}
		tlsProxy := httptest.NewTLSServer(proxy)
		t.Cleanup(tlsProxy.Close)
		proxyURL, err := url.Parse(tlsProxy.URL)
		if err != nil {
			t.Fatal(err)
		}
		if proxyURL.Scheme != "https" {
			t.Fatalf("load balancer scheme = %q, want https", proxyURL.Scheme)
		}

		client := newCookieClient(t, tlsProxy.Client().Transport)
		exerciseAuthenticationE2E(t, client, tlsProxy.URL, true, false)
	})
}

func startTestHTTPServer(t *testing.T, server *Server) {
	t.Helper()
	if err := server.Start(); err != nil {
		t.Fatalf("start NornicDB HTTP server: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Errorf("stop NornicDB HTTP server: %v", err)
		}
	})
}

// newCookieClient returns a client with a cookie jar. Without a transport it
// gets its own clone of http.DefaultTransport rather than sharing it. The
// client's idle connections are closed when the test ends: the client is
// created after the server starts, so this cleanup runs before the server
// stops. An open connection that never sent a request would otherwise hold
// http.Server.Shutdown for 5 s (net/http treats a new connection as idle only
// after 5 s), longer than startTestHTTPServer's stop timeout (#632).
func newCookieClient(t *testing.T, transport http.RoundTripper) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if transport == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	client := &http.Client{Jar: jar, Transport: transport}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func exerciseAuthenticationE2E(t *testing.T, client *http.Client, baseURL string, wantSecure, spoofForwardedProto bool) {
	t.Helper()

	resp, err := client.Get(baseURL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	loginBody := []byte(`{"username":"admin","password":"password123"}`)
	loginReq, err := http.NewRequest(http.MethodPost, baseURL+"/auth/token", bytes.NewReader(loginBody))
	if err != nil {
		t.Fatal(err)
	}
	loginReq.Header.Set("Content-Type", "application/json")
	if spoofForwardedProto {
		loginReq.Header.Set("X-Forwarded-Proto", "https")
	}
	resp, err = client.Do(loginReq)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	assertTokenCookieSecure(t, resp, wantSecure)

	resp, err = client.Get(baseURL + "/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func assertTokenCookieSecure(t *testing.T, response *http.Response, wantSecure bool) {
	t.Helper()
	for _, cookie := range response.Cookies() {
		if cookie.Name == "nornicdb_token" && cookie.Value != "" {
			if cookie.Secure != wantSecure {
				t.Fatalf("nornicdb_token Secure = %v, want %v", cookie.Secure, wantSecure)
			}
			return
		}
	}
	t.Fatal("expected nornicdb_token cookie to be set")
}

func TestHandleTokenHTTPCookieAuthenticatesMe(t *testing.T) {
	server, _ := setupTestServer(t)
	httpServer := httptest.NewServer(server.buildRouter())
	t.Cleanup(httpServer.Close)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}
	body, err := json.Marshal(map[string]string{"username": "admin", "password": "password123"})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Post(httpServer.URL+"/auth/token", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	resp, err = client.Get(httpServer.URL + "/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("auth/me status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/auth/logout", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logout status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	resp, err = client.Get(httpServer.URL + "/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("auth/me after logout status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}
