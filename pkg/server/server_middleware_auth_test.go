package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"testing"
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
