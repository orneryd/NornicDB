package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSanitizeHost(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
	}{
		{name: "host port", host: "localhost:8080", want: "localhost:8080"},
		{name: "subdomain", host: "api.example.com", want: "api.example.com"},
		{name: "ipv6 brackets removed", host: "[::1]:8080", want: "::1:8080"},
		{name: "script characters stripped", host: `evil.com\"><script>alert(1)</script>`, want: "evil.comscriptalert1script"},
		{name: "crlf stripped", host: "example.com\r\nX-Evil: yes", want: "example.comX-Evil:yes"},
		{name: "empty fallback", host: "<>(){}[]&;'\"", want: "localhost:8080"},
		{name: "path separators stripped", host: "example.com/openapi.yaml", want: "example.comopenapi.yaml"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeHost(tt.host); got != tt.want {
				t.Fatalf("sanitizeHost(%q) = %q, want %q", tt.host, got, tt.want)
			}
		})
	}
}

func TestServeSwaggerUIUsesSanitizedHTTPHostAndEscapesSpecURL(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.invalid/swagger", nil)
	req.Host = `api.example.com\"><script>alert(1)</script>`
	rec := httptest.NewRecorder()

	serveSwaggerUI(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want text/html; charset=utf-8", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `url: "http://api.example.comscriptalert1script/openapi.yaml"`) {
		t.Fatalf("body missing sanitized spec URL: %s", body)
	}
	if strings.Contains(body, `<script>alert(1)</script>/openapi.yaml`) || strings.Contains(body, `\"><script>`) {
		t.Fatalf("body contains unsanitized host content: %s", body)
	}
	if !strings.Contains(body, "SwaggerUIBundle") || !strings.Contains(body, "NornicDB API Documentation") {
		t.Fatalf("body missing expected Swagger UI content")
	}
}

func TestServeSwaggerUIUsesHTTPSForTLSAndDefaultHost(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://example.invalid/swagger", nil)
	req.Host = ""
	rec := httptest.NewRecorder()

	serveSwaggerUI(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `url: "https://localhost:8080/openapi.yaml"`) {
		t.Fatalf("body missing https default spec URL: %s", body)
	}
}
