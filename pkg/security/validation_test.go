// Package security provides security validation tests.
package security

import (
	"net"
	"strings"
	"testing"
)

// Token Validation Tests

func TestValidateToken_Valid(t *testing.T) {
	validTokens := []string{
		"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSM",
		"ya29.a0AfH6SMBx",
		"abc123-_~+/=",
		strings.Repeat("a", 8192), // Max length
	}

	for _, token := range validTokens {
		if err := ValidateToken(token); err != nil {
			t.Errorf("ValidateToken(%q) expected valid, got error: %v", token, err)
		}
	}
}

func TestValidateToken_InjectionAttacks(t *testing.T) {
	attacks := map[string]string{
		"CRLF injection":      "token\r\nX-Evil: header",
		"Newline injection":   "token\nX-Evil: header",
		"HTML injection":      "<script>alert('xss')</script>",
		"JavaScript protocol": "javascript:alert('xss')",
		"Data URI":            "data:text/html,<script>alert('xss')</script>",
		"File protocol":       "file:///etc/passwd",
		"Null byte":           "token\x00evil",
		"Too long":            strings.Repeat("a", 8193),
		"Empty":               "",
		"Semicolon":           "token;rm -rf /",
	}

	for name, token := range attacks {
		t.Run(name, func(t *testing.T) {
			if err := ValidateToken(token); err == nil {
				t.Errorf("ValidateToken() expected error for attack %q, got nil", name)
			}
		})
	}
}

// URL Validation Tests - SSRF Prevention

func TestValidateURL_Valid(t *testing.T) {
	tests := []struct {
		url    string
		policy URLPolicy
	}{
		{"https://oauth.example.com/userinfo", URLPolicy{}},
		{"https://oauth.example.com:8443/api", URLPolicy{}},
		{"http://localhost:8888/api", URLPolicy{AllowHTTP: true, HostPolicy: URLDevelopmentHost}},
		{"https://8.8.8.8/api", URLPolicy{}},
		{"https://user:synthetic%40secret@example.com/api?api_key=synthetic", URLPolicy{}},
	}

	for _, tt := range tests {
		if err := ValidateURL(tt.url, tt.policy); err != nil {
			t.Errorf("ValidateURL(%q) expected valid, got: %v", tt.url, err)
		}
	}
}

func TestValidateURL_SSRF_PrivateIPs(t *testing.T) {
	privateIPs := []string{
		"https://10.0.0.1/api",
		"https://172.16.0.1/api",
		"https://192.168.1.1/api",
		"https://169.254.169.254/latest/meta-data/", // AWS metadata
		"https://127.0.0.1/api",
	}

	for _, url := range privateIPs {
		if err := ValidateURL(url, URLPolicy{}); err == nil {
			t.Errorf("ValidateURL(%q) expected SSRF block, got nil", url)
		}
	}
}

func TestValidateURL_ProtocolSmuggling(t *testing.T) {
	protocols := map[string]string{
		"file":   "file:///etc/passwd",
		"ftp":    "ftp://example.com/data",
		"gopher": "gopher://internal:25/_MAIL",
		"dict":   "dict://internal:11211/stats",
	}

	for name, url := range protocols {
		t.Run(name, func(t *testing.T) {
			if err := ValidateURL(url, URLPolicy{}); err != ErrURLInvalidProtocol {
				t.Errorf("ValidateURL() expected protocol error for %s, got: %v", name, err)
			}
		})
	}
}

func TestValidateURL_HTTPInProduction(t *testing.T) {
	httpURL := "http://oauth.example.com/api"

	// Should fail without allowHTTP
	if err := ValidateURL(httpURL, URLPolicy{}); err != ErrURLHTTPNotAllowed {
		t.Errorf("Expected HTTP block in production, got: %v", err)
	}

	// Should succeed with allowHTTP
	if err := ValidateURL(httpURL, URLPolicy{AllowHTTP: true}); err != nil {
		t.Errorf("Expected HTTP allowed with flag, got: %v", err)
	}
}

func TestValidateURL_CloudMetadata(t *testing.T) {
	attacks := []string{
		"http://169.254.169.254/latest/meta-data/iam/security-credentials/",
		"http://169.254.169.254/metadata/instance?api-version=2021-02-01",
		"http://169.254.169.254/computeMetadata/v1/instance/",
	}

	for _, url := range attacks {
		if err := ValidateURL(url, URLPolicy{}); err == nil {
			t.Errorf("ValidateURL() failed to block metadata service: %s", url)
		}
	}
}

// Header Validation Tests

func TestValidateHeaderValue(t *testing.T) {
	valid := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"Bearer token-value-here",
		"application/json; charset=utf-8",
	}

	for _, hdr := range valid {
		if err := ValidateHeaderValue(hdr); err != nil {
			t.Errorf("ValidateHeaderValue(%q) expected valid, got: %v", hdr, err)
		}
	}

	invalid := []string{
		"value\r\nX-Injected: evil",
		"value\nX-Injected: evil",
		"value\x00injected",
		strings.Repeat("a", 5000),
	}

	for _, hdr := range invalid {
		if err := ValidateHeaderValue(hdr); err == nil {
			t.Errorf("ValidateHeaderValue(%q) expected error, got nil", hdr)
		}
	}
}

// String Sanitization Tests

func TestSanitizeString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello world", "hello world"},
		{"hello\x00world", "helloworld"},
		{"hello\x01\x02world", "helloworld"},
		{"  hello world  ", "hello world"},
	}

	for _, tt := range tests {
		result := SanitizeString(tt.input)
		if result != tt.expected {
			t.Errorf("SanitizeString(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

// Private IP Detection Tests

func TestIsPrivateIP(t *testing.T) {
	privateTests := []string{
		"10.0.0.1",
		"172.16.0.1",
		"192.168.1.1",
		"169.254.169.254",
		"127.0.0.1",
	}

	for _, ipStr := range privateTests {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			t.Fatalf("Failed to parse IP: %s", ipStr)
		}
		if !isPrivateIP(ip) {
			t.Errorf("isPrivateIP(%s) = false, expected true", ipStr)
		}
	}

	publicTests := []string{
		"8.8.8.8",
		"1.1.1.1",
		"93.184.216.34",
	}

	for _, ipStr := range publicTests {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			t.Fatalf("Failed to parse IP: %s", ipStr)
		}
		if isPrivateIP(ip) {
			t.Errorf("isPrivateIP(%s) = true, expected false", ipStr)
		}
	}
}

// Benchmark Tests

func BenchmarkValidateToken(b *testing.B) {
	token := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateToken(token)
	}
}

func BenchmarkValidateURL(b *testing.B) {
	url := "https://oauth.example.com/userinfo"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateURL(url, URLPolicy{})
	}
}
