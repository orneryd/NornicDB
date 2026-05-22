package bolt

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// fullyConfiguredOAuth returns a test OAuthConfig with all four required
// fields populated so OAuthConfig.IsConfigured() returns true.
func fullyConfiguredOAuth() *auth.OAuthConfig {
	return &auth.OAuthConfig{
		Provider:     "oauth",
		Issuer:       "https://idp.example.com",
		ClientID:     "nornic-client",
		ClientSecret: "supersecret-not-for-wire",
		CallbackURL:  "https://nornic.example.com/auth/oauth/callback",
	}
}

// TestBuildDiscoveryBody_Empty (D-Empty) — nil cfg or unconfigured cfg
// returns (nil, nil).
func TestBuildDiscoveryBody_Empty(t *testing.T) {
	body, err := buildDiscoveryBody(nil)
	if err != nil {
		t.Fatalf("nil cfg: unexpected error: %v", err)
	}
	if body != nil {
		t.Fatalf("nil cfg: expected nil body, got %q", string(body))
	}

	empty := &auth.OAuthConfig{}
	if empty.IsConfigured() {
		t.Fatalf("test premise broken: empty OAuthConfig should not be IsConfigured()")
	}
	body, err = buildDiscoveryBody(empty)
	if err != nil {
		t.Fatalf("empty cfg: unexpected error: %v", err)
	}
	if body != nil {
		t.Fatalf("empty cfg: expected nil body, got %q", string(body))
	}
}

// TestBuildDiscoveryBody_OAuth (D-OAuth) — fully configured cfg returns
// non-nil bytes; provider fields match the spec exactly.
func TestBuildDiscoveryBody_OAuth(t *testing.T) {
	cfg := fullyConfiguredOAuth()
	raw, err := buildDiscoveryBody(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if raw == nil {
		t.Fatal("expected non-nil body for configured OAuth")
	}

	var got discoveryBody
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}

	if got.DefaultProvider != "nornic-oauth" {
		t.Errorf("DefaultProvider = %q, want %q", got.DefaultProvider, "nornic-oauth")
	}
	if len(got.Providers) != 1 {
		t.Fatalf("len(Providers) = %d, want 1", len(got.Providers))
	}

	p := got.Providers[0]
	if p.ID != "nornic-oauth" {
		t.Errorf("ID = %q, want %q", p.ID, "nornic-oauth")
	}
	if p.Name != "OAuth" {
		t.Errorf("Name = %q, want %q", p.Name, "OAuth")
	}
	if p.AuthProvider != "oauth" {
		t.Errorf("AuthProvider = %q, want %q", p.AuthProvider, "oauth")
	}
	if p.AuthEndpoint != "https://idp.example.com/oauth2/v1/authorize" {
		t.Errorf("AuthEndpoint = %q", p.AuthEndpoint)
	}
	if p.TokenEndpoint != "https://idp.example.com/oauth2/v1/token" {
		t.Errorf("TokenEndpoint = %q", p.TokenEndpoint)
	}
	if p.AuthFlow != "pkce" {
		t.Errorf("AuthFlow = %q, want %q", p.AuthFlow, "pkce")
	}
	if p.ClientID != cfg.ClientID {
		t.Errorf("ClientID = %q, want %q", p.ClientID, cfg.ClientID)
	}
	if p.RedirectURI != cfg.CallbackURL {
		t.Errorf("RedirectURI = %q, want %q", p.RedirectURI, cfg.CallbackURL)
	}
	wantScopes := []string{"openid", "profile", "email"}
	if len(p.Scopes) != len(wantScopes) {
		t.Fatalf("Scopes len = %d, want %d", len(p.Scopes), len(wantScopes))
	}
	for i, s := range wantScopes {
		if p.Scopes[i] != s {
			t.Errorf("Scopes[%d] = %q, want %q", i, p.Scopes[i], s)
		}
	}
	if p.Audience != "nornic" {
		t.Errorf("Audience = %q, want %q", p.Audience, "nornic")
	}
	if p.WellKnownDiscoveryURI != "https://idp.example.com/.well-known/openid-configuration" {
		t.Errorf("WellKnownDiscoveryURI = %q", p.WellKnownDiscoveryURI)
	}
	if p.TokenTypePrincipal != "id_token" {
		t.Errorf("TokenTypePrincipal = %q, want %q", p.TokenTypePrincipal, "id_token")
	}
	if p.TokenTypeAuthentication != "id_token" {
		t.Errorf("TokenTypeAuthentication = %q, want %q", p.TokenTypeAuthentication, "id_token")
	}
}

// TestBuildDiscoveryBody_OAuthPartial (D-OAuthPartial) — missing
// ClientSecret/CallbackURL → IsConfigured() false → (nil, nil).
func TestBuildDiscoveryBody_OAuthPartial(t *testing.T) {
	cfg := &auth.OAuthConfig{
		Provider: "oauth",
		Issuer:   "https://idp.example.com",
		ClientID: "nornic-client",
		// no ClientSecret, no CallbackURL
	}
	if cfg.IsConfigured() {
		t.Fatalf("partial cfg should not report IsConfigured()")
	}

	body, err := buildDiscoveryBody(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body != nil {
		t.Fatalf("expected nil body for partial cfg, got %q", string(body))
	}
}

// TestBuildDiscoveryBody_MalformedIssuer (D-MalformedIssuer) — Issuer is
// not a syntactically valid URL → error.
func TestBuildDiscoveryBody_MalformedIssuer(t *testing.T) {
	cfg := fullyConfiguredOAuth()
	cfg.Issuer = "::not a url"

	if !cfg.IsConfigured() {
		t.Fatalf("test premise broken: cfg should still be IsConfigured() despite malformed Issuer")
	}

	body, err := buildDiscoveryBody(cfg)
	if err == nil {
		t.Fatalf("expected error for malformed Issuer, got body=%q", string(body))
	}
	if body != nil {
		t.Errorf("expected nil body when error is returned, got %q", string(body))
	}
}

// TestBuildDiscoveryBody_NoSecretLeak (D-NoSecretLeak) — ClientSecret must
// never appear in the marshalled body, and the JSON must not contain a
// "client_secret" field.
func TestBuildDiscoveryBody_NoSecretLeak(t *testing.T) {
	cfg := fullyConfiguredOAuth()
	cfg.ClientSecret = "TOPSECRET-DO-NOT-LEAK"

	body, err := buildDiscoveryBody(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body == nil {
		t.Fatal("expected non-nil body for configured OAuth")
	}

	if bytes.Contains(body, []byte("TOPSECRET")) {
		t.Errorf("body leaks ClientSecret substring: %s", string(body))
	}
	if bytes.Contains(body, []byte("client_secret")) {
		t.Errorf("body contains forbidden \"client_secret\" field: %s", string(body))
	}
}

// TestBuildDiscoveryResponse_HeadersAndStatus (D1) — empty-body case:
// status line, all five required headers, Content-Length: 0, terminator,
// no body bytes.
func TestBuildDiscoveryResponse_HeadersAndStatus(t *testing.T) {
	now := time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC)
	resp, err := buildDiscoveryResponse(nil, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !bytes.HasPrefix(resp, []byte("HTTP/1.1 200 OK\r\n")) {
		t.Fatalf("response does not start with status line; got %q", string(resp[:min(len(resp), 32)]))
	}

	requiredHeaders := []string{
		"Content-Type: application/json\r\n",
		"Access-Control-Allow-Origin: *\r\n",
		"Vary: Accept\r\n",
		"Content-Length: 0\r\n",
		"Date: ",
	}
	for _, h := range requiredHeaders {
		if !bytes.Contains(resp, []byte(h)) {
			t.Errorf("response missing required header fragment %q\nfull response:\n%s", h, string(resp))
		}
	}

	idx := bytes.Index(resp, []byte("\r\n\r\n"))
	if idx < 0 {
		t.Fatalf("response missing header/body terminator \\r\\n\\r\\n: %q", string(resp))
	}
	bodyBytes := resp[idx+4:]
	if len(bodyBytes) != 0 {
		t.Errorf("expected zero body bytes after terminator, got %d (%q)", len(bodyBytes), string(bodyBytes))
	}
}

// TestBuildDiscoveryResponse_OAuthBody — Content-Length matches body
// length, body is valid JSON.
func TestBuildDiscoveryResponse_OAuthBody(t *testing.T) {
	cfg := fullyConfiguredOAuth()
	now := time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC)

	resp, err := buildDiscoveryResponse(cfg, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	idx := bytes.Index(resp, []byte("\r\n\r\n"))
	if idx < 0 {
		t.Fatalf("response missing header/body terminator: %q", string(resp))
	}
	headers := resp[:idx]
	body := resp[idx+4:]

	if len(body) == 0 {
		t.Fatal("expected non-empty body for configured OAuth")
	}

	// Content-Length matches body length.
	clTag := []byte("Content-Length: ")
	clIdx := bytes.Index(headers, clTag)
	if clIdx < 0 {
		t.Fatalf("response missing Content-Length header: %q", string(headers))
	}
	clLine := headers[clIdx+len(clTag):]
	eol := bytes.Index(clLine, []byte("\r\n"))
	if eol < 0 {
		t.Fatalf("Content-Length header has no terminator: %q", string(clLine))
	}
	clValue := string(clLine[:eol])
	wantCL := len(body)
	gotCL := 0
	if _, err := jsonAtoi(clValue, &gotCL); err != nil {
		t.Fatalf("Content-Length not numeric: %q (%v)", clValue, err)
	}
	if gotCL != wantCL {
		t.Errorf("Content-Length = %d, want %d", gotCL, wantCL)
	}

	// Body is valid JSON matching discoveryBody.
	var parsed discoveryBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Errorf("body is not valid JSON: %v\nbody=%q", err, string(body))
	}
	if parsed.DefaultProvider != "nornic-oauth" {
		t.Errorf("parsed DefaultProvider = %q", parsed.DefaultProvider)
	}
}

// jsonAtoi is a tiny helper for parsing the Content-Length value without
// pulling in strconv at the top of the test file's imports.
func jsonAtoi(s string, out *int) (int, error) {
	return *out, json.Unmarshal([]byte(s), out)
}

// TestBuildDiscoveryResponse_DateHeader — fixed time produces a
// deterministic RFC1123 GMT Date header.
func TestBuildDiscoveryResponse_DateHeader(t *testing.T) {
	now := time.Date(2026, 5, 22, 14, 30, 45, 0, time.UTC)
	want := "Date: " + now.UTC().Format(http.TimeFormat) + "\r\n"

	resp, err := buildDiscoveryResponse(nil, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Contains(resp, []byte(want)) {
		t.Errorf("response missing expected Date header line %q\nfull response:\n%s", want, string(resp))
	}

	// Same time → same response bytes (determinism).
	resp2, err := buildDiscoveryResponse(nil, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(resp, resp2) {
		t.Errorf("expected deterministic output for fixed time; got differing bytes")
	}

	// Non-UTC input is normalized to GMT (.UTC() called internally).
	loc, _ := time.LoadLocation("America/Los_Angeles")
	respLA, err := buildDiscoveryResponse(nil, now.In(loc))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Contains(respLA, []byte(want)) {
		t.Errorf("non-UTC input should still produce GMT Date header %q", want)
	}
}

// TestIsWebSocketUpgrade (D2/D3/D4) — case-insensitive header detection,
// requires all three signals (Upgrade non-empty, Connection contains
// "upgrade", Upgrade contains "websocket").
func TestIsWebSocketUpgrade(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{
			name:    "plain GET no headers",
			headers: nil,
			want:    false,
		},
		{
			name: "lowercase upgrade websocket (D2)",
			headers: map[string]string{
				"Connection": "upgrade",
				"Upgrade":    "websocket",
			},
			want: true,
		},
		{
			name: "uppercase UPGRADE WEBSOCKET (D3)",
			headers: map[string]string{
				"Connection": "UPGRADE",
				"Upgrade":    "WEBSOCKET",
			},
			want: true,
		},
		{
			name: "mixed case Upgrade WebSocket",
			headers: map[string]string{
				"Connection": "Upgrade",
				"Upgrade":    "WebSocket",
			},
			want: true,
		},
		{
			name: "Connection list keep-alive, Upgrade",
			headers: map[string]string{
				"Connection": "keep-alive, Upgrade",
				"Upgrade":    "websocket",
			},
			want: true,
		},
		{
			name: "h2c upgrade is not a websocket upgrade (D4)",
			headers: map[string]string{
				"Connection": "upgrade",
				"Upgrade":    "h2c",
			},
			want: false,
		},
		{
			name: "Upgrade websocket but no Connection header",
			headers: map[string]string{
				"Upgrade": "websocket",
			},
			want: false,
		},
		{
			name: "Connection upgrade but no Upgrade header",
			headers: map[string]string{
				"Connection": "upgrade",
			},
			want: false,
		},
		{
			name: "Connection close, Upgrade websocket — no upgrade signal in Connection",
			headers: map[string]string{
				"Connection": "close",
				"Upgrade":    "websocket",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tt.headers {
				h.Set(k, v)
			}
			got := isWebSocketUpgrade(h)
			if got != tt.want {
				t.Errorf("isWebSocketUpgrade(%v) = %v, want %v", tt.headers, got, tt.want)
			}
		})
	}
}

// min is provided here for builds against Go versions that don't have the
// built-in (the package may target a slightly older toolchain).
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Ensure strings import is used even if the helpers above don't reference it
// directly. (Kept for future header-extraction helpers in this test file.)
var _ = strings.ToLower
