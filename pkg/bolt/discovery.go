package bolt

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// discoveryProvider mirrors a single SSO provider entry in the Bolt-port
// discovery response, matching the JSON shape Neo4j Enterprise drivers expect.
type discoveryProvider struct {
	ID                      string   `json:"id"`
	Name                    string   `json:"name"`
	AuthProvider            string   `json:"auth_provider"`
	AuthEndpoint            string   `json:"auth_endpoint"`
	TokenEndpoint           string   `json:"token_endpoint"`
	AuthFlow                string   `json:"auth_flow"`
	ClientID                string   `json:"client_id"`
	RedirectURI             string   `json:"redirect_uri"`
	Scopes                  []string `json:"scopes"`
	Audience                string   `json:"audience"`
	WellKnownDiscoveryURI   string   `json:"well_known_discovery_uri"`
	TokenTypePrincipal      string   `json:"token_type_principal"`
	TokenTypeAuthentication string   `json:"token_type_authentication"`
}

// discoveryBody is the JSON payload emitted on the Bolt port for plain
// (non-WebSocket-upgrade) HTTP requests when SSO is configured.
type discoveryBody struct {
	DefaultProvider string              `json:"default_provider,omitempty"`
	Providers       []discoveryProvider `json:"providers,omitempty"`
}

// validateDiscoveryURL ensures s parses as a URL with a non-empty scheme and host.
func validateDiscoveryURL(field, s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("discovery: %s is not a valid URL: %w", field, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("discovery: %s must have a scheme and host (got %q)", field, s)
	}
	return nil
}

// buildDiscoveryBody returns marshalled JSON for the configured-OAuth case
// or nil for the empty-body Community-parity case. Validates Issuer and
// CallbackURL syntax; returns error on malformed URLs.
func buildDiscoveryBody(oauthCfg *auth.OAuthConfig) ([]byte, error) {
	if oauthCfg == nil || !oauthCfg.IsConfigured() {
		return nil, nil
	}

	if err := validateDiscoveryURL("Issuer", oauthCfg.Issuer); err != nil {
		return nil, err
	}
	if err := validateDiscoveryURL("CallbackURL", oauthCfg.CallbackURL); err != nil {
		return nil, err
	}

	issuer := strings.TrimRight(oauthCfg.Issuer, "/")

	body := discoveryBody{
		DefaultProvider: "nornic-oauth",
		Providers: []discoveryProvider{
			{
				ID:                      "nornic-oauth",
				Name:                    "OAuth",
				AuthProvider:            "oauth",
				AuthEndpoint:            issuer + "/oauth2/v1/authorize",
				TokenEndpoint:           issuer + "/oauth2/v1/token",
				AuthFlow:                "pkce",
				ClientID:                oauthCfg.ClientID,
				RedirectURI:             oauthCfg.CallbackURL,
				Scopes:                  []string{"openid", "profile", "email"},
				Audience:                "nornic",
				WellKnownDiscoveryURI:   issuer + "/.well-known/openid-configuration",
				TokenTypePrincipal:      "id_token",
				TokenTypeAuthentication: "id_token",
			},
		},
	}

	return json.Marshal(body)
}

// buildDiscoveryResponse returns the full pre-encoded HTTP/1.1 response
// (status line + 5 headers + body + CRLFs). The Date header is rebuilt
// each call; production callers should cache the slice and refresh it on
// a 1s ticker. Returns error if buildDiscoveryBody errors.
func buildDiscoveryResponse(oauthCfg *auth.OAuthConfig, now time.Time) ([]byte, error) {
	body, err := buildDiscoveryBody(oauthCfg)
	if err != nil {
		return nil, err
	}

	bodyLen := len(body) // nil body => 0
	date := now.UTC().Format(http.TimeFormat)

	var b strings.Builder
	b.WriteString("HTTP/1.1 200 OK\r\n")
	b.WriteString("Content-Type: application/json\r\n")
	b.WriteString("Access-Control-Allow-Origin: *\r\n")
	b.WriteString("Vary: Accept\r\n")
	fmt.Fprintf(&b, "Content-Length: %d\r\n", bodyLen)
	b.WriteString("Date: ")
	b.WriteString(date)
	b.WriteString("\r\n")
	b.WriteString("\r\n")

	out := make([]byte, 0, b.Len()+bodyLen)
	out = append(out, b.String()...)
	if bodyLen > 0 {
		out = append(out, body...)
	}
	return out, nil
}

// isWebSocketUpgrade reports whether the HTTP request headers look like a
// WebSocket upgrade. Mirrors Neo4j's DiscoveryResponseHandler.isWebsocketUpgrade
// (case-insensitive on all three fields).
func isWebSocketUpgrade(h http.Header) bool {
	upgrade := h.Get("Upgrade")
	connection := h.Get("Connection")
	return upgrade != "" &&
		strings.Contains(strings.ToLower(connection), "upgrade") &&
		strings.Contains(strings.ToLower(upgrade), "websocket")
}
