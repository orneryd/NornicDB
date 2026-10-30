package bolt

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
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
		return fmt.Errorf("discovery: %s is not a valid URL: %w", field, observability.RedactError(err, observability.ClientCredentials(s)...))
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("discovery: %s must have a scheme and host (got %q)", field, observability.RedactURL(s))
	}
	return nil
}

// buildDiscoveryResponse returns the full pre-encoded HTTP/1.1 response
// (status line + 5 headers + body + CRLFs). The Date header is rebuilt
// each call; production callers should cache the slice and refresh it on
// a 1s ticker. Configured OAuth URLs are validated before any response is emitted.
// RedirectURI must match CallbackURL exactly for the actual OAuth flow.
func buildDiscoveryResponse(oauthCfg *auth.OAuthConfig, now time.Time) ([]byte, error) {
	var body []byte
	if oauthCfg != nil && oauthCfg.IsConfigured() {
		if err := security.ValidateURL(oauthCfg.Issuer, security.URLPolicy{SchemePolicy: security.URLAbsoluteScheme, MaxLength: -1, HostPolicy: security.URLAnyHost, RejectCredentials: true}); err != nil {
			return nil, fmt.Errorf("invalid OAuth issuer: %w", err)
		}
		if err := validateDiscoveryURL("CallbackURL", oauthCfg.CallbackURL); err != nil {
			return nil, err
		}

		issuer := oauthCfg.Issuer

		payload := discoveryBody{
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
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
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

	out := make([]byte, 0, util.SafePreallocSum(b.Len(), bodyLen))
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
