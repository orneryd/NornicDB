package auth

import (
	"fmt"
	"net/url"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// ValidateOAuthIssuer checks public issuer configuration before it is used by
// OAuth requests or advertised to clients. Credentials belong in the dedicated
// client secret field; an issuer must never need a diagnostic URL projection.
func ValidateOAuthIssuer(issuer string) error {
	u, err := url.Parse(issuer)
	if err != nil {
		return fmt.Errorf("invalid OAuth issuer: %w", observability.RedactError(err, observability.ClientCredentials(issuer)...))
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("OAuth issuer must have a scheme and host")
	}
	if u.User != nil || len(observability.ClientCredentials(issuer)) != 0 {
		return fmt.Errorf("OAuth issuer must not contain credentials")
	}
	return nil
}
