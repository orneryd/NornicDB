package bolt

import (
	"strings"
	"testing"
	"time"
)

func TestR3DiscoveryRejectsCredentialIssuer(t *testing.T) {
	const credential = "synthetic-discovery-issuer-846291"
	for _, issuer := range []string{"https://user:" + credential + "@idp.example", "https://" + credential + "@idp.example", "https://idp.example?api_key=" + credential, "https://idp.example#access_token=%26" + credential} {
		cfg := fullyConfiguredOAuth()
		cfg.Issuer = issuer
		body, err := buildDiscoveryResponse(cfg, time.Time{})
		if err == nil || body != nil {
			t.Error("credential-bearing issuer was advertised instead of rejected; capture withheld")
		} else if strings.Contains(err.Error(), credential) {
			t.Error("issuer validation disclosed credential; capture withheld")
		}
		if cfg.Issuer != issuer {
			t.Error("issuer validation mutated configuration")
		}
	}
}
