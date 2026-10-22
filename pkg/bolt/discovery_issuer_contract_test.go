package bolt

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestR3DiscoveryRejectsCredentialIssuer(t *testing.T) {
	const credential = "synthetic-discovery-issuer-846291"
	for _, issuer := range []string{"https://user:" + credential + "@idp.example", "https://" + credential + "@idp.example", "https://idp.example?api_key=" + credential, "https://idp.example#access_token=%26" + credential} {
		cfg := fullyConfiguredOAuth()
		cfg.Issuer = issuer
		body, err := buildDiscoveryBody(cfg)
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

func TestR3DiscoveryPreservesEndpoints(t *testing.T) {
	cfg := fullyConfiguredOAuth()
	cfg.Issuer = "https://idp.example/realm%2Fpublic"
	cfg.CallbackURL = "https://app.example/callback?state=public&code=route&key=public"
	raw, err := buildDiscoveryBody(cfg)
	if err != nil {
		t.Fatal("public issuer was rejected")
	}
	var body discoveryBody
	if json.Unmarshal(raw, &body) != nil || len(body.Providers) != 1 {
		t.Fatal("discovery missing")
	}
	p := body.Providers[0]
	if p.AuthEndpoint != cfg.Issuer+"/oauth2/v1/authorize" || p.TokenEndpoint != cfg.Issuer+"/oauth2/v1/token" || p.WellKnownDiscoveryURI != cfg.Issuer+"/.well-known/openid-configuration" || p.RedirectURI != cfg.CallbackURL {
		t.Error("discovery rewrote public protocol URLs")
	}
}
