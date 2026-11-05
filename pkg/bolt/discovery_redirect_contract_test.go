package bolt

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestDiscoveryPreservesPublicRedirectURI(t *testing.T) {
	for _, callback := range []string{
		"https://nornic.example.test/callback?state=fixed-state&code=public-code&key=routing-key",
		"https://nornic.example.test/callback?api_key=public-route&state=one&state=two&path=%2Fteam%2Fa+b#screen",
	} {
		cfg := fullyConfiguredOAuth()
		cfg.CallbackURL = callback
		raw, err := buildDiscoveryResponse(cfg, time.Time{})
		if err != nil {
			t.Fatal("discovery failed; details withheld")
		}
		raw = readDiscoveryHTTPBody(t, raw)
		var response discoveryBody
		if err := json.Unmarshal(raw, &response); err != nil || len(response.Providers) != 1 {
			t.Fatal("discovery response missing")
		}
		if response.Providers[0].RedirectURI != cfg.CallbackURL {
			t.Error("discovery redirect_uri differs from the OAuth flow's configured URI")
		}
		if bytes.Contains(raw, []byte(cfg.ClientSecret)) {
			t.Error("discovery disclosed the OAuth client secret; capture withheld")
		}
	}
}
