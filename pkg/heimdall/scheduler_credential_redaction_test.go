package heimdall

import (
	"errors"
	"strings"
	"testing"
)

func TestNewManagerRedactsProviderInitializationErrors(t *testing.T) {
	const provider = "credential-redaction-test"
	apiURL := "https://synthetic-user:synthetic-pass@example.invalid/v1"
	apiKey := "synthetic-api-key"

	previous, existed := heimdallProviderFactories[provider]
	RegisterHeimdallProvider(provider, func(cfg Config) (Generator, error) {
		return nil, errors.New("provider initialization failed: " + cfg.APIURL + " key " + cfg.APIKey)
	})
	t.Cleanup(func() {
		if existed {
			heimdallProviderFactories[provider] = previous
		} else {
			delete(heimdallProviderFactories, provider)
		}
	})

	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Provider = provider
	cfg.APIURL = apiURL
	cfg.APIKey = apiKey

	_, err := NewManager(cfg)
	if err == nil {
		t.Fatal("expected the test provider to fail initialization")
	}
	for _, value := range []string{"synthetic-user", "synthetic-pass", apiKey} {
		if strings.Contains(err.Error(), value) {
			t.Fatal("provider initialization error exposed a credential")
		}
	}
}
