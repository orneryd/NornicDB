package auth

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

type credentialedOAuthFailureTransport struct{}

func (credentialedOAuthFailureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var body []byte
	if request.Body != nil {
		body, _ = io.ReadAll(request.Body)
	}
	return nil, fmt.Errorf("synthetic OAuth failure for %s %s %s", request.URL.String(), request.Header.Get("Authorization"), string(body))
}

func TestCredentialedOAuthClientErrorsAreRedacted(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "oauth", "credential"}, "-")
	t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	t.Setenv("NORNICDB_OAUTH_ISSUER", "https://provider.example.test/proxy")
	t.Setenv("NORNICDB_OAUTH_CLIENT_ID", "public-client")
	t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "client-"+credential)
	t.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "https://app.example.test/callback")

	previous := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: credentialedOAuthFailureTransport{}}
	t.Cleanup(func() { http.DefaultClient = previous })

	manager := NewOAuthManager(nil)
	_, exchangeErr := manager.ExchangeCode("code-" + credential)
	_, userInfoErr := manager.GetUserInfo("access-" + credential)
	refreshErr := manager.RefreshOAuthToken(&User{}, "refresh-"+credential)
	for _, err := range []error{exchangeErr, userInfoErr, refreshErr} {
		if err == nil {
			t.Fatal("OAuth transport failure was not returned")
		}
		if strings.Contains(err.Error(), credential) {
			t.Fatal("OAuth error exposed the synthetic credential")
		}
		if !strings.Contains(err.Error(), observability.RedactedPlaceholder()) {
			t.Fatal("OAuth error did not retain the redaction marker")
		}
	}
}
