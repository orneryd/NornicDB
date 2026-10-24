package auth

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

type r3IssuerTransport func(*http.Request) (*http.Response, error)

func (f r3IssuerTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestR3OAuthRejectsCredentialIssuer(t *testing.T) {
	const credential = "synthetic-oauth-issuer-846291"
	var calls atomic.Int32
	previous := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: r3IssuerTransport(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader("fixture rejection"))}, nil
	})}
	t.Cleanup(func() { http.DefaultClient = previous })
	for _, issuer := range []string{"https://user:" + credential + "@idp.example", "https://" + credential + "@idp.example", "https://idp.example?api_key=" + credential, "https://idp.example#access_token=%26" + credential} {
		t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
		t.Setenv("NORNICDB_OAUTH_ISSUER", issuer)
		t.Setenv("NORNICDB_OAUTH_CLIENT_ID", "public-client")
		t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "synthetic-client-value")
		t.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "https://app.example/callback")
		manager := NewOAuthManager(nil)
		_, _, authErr := manager.GenerateAuthURL("https://app.example")
		_, exchangeErr := manager.ExchangeCode("synthetic-code")
		_, infoErr := manager.GetUserInfo("synthetic-access-value")
		refreshErr := manager.RefreshOAuthToken(&User{}, "synthetic-refresh-value")
		for _, err := range []error{authErr, exchangeErr, infoErr, refreshErr} {
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "issuer") {
				t.Error("OAuth operation did not reject its issuer at validation")
			} else if strings.Contains(err.Error(), credential) {
				t.Error("OAuth issuer validation disclosed credential; capture withheld")
			}
		}
	}
	if calls.Load() != 0 {
		t.Error("OAuth sent requests before rejecting credential-bearing issuers")
	}
}
