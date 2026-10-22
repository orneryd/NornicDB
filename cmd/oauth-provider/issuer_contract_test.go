package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestR3ProviderRejectsCredentialIssuer(t *testing.T) {
	const credential = "synthetic-provider-issuer-846291"
	for _, issuer := range []string{"https://user:" + credential + "@idp.example", "https://idp.example?api_key=" + credential, "https://idp.example#access_token=%26" + credential} {
		provider, err := NewOAuthProvider("public-client", "synthetic-client-value", issuer, testRedirectURI)
		if err == nil || provider != nil {
			t.Error("provider accepted a credential-bearing issuer")
		} else if strings.Contains(err.Error(), credential) {
			t.Error("provider validation disclosed credential; capture withheld")
		}
	}
}

func TestR3ProviderDiscoveryEndpoints(t *testing.T) {
	const issuer = "https://idp.example/realm%2Fpublic"
	provider, err := NewOAuthProvider("public-client", "synthetic-client-value", issuer, testRedirectURI)
	if err != nil {
		t.Fatal("public issuer rejected")
	}
	response := httptest.NewRecorder()
	provider.handleDiscovery(response, httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil))
	var body map[string]any
	if json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatal("discovery missing")
	}
	for field, suffix := range map[string]string{"issuer": "", "authorization_endpoint": "/oauth2/v1/authorize", "token_endpoint": "/oauth2/v1/token", "userinfo_endpoint": "/oauth2/v1/userinfo"} {
		if body[field] != issuer+suffix {
			t.Error("provider discovery changed a public protocol endpoint")
		}
	}
}
