package main

import (
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
