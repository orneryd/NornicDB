package main

import (
	"net/url"
	"strings"
	"testing"
)

func TestNewOAuthProviderRejectsCredentialQueryInRedirectURI(t *testing.T) {
	credential := "synthetic-callback-value"
	redirectURI := "http://localhost:7474/auth/callback?access_token=" + url.QueryEscape(credential)

	provider, err := NewOAuthProvider("test-client", "synthetic-client-secret", defaultIssuer, redirectURI)
	if provider != nil || err == nil {
		t.Fatal("accepted a credential-bearing callback URI")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatal("callback validation error disclosed the supplied value")
	}
}
