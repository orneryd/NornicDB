package main

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestCredentialRegisteredCallbackErrorPublic(t *testing.T) {
	const secret = "synthetic/oauth-token-871429"
	endpoint := "https://" + url.PathEscape(secret) + "@localhost/%zz?api_key=" + url.QueryEscape(secret)
	provider, err := NewOAuthProvider("public-client", "synthetic-client-value", "http://localhost:8888", endpoint)
	if provider != nil || err == nil {
		t.Fatal("malformed registered callback accepted")
	}
	var cause *url.Error
	if !errors.As(err, &cause) {
		t.Fatal("URL parse cause was lost")
	}
	for _, value := range []string{secret, url.PathEscape(secret), url.QueryEscape(secret)} {
		if strings.Contains(err.Error(), value) {
			t.Error("registered callback error contains configured credential; capture withheld")
		}
	}
}
