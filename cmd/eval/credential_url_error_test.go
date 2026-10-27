package main

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type trackingCredentialTransport bool

func (t *trackingCredentialTransport) RoundTrip(*http.Request) (*http.Response, error) {
	*t = true
	return nil, http.ErrHandlerTimeout
}

func TestCredentialConfiguredURLErrors(t *testing.T) {
	const endpoint = "https://synthetic-user:synthetic-password-871429@example.invalid/%zz?api_key=synthetic-query-317"
	for _, operation := range []func(string) error{validateURL, checkHealth, createSampleData} {
		err := operation(endpoint)
		if err == nil {
			t.Fatal("malformed configured endpoint accepted")
		}
		for _, secret := range []string{"synthetic-user", "synthetic-password-871429", "synthetic-query-317"} {
			if strings.Contains(err.Error(), secret) {
				t.Error("configured endpoint error contains credentials; capture withheld")
			}
		}
	}
}

func TestHTTPSearcherRejectsCredentialURLBeforeRequest(t *testing.T) {
	endpoint := (&url.URL{
		Scheme: "http",
		User:   url.UserPassword("synthetic-user", "synthetic-password"),
		Host:   "localhost:7474",
	}).String()
	transport := trackingCredentialTransport(false)
	searcher := &HTTPSearcher{
		url:    endpoint,
		client: &http.Client{Transport: &transport},
	}

	_, err := searcher.Search(context.Background(), "synthetic query")
	if err == nil {
		t.Fatal("accepted a credential-bearing search URL")
	}
	if transport {
		t.Fatal("sent a request before rejecting the URL")
	}
	for _, value := range []string{"synthetic-user", "synthetic-password"} {
		if strings.Contains(err.Error(), value) {
			t.Fatal("search URL validation error disclosed a credential")
		}
	}
}
