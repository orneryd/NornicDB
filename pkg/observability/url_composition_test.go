package observability

import (
	"net/url"
	"testing"
)

func TestComposeRequestURLAppendPreservesConfiguredURL(t *testing.T) {
	base := "https://client-user:client-password@example.invalid/root%2Ftenant?api_key=first%2Bvalue&region=eu&api_key=second%20value"
	got, err := ComposeRequestURL(base, "/v1/embed?format=a%2Bb&format=c", AppendRequestRoute)
	if err != nil {
		t.Fatal("append route failed")
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal("composed URL did not parse")
	}
	if parsed.EscapedPath() != "/root%2Ftenant/v1/embed" {
		t.Error("escaped base path was not preserved")
	}
	if parsed.RawQuery != "api_key=first%2Bvalue&region=eu&api_key=second%20value&format=a%2Bb&format=c" {
		t.Error("ordered raw queries were not preserved")
	}
	password, hasPassword := parsed.User.Password()
	if parsed.User.Username() != "client-user" || !hasPassword || password != "client-password" {
		t.Error("configured userinfo was not preserved")
	}
}

func TestComposeRequestURLFollowupMergesConfiguredAuthentication(t *testing.T) {
	base := "https://client-user:client-password@configured.invalid/proxy?api_key=first%2Bvalue&region=eu&api_key=second%20value"
	got, err := ComposeRequestURL(base, "https://server.invalid/db/graph/tx/7/commit?bookmark=a%2Bb&bookmark=c", MergeRequestFollowup)
	if err != nil {
		t.Fatal("follow-up merge failed")
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal("merged follow-up URL did not parse")
	}
	if parsed.Host != "server.invalid" || parsed.EscapedPath() != "/db/graph/tx/7/commit" {
		t.Error("server follow-up location was not adopted")
	}
	if parsed.RawQuery != "api_key=first%2Bvalue&region=eu&api_key=second%20value&bookmark=a%2Bb&bookmark=c" {
		t.Error("configured and follow-up queries were not merged in order")
	}
	password, hasPassword := parsed.User.Password()
	if parsed.User.Username() != "client-user" || !hasPassword || password != "client-password" {
		t.Error("configured follow-up userinfo was not retained")
	}
}

func TestComposeRequestURLRejectsAbsoluteAppendRoute(t *testing.T) {
	if _, err := ComposeRequestURL("https://example.invalid/base", "https://other.invalid/route", AppendRequestRoute); err == nil {
		t.Fatal("absolute append route was accepted")
	}
}
