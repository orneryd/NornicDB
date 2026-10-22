package observability

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

var errSyntheticTransport = errors.New("synthetic transport sentinel")

func TestRedactClientErrorRebuildsCredentialedURLChains(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "client", "credential"}, "-")
	configured := "https://user:pass@example.test/proxy?api_key=" + credential + "&region=public"
	original := fmt.Errorf("provider SDK: %w", &url.Error{
		Op:  "Post",
		URL: configured + "&api_key=second",
		Err: fmt.Errorf("dial failed: %w", errSyntheticTransport),
	})

	safe := RedactClientError(original, configured)
	if safe == nil {
		t.Fatal("client error was lost")
	}
	if strings.Contains(safe.Error(), credential) || strings.Contains(safe.Error(), "user:pass") {
		t.Fatal("client error retained configured credentials")
	}
	if !strings.Contains(safe.Error(), RedactedPlaceholder()) {
		t.Fatal("client error did not retain the redaction marker")
	}
	if !errors.Is(safe, errSyntheticTransport) {
		t.Fatal("client error lost its transport sentinel")
	}
	var requestError *url.Error
	if !errors.As(safe, &requestError) {
		t.Fatal("client error lost its URL classification")
	}
	if strings.Contains(requestError.URL, credential) || strings.Contains(requestError.URL, "user:pass") {
		t.Fatal("errors.As recovered a credential-bearing URL error")
	}
}

func TestNormalizeRequestBaseURLPreservesRawCredentialQuery(t *testing.T) {
	rawQuery := "api_key=first%2Bvalue&region=eu&api_key=second%20value"
	normalized, err := NormalizeRequestBaseURL("https://user:pass@example.test/proxy///?" + rawQuery + "#state=public")
	if err != nil {
		t.Fatal("base URL normalization failed")
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		t.Fatal("normalized URL could not be parsed")
	}
	if parsed.EscapedPath() != "/proxy" {
		t.Fatal("base URL path was not normalized")
	}
	if parsed.RawQuery != rawQuery {
		t.Fatal("base URL normalization changed the raw query")
	}
	if parsed.User == nil || parsed.User.String() != "user:pass" {
		t.Fatal("base URL normalization changed userinfo")
	}
	if parsed.EscapedFragment() != "state=public" {
		t.Fatal("base URL normalization changed the fragment")
	}
}
