package observability

import (
	"net/url"
	"strings"
	"testing"
)

func TestURLCredentialAliases(t *testing.T) {
	const credential = "synthetic-url-alias-credential-6248913"
	aliases := []string{"access_token", "api.key", "clientSecret", "password", "AWSAccessKeyId", "X-Amz-Credential", "X-Amz-Signature", "X-Goog-Signature", "X-Amz-Security-Token"}
	for _, name := range DefaultRedactKeys {
		if alias, ok := strings.CutPrefix(name, "url_query_"); ok {
			aliases = append(aliases, alias)
		}
	}
	for _, alias := range aliases {
		raw := "https://example.test/path?model=probe&auth_provider=oidc&" + url.QueryEscape(alias) + "=" + credential
		for _, input := range []string{raw, strings.Replace(raw, "?", "#", 1)} {
			display := RedactURL(input)
			if strings.Contains(display, credential) {
				t.Error("URL alias disclosed a credential; value withheld")
			}
			if !strings.Contains(display, "model=probe&auth_provider=oidc") {
				t.Error("URL redaction changed ordinary options")
			}
			if values := ClientCredentials(input); len(values) != 1 || values[0] != credential {
				t.Error("URL adapter lost its credential context")
			}
		}
	}
}
