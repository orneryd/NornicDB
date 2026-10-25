package observability

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestR3EscapedFragmentCredentials(t *testing.T) {
	const marker = "synthetic-fragment-tail-846291"
	for _, value := range []string{"&" + marker, "prefix&" + marker + "=suffix", "=" + marker, "%26" + marker, "+" + marker} {
		for _, field := range []string{"access_token", "access%5Ftoken", "X-Amz-Signature"} {
			raw := "https://example.test/path#mode=public%26route%3Dvalue&" + field + "=" + url.QueryEscape(value) + "&view=plain"
			displayed := RedactURL(raw)
			if strings.Contains(displayed, marker) || !strings.Contains(displayed, "mode=public%26route%3Dvalue") || !strings.Contains(displayed, "&view=plain") {
				t.Error("fragment projection leaked a credential or changed public fields; capture withheld")
			}
			credentials := ClientCredentials(raw)
			if len(credentials) != 1 || credentials[0] != value {
				t.Error("fragment extraction lost part of a credential; capture withheld")
			}
			cause := errors.New("provider reflected " + value + " at " + raw)
			err := RedactError(cause, credentials...)
			if strings.Contains(err.Error(), marker) || !errors.Is(err, cause) {
				t.Error("fragment credential escaped provider error projection or changed its cause")
			}
			if strings.Contains(DisplayValue("endpoint", raw), marker) || strings.Contains(RedactText(raw), marker) {
				t.Error("fragment credential escaped a config/text consumer; capture withheld")
			}
		}
	}
	for _, raw := range []string{"https://example.test/#route%3Dpublic%26access_token%3Dliteral", "https://example.test/#mode=public%26route%3Dvalue"} {
		if RedactURL(raw) != raw || len(ClientCredentials(raw)) != 0 {
			t.Error("escaped public fragment acquired credential field syntax")
		}
	}
}
