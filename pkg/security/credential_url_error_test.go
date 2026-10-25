package security

import (
	"errors"
	"strings"
	"testing"
)

func TestCredentialURLValidationError(t *testing.T) {
	for _, endpoint := range []string{
		"https://synthetic-user:synthetic-password-871429@example.invalid/%zz",
		"https://example.invalid/%zz?api_key=synthetic-query-317",
	} {
		err := ValidateURL(endpoint, URLPolicy{AllowHTTP: true, HostPolicy: URLDevelopmentHost})
		if !errors.Is(err, ErrURLInvalid) {
			t.Fatal("URL validation identity changed")
		}
		for _, secret := range []string{"synthetic-user", "synthetic-password-871429", "synthetic-query-317"} {
			if strings.Contains(err.Error(), secret) {
				t.Error("URL validation reflects a configured credential; capture withheld")
			}
		}
	}
}
