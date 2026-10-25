package security

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURLPolicyCredentialFreeAbsoluteEndpoints(t *testing.T) {
	policy := URLPolicy{SchemePolicy: URLAbsoluteScheme, MaxLength: -1, HostPolicy: URLAnyHost, RejectCredentials: true}
	for _, endpoint := range []string{"https://idp.example", "http://localhost:8888", "custom+idp://issuer.example/path", "https://idp.example/" + strings.Repeat("x", MaxURLLength)} {
		t.Run(endpoint[:min(len(endpoint), 50)], func(t *testing.T) { require.NoError(t, ValidateURL(endpoint, policy)) })
	}
	for _, endpoint := range []string{"https://user:synthetic-secret@idp.example", "https://idp.example?api_key=synthetic-secret", "https://idp.example#access_token=synthetic-secret", "relative/path", "https:///missing-host"} {
		t.Run(endpoint, func(t *testing.T) {
			err := ValidateURL(endpoint, policy)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "synthetic-secret")
		})
	}
}
