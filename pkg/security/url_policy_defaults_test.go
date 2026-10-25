package security

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURLPolicyZeroValuePreservesProductionDefaults(t *testing.T) {
	for _, test := range []struct {
		name string
		url  string
		want error
	}{
		{"public HTTPS", "https://example.com/path", nil},
		{"HTTP", "http://example.com/path", ErrURLHTTPNotAllowed},
		{"localhost", "https://localhost/path", ErrURLLocalhost},
		{"loopback", "https://127.0.0.1/path", ErrURLPrivateIP},
		{"private IPv4", "https://192.168.1.30/path", ErrURLPrivateIP},
		{"private IPv6", "https://[fc00::1]/path", ErrURLPrivateIP},
		{"length", "https://example.com/" + strings.Repeat("x", MaxURLLength), ErrURLTooLong},
		{"protocol", "file:///tmp/fixture", ErrURLInvalidProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateURL(test.url, URLPolicy{})
			if test.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestURLPolicyExplicitLengthSelection(t *testing.T) {
	endpoint := "https://example.com/path"
	require.NoError(t, ValidateURL(endpoint, URLPolicy{MaxLength: len(endpoint)}))
	require.ErrorIs(t, ValidateURL(endpoint, URLPolicy{MaxLength: len(endpoint) - 1}), ErrURLTooLong)
	require.NoError(t, ValidateURL(endpoint+strings.Repeat("x", MaxURLLength), URLPolicy{MaxLength: -1}))
}
