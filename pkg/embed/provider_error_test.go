package embed

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderErrorsClassifyRetryableFailures(t *testing.T) {
	for _, status := range []int{408, 409, 425, 429, 500, 503} {
		require.True(t, IsRetryableError(&ProviderError{StatusCode: status}), "status %d", status)
	}
	for _, status := range []int{400, 401, 403, 404, 422} {
		require.False(t, IsRetryableError(&ProviderError{StatusCode: status}), "status %d", status)
	}
	require.True(t, IsRetryableError(errors.New("connection reset")))
	require.False(t, IsRetryableError(nil))
}
