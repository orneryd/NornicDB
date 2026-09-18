package embed

import (
	"errors"
	"fmt"
)

// ProviderError reports a failed remote embedding request and whether retrying
// the same request can reasonably succeed without changing its input.
type ProviderError struct {
	Provider   string
	StatusCode int
	Body       string
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("%s returned %d: %s", e.Provider, e.StatusCode, e.Body)
}

// Retryable reports whether the status represents a transient failure.
func (e *ProviderError) Retryable() bool {
	return e.StatusCode == 408 || e.StatusCode == 409 || e.StatusCode == 425 ||
		e.StatusCode == 429 || e.StatusCode >= 500
}

// IsRetryableError defaults unknown errors to retryable because transports,
// timeouts, and third-party embedders may recover on a later attempt.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	type retryable interface{ Retryable() bool }
	var classified retryable
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	return true
}
