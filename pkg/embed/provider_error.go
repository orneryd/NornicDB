package embed

import (
	"errors"
	"fmt"
	"math"
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

// UnusableEmbeddingError reports a provider answer that isn't one usable
// vector per input: a vector missing (Got < Want inputs), empty, non-finite,
// or of another dimension than the embedder's. It is a failed embedding, never a
// result to search with or cache. It isn't
// retryable, so the embed worker splits a batch down to the input the
// provider can't embed instead of retrying the whole batch; a search asks
// again on its next call, since nothing is cached.
type UnusableEmbeddingError struct {
	Input      int  // the input's index; -1 when vectors are missing
	Dimensions int  // the vector's length
	Want       int  // the expected count (Input -1) or dimension
	NonFinite  bool // at least one coordinate is NaN or infinity
}

func (e *UnusableEmbeddingError) Error() string {
	switch {
	case e.Input < 0:
		return fmt.Sprintf("embedding provider returned %d vectors for %d inputs", e.Dimensions, e.Want)
	case e.NonFinite:
		return fmt.Sprintf("embedding provider returned a non-finite value for input %d", e.Input)
	case e.Dimensions == 0:
		return fmt.Sprintf("embedding provider returned an empty vector for input %d", e.Input)
	}
	return fmt.Sprintf("embedding provider returned %d dimensions for input %d; the embedder has %d", e.Dimensions, e.Input, e.Want)
}

// Retryable is false: retrying the same batch would get the same answer for
// the input; the embed worker isolates it instead.
func (e *UnusableEmbeddingError) Retryable() bool { return false }

// CheckEmbeddings returns an *UnusableEmbeddingError unless vectors holds
// count non-empty, finite vectors, each of dimensions values when dimensions > 0.
func CheckEmbeddings(vectors [][]float32, count, dimensions int) error {
	if len(vectors) != count {
		return &UnusableEmbeddingError{Input: -1, Dimensions: len(vectors), Want: count}
	}
	for index, vector := range vectors {
		if len(vector) == 0 {
			return &UnusableEmbeddingError{Input: index}
		}
		if dimensions > 0 && len(vector) != dimensions {
			return &UnusableEmbeddingError{Input: index, Dimensions: len(vector), Want: dimensions}
		}
		for _, value := range vector {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return &UnusableEmbeddingError{Input: index, Dimensions: len(vector), Want: dimensions, NonFinite: true}
			}
		}
	}
	return nil
}
