package bolt

import (
	"fmt"
	"time"
)

// validateTransactionTimeout implements Neo4j 5.26 metadata semantics: null or
// non-positive disables expiry, while huge positive longs saturate safely.
func validateTransactionTimeout(metadata map[string]any) (time.Duration, error) {
	value, present := metadata["tx_timeout"]
	if !present || value == nil {
		return 0, nil
	}
	milliseconds, ok := value.(int64)
	if !ok {
		//nolint:staticcheck // Neo4j's client-visible wire message is capitalized.
		return 0, fmt.Errorf("Invalid value for tx_timeout: Expected long, got %T", value)
	}
	if milliseconds <= 0 {
		return 0, nil
	}
	const maxMilliseconds = int64((1<<63 - 1) / int64(time.Millisecond))
	if milliseconds > maxMilliseconds {
		return time.Duration(1<<63 - 1), nil
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}
