package cypher

import (
	"testing"
	"time"
)

// Force the logging gate without requiring a fast query to consume a real
// clock tick. Deadlines, cancellation, execution, and log handlers remain real.
func forceSlowQueryLogging(t testing.TB, executor *StorageExecutor) {
	t.Helper()
	executor.SetSlowQueryThreshold(time.Nanosecond)
	executor.executionElapsed = func(time.Time) time.Duration { return time.Millisecond }
}
