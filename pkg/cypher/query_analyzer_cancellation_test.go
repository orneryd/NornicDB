package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQueryAnalyzerCancellationDoesNotDependOnCacheWarmth pins that a cancelled
// request gets its cancellation on the fresh path and on both cache hits, and
// that the cached analysis stays usable for a live request.
func TestQueryAnalyzerCancellationDoesNotDependOnCacheWarmth(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewQueryAnalyzer(16).Analyze(cancelled, "RETURN 1")
	require.ErrorIs(t, err, context.Canceled)

	// The first analysis can register the built-in procedures, which moves the
	// registry generation past its own answer; the second leaves it current,
	// so the cancelled requests below take the cache hit's path.
	analyzer := NewQueryAnalyzer(16)
	_, err = analyzer.Analyze(context.Background(), "RETURN 1")
	require.NoError(t, err)
	live, err := analyzer.Analyze(context.Background(), "RETURN 1")
	require.NoError(t, err)

	_, err = analyzer.Analyze(cancelled, "RETURN 1")
	require.ErrorIs(t, err, context.Canceled, "exact text hit")
	_, err = analyzer.Analyze(cancelled, "RETURN /* c */ 1")
	require.ErrorIs(t, err, context.Canceled, "canonical text hit")

	again, err := analyzer.Analyze(context.Background(), "RETURN 1")
	require.NoError(t, err)
	require.Same(t, live, again)
}
