package observability_test

import (
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSafeRedactionStreamBoundaryPreservesCompleteProtectedSpans(t *testing.T) {
	const uri = "https://viewer:synthetic-pass@example.invalid/path"
	for _, tc := range []struct {
		name, text     string
		boundary, want int
		credentials    []string
	}{
		{name: "negative", text: "hello", boundary: -1, want: 0},
		{name: "past end", text: "hello", boundary: 6, want: 0},
		{name: "safe text", text: "hello world", boundary: 6, want: 6, credentials: []string{"another secret"}},
		{name: "inside credential", text: "prefix alpha beta suffix", boundary: len("prefix alpha "), want: len("prefix "), credentials: []string{"alpha beta"}},
		{name: "partial credential", text: "prefix alpha ", boundary: len("prefix alpha "), want: len("prefix "), credentials: []string{"alpha beta"}},
		{name: "escaped credential", text: "prefix alpha%20beta suffix", boundary: len("prefix alpha%20"), want: len("prefix "), credentials: []string{"alpha beta"}},
		{name: "inside URL", text: "prefix " + uri + " suffix", boundary: len("prefix https://viewer:"), want: len("prefix ")},
		{name: "before URL", text: "prefix " + uri + " suffix", boundary: len("prefix "), want: len("prefix ")},
		{name: "complete URL", text: "prefix " + uri + " suffix", boundary: len("prefix " + uri), want: len("prefix " + uri)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, observability.SafeRedactionStreamBoundary(tc.text, tc.boundary, tc.credentials))
		})
	}
}
