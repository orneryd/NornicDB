//go:build cgo && !nolocalllm && !yzma && (darwin || linux)

package localllm

import (
	"strings"
	"testing"
)

// Autocomplete's public handler separately asserts nil text stops and its
// retained output bound. Exercise the actual native generator's stop owner
// without loading a model: text stops cannot distinguish quoted Cypher data.
func TestAutocompleteQuotedTextStopBoundary(t *testing.T) {
	for _, marker := range []string{"The user", "They are", "I need to", "IMPORTANT", "Rules:", "Available", "Complete query:", "\n\n\n"} {
		t.Run(marker, func(t *testing.T) {
			query := "RETURN 'before " + marker + " after' AS value"
			stops := []string{marker}
			visible, _, stopped := splitVisibleTextForStopTokens(query, stops, longestStopTokenLength(stops))
			if !stopped || visible != "RETURN 'before " {
				t.Fatalf("unsafe text-stop control: visible=%q stopped=%v", visible, stopped)
			}
			// Streaming must also retain a marker split across token chunks.
			var output strings.Builder
			for _, part := range []string{query[:17], query[17:]} {
				visible, remaining, stopped := splitVisibleTextForStopTokens(part, nil, longestStopTokenLength(nil))
				if stopped || remaining != "" {
					t.Fatalf("nil text stops changed emitted chunk: remaining=%q stopped=%v", remaining, stopped)
				}
				output.WriteString(visible)
			}
			if output.String() != query {
				t.Fatalf("emitted=%q want=%q", output.String(), query)
			}
		})
	}
}
