package heimdall

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func v152RCCLegacyAutocompleteCleanup(completion string) string {
	clean := strings.TrimSpace(completion)
	clean = strings.TrimPrefix(clean, "```cypher")
	clean = strings.TrimPrefix(clean, "```")
	clean = strings.TrimSuffix(clean, "```")
	clean = strings.TrimSpace(clean)
	parts := regexp.MustCompile(`\s{2,}|\n{2,}`).Split(clean, -1)
	if len(parts) > 0 {
		clean = parts[0]
	}
	return clean
}

// v152RCCFenceOnlyCleanup is the test-side corrected copy: markdown fences
// are framing only when they surround the completion. Cypher bytes inside the
// frame, including whitespace and escaped backticks, are not edited.
func v152RCCFenceOnlyCleanup(completion string) string {
	clean := strings.TrimSpace(completion)
	if strings.HasPrefix(clean, "```cypher\n") {
		clean = strings.TrimPrefix(clean, "```cypher\n")
	} else if strings.HasPrefix(clean, "```\n") {
		clean = strings.TrimPrefix(clean, "```\n")
	}
	clean = strings.TrimSuffix(clean, "\n```")
	return strings.TrimSpace(clean)
}

// TestV152RCCAutocompleteCleanupCause proves the public handler's wrong output
// is exactly the byte-pattern cleanup at handler.go:555-613. The test-side
// fence-only copy removes that step and preserves both failing witnesses while
// the ordinary and fenced controls remain correct.
func TestV152RCCAutocompleteCleanupCause(t *testing.T) {
	nor281InstallAutocompleteSubsystem(t)
	tests := []struct {
		name, completion string
		trigger          bool
	}{
		{"double_space_quoted_alias", "RETURN 1 AS `a  b`", true},
		{"escaped_trailing_backtick", "RETURN 1 AS `a```", true},
		{"ordinary_alias_control", "RETURN 1 AS one", false},
		{"fenced_control", "```cypher\nRETURN 1 AS one\n```", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := nor281Suggestion(t, nor281PostAutocomplete(t, nor281AutocompleteHandler(t, test.completion, nil), `{"query":"RETURN 1 AS"}`))
			legacy := v152RCCLegacyAutocompleteCleanup(test.completion)
			corrected := v152RCCFenceOnlyCleanup(test.completion)
			want := test.completion
			if test.name == "fenced_control" {
				want = "RETURN 1 AS one"
			}
			t.Logf("RCC_HEIMDALL_CLEANUP owner=pkg/heimdall/handler.go:555-613 trigger=%v actual=%q legacy=%q corrected=%q want=%q", test.trigger, actual, legacy, corrected, want)
			require.Equal(t, legacy, actual, "public handler must exhibit the named cleanup step")
			require.Equal(t, want, corrected, "test-side corrected copy must preserve statement bytes and controls")
			if test.trigger {
				require.NotEqual(t, want, actual, "trigger must take the destructive cleanup path")
			} else {
				require.Equal(t, want, actual, "control must stay on the correct path")
			}
		})
	}
}
