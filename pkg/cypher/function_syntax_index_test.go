package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxManyCommentedArguments(t *testing.T) {
	for _, count := range []int{2000, 4000} {
		for _, kind := range []string{"block", "line"} {
			text := functionFactConsumerInput(count, kind)
			require.Greater(t, len(text), 16384)
			arguments := namedFunctionArguments(text, "coalesce")
			require.Len(t, arguments, count)
			for _, argument := range arguments {
				require.Equal(t, "null,1", strings.TrimSpace(argument))
			}
		}
	}
}

func TestFunctionSyntaxScalarSubqueryMembership(t *testing.T) {
	for _, sample := range []struct {
		text string
		want []string
	}{
		{"sum(0) + COUNT { RETURN max(1) AS n } + sum(2)", []string{"sum(0)", "sum(2)"}},
		{"COUNT { RETURN COUNT { RETURN max(1) AS n } AS n } + avg(2)", []string{"avg(2)"}},
		{"COUNT { RETURN max(1) AS n } + COLLECT { RETURN min(2) AS n } + sum(3)", []string{"sum(3)"}},
		// An unclosed body has no closed lexical scope; syntax validation
		// rejects it, while its calls remain available to argument validation.
		{"COUNT { RETURN max(1)", []string{"max(1)"}},
	} {
		var got []string
		for _, span := range findAggregateSpans(sample.text) {
			got = append(got, sample.text[span.start:span.end])
		}
		require.Equal(t, sample.want, got, sample.text)
	}
	for _, count := range []int{2000, 4000} {
		text := functionFactConsumerInput(count, "subqueries")
		require.Greater(t, len(text), 16384)
		require.False(t, containsAggregateFunc(text))
	}
}
