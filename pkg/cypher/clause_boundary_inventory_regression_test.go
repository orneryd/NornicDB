package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Uses only pre-correction APIs, so the same fixture can qualify a red carrier
// of the previous published head without a compatibility shim.
func TestStatementVocabularyInventoryRegressions(t *testing.T) {
	t.Run("call-predicate-before-let", func(t *testing.T) {
		require.Equal(t, "WHERE x = 1 AND x > 0 LET y = x RETURN y",
			buildCallTailPredicateInjection("WHERE x = 1 LET y = x RETURN y", []string{"x > 0"}))
	})
	t.Run("load-after-pattern", func(t *testing.T) {
		require.NoError(t, validateLeadingNodePatternTransition("MATCH (n) LOAD CSV FROM 'file:///rows.csv' AS row RETURN row"))
	})
	t.Run("match-body-before-load", func(t *testing.T) {
		require.Equal(t, "MATCH (n)", strings.TrimSpace(extractMatchClause("MATCH (n) LOAD CSV FROM 'file:///rows.csv' AS row RETURN row")))
	})
}
