package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCaseReaderRetainsDistinctPartPositions(t *testing.T) {
	const body = "COUNT { RETURN 1 UNION ALL RETURN 1 }"
	for _, source := range []string{
		"CASE WHEN flag THEN " + body + " ELSE " + body + " END",
		"CASE '火' WHEN '火' THEN " + body + " ELSE " + body + " END",
	} {
		var positions caseSourcePositions
		parsed, err := parseCaseExpression(source, &positions)
		require.NoError(t, err)
		require.Len(t, parsed.whenClauses, 1)
		require.Len(t, positions.conditions, 1)
		require.Len(t, positions.results, 1)
		first, second := positions.start(caseResult, 0), positions.start(caseElse, 0)
		require.Less(t, first, second)
		require.Equal(t, body, source[first:first+len(body)])
		require.Equal(t, body, source[second:second+len(body)])
		condition := parsed.whenClauses[0].condition
		if parsed.isSimple {
			condition = parsed.whenClauses[0].value
			require.Equal(t, parsed.testExpression, source[positions.test:positions.test+len(parsed.testExpression)])
		}
		when := positions.start(caseWhen, 0)
		require.Equal(t, condition, source[when:when+len(condition)])
	}
}
