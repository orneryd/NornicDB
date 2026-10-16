package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparationStrictReaderCancellation(t *testing.T) {
	readers := []struct {
		name    string
		legacy  func(string) bool
		checked func(string, *pipelineCancellation) bool
	}{
		{"not_in", containsNotInOperator, containsNotInOperatorWithCancellation},
		{"list_comma", containsTrailingListComma, containsTrailingListCommaWithCancellation},
		{"adjacent_string", hasAdjacentStringLiterals, hasAdjacentStringLiteralsWithCancellation},
		{"adjacent_operand", hasAdjacentOperands, hasAdjacentOperandsWithCancellation},
	}
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			for _, query := range []string{
				"RETURN '" + strings.Repeat("x", 129) + "' AS value",
				"RETURN /*" + strings.Repeat("x", 129) + "*/ 1",
				"RETURN `" + strings.Repeat("x", 129) + "`",
				"RETURN " + strings.Repeat("7", 129),
				"RETURN $" + strings.Repeat("name", 33),
				"RETURN " + strings.Repeat("name", 33) + " $value",
				"RETURN [1," + strings.Repeat(" ", 129) + "2]",
				"RETURN 'a' 'b'", "RETURN 1 NOT IN [1]", "RETURN [1,]",
			} {
				want := reader.legacy(query)
				runPreparationBudgetSweep(t, func(ctx context.Context, probe *pipelineCancellation) {
					actual := reader.checked(query, probe)
					if probe.err == nil {
						require.Equal(t, want, actual)
					} else {
						require.False(t, actual)
					}
				}, phases...)
			}
		})
	}
	require.False(t, isSyntaxBoundaryKeyword(strings.Repeat("identifier", 100000)))
}
