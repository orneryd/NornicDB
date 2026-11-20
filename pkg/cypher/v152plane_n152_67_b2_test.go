package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Fresh witnesses reconstructed from NOR-281's described input classes. The
// historical fc79 probe file was not recovered; these are not its exact bytes.
// Test the actual executor so an autocomplete failure cannot be explained by
// an invalid model-generated statement. Header spelling is deliberately not
// qualified here: the separate NOR-54/NOR-66 result-header finding is out of scope.
func TestNOR281AutocompleteQuotedWitnessesAreExecutable(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		for _, query := range []string{
			"RETURN 1 AS ordinary",
			"RETURN 1 AS `ordinary`",
			"RETURN 1 AS `a  b`",
			"RETURN 1 AS `a```",
			"RETURN 1 AS `a``b`",
		} {
			t.Run(query, func(t *testing.T) {
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Len(t, result.Columns, 1)
				require.Len(t, result.Rows, 1)
				require.Len(t, result.Rows[0], 1)
				require.EqualValues(t, 1, result.Rows[0][0])
				t.Logf("valid input=%q actual header=%q row=%v", query, result.Columns, result.Rows[0])
			})
		}
	})
}
