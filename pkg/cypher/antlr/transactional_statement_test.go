package antlr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Placement is syntax supplied to the single statement validator. These are
// the enclosing-query shapes independently exercised through the public matrix.
func TestTransactionalStatementPlacementReader(t *testing.T) {
	for _, sample := range []struct {
		query      string
		placements []string
	}{
		{"CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS", []string{""}},
		{"EXPLAIN CALL { CREATE (:X) } IN TRANSACTIONS OF 0 ROWS", []string{""}},
		{"CREATE (:X) WITH 1 AS x CALL { CREATE (:Y) } IN TRANSACTIONS OF 1 ROWS", []string{"write"}},
		{"CALL { CREATE (:X) } CALL { CREATE (:Y) } IN TRANSACTIONS OF 1 ROWS", []string{"", "write"}},
		{"CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS CALL { CREATE (:Y) } IN TRANSACTIONS OF $n ROWS", []string{"", ""}},
		{"CALL { CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS }", []string{"", "nested"}},
		{"CALL { CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS } IN TRANSACTIONS OF 1 ROWS", []string{"", "nested"}},
		{"CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS RETURN 1 AS x UNION ALL RETURN 2 AS x", []string{"union"}},
		{"RETURN 1 AS x UNION ALL CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS RETURN 2 AS x", []string{"union"}},
		{"CALL { RETURN 1 AS x UNION ALL RETURN 2 AS x } IN TRANSACTIONS OF 1 ROWS RETURN x", []string{""}},
		{"RETURN COUNT { CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS RETURN 1 AS x } AS n", []string{"nested"}},
		{"RETURN EXISTS { CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS } AS n", []string{"nested"}},
		{"RETURN COLLECT { CALL { CREATE (:X) } IN TRANSACTIONS OF 1 ROWS RETURN 1 AS x } AS n", []string{"nested"}},
		{"RETURN 'CALL IN TRANSACTIONS' AS text", nil},
	} {
		t.Run(sample.query, func(t *testing.T) {
			calls, err := ReadTransactionalCalls(context.Background(), sample.query)
			require.NoError(t, err)
			require.Len(t, calls, len(sample.placements))
			for index, call := range calls {
				require.Equal(t, sample.placements[index], call.PlacementError)
			}
		})
	}
}
