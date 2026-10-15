package cypher

import (
	"context"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCaseReviewKeywordBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		syntax      bool
		want        interface{}
	}{
		{"sibling_ambiguous_not_end", `WITH 1 AS not RETURN CASE WHEN true THEN not END + CASE WHEN true THEN not END AS v`, true, nil},
		{"simple_case_not_end", `WITH false AS end RETURN CASE NOT end WHEN true THEN 1 ELSE 2 END AS v`, false, int64(1)},
		{"simple_case_true_end", `WITH true AS end RETURN CASE NOT end WHEN true THEN 1 ELSE 2 END AS v`, false, int64(2)},
		{"simple_case_null_end", `WITH null AS end RETURN CASE NOT end WHEN true THEN 1 ELSE 2 END AS v`, false, int64(2)},
		{"grouped_sibling_cases", `WITH 1 AS not RETURN (CASE WHEN true THEN not END) + (CASE WHEN true THEN not END) AS v`, false, int64(2)},
		{"commented_not_iterator", `RETURN [not /* iterator */ IN [1,2] | CASE WHEN true THEN not END] AS v`, false, []interface{}{int64(1), int64(2)}},
		{"not_iterator", `RETURN [not IN [1,2] | CASE WHEN true THEN CASE WHEN true THEN not END ELSE 0 END] AS v`, false, []interface{}{int64(1), int64(2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_review"))
			result, err := e.Execute(context.Background(), tc.query, nil)
			if tc.syntax {
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			} else {
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
			}
		})
	}
}
