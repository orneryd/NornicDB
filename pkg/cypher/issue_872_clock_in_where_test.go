package cypher

// NornicDB #872: in WHERE, every temporal clock call read the current time, so
// WHERE datetime() = datetime() dropped every row. A predicate now reads the
// statement's clock, as RETURN does. Answers are Neo4j 5.26.30's.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue872ClockCallsInWhereReadOneInstant(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "COMMIT", nil) })
			}
			for _, tc := range []struct {
				query string
				want  interface{}
			}{
				{"UNWIND range(1,3) AS i WITH i WHERE datetime.statement() = datetime.statement() RETURN count(*)", int64(3)},
				{"UNWIND range(1,3) AS i WITH i WHERE datetime() = datetime() RETURN count(*)", int64(3)},
				{"UNWIND range(1,3) AS i WITH i WHERE date() = date() AND time() = time() AND localtime() = localtime() RETURN count(*)", int64(3)},
				{"UNWIND range(1,3) AS i WITH i WHERE localdatetime.statement() = localdatetime.statement() AND datetime.transaction() = datetime.transaction() RETURN count(*)", int64(3)},
				{"UNWIND range(1,3) AS i WITH i, datetime.statement() AS s WHERE s = datetime.statement() AND NOT s <> datetime.statement() RETURN count(*)", int64(3)},
				{"UNWIND range(1,3) AS i WITH i WHERE i = 2 OR datetime() <> datetime() RETURN collect(i)", []interface{}{int64(2)}},
				{"UNWIND range(1,3) AS i WITH i WHERE datetime() IN [datetime()] RETURN count(*)", int64(3)},
			} {
				res, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, [][]interface{}{{tc.want}}, res.Rows, tc.query)
			}
		})
	}
}
