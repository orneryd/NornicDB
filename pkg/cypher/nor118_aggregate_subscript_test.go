package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNOR118AggregateResultSubscriptKeepsTypeErrors(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newUnitExecutor(t)
		for _, query := range []string{
			"UNWIND [1, 2] AS x RETURN collect(x)['bad'] AS value",
			"UNWIND [[1], [2]] AS x RETURN collect(x[false])[0] AS value",
		} {
			_, err := exec.Execute(t.Context(), query, nil)
			require.Error(t, err, query)
			var diagnostic interface {
				BoltErrorCode() string
				BoltErrorDetail() string
			}
			require.True(t, errors.As(err, &diagnostic), "%s: %v", query, err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", diagnostic.BoltErrorCode(), query)
			require.Equal(t, "InvalidArgumentType", diagnostic.BoltErrorDetail(), query)
		}
	})
}
