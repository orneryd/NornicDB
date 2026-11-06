package cypher

import (
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDeletedEntityReadsPreserveQuotedIdentityAndNullTests(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		_, err := exec.Execute(t.Context(), "CREATE (:QuotedDeleted {p:1})", nil)
		require.NoError(t, err)
		for _, sample := range []struct {
			expression string
			notFound   bool
		}{
			{"labels(n)", true},
			{"`labels`(n)", true},
			{"labels(`n`)", true},
			{"`n`.p", true},
			{"n.p IS /* gap */ NULL", false},
			{"n.`p` IS NULL", false},
		} {
			t.Run(sample.expression, func(t *testing.T) {
				_, err := exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, err := exec.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
				result, err := exec.Execute(t.Context(), "MATCH (n:QuotedDeleted) DELETE n RETURN "+sample.expression+" AS v", nil)
				if sample.notFound {
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)
				} else {
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{true}}, result.Rows)
				}
			})
		}
	})
}

func TestDeletedEntityIterationReadsCrossTrivia(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		_, err := exec.Execute(t.Context(), "CREATE (:IterationDeleted {p:1})", nil)
		require.NoError(t, err)
		for _, projection := range []string{
			"[x IN ns | x.p]",
			"[x IN /* gap */ ns | x.p]",
			"[x /* gap */ IN ns | x.p]",
			"[x IN `ns` | x.p]",
		} {
			t.Run(projection, func(t *testing.T) {
				_, err := exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, err := exec.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
				_, err = exec.Execute(t.Context(), "MATCH (n:IterationDeleted) DELETE n WITH collect(n) AS ns RETURN "+projection+" AS v", nil)
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)
			})
		}
	})
}
