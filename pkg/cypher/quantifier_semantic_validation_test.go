package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuantifierNumericPredicatesRejectStaticNonNumericLists(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, function := range []string{"all", "any", "none", "single"} {
		for _, list := range []string{"['Clara']", "[false, true]", "['Clara', 'Bob', 'Dave', 'Alice']"} {
			query := "RETURN " + function + "(x IN " + list + " WHERE x % 2 = 0) AS result"
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err, query)
			semantic, ok := err.(*SemanticError)
			require.True(t, ok, "%s: %T", query, err)
			require.Equal(t, "InvalidArgumentType", semantic.Detail)
		}
	}
}

func TestQuantifierNumericPredicatesAcceptStaticNumericLists(t *testing.T) {
	exec, _ := newTestExecutor(t)
	result, err := exec.Execute(context.Background(), "RETURN none(x IN [1, 3, 5] WHERE x % 2 = 0) AS result", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}

func TestQuantifierValidationIgnoresTextAndComments(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, query := range []string{
		"RETURN 'none(x IN [false] WHERE x % 2 = 0)' AS text",
		"RETURN 1 AS value // none(x IN [false] WHERE x % 2 = 0)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
	}
}
