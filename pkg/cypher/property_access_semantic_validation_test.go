package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPropertyAccessRejectsStaticallyNonPropertyValues(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	for _, expression := range []string{"123", "42.45", "true", "false", "'string'", "[123, true]"} {
		t.Run(expression, func(t *testing.T) {
			_, err := exec.Execute(ctx, "WITH "+expression+" AS value RETURN value.num", nil)
			require.Error(t, err)
			var semanticError *SemanticError
			require.True(t, errors.As(err, &semanticError))
			require.Equal(t, "InvalidArgumentType", semanticError.Detail)
		})
	}
}

func TestPropertyAccessRetainsMapAndNullSemantics(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	result, err := exec.Execute(ctx, "WITH {num: 7} AS value RETURN value.num", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)

	result, err = exec.Execute(ctx, "WITH null AS value RETURN value.num", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, result.Rows)
}

func TestPropertyAccessSupportsDelimitedMapKeys(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	result, err := exec.Execute(ctx, "WITH {name: 'Mats', `a.b`: 'dot', `back``tick`: 'escaped'} AS value RETURN value.`name`, value.`a.b`, value.`back``tick`", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Mats", "dot", "escaped"}}, result.Rows)
}
