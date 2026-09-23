package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCypherListEqualityPropagatesOnlyUnresolvedNullComparisons(t *testing.T) {
	require.Nil(t, cypherEquality([]interface{}{int64(1), int64(2)}, []interface{}{nil, int64(2)}))
	require.Equal(t, false, cypherEquality([]interface{}{int64(1), int64(2)}, []interface{}{nil, "different"}))
	require.Equal(t, false, cypherEquality([]interface{}{int64(1)}, []interface{}{int64(1), nil}))
	require.Nil(t, cypherEquality(
		[]interface{}{[]interface{}{int64(1)}, []interface{}{"value", "same"}},
		[]interface{}{[]interface{}{int64(1)}, []interface{}{nil, "same"}},
	))
}
