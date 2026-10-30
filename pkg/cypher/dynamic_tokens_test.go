package cypher

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// A dynamic label or property key value follows Neo4j 5.26.30's rule: a
// string, a list of strings for labels; empty names and names with a null
// byte are TokenNameErrors; anything else is a TypeError (#907).
func TestDynamicTokenValues(t *testing.T) {
	requireCode := func(t *testing.T, err error, code string) {
		t.Helper()
		require.Error(t, err)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, err.Error())
	}
	for value, want := range map[interface{}][]string{
		"A":   {"A"},
		"A B": {"A B"},
		"`x`": {"`x`"},
	} {
		names, err := dynamicLabelNames(value)
		require.NoError(t, err)
		require.Equal(t, want, names)
	}
	names, err := dynamicLabelNames([]interface{}{"A", "B"})
	require.NoError(t, err)
	require.Equal(t, []string{"A", "B"}, names)
	names, err = dynamicLabelNames([]string{"C"})
	require.NoError(t, err)
	require.Equal(t, []string{"C"}, names)
	names, err = dynamicLabelNames([]interface{}{})
	require.NoError(t, err)
	require.Empty(t, names)

	for _, value := range []interface{}{nil, int64(1), []interface{}{"A", nil}, []interface{}{"A", int64(1)}, map[string]interface{}{}} {
		_, err := dynamicLabelNames(value)
		requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	}
	for _, value := range []interface{}{"", "a\x00b", []interface{}{"A", ""}, []string{""}} {
		_, err := dynamicLabelNames(value)
		requireCode(t, err, "Neo.ClientError.Schema.TokenNameError")
	}

	key, err := dynamicPropertyKey("k", false)
	require.NoError(t, err)
	require.Equal(t, "k", key)
	_, err = dynamicPropertyKey("", false)
	requireCode(t, err, "Neo.ClientError.Schema.TokenNameError")
	key, err = dynamicPropertyKey("", true)
	require.NoError(t, err)
	require.Empty(t, key)
	for _, value := range []interface{}{nil, int64(1), []interface{}{"k"}} {
		for _, removing := range []bool{false, true} {
			_, err := dynamicPropertyKey(value, removing)
			requireCode(t, err, "Neo.ClientError.Statement.TypeError")
		}
	}
}
