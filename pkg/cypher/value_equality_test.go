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

func TestListOperandsRejectLiteralNonLists(t *testing.T) {
	for expression, typeName := range map[string]string{
		"RETURN 1 IN true":                     "Boolean",
		"RETURN 1 IN 12":                       "Integer",
		"RETURN 1 IN 1.5":                      "Float",
		"RETURN 1 IN 'value'":                  "String",
		"RETURN 1 IN {value: []}":              "Map",
		"RETURN 1 IN -3 AND true":              "Integer",
		"RETURN any(x IN 'ab' WHERE true)":     "String",
		"RETURN [x IN {a: 1} | x]":             "Map",
		"RETURN reduce(a = 0, x IN 5 | a + x)": "Integer",
		`RETURN all(x IN "a\"b" WHERE true)`:   "String",
	} {
		err := validateListOperands(expression, nil)
		require.Error(t, err, expression)
		require.Contains(t, err.Error(), "Type mismatch: expected List<T> but was "+typeName, expression)
	}
	for _, expression := range []string{
		"RETURN 1 IN [1, 2]",
		"RETURN null IN null",
		"RETURN 1 IN 'a' + 'b'",
		"RETURN 1 IN n.list",
		"MATCH (n:IN) RETURN n",
		"MATCH ()-[:IN]->() RETURN 1",
		"RETURN {in: 5} AS m",
		"CALL { CREATE (:N) } IN TRANSACTIONS",
		"RETURN 'x IN 5' AS s",
		"RETURN 1 IN {",
		"RETURN 1 IN  ",
	} {
		require.NoError(t, validateListOperands(expression, nil), expression)
	}
}
