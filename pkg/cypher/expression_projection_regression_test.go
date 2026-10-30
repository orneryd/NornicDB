package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAggregateArithmeticUsesProjectedAggregateValue(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (:Item {price: 10.5}), (:Item {price: 20.0}), (:Item {price: 5.25})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, "MATCH (i:Item) RETURN round(avg(i.price) * 100) / 100 AS average", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{float64(11.92)}}, result.Rows)
}

func TestReduceProjectsAlongsideAggregation(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Lin'})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH (person:Person)
		WITH collect(person.name) AS names
		RETURN size(names) AS count, reduce(total = 0, value IN [1, 2, 3] | total + value) AS sum
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(6)}}, result.Rows)
}

func TestMapProjectionCombinesSelectedAndComputedProperties(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (:Item {sku: 'a1', quantity: 3})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, "MATCH (i:Item {sku: 'a1'}) RETURN i {.sku, double: i.quantity * 2} AS item", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{map[string]interface{}{"sku": "a1", "double": int64(6)}}}, result.Rows)
}

func TestLiteralMapsRemainDistinctFromMapProjectionSyntax(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	tests := []struct {
		query string
		want  interface{}
	}{
		{"RETURN {a: {}} AS value", map[string]interface{}{"a": map[string]interface{}{}}},
		{"RETURN {k: 1} = {k: 1} AS value", true},
		{"RETURN keys({name: 'Alice', address: {city: 'London'}}) AS value", []interface{}{"address", "name"}},
		{"RETURN [{a: {b: 1}}, {a: {b: 2}}] AS value", []interface{}{
			map[string]interface{}{"a": map[string]interface{}{"b": int64(1)}},
			map[string]interface{}{"a": map[string]interface{}{"b": int64(2)}},
		}},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, test.want, result.Rows[0][0])
		})
	}
}
