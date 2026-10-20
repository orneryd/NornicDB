package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCypherValueIndexTupleContracts(t *testing.T) {
	// An empty row, a null column and an empty-list column are distinct
	// equivalence classes. Numeric representations share their class.
	tuples := []struct {
		values []interface{}
		class  string
	}{
		{nil, "empty row"},
		{[]interface{}{}, "empty row"},
		{[]interface{}{nil}, "null column"},
		{[]interface{}{[]interface{}{}}, "empty list"},
		{[]interface{}{int64(1)}, "one"},
		{[]interface{}{float64(1)}, "one"},
	}
	for _, reverse := range []bool{false, true} {
		var index cypherValueIndex
		positions := map[string]int{}
		for step := range tuples {
			i := step
			if reverse {
				i = len(tuples) - step - 1
			}
			tuple := tuples[i]
			want, exists := positions[tuple.class]
			if !exists {
				want = len(positions)
				positions[tuple.class] = want
			}
			position, found := index.addTuple(tuple.values)
			require.Equal(t, exists, found, "reverse=%v tuple=%d", reverse, i)
			require.Equal(t, want, position, "reverse=%v tuple=%d", reverse, i)
		}
		for _, tuple := range tuples {
			position, found := index.addTuple(tuple.values)
			require.True(t, found)
			require.Equal(t, positions[tuple.class], position)
		}
	}
}
