package fabric

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeIterationPredicateDescriptor(t *testing.T) {
	query := ReadQueryStructure("UNWIND [] AS item WHERE item=1 RETURN item")
	require.NoError(t, query.Err)
	clause := query.First("UNWIND")
	require.NotNil(t, clause)
	require.Equal(t, "[]", clause.Expression)
	require.Equal(t, "item", clause.Name)
	require.Equal(t, "item=1", clause.Predicate)
	var predicate bool
	for _, operand := range clause.Operands() {
		if operand.Predicate {
			require.Equal(t, clause.Predicate, operand.Input.Text())
			predicate = true
		}
	}
	require.True(t, predicate, "the declaration and operand views retain the same native predicate")
}
