package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Expectations are checked independently against Neo4j 5.26.30, including
// unknown comparisons inside nested lists and maps in indexed membership.
func TestOperationOwnerMembershipResults(t *testing.T) {
	for _, tc := range []struct {
		name              string
		value, list, want interface{}
	}{
		{"null_list", int64(1), nil, nil},
		{"empty_list", nil, []interface{}{}, false},
		{"null_value", nil, []interface{}{int64(1)}, nil},
		{"match_after_null", int64(1), []interface{}{nil, int64(1)}, true},
		{"unknown_item", int64(2), []interface{}{int64(1), nil}, nil},
		{"missing_item", int64(2), []interface{}{int64(1)}, false},
		{"numeric_match", int64(1), []interface{}{float64(1)}, true},
		{"nested_unknown", []interface{}{int64(1), nil}, []interface{}{[]interface{}{int64(1), nil}}, nil},
		{"nested_unequal", []interface{}{int64(1), nil}, []interface{}{[]interface{}{int64(2), nil}}, false},
		{"map_unknown", map[string]interface{}{"v": nil}, []interface{}{map[string]interface{}{"v": nil}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, ok := cypherMembership(tc.value, tc.list)
			require.True(t, ok)
			require.Equal(t, tc.want, value)
			value, ok = rowMembershipOfValues(tc.value, tc.list, false)
			require.True(t, ok)
			require.Equal(t, tc.want, value)
			exec := &StorageExecutor{}
			predicate := exec.makeCompiledParamMembershipPredicate(func(b binding, _ map[string]interface{}) (interface{}, bool) {
				return b["n"].Properties["v"], true
			}, "items")
			row := binding{"n": &storage.Node{Properties: map[string]interface{}{"v": tc.value}}}
			require.Equal(t, tc.want, predicate(row, map[string]interface{}{"items": tc.list}).value())
		})
	}
}

func TestOperationOwnerListLiteralResults(t *testing.T) {
	exec := &StorageExecutor{}
	ctx := withExpressionFailureSlot(context.Background())
	for _, tc := range []struct {
		literal string
		want    []interface{}
	}{
		{"[]", []interface{}{}},
		{"[1,'a',[null,2],{v:true}]", []interface{}{int64(1), "a", []interface{}{nil, int64(2)}, map[string]interface{}{"v": true}}},
	} {
		value, ok, err := exec.evaluateRowValue(tc.literal, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, tc.want, value)
		require.Equal(t, tc.want, exec.parseArrayValue(ctx, tc.literal))
		require.Equal(t, tc.want, exec.evaluateExpressionWithContext(ctx, tc.literal, nil, nil))
		require.NoError(t, getExpressionFailure(ctx))
	}
}
