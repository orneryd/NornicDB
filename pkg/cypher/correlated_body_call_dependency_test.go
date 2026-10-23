package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Import selection and the independent-CALL cache must see the same complete
// scope as an explicitly imported body, including its nested wildcard read.
func TestCorrelatedBodyCallImportValidity(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call-dependency"))
	_, err := exec.Execute(ctx, "CREATE (:Outer {id:1}), (:Target {n:1}), (:Target {n:2})", nil)
	require.NoError(t, err)
	body := "RETURN COLLECT { MATCH (b:Target) RETURN * } AS v"
	_, expected := exec.Execute(ctx, "MATCH (a:Outer) CALL (a) { "+body+" } RETURN v", nil)
	require.Error(t, expected)
	for _, call := range []string{
		"CALL (*) { ", "CALL { ", "CALL { WITH * ", "CALL { WITH a ",
	} {
		t.Run(call, func(t *testing.T) {
			_, actual := exec.Execute(ctx, "MATCH (a:Outer) "+call+body+" } RETURN v", nil)
			require.Error(t, actual)
			require.Equal(t, expected.Error(), actual.Error())
		})
	}
}

func TestCorrelatedBodySeedDependencyColumns(t *testing.T) {
	seed := &ExecuteResult{Columns: []string{"a", "other"}}
	for _, tc := range []struct {
		body string
		want []string
	}{
		{"MATCH (b:Target) RETURN *", []string{"a", "other"}},
		{"WITH * MATCH (b:Target) RETURN b", []string{"a", "other"}},
		{"RETURN COLLECT { RETURN * } AS v", []string{"a", "other"}},
		{"MATCH (b:Target) RETURN b", []string{}},
		{"MATCH (b:Target) WHERE b.n = a RETURN b", []string{"a"}},
	} {
		t.Run(tc.body, func(t *testing.T) {
			require.Equal(t, tc.want, detectReferencedCallSubquerySeedColumns(seed, tc.body))
		})
	}
}
