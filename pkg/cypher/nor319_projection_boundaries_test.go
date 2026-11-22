package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNOR319ProjectionOperandBoundaries(t *testing.T) {
	checker := staticOperatorChecker{scope: staticTypeScope{
		kinds:  matchSemanticScope{"a": matchBindingNode, "n": matchBindingNode},
		values: map[string]string{"i": "Integer", "skip": "Map"},
	}}
	for _, expression := range []string{
		"{k:7} = a{.k}", "a{.k} = {k:7}", "NOT ({k:8} = a{.k})",
		"a{.k} <> {k:8}", "a{.k} = a{.k}",
	} {
		t.Run(expression, func(t *testing.T) {
			operand, err := checker.check(t.Context(), expression)
			require.NoError(t, err)
			require.Equal(t, "Boolean", operand.typeName())
		})
	}
	for _, expression := range []string{
		"1{.k}", "(a){.k}", "a.k{.k}", "head([a]){.k}", "[a]{.k}",
		"1{k:1}", "(a){k:1}", "a.k{k:1}", "head([a]){k:1}", "[a]{k:1}",
		"head([a{.k}]){.k}", "head([a{.k}]){k:1}",
		"n . prop{k:1}", "n. prop{.k}",
	} {
		t.Run(expression, func(t *testing.T) {
			_, err := checker.check(t.Context(), expression)
			require.Error(t, err)
			require.Contains(t, err.Error(), "only a variable can be projected")
		})
	}
	for _, expression := range []string{"i{.k}", "{k:1} = i{.k}", "a{k:1 - 'x'}"} {
		t.Run(expression, func(t *testing.T) {
			_, err := checker.check(t.Context(), expression)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Type mismatch")
		})
	}
	operand, err := checker.check(t.Context(), "skip{.k}")
	require.NoError(t, err)
	require.Equal(t, "Map", operand.typeName())
}

func TestNOR319ProjectionPublicBoundaries(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, expression := range []string{"{k:7} = a{.k}", "a{.k} = {k:7}", "NOT ({k:8} = a{.k})"} {
			t.Run(expression, func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), "CREATE (:A {k:7})", nil)
				require.NoError(t, err)
				result, err := exec.Execute(t.Context(), "MATCH (a:A) RETURN "+expression+" AS same", nil)
				require.NoError(t, err)
				require.Equal(t, []string{"same"}, result.Columns)
				require.Equal(t, [][]any{{true}}, result.Rows)
			})
		}
		for _, expression := range []string{
			"head([a{.k}]){.k}", "head([a{.k}]){k:1}", "a . prop{k:1}", "a. prop{.k}",
		} {
			t.Run(expression, func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), "MATCH (a:A) RETURN "+expression, nil)
				require.Error(t, err)
			})
		}
		exec, _ := newTestExecutor(t)
		_, err := exec.Execute(t.Context(), "MATCH (a:A) RETURN missing{.k}", nil)
		require.ErrorContains(t, err, "Variable `missing` not defined")
		_, err = exec.Execute(t.Context(), "WITH 1 AS a RETURN a{.k}", nil)
		require.ErrorContains(t, err, "Type mismatch")
	})
}
