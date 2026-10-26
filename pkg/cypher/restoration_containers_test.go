package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultRestorationWithoutRewriteKeepsResultAndError(t *testing.T) {
	// Ordinary unquoted Fabric statements have no quoted-name rewrite. The
	// retired nil receiver's identity contract belongs to the shared entry.
	input := &ExecuteResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}
	cause := errors.New("unchanged diagnostic")
	result, err := (resultRestoration{}).restore(input, cause, nil)
	require.Same(t, input, result)
	require.Same(t, cause, err)
	result, err = (resultRestoration{}).restore(nil, cause, nil)
	require.Nil(t, result)
	require.Same(t, cause, err)
}

func TestQuotedRestorationDetachesUnchangedContainers(t *testing.T) {
	_, names := canonicalizeQuotedVariablesWithBindings(t.Context(), "EXPLAIN WITH 1 AS `a b` RETURN `a b`", nil)
	require.NotNil(t, names)
	input := &ExecuteResult{
		Metadata: map[string]any{
			"nested": map[string]any{"number": int64(42)},
			"values": []any{int64(1)},
			"rows":   [][]any{{int64(2)}},
		},
		Rows: [][]any{{map[string]any{"number": int64(7)}, []any{int64(8)}}},
	}
	result, err := (resultRestoration{names: names}).restore(input, nil, nil)
	require.NoError(t, err)
	require.NotSame(t, input, result)
	// No text changes in this result. Detachment must still protect the cache
	// at every mutable layer, including both the row and outer row slices.
	result.Metadata["new"] = true
	result.Metadata["nested"].(map[string]any)["number"] = int64(99)
	result.Metadata["values"].([]any)[0] = int64(99)
	result.Metadata["rows"].([][]any)[0][0] = int64(99)
	result.Rows[0][0].(map[string]any)["number"] = int64(99)
	result.Rows[0][1].([]any)[0] = int64(99)
	require.NotContains(t, input.Metadata, "new")
	require.Equal(t, int64(42), input.Metadata["nested"].(map[string]any)["number"])
	require.Equal(t, []any{int64(1)}, input.Metadata["values"])
	require.Equal(t, [][]any{{int64(2)}}, input.Metadata["rows"])
	require.Equal(t, [][]any{{map[string]any{"number": int64(7)}, []any{int64(8)}}}, input.Rows)
	result.Rows[0][0] = nil
	require.Equal(t, map[string]any{"number": int64(7)}, input.Rows[0][0])
	result.Rows[0] = nil
	require.Len(t, input.Rows[0], 2)
}

func TestRestoredResultContainerOwnership(t *testing.T) {
	t.Run("source fragments keep keys and unchanged nil containers", func(t *testing.T) {
		_, rewrite := canonicalizeQueryText(originalBraceView("EXPLAIN MATCH (n) WHERE n.x IS  NULL RETURN n"))
		require.NotNil(t, rewrite)
		var nilMap map[string]any
		var nilSlice []any
		var nilRows [][]any
		input := &ExecuteResult{Metadata: map[string]any{
			"n.x IS NULL": []any{map[string]any{"fragment": "n.x IS NULL", "number": int64(9007199254740993)}},
			"rows":        [][]any{{"n.x IS NULL"}, nil},
			"map":         nilMap, "slice": nilSlice, "nilRows": nilRows,
		}}
		result, err := (resultRestoration{source: rewrite}).restore(input, nil, nil)
		require.NoError(t, err)
		nested := result.Metadata["n.x IS NULL"].([]any)[0].(map[string]any)
		require.Equal(t, "n.x IS  NULL", nested["fragment"])
		require.Equal(t, int64(9007199254740993), nested["number"])
		require.Equal(t, [][]any{{"n.x IS  NULL"}, nil}, result.Metadata["rows"])
		require.Nil(t, result.Metadata["map"])
		require.Nil(t, result.Metadata["slice"])
		require.Nil(t, result.Metadata["nilRows"])
		require.Equal(t, "n.x IS NULL", input.Metadata["n.x IS NULL"].([]any)[0].(map[string]any)["fragment"])
		require.Equal(t, [][]any{{"n.x IS NULL"}, nil}, input.Metadata["rows"])
	})
	t.Run("quoted names restore keys and copy empty containers", func(t *testing.T) {
		_, names := canonicalizeQuotedVariablesWithBindings(t.Context(), "EXPLAIN WITH 1 AS `a b` RETURN `a b`", nil)
		require.NotNil(t, names)
		var internal string
		for identifier := range names.internal {
			internal = identifier
		}
		require.NotEmpty(t, internal)
		var nilMap map[string]any
		var nilSlice []any
		var nilRows [][]any
		input := &ExecuteResult{Metadata: map[string]any{
			internal: []any{map[string]any{"fragment": internal, "number": int64(9007199254740993)}},
			"rows":   [][]any{{internal}, nil},
			"map":    nilMap, "slice": nilSlice, "nilRows": nilRows,
		}}
		result, err := (resultRestoration{names: names}).restore(input, nil, nil)
		require.NoError(t, err)
		// Metadata is rendered Cypher, so the restored spelling includes quotes.
		nested := result.Metadata["`a b`"].([]any)[0].(map[string]any)
		require.Equal(t, "`a b`", nested["fragment"])
		require.Equal(t, int64(9007199254740993), nested["number"])
		require.Equal(t, [][]any{{"`a b`"}, {}}, result.Metadata["rows"])
		require.NotNil(t, result.Metadata["map"])
		require.Empty(t, result.Metadata["map"])
		require.NotNil(t, result.Metadata["slice"])
		require.Empty(t, result.Metadata["slice"])
		require.NotNil(t, result.Metadata["nilRows"])
		require.Empty(t, result.Metadata["nilRows"])
		require.Equal(t, internal, input.Metadata[internal].([]any)[0].(map[string]any)["fragment"])
		require.Equal(t, [][]any{{internal}, nil}, input.Metadata["rows"])
	})
}

var restorationBenchmarkResult *ExecuteResult

func BenchmarkResultRestorationContainers(b *testing.B) {
	_, rewrite := canonicalizeQueryText(originalBraceView("EXPLAIN MATCH (n) WHERE n.x IS  NULL RETURN n"))
	_, names := canonicalizeQuotedVariablesWithBindings(b.Context(), "EXPLAIN WITH 1 AS `a b` RETURN `a b`", nil)
	var internal string
	for identifier := range names.internal {
		internal = identifier
	}
	for _, tc := range []struct {
		name   string
		text   string
		quoted bool
	}{
		{"source-unchanged", "plain", false},
		{"source-changed", "n.x IS NULL", false},
		{"quoted", internal, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			input := &ExecuteResult{Metadata: map[string]any{"details": []any{map[string]any{"fragment": tc.text, "number": int64(42)}}, "rows": [][]any{{tc.text}, nil}}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if tc.quoted {
					restorationBenchmarkResult, err = (resultRestoration{names: names}).restore(input, nil, nil)
				} else {
					restorationBenchmarkResult, err = (resultRestoration{source: rewrite}).restore(input, nil, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
