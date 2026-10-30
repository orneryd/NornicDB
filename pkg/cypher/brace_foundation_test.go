package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestBraceFoundationAcceptedSourceAdmission(t *testing.T) {
	for _, query := range []string{
		"\ufeffEXPLAIN MATCH （a）--＞（b ｛k： a.k，｝） RETURN b AS out",
		"UNWIND [] AS x MATCH （a）--＞（b ｛k： a.k，｝） RETURN b AS out",
		"CYPHER 25 ｛ RETURN ｛k： 1，｝ AS out ｝ UNION ｛ RETURN {} AS out ｝",
	} {
		t.Run(query, func(t *testing.T) {
			require.True(t, isMapEntrySyntaxError(validateStaticMapKeys(query)))
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			_, err := executor.Execute(t.Context(), query, nil)
			require.True(t, isMapEntrySyntaxError(err), "public admission: %v", err)
			_, err = executor.executeInternal(t.Context(), query, nil)
			require.True(t, isMapEntrySyntaxError(err), "internal admission: %v", err)
			_, err = NewParser().Parse(query)
			require.True(t, isMapEntrySyntaxError(err), "parser admission: %v", err)
			_, err = NewASTBuilder().Build(query)
			require.True(t, isMapEntrySyntaxError(err), "AST admission: %v", err)
		})
	}
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	result, err := executor.Execute(t.Context(), "RET\u200bURN ｛café： $value｝ AS out", map[string]interface{}{"value": int64(7)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{map[string]interface{}{"café": int64(7)}}}, result.Rows)
}

func TestBraceFoundationParameterConsumerAgreement(t *testing.T) {
	for _, query := range []string{
		"RETURN $\u00a0x, $\u2003x, $\u0085x",
		"RETURN $café\u00a0AS x, $0, $a$b, $`a``b`",
		"RETURN $all(['A']), $any(['B']), $(x), a$b, $p",
	} {
		var references, adapted, tokens []cyphertext.Span
		cyphertext.ParameterReferences(query, func(_ string, start, end int) {
			references = append(references, cyphertext.Span{Start: start, End: end})
		})
		scanParameterReferences(query, func(_ int, start, end int) {
			adapted = append(adapted, cyphertext.Span{Start: start - 1, End: end})
		})
		reader := cyphertext.NewReader(query, 0)
		for token, ok := reader.Next(); ok; token, ok = reader.Next() {
			if token.Kind == cyphertext.Parameter {
				tokens = append(tokens, cyphertext.Span{Start: token.Start, End: token.End})
			}
		}
		require.Equal(t, references, tokens, query)
		require.Equal(t, references, adapted, query)
	}
}
