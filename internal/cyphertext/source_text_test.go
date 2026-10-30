package cyphertext

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSourceOriginalTextMappingAndRecovery(t *testing.T) {
	source := NewSource(t.Context(), `\u03bbX`, RawSource)
	require.NoError(t, source.Err)
	rewritten := source.Rewrite("internalX", []SourceEdit{{Original: Span{Start: 0, End: 2}, Accepted: Span{Start: 0, End: 8}}})
	text, ok := rewritten.OriginalText(Span{Start: 0, End: 8}, SourceExactBoundaries, nil)
	require.True(t, ok)
	require.Equal(t, `\u03bb`, text)
	text, ok = rewritten.OriginalText(Span{Start: 2, End: 3}, SourceExtent, nil)
	require.True(t, ok)
	require.Equal(t, `\u03bb`, text)
	called := false
	text, ok = rewritten.OriginalText(Span{Start: 2, End: 3}, SourceExactBoundaries, func(text string) string { called = true; return "recovered:" + text })
	require.True(t, ok)
	require.Equal(t, "recovered:t", text)
	require.True(t, called)
	text, ok = rewritten.OriginalText(Span{Start: 2, End: 3}, SourceExactBoundaries, nil)
	require.False(t, ok)
	require.Empty(t, text)
	text, ok = rewritten.Window(0, 9).OriginalText(Span{Start: 0, End: 9}, SourceExtent, nil)
	require.True(t, ok)
	require.Equal(t, `\u03bbX`, text)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := source.CodepointBoundaries(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSourceOriginalTextEmptyAndInvalidExtents(t *testing.T) {
	source := NewSource(t.Context(), "", AcceptedSource)
	text, ok := source.OriginalText(Span{}, SourceExtent, nil)
	require.True(t, ok)
	require.Empty(t, text, "a mapped empty span is valid")
	for _, span := range []Span{{Start: -1, End: 0}, {Start: 0, End: 1}, {Start: 1, End: 0}} {
		called := false
		text, ok = source.OriginalText(span, SourceExtent, func(string) string { called = true; return "unexpected" })
		require.False(t, ok)
		require.Empty(t, text)
		require.False(t, called, "invalid input extents cannot invoke recovery")
	}
	invalidOriginal := &Source{Original: "x", Text: "xx"}
	text, ok = invalidOriginal.OriginalText(Span{Start: 0, End: 2}, SourceExtent, nil)
	require.False(t, ok)
	require.Empty(t, text, "an invalid mapped original extent is not a valid empty string")
}
