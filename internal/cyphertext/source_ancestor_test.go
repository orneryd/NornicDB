package cyphertext

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSpanInAncestorPreservesOnlyUnchangedTokenIdentity(t *testing.T) {
	root := &Source{Original: "raw", Text: "raw"}
	generated := root.Rewrite("CALL x", []SourceEdit{{Original: Span{0, 3}, Accepted: Span{0, 6}}})
	prefixed := generated.Rewrite("prefix CALL x", []SourceEdit{{Original: Span{0, 0}, Accepted: Span{0, 7}}})
	window := prefixed.Window(7, 13)
	got, ok := window.SpanInAncestor(generated, Span{0, 4})
	require.True(t, ok)
	require.Equal(t, Span{0, 4}, got)
	_, ok = window.SpanInAncestor(root, Span{0, 4})
	require.False(t, ok, "generated token has no unchanged identity in raw input")
	_, ok = window.SpanInAncestor(&Source{Text: generated.Text}, Span{0, 4})
	require.False(t, ok, "equal spelling is not source ancestry")
	changed := generated.Rewrite("CALL x", []SourceEdit{{Original: Span{0, 4}, Accepted: Span{0, 4}}})
	_, ok = changed.SpanInAncestor(generated, Span{0, 4})
	require.False(t, ok, "replaced bytes do not acquire the old token's identity")
	_, ok = window.SpanInAncestor(generated, Span{-1, 4})
	require.False(t, ok)
}
