package antlr

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// Author: #1663 lane. The accepted arena and original presentation differ in
// width before the map. Source remains the only owner of that conversion.
func TestCodeSourceUsesAcceptedArenaExtent(t *testing.T) {
	raw := `RETURN '\u03bb' AS name, {x: 1} AS value`
	source := cyphertext.NewSource(t.Context(), raw, cyphertext.RawSource)
	require.NoError(t, source.Err)
	syntax, err := ParseSyntax(t.Context(), raw, StatementEntry, false, RawSource)
	require.NoError(t, err)
	var mapNode SyntaxNode
	syntax.Walk(func(node SyntaxNode) func() {
		if node.Rule() == CypherParserRULE_mapLit {
			mapNode = node
		}
		return nil
	})
	require.True(t, mapNode.Valid())
	view := mapNode.CodeSource(source)
	require.Equal(t, `{x: 1}`, view.Text)
	span, valid := view.OriginalSpan(cyphertext.Span{End: len(view.Text)}, cyphertext.SourceExtent)
	require.True(t, valid)
	start := strings.Index(raw, `{x: 1}`)
	require.Equal(t, cyphertext.Span{Start: start, End: start + len(`{x: 1}`)}, span)
	require.Same(t, source.OriginalView(), view.OriginalView())
}
