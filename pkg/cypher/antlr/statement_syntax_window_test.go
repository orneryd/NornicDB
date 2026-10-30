// Author: Codex R17 implementation agent, 29 October 2026.
package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// Enumerate the compatibility contract independently of requested-extent
// traversal: canonical preorder, exact original extents and repeated spellings.
func TestStatementSyntaxRequestedExtentCompatibility(t *testing.T) {
	for _, source := range []string{
		"WITH 1 AS x CALL (x) { RETURN x AS y } RETURN y",
		"CALL () { RETURN 1 AS x } CALL () { RETURN 1 AS x } RETURN x",
		"RETURN /* 雪 */ ['é', '雪'] AS values",
	} {
		for _, stage := range []SourceStage{RawSource, AcceptedSource} {
			for _, dialect := range []bool{false, true} {
				syntax, err := ParseSyntax(context.Background(), source, StatementEntry, dialect, stage)
				require.NoError(t, err)
				expected := map[syntaxInspectionKey]syntaxInspection{}
				syntax.Walk(func(node SyntaxNode) func() {
					item := node.data.nodes[node.id]
					if item.rule < 0 || item.end <= item.start {
						return nil
					}
					span, exact := node.data.view.OriginalSpan(cyphertext.Span{Start: item.start, End: item.end}, cyphertext.SourceExactBoundaries)
					if !exact {
						return nil
					}
					put := func(entry SyntaxEntry) {
						key := syntaxInspectionKey{source: node.data.view.Original[span.Start:span.End], entry: entry}
						prior, found := expected[key]
						if found {
							if prior.start != span.Start || prior.end != span.End {
								prior.ambiguous = true
								expected[key] = prior
							}
							return
						}
						expected[key] = syntaxInspection{data: node.data, node: node.id, start: span.Start, end: span.End, codeStart: item.start, codeEnd: item.end}
					}
					put(fragmentEntry)
					if entry, ok := inspectionEntry(item.rule); ok {
						put(entry)
					}
					return nil
				})
				root := retainedSyntaxRoot{data: syntax.data}
				for key, want := range expected {
					got, found := root.inspection(key, false)
					require.True(t, found, "%q", key.source)
					require.Equal(t, want.ambiguous, got.ambiguous, "%q", key.source)
					if !want.ambiguous {
						require.Equal(t, want.node, got.node, "%q", key.source)
						require.Equal(t, want.start, got.start)
						require.Equal(t, want.end, got.end)
					}
				}
			}
		}
	}
}

func TestStatementSyntaxCodeSourceRetainsOccurrenceMapping(t *testing.T) {
	source := "WITH 1 AS x CALL (x) { RETURN /* 雪 */ x AS value } RETURN value"
	syntax, err := ParseSyntax(context.Background(), source, StatementEntry, false, RawSource)
	require.NoError(t, err)
	var returned SyntaxNode
	syntax.Walk(func(node SyntaxNode) func() {
		if node.Rule() == CypherParserRULE_returnSt && strings.Contains(node.Text(), "/* 雪 */") {
			returned = node
		}
		return nil
	})
	require.True(t, returned.Valid())
	view := returned.CodeSource(nil)
	require.Equal(t, returned.Code(), view.Text)
	original, ok := view.OriginalText(cyphertext.Span{Start: 0, End: len(view.Text)}, cyphertext.SourceExactBoundaries, nil)
	require.True(t, ok)
	require.Equal(t, returned.Text(), original)
	alias := strings.LastIndex(view.Text, "value")
	mapped, ok := view.OriginalSpan(cyphertext.Span{Start: alias, End: alias + len("value")}, cyphertext.SourceExactBoundaries)
	require.True(t, ok)
	require.Equal(t, strings.Index(source, "value"), mapped.Start)
	require.Equal(t, "value", source[mapped.Start:mapped.End])
	require.Equal(t, syntax.Origin(), returned.Statement().Origin())
	require.Same(t, syntax.data, returned.Statement().data)
}
