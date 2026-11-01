// Author: Codex R17 implementation agent, 27 October 2026.
package antlr

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestStatementSyntaxNodeReadersShareRetainedRoot(t *testing.T) {
	source := "MATCH p=(n:Node)-[:LINK]->(m) RETURN p {.*} AS result"
	ctx := WithNewStatementSyntax(context.Background(), source)
	first, err := QuerySyntax(ctx, ReadStatement(source), false, AcceptedSource)
	require.NoError(t, err)
	again, err := InspectSyntax(ctx, ReadStatement(source), false, AcceptedSource)
	require.NoError(t, err)
	require.Same(t, first.data, again.data)
	again.Walk(func(node SyntaxNode) func() { _ = node.Code(); return nil })
	_, err = LeadingCallSyntax(ctx, ReadStatement(source), false, AcceptedSource)
	require.NoError(t, err)
	require.Len(t, statementSyntaxScopeFrom(ctx).roots, 1)
}

func TestStatementSyntaxOriginalOwnerRejectsUnrelatedReplacements(t *testing.T) {
	original := "WITH 1 AS x CALL (x) { RETURN x AS originalValue } RETURN originalValue"
	ctx := WithNewStatementSyntax(context.Background(), original)
	whole, err := QuerySyntax(ctx, ReadStatement(original), false, AcceptedSource)
	require.NoError(t, err)
	window, err := InspectSyntax(ctx, ReadStatement("RETURN x AS originalValue"), false, AcceptedSource)
	require.NoError(t, err)
	for i := 0; i < 25; i++ {
		current := fmt.Sprintf("RETURN %d AS currentValue", i)
		ctx = WithStatementSyntax(ctx, original, current)
		_, err := QuerySyntax(ctx, ReadStatement(current), false, AcceptedSource)
		require.Error(t, err)
		repeated, err := InspectSyntax(ctx, ReadStatement("RETURN x AS originalValue"), false, AcceptedSource)
		require.NoError(t, err)
		require.Equal(t, window.Origin(), repeated.Origin())
		require.Same(t, &whole.data.nodes[0], &repeated.data.nodes[0])
		require.Len(t, statementSyntaxScopeFrom(ctx).roots, 1)
	}
}

func TestStatementSyntaxRawAndAcceptedViewsShareOneGrammarProduct(t *testing.T) {
	original := `W\u0049TH 1 AS x RETURN x`
	source := cyphertext.NewSource(context.Background(), original, RawSource)
	require.NoError(t, source.Err)
	ctx := cyphertext.WithSourceView(context.Background(), source)
	ctx = WithNewStatementSyntax(ctx, original, source.Text)
	raw, err := QuerySyntax(ctx, ReadStatement(original), false, RawSource)
	require.NoError(t, err)
	accepted, err := QuerySyntax(ctx, ReadStatement(source.Text), false, AcceptedSource)
	require.NoError(t, err)
	require.Same(t, &raw.data.nodes[0], &accepted.data.nodes[0])
	require.Equal(t, original, raw.Source())
	require.Equal(t, source.Text, accepted.Source())
	require.Len(t, statementSyntaxScopeFrom(ctx).roots, 1)
	_, err = QuerySyntax(ctx, ReadStatement(original), false, SourceStage(255))
	require.EqualError(t, err, "invalid source stage")
	require.Len(t, statementSyntaxScopeFrom(ctx).roots, 1)
}

func TestStatementSyntaxRawWindowUsesCanonicalSourceMapping(t *testing.T) {
	source := `W\u0049TH 1 AS x CALL (x) { RETURN x AS result } RETURN result`
	ctx := WithStatementSyntax(context.Background(), source)
	whole, err := ParseSyntax(ctx, ReadStatement(source), StatementEntry, false, RawSource)
	require.NoError(t, err)
	suffix := source[strings.Index(source, "CALL"):]
	call, err := LeadingCallSyntax(ctx, ReadStatement(suffix), false, RawSource)
	require.NoError(t, err)
	require.True(t, call.Valid())
	require.Same(t, &whole.data.nodes[0], &call.data.nodes[0])
	require.Equal(t, suffix, call.Source())
	start, end := call.Span()
	require.Zero(t, start)
	require.Equal(t, "CALL (x) { RETURN x AS result }", suffix[start:end])
	require.Equal(t, "CALL (x) { RETURN x AS result }", call.Code())
	for i := 0; i < (StatementSyntax{call.data}).TokenCount(); i++ {
		token := (StatementSyntax{call.data}).Token(i)
		start, end := token.Span()
		require.Equal(t, suffix[start:end], token.Text())
	}
}

func TestStatementSyntaxLazyViewsAreConcurrentAndPreserveEffects(t *testing.T) {
	source := "CALL () { CALL db.labels() YIELD label RETURN label } CREATE (:LazyEffects)"
	ctx := WithStatementSyntax(context.Background(), source)
	whole, err := ParseSyntax(ctx, ReadStatement(source), StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.True(t, whole.Effects().Writes())
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			view, err := InspectSyntax(ctx, ReadStatement("CALL db.labels() YIELD label RETURN label"), false, AcceptedSource)
			if err != nil {
				t.Error(err)
				return
			}
			if view.Effects().Writes() {
				t.Error("a view inherited the enclosing CREATE effect")
			}
			if view.Effects().CallCount() != 1 || view.Effects().Call(0).Name() != "db.labels" {
				t.Error("retained procedure effects were lost")
			}
		}()
	}
	wait.Wait()
}

func TestStatementSyntaxCanonicalPresentationKeepsOriginalArena(t *testing.T) {
	original := "\n RETURN 1 AS value \n"
	source := cyphertext.NewSource(t.Context(), original, AcceptedSource)
	ctx := cyphertext.WithSourceView(t.Context(), source)
	ctx = WithNewStatementSyntax(ctx, original)
	first, err := QuerySyntax(ctx, ReadStatement(original), false, AcceptedSource)
	require.NoError(t, err)
	current := source.Window(2, len(original)-2)
	ctx = cyphertext.WithSourceView(ctx, current)
	ctx = WithStatementSyntax(ctx, original, current.Text)
	second, err := QuerySyntax(ctx, ReadStatement(current.Text), false, AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, "RETURN 1 AS value", second.Source())
	require.Same(t, &first.data.nodes[0], &second.data.nodes[0])
	require.Len(t, statementSyntaxScopeFrom(ctx).roots, 1)
}

func TestStatementSyntaxUseCursorBelongsToItsSourceWindow(t *testing.T) {
	for _, prefix := range []string{"", "  "} {
		fragment := prefix + "USE comp.one MATCH (n:Example) RETURN n"
		whole := "WITH 1 AS seed CALL { " + fragment + " } RETURN n"
		ctx := WithNewStatementSyntax(t.Context(), whole)
		root, err := QuerySyntax(ctx, ReadStatement(whole), false, AcceptedSource)
		require.NoError(t, err)
		source := cyphertext.NewSource(ctx, fragment, AcceptedSource)
		reference, err := ReadUseReference(ctx, source)
		require.NoError(t, err)
		require.Equal(t, "comp.one", reference.Name)
		require.Equal(t, len(prefix+"USE comp.one"), reference.AcceptedEnd)
		require.Equal(t, reference.AcceptedEnd, reference.End)
		require.Equal(t, " MATCH (n:Example) RETURN n", fragment[reference.AcceptedEnd:])
		require.Len(t, statementSyntaxScopeFrom(ctx).roots, 1)
		require.Same(t, root.data, statementSyntaxScopeFrom(ctx).roots[0].data)
	}
}

func TestStatementSyntaxConcurrentSourceViewsKeepTheirOwnWindow(t *testing.T) {
	original := "CALL () { RETURN 1 AS a } CALL () { RETURN 2 AS b } CREATE (:OutsideView) RETURN a,b"
	source := cyphertext.NewSource(t.Context(), original, AcceptedSource)
	ctx := WithNewStatementSyntax(cyphertext.WithSourceView(t.Context(), source), original)
	whole, err := QuerySyntax(ctx, ReadStatement(original), false, AcceptedSource)
	require.NoError(t, err)
	require.True(t, whole.Effects().Writes())
	var wait sync.WaitGroup
	for _, body := range []string{"RETURN 1 AS a", "RETURN 2 AS b"} {
		start := strings.Index(original, body)
		current := source.Window(start, start+len(body)).Rewrite(strings.Replace(body, "RETURN ", "RETURN  ", 1), []cyphertext.SourceEdit{{Original: cyphertext.Span{Start: 6, End: 7}, Accepted: cyphertext.Span{Start: 6, End: 8}}})
		for i := 0; i < 4; i++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				local := cyphertext.WithSourceView(ctx, current)
				for n := 0; n < 10; n++ {
					view, err := QuerySyntax(local, ReadStatement(current.Text), false, AcceptedSource)
					if err != nil {
						t.Error(err)
						return
					}
					if view.Source() != current.Text || view.Effects().Writes() || &view.data.nodes[0] != &whole.data.nodes[0] {
						t.Errorf("source view escaped its retained window: %q", view.Source())
						return
					}
				}
			}()
		}
	}
	wait.Wait()
	require.Len(t, statementSyntaxScopeFrom(ctx).roots, 1)
}
