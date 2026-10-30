// Author: Codex R17 implementation agent, 27 October 2026.
package antlr

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementSyntaxNodeReadersShareRetainedRoots(t *testing.T) {
	original := "/* submitted */ MATCH p=(n:Node)-[:LINK]->(m) RETURN p {.*} AS result"
	current := strings.TrimPrefix(original, "/* submitted */ ")
	_, err := InspectSyntax(context.Background(), original, false, AcceptedSource)
	require.NoError(t, err)
	ctx := WithStatementSyntax(context.Background(), original, current)
	products := make(map[string]*statementSyntaxData)
	for _, source := range []string{original, current} {
		syntax, err := QuerySyntax(ctx, source, false, AcceptedSource)
		require.NoError(t, err)
		products[source] = syntax.data
		syntax.Walk(func(node SyntaxNode) func() { _ = node.Code(); return nil })
		_, _ = syntax.Window(0, len(source), CypherParserRULE_subqueryBody)
		_, err = LeadingCallSyntax(ctx, source, false, AcceptedSource)
		require.NoError(t, err)
	}
	scope := statementSyntaxScopeFrom(ctx)
	require.Len(t, scope.roots, 2)
	for _, root := range scope.roots {
		require.Same(t, products[root.key.source], root.data, "node-bearing readers share the retained product")
	}
}

func TestStatementSyntaxOriginalSyntaxSurvivesCurrentReplacement(t *testing.T) {
	original := "WITH 1 AS x CALL (x) { RETURN x AS originalValue } RETURN originalValue"
	ctx := WithStatementSyntax(context.Background(), original)
	_, err := QuerySyntax(ctx, original, false, AcceptedSource)
	require.NoError(t, err)
	window, err := InspectSyntax(ctx, "RETURN x AS originalValue", false, AcceptedSource)
	require.NoError(t, err)
	scope := statementSyntaxScopeFrom(ctx)
	key := statementSyntaxKey{source: original, stage: AcceptedSource}
	root := scope.rootLocked(key)
	originalData := root.data
	for i := 0; i < 25; i++ {
		current := fmt.Sprintf("RETURN %d AS currentValue", i)
		ctx = WithStatementSyntax(ctx, original, current)
		_, err := QuerySyntax(ctx, current, false, AcceptedSource)
		require.NoError(t, err)
		repeated, err := InspectSyntax(ctx, "RETURN x AS originalValue", false, AcceptedSource)
		require.NoError(t, err)
		require.Same(t, &window.data.nodes[0], &repeated.data.nodes[0], "unchanged original views keep their CST storage")
		require.Equal(t, window.Origin(), repeated.Origin())
		require.Equal(t, window.Source(), repeated.Source())
		require.Same(t, root, scope.rootLocked(key))
		require.Same(t, originalData, root.data)
		require.Len(t, scope.roots, 2, "obsolete current roots must not accumulate")
		for _, unused := range scope.roots[len(scope.roots):cap(scope.roots)] {
			require.Nil(t, unused, "removed roots must not remain reachable through slice capacity")
		}
	}
}

func TestStatementSyntaxScopePartitionsExplicitStagesAndDialects(t *testing.T) {
	original := `RETURN '\u0061' AS originalValue`
	current := `RETURN '\u0062' AS currentValue`
	ctx := WithStatementSyntax(context.Background(), original, current)
	origins := make(map[SyntaxOrigin]bool)
	for _, source := range []string{original, current} {
		for _, stage := range []SourceStage{RawSource, AcceptedSource} {
			for _, dialect := range []bool{false, true} {
				syntax, err := QuerySyntax(ctx, source, dialect, stage)
				require.NoError(t, err)
				require.False(t, origins[syntax.Origin()], "separate grammar products must not share node namespaces")
				origins[syntax.Origin()] = true
			}
		}
	}
	scope := statementSyntaxScopeFrom(ctx)
	require.Len(t, scope.roots, 8)
	_, err := QuerySyntax(ctx, original, false, SourceStage(255))
	require.EqualError(t, err, "invalid source stage")
	require.Len(t, scope.roots, 8)
}

func TestStatementSyntaxRawWindowUsesCanonicalSourceMapping(t *testing.T) {
	source := `W\u0049TH 1 AS x CALL (x) { RETURN x AS result } RETURN result`
	ctx := WithStatementSyntax(context.Background(), source)
	whole, err := ParseSyntax(ctx, source, StatementEntry, false, RawSource)
	require.NoError(t, err)
	suffix := source[strings.Index(source, "CALL"):]
	call, err := LeadingCallSyntax(ctx, suffix, false, RawSource)
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
	whole, err := ParseSyntax(ctx, source, StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.True(t, whole.Effects().Writes())
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			view, err := InspectSyntax(ctx, "CALL db.labels() YIELD label RETURN label", false, AcceptedSource)
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
