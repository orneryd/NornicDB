package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

var statementPresentationRewrites = newBoundedCache[string, *queryRewrite](4096, boundedCacheStringShardIndex)

// canonicalizeStatementText preserves the written expressions that also name
// result columns. Removing their trivia can change UNION's column agreement
// before execution, even if a deferred display rewrite restores the final keys.
// The shared CST supplies these spans; normalization remains the existing edit
// owner, and no second scope or result-header analysis runs here.
func canonicalizeStatementText(ctx context.Context, source string) (string, *queryRewrite) {
	canonical, rewrite := canonicalizeQueryText(statementBraceView(ctx, source))
	return preserveStatementPresentation(ctx, source, canonical, rewrite)
}

// preserveStatementPresentation composes lexical normalization with the
// grammar-owned source spans that are public expression column names.
func preserveStatementPresentation(ctx context.Context, source, canonical string, rewrite *queryRewrite) (string, *queryRewrite) {
	if rewrite == nil {
		return canonical, nil
	}
	if prior, found := statementPresentationRewrites.get(source); found {
		if prior == nil {
			return source, nil
		}
		return prior.Text, prior
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if ctx.Err() != nil {
		return source, nil
	}
	if err != nil {
		// Normalization does not admit a statement. Its existing command or
		// syntax owner still receives this text and determines its outcome.
		return canonical, rewrite
	}
	protected := statementExpressionNameSpans(syntax)
	var output strings.Builder
	var kept []cyphertext.SourceEdit
	last, removed := 0, false
	for _, edit := range rewrite.Edits {
		preserve := false
		for _, span := range protected {
			if edit.Original.Start < span.end && edit.Original.End > span.start || edit.Original.Start == edit.Original.End && span.start <= edit.Original.Start && edit.Original.Start < span.end {
				preserve = true
				break
			}
		}
		if preserve {
			removed = true
			continue
		}
		output.WriteString(source[last:edit.Original.Start])
		start := output.Len()
		output.WriteString(canonical[edit.Accepted.Start:edit.Accepted.End])
		kept = append(kept, cyphertext.SourceEdit{Original: cyphertext.Span{Start: edit.Original.Start, End: edit.Original.End}, Accepted: cyphertext.Span{Start: start, End: output.Len()}})
		last = edit.Original.End
	}
	if !removed {
		if ctx.Err() != nil {
			return source, nil
		}
		statementPresentationRewrites.put(source, rewrite)
		return canonical, rewrite
	}
	output.WriteString(source[last:])
	canonical = output.String()
	if canonical == source {
		if ctx.Err() != nil {
			return source, nil
		}
		statementPresentationRewrites.put(source, nil)
		return source, nil
	}
	rewrite = &queryRewrite{Source: cyphertext.Source{Original: source, Text: canonical, Edits: kept}, verbatimColumns: rewrite.verbatimColumns, commentsAdmitted: rewrite.commentsAdmitted}
	if ctx.Err() != nil {
		return source, nil
	}
	statementPresentationRewrites.put(source, rewrite)
	return canonical, rewrite
}

// statementExpressionNameSpans identifies source whose trivia is an output name.
// Cache keys and presentation normalization preserve the same grammar-owned spans.
type statementPresentationSpan struct{ start, end int }

func statementExpressionNameSpans(syntax cypherantlr.StatementSyntax) []statementPresentationSpan {
	var protected []statementPresentationSpan
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if node.Rule() != cypherantlr.CypherParserRULE_projectionItem || node.ChildRule(cypherantlr.CypherParserRULE_symbol).Valid() {
			return nil
		}
		expression := node.ChildRule(cypherantlr.CypherParserRULE_expression)
		if !expression.Valid() {
			return nil
		}
		if _, variable := projectionVariableFromSyntax(expression); variable {
			return nil
		}
		start, end := expression.Span()
		protected = append(protected, statementPresentationSpan{start, end})
		return nil
	})
	return protected
}
