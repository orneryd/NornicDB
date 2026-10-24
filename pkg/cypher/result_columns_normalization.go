package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

var statementPresentationRewrites = newBoundedCache[string, *queryRewrite](4096, boundedCacheStringShardIndex)

// canonicalizeStatementText preserves the written expressions that also name
// result columns. Removing their trivia can change UNION's column agreement
// before execution, even if a deferred display rewrite restores the final keys.
// The shared CST supplies these spans; normalization remains the existing edit
// owner, and no second scope or result-header analysis runs here.
func canonicalizeStatementText(source string) (string, *queryRewrite) {
	canonical, rewrite := canonicalizeQueryText(source)
	if rewrite == nil {
		return canonical, nil
	}
	if prior, found := statementPresentationRewrites.get(source); found {
		if prior == nil {
			return source, nil
		}
		return prior.canonical, prior
	}
	syntax, err := cypherantlr.ParseSyntax(context.Background(), source, cypherantlr.StatementEntry, false)
	if err != nil {
		// Normalization does not admit a statement. Its existing command or
		// syntax owner still receives this text and determines its outcome.
		return canonical, rewrite
	}
	type sourceSpan struct{ start, end int }
	var protected []sourceSpan
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
		protected = append(protected, sourceSpan{start, end})
		return nil
	})
	var output strings.Builder
	var kept []queryTextEdit
	last, removed := 0, false
	for _, edit := range rewrite.edits {
		preserve := false
		for _, span := range protected {
			if edit.origStart < span.end && edit.origEnd > span.start || edit.origStart == edit.origEnd && span.start <= edit.origStart && edit.origStart < span.end {
				preserve = true
				break
			}
		}
		if preserve {
			removed = true
			continue
		}
		output.WriteString(source[last:edit.origStart])
		start := output.Len()
		output.WriteString(canonical[edit.canonStart:edit.canonEnd])
		kept = append(kept, queryTextEdit{origStart: edit.origStart, origEnd: edit.origEnd, canonStart: start, canonEnd: output.Len()})
		last = edit.origEnd
	}
	if !removed {
		statementPresentationRewrites.put(source, rewrite)
		return canonical, rewrite
	}
	output.WriteString(source[last:])
	canonical = output.String()
	if canonical == source {
		statementPresentationRewrites.put(source, nil)
		return source, nil
	}
	rewrite = &queryRewrite{original: source, canonical: canonical, edits: kept, verbatimColumns: rewrite.verbatimColumns}
	statementPresentationRewrites.put(source, rewrite)
	return canonical, rewrite
}
