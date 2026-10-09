package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// cypherVersionKey carries the statement's Cypher language version ("5" or
// "25") to the subquery bodies it evaluates.
type cypherVersionKey struct{}

// withCypherVersion records the language version of the statement that
// starts here. A nested statement (a subquery body run on its own, with no
// CYPHER prefix) keeps its parent's version.
func withCypherVersion(ctx context.Context, query string) context.Context {
	if _, ok := ctx.Value(cypherVersionKey{}).(string); ok {
		return ctx
	}
	return context.WithValue(ctx, cypherVersionKey{}, cypherGrammarVersion(query))
}

// cypherVersionFromContext is the statement's language version, "5" by
// default.
func cypherVersionFromContext(ctx context.Context) string {
	if version, ok := ctx.Value(cypherVersionKey{}).(string); ok {
		return version
	}
	return "5"
}

// cypher5ImportGrouping applies Cypher 5's scoping of an EXISTS / COUNT /
// COLLECT { … } body's imported variables (the outer row's names) to the
// body's aggregating projections. In Cypher 5 the imports stay in scope
// through every clause, so:
//
//   - an aggregating WITH groups by the imports read after it: over no rows
//     it has no row, where Cypher 25 has one (WITH count(*) AS c RETURN c + g
//     is [] in Cypher 5, [1] in Cypher 25);
//   - a projected item that aggregates and reads an import outside its
//     aggregates is Neo4j 5's implicit-grouping error (RETURN count(*) + g).
//
// Cypher 25 aggregates over the body's rows alone, as the body runs.
func cypher5ImportGrouping(body string, imports map[string]interface{}) (string, error) {
	if len(imports) == 0 || !containsAggregateFunc(body) {
		return body, nil
	}
	words := projectionWords(body)
	type insertion struct {
		at   int
		text string
	}
	var insertions []insertion
	for index, word := range words {
		if word.depth != 0 || word.upper != "WITH" && word.upper != "RETURN" || precededByWord(body, word.start, "STARTS", "ENDS") {
			continue
		}
		itemsStart := skipASCIISpaces(body, word.end, len(body))
		if matchKeywordAt(body, itemsStart, "DISTINCT") {
			itemsStart = skipASCIISpaces(body, itemsStart+len("DISTINCT"), len(body))
		}
		itemsEnd := len(body)
		for _, next := range words[index+1:] {
			if next.depth == 0 && groupByKeysEnd(next.upper, "WITH") {
				itemsEnd = next.start
				break
			}
		}
		items := splitTopLevelComma(body[itemsStart:itemsEnd])
		aggregating := false
		projected := map[string]bool{}
		for _, item := range items {
			expression, alias := projectionItemAlias(item)
			projected[alias] = true
			spans := findAggregateSpans(expression)
			if len(spans) == 0 {
				continue
			}
			aggregating = true
			outside := expression
			for i := len(spans) - 1; i >= 0; i-- {
				outside = outside[:spans[i].start] + " " + outside[spans[i].end:]
			}
			for _, name := range expressionVariables(outside) {
				if _, imported := imports[name]; imported {
					return "", localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
						localization.CypherCoreGroupByImplicitGroupingKey(name))
				}
			}
		}
		if !aggregating || word.upper != "WITH" {
			continue
		}
		var keys []string
		for _, name := range expressionVariables(body[itemsEnd:]) {
			if _, imported := imports[name]; imported && !projected[name] {
				keys = append(keys, name)
			}
		}
		if len(keys) > 0 {
			insertions = append(insertions, insertion{at: itemsStart, text: strings.Join(keys, ", ") + ", "})
		}
	}
	for i := len(insertions) - 1; i >= 0; i-- {
		body = body[:insertions[i].at] + insertions[i].text + body[insertions[i].at:]
	}
	return body, nil
}
