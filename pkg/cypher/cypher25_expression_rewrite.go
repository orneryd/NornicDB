package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Cypher 25 expression forms (Neo4j 2025.06 to 2026.02), as expressions
// every evaluator already reads, rewritten once where a statement enters the
// executor:
//
//   - RETURN ALL / WITH ALL: the default projection, written out (as
//     DISTINCT's opposite); the ALL is dropped. RETURN all(x IN l WHERE p)
//     is still the list predicate;
//   - s"text {expr} text": the parts joined with +, each {expr} converted as
//     Neo4j converts an interpolated value (interpolatedValue); null in, null
//     out; \{ is a brace;
//   - {k: v IN map [WHERE p] | key: value}: a map built from each entry, as
//     __nornic_map_from_pairs over nested list comprehensions that bind k
//     and v (an internal function, so the form doesn't depend on APOC);
//   - RETURN / WITH … GROUP BY keys: the grouping it names (groupByEdits).
//
// Each pass's edits are kept (queryRewrite), so columns and messages show the
// client's text.

func init() {
	cypherfn.RegisterInvocation(interpolateFunction, fnInterpolate)
	cypherfn.RegisterInvocation(mapFromPairsFunction, fnMapFromPairs)
}

// interpolateFunction converts an interpolated value to its text.
const interpolateFunction = "__nornic_interpolate"

// mapFromPairsFunction builds a map comprehension's map from its [key,
// value] pairs.
const mapFromPairsFunction = "__nornic_map_from_pairs"

// desugarCypher25Expressions rewrites the forms above in turn and returns
// the result with each pass's rewrite, in order (restore them in reverse).
// cypher25 is the statement's version, which decides where RETURN ALL
// begins (projectionAllEdits).

func desugarCypher25ExpressionView(view braceView, cypher25 bool) (string, []*queryRewrite, error) {
	query := view.text()
	var rewrites []*queryRewrite
	projectionAll := func(view braceView) ([]labelRewriteEdit, error) { return projectionAllEdits(view, cypher25) }
	for _, pass := range []func(braceView) ([]labelRewriteEdit, error){interpolationViewEdits, mapComprehensionViewEdits, projectionAll, groupByEdits} {
		edits, err := pass(view)
		if err != nil {
			return query, nil, err
		}
		if len(edits) == 0 {
			continue
		}
		var rewrite *queryRewrite
		query, rewrite = applyLabelRewriteEdits(query, edits, true)
		rewrites = append(rewrites, rewrite)
		source := sourceRewriteView(view.source, rewrite)
		view.source = source
	}
	return query, rewrites, nil
}

// projectionAllEdits drops the ALL of RETURN ALL and WITH ALL; a DISTINCT
// after it is a variable, so an expression right after that DISTINCT is
// Neo4j's "Invalid input" (RETURN ALL DISTINCT g). In a Cypher 25 statement
// a sign or a list right after ALL, spaced or not, starts the projected
// expression (RETURN all + 1 is RETURN ALL +1, all[0] is ALL [0]), as in
// Neo4j 2026.09; a Cypher 5 statement reads them as the variable all
// (Neo4j 5.26).
func projectionAllEdits(view braceView, cypher25 bool) ([]labelRewriteEdit, error) {
	query := view.text()
	if indexASCIIFold(query, "all") < 0 {
		return nil, nil
	}
	var edits []labelRewriteEdit
	var err error
	forEachQueryWord(query, func(start, end int) {
		word := query[start:end]
		if err != nil {
			return
		}
		if !strings.EqualFold(word, "RETURN") && !strings.EqualFold(word, "WITH") {
			return
		}
		if strings.EqualFold(word, "WITH") && precededByWord(query, start, "STARTS", "ENDS") {
			return
		}
		all := skipASCIISpaces(query, end, len(query))
		if all == end || !matchKeywordAt(query, all, "ALL") {
			return
		}
		next := skipASCIISpaces(query, all+3, len(query))
		if next >= len(query) {
			return
		}
		// In Cypher 5, a complete binding clause ends WITH's all projection.
		// Cypher 25 retains its ALL modifier before keyword-shaped subscripts.
		if !cypher25 && equalFoldASCII(word, "WITH") {
			if _, clause := splitUnwindBody(view.window(next, len(query))); clause {
				return
			}
		}
		signOrList := cypher25 && strings.IndexByte("+-[", query[next]) >= 0
		if !signOrList && (next == all+3 || !startsProjectedExpression(query, next)) {
			return
		}
		if query[next] == '(' {
			if close := findMatchingParen(query, next); close > 0 {
				if _, _, _, _, comprehension := parseListComprehension(view.window(next+1, close)); comprehension {
					return // all(x IN list WHERE …), the list predicate
				}
			}
		}
		// After ALL, DISTINCT is a name (RETURN ALL distinct AS v), as in
		// Neo4j: it stays one, quoted.
		if matchKeywordAt(query, next, "DISTINCT") {
			distinctEnd := next + len("DISTINCT")
			if after := skipASCIISpaces(query, distinctEnd, len(query)); after > distinctEnd && after < len(query) && startsOperand(query, after) &&
				startsProjectedExpression(query, after) && !startsWithClauseKeyword(query[after:]) && !matchKeywordAt(query, after, "GROUP") {
				token := strings.Fields(query[after:])[0]
				if identifierEnd, identifier := cyphertext.ScanIdentifier(query, after); identifier {
					token = query[after:identifierEnd.End]
				}
				err = localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreInvalidInput(token))
				return
			}
			edits = append(edits, labelRewriteEdit{start: all, end: distinctEnd, text: "`" + query[next:distinctEnd] + "`"})
			return
		}
		edits = append(edits, labelRewriteEdit{start: all, end: next, text: ""})
	})
	return edits, err
}

// startsOperand reports whether an operand (a name, a number, a string or a
// parameter) starts with c.
func startsOperand(query string, at int) bool {
	c := query[at]
	return cyphertext.NameStartAt(query, at) || isDigitByte(c) || isCypherQuoteByte(c) || c == '$'
}

// startsProjectedExpression reports whether an expression, rather than a
// keyword that would make ALL a variable (RETURN all AS x, RETURN all ORDER
// BY …), starts at query[at].
func startsProjectedExpression(query string, at int) bool {
	if startsWithClauseAfterName(query[at:]) {
		return false
	}
	if strings.IndexByte(",;)}=<>+-*/%^.[|", query[at]) >= 0 {
		return false
	}
	for _, keyword := range []string{"AS", "ORDER", "SKIP", "LIMIT", "WHERE", "UNION", "AND", "OR", "XOR", "IS", "IN", "CONTAINS", "STARTS", "ENDS"} {
		if matchKeywordAt(query, at, keyword) {
			return false
		}
	}
	return true
}

// precededByWord reports whether one of words ends right before the
// whitespace before query[start].
func precededByWord(query string, start int, words ...string) bool {
	end := skipBackSpaces(query, start)
	for _, word := range words {
		if end >= len(word) && strings.EqualFold(query[end-len(word):end], word) && (end == len(word) || !cyphertext.NamePartAt(query, end-len(word)-1)) {
			return true
		}
	}
	return false
}

// forEachQueryWord calls visit with each word outside quotes and comments.
func forEachQueryWord(query string, visit func(start, end int)) {
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case isCypherQuoteByte(c):
			i = skipCypherQuotedText(query, i, c) - 1
		case c == '/':
			if end := queryCommentEnd(query, i); end >= 0 {
				i = end - 1
			}
		case cyphertext.NamePartAt(query, i) && !isDigitByte(c):
			if i > 0 && (cyphertext.NamePartAt(query, i-1) || query[i-1] == '$') {
				continue
			}
			j := i
			j = cyphertext.NamePartEnd(query, j)
			visit(i, j)
			i = j - 1
		}
	}
}

// interpolationEdits rewrites each s"…" / s'…' string as the concatenation
// of its parts.

func interpolationViewEdits(view braceView) ([]labelRewriteEdit, error) {
	query := view.text()
	var edits []labelRewriteEdit
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case isCypherBacktickByte(c):
			i = skipCypherQuotedText(query, i, c) - 1
		case isCypherStringQuoteByte(c):
			if i > 0 && query[i-1]|0x20 == 's' && (i == 1 || !cyphertext.NamePartAt(query, i-2) && query[i-2] != '.' && query[i-2] != '$') {
				value, end, err := interpolatedStringRewrite(view, i)
				if err != nil {
					return nil, err
				}
				edits = append(edits, labelRewriteEdit{start: i - 1, end: end, text: value.text, copies: value.copies, constructed: value.constructed, entries: value.entries})
				i = end - 1
				continue
			}
			i = skipCypherQuotedText(query, i, c) - 1
		case c == '/':
			if end := queryCommentEnd(query, i); end >= 0 {
				i = end - 1
			}
		}
	}
	return edits, nil
}

// interpolatedStringText reads the interpolated string whose quote is at
// query[open]: the expression it stands for and the index after it.

func interpolatedStringRewrite(view braceView, open int) (rewritePredicate, int, error) {
	query := view.text()
	delimiter := query[open]
	end, closed := view.stringEnd(open)
	var literal strings.Builder
	var parts []rewritePredicate
	flush := func() {
		parts = append(parts, generatedPredicate(string(delimiter)+literal.String()+string(delimiter)))
		literal.Reset()
	}
	syntaxError := func(message localization.Message) (rewritePredicate, int, error) {
		return rewritePredicate{}, 0, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", message)
	}
	if !closed {
		return syntaxError(localization.CypherCoreInvalidInput(string(delimiter)))
	}
	for i := open + 1; i < end-1; i++ {
		c := query[i]
		switch {
		case c == '\\' && i+1 < len(query):
			if cyphertext.IsOpenBrace(query[i+1]) || cyphertext.IsCloseBrace(query[i+1]) {
				literal.WriteByte(query[i+1])
			} else {
				literal.WriteString(query[i : i+2])
			}
			i++
		case cyphertext.IsCloseBrace(c):
			return syntaxError(localization.CypherCoreInvalidInput("}"))
		case cyphertext.IsOpenBrace(c):
			close, known := view.interpolationEnd(i)
			if !known {
				return syntaxError(localization.CypherCoreInvalidInput("{"))
			}
			span := cyphertext.TrimWhitespaceSpan(query, cyphertext.Span{Start: i + 1, End: close})
			if span.Start == span.End {
				return syntaxError(localization.CypherCoreInvalidInputExpectedExpression("}"))
			}
			edits, err := interpolationViewEdits(view.window(span.Start, span.End))
			if err != nil {
				return rewritePredicate{}, 0, err
			}
			text, rewrite := applyLabelRewriteEdits(query[span.Start:span.End], edits, true)
			value := rewritePredicate{text: text, copies: rewrite.copies, constructed: rewrite.constructed, entries: rewrite.entries}
			for index := range value.copies {
				value.copies[index].Original.Start += span.Start
				value.copies[index].Original.End += span.Start
			}
			flush()
			parts = append(parts, wrapRewritePredicate(interpolateFunction+"(", value, ")"))
			i = close
		default:
			literal.WriteByte(c)
		}
	}
	if len(parts) == 0 || literal.Len() > 0 {
		flush()
	}
	return wrapRewritePredicate("(", joinRewritePredicates(parts, " + "), ")"), end, nil
}

// interpolationEnd is the index of the } that closes the interpolation
// opened at query[open], -1 when none does: braces nest, and quoted text
// (an inner interpolated string included) is skipped.

// fnInterpolate is an interpolated value as text: a string, boolean, number,
// temporal value or duration as toString writes it; null for null; any other
// value is Neo4j's TypeError.
func fnInterpolate(ctx cypherfn.InvocationContext, args []string) (interface{}, error) {
	values, err := evalArgs(ctx, args)
	if err != nil || len(values) != 1 || values[0] == nil {
		return nil, err
	}
	switch values[0].(type) {
	case []interface{}, map[string]interface{}, *storage.Node, *storage.Edge, CypherPoint, *CypherPoint:
	default:
		if text, ok := convertToStringOrNull(values[0]).(string); ok {
			return text, nil
		}
	}
	return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherCoreInterpolationWrongType(valueTypeOf(values[0]).render(true)))
}

// fnMapFromPairs is the map a map comprehension builds from its [key, value]
// pairs, as Neo4j does: a later key replaces an earlier one, a null value is
// kept, and a key that isn't a string is a TypeError.
func fnMapFromPairs(ctx cypherfn.InvocationContext, args []string) (interface{}, error) {
	values, err := evalArgs(ctx, args)
	if err != nil || len(values) != 1 || values[0] == nil {
		return nil, err
	}
	pairs, _ := toInterfaceSlice(values[0])
	result := make(map[string]interface{}, len(pairs))
	for _, item := range pairs {
		pair, _ := toInterfaceSlice(item)
		if len(pair) != 2 {
			continue
		}
		key, isString := pair[0].(string)
		if !isString {
			return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
				localization.CypherCoreMapComprehensionKeyNotString(valueTypeOf(pair[0]).render(true)))
		}
		result[key] = pair[1]
	}
	return result, nil
}

// mapComprehensionEdits rewrites each {k: v IN map [WHERE p] | key: value}.
func mapComprehensionViewEdits(view braceView) ([]labelRewriteEdit, error) {
	query := view.text()
	if !strings.Contains(query, "|") {
		return nil, nil
	}
	var edits []labelRewriteEdit
	reader := cyphertext.NewReader(query, 0)
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		if !token.Is(query, "{") {
			continue
		}
		fact, found := view.at(token.Start)
		if !found || fact.kind != literalBrace {
			continue
		}
		close, found := view.close(token.Start)
		if !found {
			continue
		}
		if value, ok := mapComprehensionRewrite(view, token.End, close); ok {
			edits = append(edits, labelRewriteEdit{start: token.Start, end: close + 1, text: value.text, copies: value.copies, constructed: value.constructed, entries: value.entries})
			reader.Seek(close + 1)
		}
	}
	return edits, nil
}

// The comprehension grammar owns its bindings and expression semantics. Its
// parts retain exact windows of the original brace product through copying.
func mapComprehensionRewrite(view braceView, start, close int) (rewritePredicate, bool) {
	query := view.text()
	body := query[start:close]
	key, ok := cyphertext.ScanIdentifier(body, skipASCIISpaces(body, 0, len(body)))
	colon := skipASCIISpaces(body, key.End, len(body))
	if !ok || colon >= len(body) || body[colon] != ':' {
		return rewritePredicate{}, false
	}
	value, ok := cyphertext.ScanIdentifier(body, skipASCIISpaces(body, colon+1, len(body)))
	in := skipASCIISpaces(body, value.End, len(body))
	if !ok || !matchKeywordAt(body, in, "IN") {
		return rewritePredicate{}, false
	}
	restStart := start + in + 2
	rest := query[restStart:close]
	bar := rowTopLevelPipeIndex(rest)
	if bar < 0 {
		return rewritePredicate{}, false
	}
	sourceSpan := cyphertext.TrimWhitespaceSpan(query, cyphertext.Span{Start: restStart, End: restStart + bar})
	predicateSpan := cyphertext.Span{}
	if where := topLevelKeywordIndex(view.window(sourceSpan.Start, sourceSpan.End), "WHERE"); where >= 0 {
		predicateSpan = cyphertext.TrimWhitespaceSpan(query, cyphertext.Span{Start: sourceSpan.Start + where + len("WHERE"), End: sourceSpan.End})
		sourceSpan = cyphertext.TrimWhitespaceSpan(query, cyphertext.Span{Start: sourceSpan.Start, End: sourceSpan.Start + where})
	}
	projectionStart := restStart + bar + 1
	projection := query[projectionStart:close]
	keyEnd := topLevelByteIndex(projection, 0, len(projection), ':')
	if sourceSpan.Start == sourceSpan.End || keyEnd < 0 {
		return rewritePredicate{}, false
	}
	keySpan := cyphertext.TrimWhitespaceSpan(query, cyphertext.Span{Start: projectionStart, End: projectionStart + keyEnd})
	valueSpan := cyphertext.TrimWhitespaceSpan(query, cyphertext.Span{Start: projectionStart + keyEnd + 1, End: close})
	if keySpan.Start == keySpan.End || valueSpan.Start == valueSpan.End {
		return rewritePredicate{}, false
	}
	part := func(span cyphertext.Span) rewritePredicate {
		if span.Start == span.End {
			return rewritePredicate{}
		}
		window := view.window(span.Start, span.End)
		edits, _ := mapComprehensionViewEdits(window)
		text, rewrite := applyLabelRewriteEdits(window.text(), edits, true)
		result := rewritePredicate{text: text, copies: rewrite.copies, constructed: rewrite.constructed, entries: rewrite.entries}
		for index := range result.copies {
			result.copies[index].Original.Start += span.Start
			result.copies[index].Original.End += span.Start
		}
		return result
	}
	source, predicate, keyExpression, valueExpression := part(sourceSpan), part(predicateSpan), part(keySpan), part(valueSpan)
	keyName, valueName := labelExpressionNameText(key.Value), labelExpressionNameText(value.Value)
	entry := wrapRewritePredicate("[(", source, ")["+keyName+"]]")
	filter := rewritePredicate{}
	if predicate.text != "" {
		filter = joinRewritePredicates([]rewritePredicate{generatedPredicate(" WHERE any(" + valueName + " IN "), entry, generatedPredicate(" WHERE "), predicate, generatedPredicate(")")}, "")
	}
	return joinRewritePredicates([]rewritePredicate{
		generatedPredicate("CASE WHEN ("), source, generatedPredicate(") IS NULL THEN null ELSE " + mapFromPairsFunction + "([" + keyName + " IN keys("), source, generatedPredicate(")"), filter,
		generatedPredicate(" | [" + valueName + " IN "), entry, generatedPredicate(" | ["), keyExpression, generatedPredicate(", "), valueExpression, generatedPredicate("]][0]]) END"),
	}, ""), true
}
