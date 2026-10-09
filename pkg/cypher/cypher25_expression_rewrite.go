package cypher

import (
	"strings"

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
//     apoc.map.fromPairs over nested list comprehensions that bind k and v;
//   - RETURN / WITH … GROUP BY keys: the grouping it names (groupByEdits).
//
// Each pass's edits are kept (queryRewrite), so columns and messages show the
// client's text.

func init() {
	cypherfn.Register(interpolateFunction, fnInterpolate)
}

// interpolateFunction converts an interpolated value to its text.
const interpolateFunction = "__nornic_interpolate"

// desugarCypher25Expressions rewrites the forms above in turn and returns
// the result with each pass's rewrite, in order (restore them in reverse).
func desugarCypher25Expressions(query string) (string, []*queryRewrite, error) {
	var rewrites []*queryRewrite
	for _, pass := range []func(string) ([]labelRewriteEdit, error){interpolationEdits, mapComprehensionEdits, projectionAllEdits, groupByEdits} {
		edits, err := pass(query)
		if err != nil {
			return query, nil, err
		}
		if len(edits) == 0 {
			continue
		}
		var rewrite *queryRewrite
		query, rewrite = applyLabelRewriteEdits(query, edits, true)
		rewrites = append(rewrites, rewrite)
	}
	return query, rewrites, nil
}

// projectionAllEdits drops the ALL of RETURN ALL and WITH ALL.
func projectionAllEdits(query string) ([]labelRewriteEdit, error) {
	if !containsFold(query, "ALL") {
		return nil, nil
	}
	var edits []labelRewriteEdit
	forEachQueryWord(query, func(start, end int) {
		word := query[start:end]
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
		if next == all+3 || next >= len(query) || !startsProjectedExpression(query, next) {
			return
		}
		if query[next] == '(' {
			if close := findMatchingParen(query, next); close > 0 {
				if _, _, _, _, comprehension := parseListComprehension(query[next+1 : close]); comprehension {
					return // all(x IN list WHERE …), the list predicate
				}
			}
		}
		edits = append(edits, labelRewriteEdit{start: all, end: next, text: ""})
	})
	return edits, nil
}

// startsProjectedExpression reports whether an expression, rather than a
// keyword that would make ALL a variable (RETURN all AS x, RETURN all ORDER
// BY …), starts at query[at].
func startsProjectedExpression(query string, at int) bool {
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
		if end >= len(word) && strings.EqualFold(query[end-len(word):end], word) && (end == len(word) || !isIdentByte(query[end-len(word)-1])) {
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
		case c == '\'' || c == '"' || c == '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case c == '/':
			if end := queryCommentEnd(query, i); end >= 0 {
				i = end - 1
			}
		case isIdentByte(c) && !isDigitByte(c):
			if i > 0 && (isIdentByte(query[i-1]) || query[i-1] == '$') {
				continue
			}
			j := i
			for j < len(query) && isIdentByte(query[j]) {
				j++
			}
			visit(i, j)
			i = j - 1
		}
	}
}

// interpolationEdits rewrites each s"…" / s'…' string as the concatenation
// of its parts.
func interpolationEdits(query string) ([]labelRewriteEdit, error) {
	var edits []labelRewriteEdit
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case c == '\'' || c == '"':
			if i > 0 && query[i-1]|0x20 == 's' && (i == 1 || !isIdentByte(query[i-2]) && query[i-2] != '.' && query[i-2] != '$') {
				text, end, err := interpolatedStringText(query, i)
				if err != nil {
					return nil, err
				}
				edits = append(edits, labelRewriteEdit{start: i - 1, end: end, text: text})
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
func interpolatedStringText(query string, open int) (string, int, error) {
	quote := query[open]
	var out, literal strings.Builder
	parts := 0
	flush := func() {
		if parts > 0 {
			out.WriteString(" + ")
		}
		out.WriteByte(quote)
		out.WriteString(literal.String())
		out.WriteByte(quote)
		literal.Reset()
		parts++
	}
	syntaxError := func(message localization.Message) (string, int, error) {
		return "", 0, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", message)
	}
	for i := open + 1; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '\\' && i+1 < len(query):
			if query[i+1] == '{' || query[i+1] == '}' {
				literal.WriteByte(query[i+1])
			} else {
				literal.WriteString(query[i : i+2])
			}
			i++
		case c == quote:
			if parts == 0 || literal.Len() > 0 {
				flush()
			}
			return "(" + out.String() + ")", i + 1, nil
		case c == '}':
			return syntaxError(localization.CypherCoreInvalidInput("}"))
		case c == '{':
			close := interpolationEnd(query, i)
			if close < 0 {
				return syntaxError(localization.CypherCoreInvalidInput("{"))
			}
			expression := strings.TrimSpace(query[i+1 : close])
			if expression == "" {
				return syntaxError(localization.CypherCoreInvalidInputExpectedExpression("}"))
			}
			// An interpolated string inside the expression.
			edits, err := interpolationEdits(expression)
			if err != nil {
				return "", 0, err
			}
			if len(edits) > 0 {
				expression, _ = applyLabelRewriteEdits(expression, edits, true)
			}
			flush()
			out.WriteString(" + " + interpolateFunction + "(" + expression + ")")
			i = close
		default:
			literal.WriteByte(c)
		}
	}
	return syntaxError(localization.CypherCoreInvalidInput(string(quote)))
}

// interpolationEnd is the index of the } that closes the interpolation
// opened at query[open], -1 when none does: braces nest, and quoted text
// (an inner interpolated string included) is skipped.
func interpolationEnd(query string, open int) int {
	depth := 0
	for i := open; i < len(query); i++ {
		switch c := query[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

// fnInterpolate is an interpolated value as text: a string, boolean, number,
// temporal value or duration as toString writes it; null for null; any other
// value is Neo4j's TypeError.
func fnInterpolate(ctx cypherfn.Context, args []string) (interface{}, error) {
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

// mapComprehensionEdits rewrites each {k: v IN map [WHERE p] | key: value}.
func mapComprehensionEdits(query string) ([]labelRewriteEdit, error) {
	if !strings.Contains(query, "|") {
		return nil, nil
	}
	var edits []labelRewriteEdit
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case c == '{':
			close := findMatchingDelimiter(query, i, '{', '}')
			if close < 0 {
				return edits, nil
			}
			if text, ok := mapComprehensionText(query[i+1 : close]); ok {
				edits = append(edits, labelRewriteEdit{start: i, end: close + 1, text: text})
				i = close
			}
		}
	}
	return edits, nil
}

// mapComprehensionText is the expression a map comprehension's body (the
// text between its braces) stands for; ok is false when body isn't one. A
// map comprehension inside its parts is rewritten too. Each key binds k, and
// a one-item comprehension over its value binds v:
//
//	apoc.map.fromPairs([k IN keys(m) WHERE any(v IN [m[k]] WHERE p) |
//	    [v IN [m[k]] | [key, value]][0]])
func mapComprehensionText(body string) (string, bool) {
	key, end, ok := scanIdentifierToken(body, skipASCIISpaces(body, 0, len(body)))
	colon := skipASCIISpaces(body, end, len(body))
	if !ok || colon >= len(body) || body[colon] != ':' {
		return "", false
	}
	value, end, ok := scanIdentifierToken(body, skipASCIISpaces(body, colon+1, len(body)))
	in := skipASCIISpaces(body, end, len(body))
	if !ok || !matchKeywordAt(body, in, "IN") {
		return "", false
	}
	rest := body[in+2:]
	bar := rowTopLevelPipeIndex(rest)
	if bar < 0 {
		return "", false
	}
	source, predicate := strings.TrimSpace(rest[:bar]), ""
	if where := topLevelKeywordIndex(source, "WHERE"); where >= 0 {
		source, predicate = strings.TrimSpace(source[:where]), strings.TrimSpace(source[where+len("WHERE"):])
	}
	projection := rest[bar+1:]
	keyEnd := topLevelByteIndex(projection, 0, len(projection), ':')
	if source == "" || keyEnd < 0 {
		return "", false
	}
	keyExpression, valueExpression := strings.TrimSpace(projection[:keyEnd]), strings.TrimSpace(projection[keyEnd+1:])
	if keyExpression == "" || valueExpression == "" {
		return "", false
	}
	// Map comprehensions nested in the parts.
	for _, part := range []*string{&source, &predicate, &keyExpression, &valueExpression} {
		if edits, _ := mapComprehensionEdits(*part); len(edits) > 0 {
			*part, _ = applyLabelRewriteEdits(*part, edits, true)
		}
	}
	keyName, valueName := labelExpressionNameText(key), labelExpressionNameText(value)
	entry := "[(" + source + ")[" + keyName + "]]"
	filter := ""
	if predicate != "" {
		filter = " WHERE any(" + valueName + " IN " + entry + " WHERE " + predicate + ")"
	}
	return "CASE WHEN (" + source + ") IS NULL THEN null ELSE apoc.map.fromPairs([" + keyName + " IN keys(" + source + ")" + filter +
		" | [" + valueName + " IN " + entry + " | [" + keyExpression + ", " + valueExpression + "]][0]]) END", true
}
