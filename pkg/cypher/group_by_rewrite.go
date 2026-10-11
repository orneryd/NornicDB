package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Cypher 25's GROUP BY (Neo4j 2026.02) names a RETURN or WITH's grouping
// keys. They may be projected items (by alias or expression) or expressions
// that aren't projected; every projected item that doesn't aggregate must be
// one, or be computed from them (n.name + '!' grouped by n.name). Without an
// aggregate, the rows are grouped all the same (one row per key).
//
// groupByEdits rewrites each as the implicit grouping every route runs:
//
//   - the keys are the items that don't aggregate, as written: GROUP BY is
//     dropped (and DISTINCT added when nothing aggregates);
//   - otherwise: WITH k1 AS g1, … , aggregates AS a1, … groups by the keys,
//     and the RETURN or WITH projects the items from them.

// groupByVariable names a grouping key or aggregate the rewrite projects; a *
// projection doesn't list it (isGeneratedVariable).
func groupByVariable(kind string, index int) string {
	return generatedVariablePrefix + kind + strconv.Itoa(index)
}

// groupByEdits rewrites each RETURN / WITH … GROUP BY ….
func groupByEdits(query string) ([]labelRewriteEdit, error) {
	if !containsFold(query, "GROUP") {
		return nil, nil
	}
	var edits []labelRewriteEdit
	words := projectionWords(query)
	for index, word := range words {
		if word.upper != "GROUP" || index+1 >= len(words) || words[index+1].upper != "BY" || words[index+1].depth != word.depth {
			continue
		}
		projection := -1
		for back := index - 1; back >= 0; back-- {
			if words[back].depth == word.depth && (words[back].upper == "RETURN" || words[back].upper == "WITH") && !precededByWord(query, words[back].start, "STARTS", "ENDS") {
				projection = back
				break
			}
		}
		if projection < 0 {
			continue
		}
		keysStart, keysEnd := words[index+1].end, len(query)
		for _, next := range words[index+2:] {
			if next.depth < word.depth {
				keysEnd = skipBackSpaces(query, next.start)
				break
			}
			if next.depth == word.depth && groupByKeysEnd(next.upper, words[projection].upper) {
				keysEnd = next.start
				break
			}
		}
		if closing := enclosingClose(query, word.start); closing >= 0 && closing < keysEnd {
			keysEnd = closing
		}
		edit, err := groupByEdit(query, words[projection], word.start, keysStart, keysEnd)
		if err != nil {
			return nil, err
		}
		edits = append(edits, edit)
	}
	return edits, nil
}

// groupByKeysEnd reports whether a word ends a GROUP BY's keys: a
// projection modifier, a WITH's WHERE, a UNION or the next clause.
func groupByKeysEnd(upper, clause string) bool {
	switch upper {
	case "ORDER", "SKIP", "OFFSET", "LIMIT", "UNION", "NEXT":
		return true
	case "WHERE":
		return clause == "WITH"
	}
	for _, keyword := range queryStructureClauseStarts {
		if upper == keyword {
			return true
		}
	}
	return false
}

// projectionWord is a word outside quotes with its bracket depth.
type projectionWord struct {
	start, end, depth int
	upper             string
}

func projectionWords(query string) []projectionWord {
	var words []projectionWord
	depth := 0
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case c == '(' || c == '[' || c == '{':
			depth++
		case c == ')' || c == ']' || c == '}':
			depth--
		case isIdentByte(c) && !isDigitByte(c):
			if i > 0 && (isIdentByte(query[i-1]) || query[i-1] == '$' || query[i-1] == '.') {
				continue
			}
			j := i
			for j < len(query) && isIdentByte(query[j]) {
				j++
			}
			words = append(words, projectionWord{start: i, end: j, depth: depth, upper: strings.ToUpper(query[i:j])})
			i = j - 1
		}
	}
	return words
}

// enclosingClose is the index of the bracket that closes the one enclosing
// query[at], -1 at the top level.
func enclosingClose(query string, at int) int {
	depth := 0
	for i := at; i < len(query); i++ {
		switch c := query[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}

// groupByItem is a projected item: its expression and column name, and
// whether it aggregates.
type groupByItem struct {
	expression, alias string
	aggregates        bool
}

// groupByEdit rewrites the projection whose keyword is clause, with GROUP
// at groupStart and its keys at query[keysStart:keysEnd].
func groupByEdit(query string, clause projectionWord, groupStart, keysStart, keysEnd int) (labelRewriteEdit, error) {
	itemsStart := skipASCIISpaces(query, clause.end, groupStart)
	distinct := matchKeywordAt(query[:groupStart], itemsStart, "DISTINCT")
	if distinct {
		itemsStart += len("DISTINCT")
	}
	var items []groupByItem
	for _, text := range splitTopLevelComma(query[itemsStart:groupStart]) {
		expression, alias := projectionItemAlias(text)
		items = append(items, groupByItem{expression: expression, alias: alias, aggregates: containsAggregateFunc(expression)})
	}
	var keys []string
	keysText := strings.TrimSpace(query[keysStart:keysEnd])
	if keysText == "" {
		// GROUP BY with no keys: Neo4j's "Invalid input", at what follows.
		token := ""
		if rest := strings.Fields(query[keysEnd:]); len(rest) > 0 {
			token = rest[0]
		}
		return labelRewriteEdit{}, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInputExpectedExpression(token))
	}
	if keysText != "()" {
		for _, key := range splitTopLevelComma(keysText) {
			// A key that names a projected item is that item's expression.
			for _, item := range items {
				if !item.aggregates && item.alias == key {
					key = item.expression
					break
				}
			}
			if !containsString(keys, key) {
				keys = append(keys, key)
			}
		}
	}
	aggregating := false
	var plain []string
	for _, item := range items {
		if item.aggregates {
			aggregating = true
		} else if !containsString(plain, item.expression) {
			plain = append(plain, item.expression)
		}
	}
	// The keys are the plain items: the implicit grouping, as written.
	if sameStrings(keys, plain) {
		text := ""
		if !aggregating && !distinct {
			text = "DISTINCT "
		}
		text += strings.TrimRight(query[itemsStart:groupStart], " \t\r\n")
		if keysEnd < len(query) && !isASCIISpace(query[keysEnd]) {
			text += " "
		}
		return labelRewriteEdit{start: itemsStart, end: keysEnd, text: text}, nil
	}
	// WITH keys AS g…, aggregates AS a…, then the projection from them.
	var grouping, projected []string
	for index, key := range keys {
		grouping = append(grouping, key+" AS "+groupByVariable("gb", index))
	}
	for index, item := range items {
		column := labelExpressionNameText(item.alias)
		if item.aggregates {
			name := groupByVariable("agg", index)
			grouping = append(grouping, item.expression+" AS "+name)
			projected = append(projected, name+" AS "+column)
			continue
		}
		expression, covered := replaceGroupingKeys(item.expression, keys)
		if !covered {
			return labelRewriteEdit{}, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
				localization.CypherCoreGroupByImplicitGroupingKey(strings.Join(expressionVariables(expression), ", ")))
		}
		projected = append(projected, expression+" AS "+column)
	}
	// DISTINCT applies to the grouped rows, which their keys already make
	// distinct: projecting a key away doesn't remove a row (RETURN DISTINCT
	// n.name, count(*) GROUP BY n.name, n.age keeps both Ann rows in Neo4j).
	keyword := query[clause.start:clause.end]
	text := "WITH " + strings.Join(grouping, ", ") + " " + keyword + " " + strings.Join(projected, ", ")
	if keysEnd < len(query) && !isASCIISpace(query[keysEnd]) {
		text += " "
	}
	return labelRewriteEdit{start: clause.start, end: keysEnd, text: text}, nil
}

// projectionItemAlias splits a projected item into its expression and
// column name: the alias after its last top-level AS, or the expression.
func projectionItemAlias(item string) (string, string) {
	item = strings.TrimSpace(item)
	as := -1
	for _, word := range projectionWords(item) {
		if word.depth == 0 && word.upper == "AS" {
			as = word.start
		}
	}
	if as < 0 {
		return item, item
	}
	alias := strings.TrimSpace(item[as+len("AS"):])
	if name, end, ok := scanIdentifierToken(alias, 0); ok && end == len(alias) {
		alias = name
	}
	return strings.TrimSpace(item[:as]), alias
}

// replaceGroupingKeys writes each grouping key in expression as its
// variable; covered is false when a variable is left over (the item isn't
// computed from the keys).
func replaceGroupingKeys(expression string, keys []string) (string, bool) {
	for index, key := range keys {
		expression = replaceWholeExpression(expression, key, groupByVariable("gb", index))
	}
	return expression, len(expressionVariables(expression)) == 0
}

// replaceWholeExpression replaces each occurrence of old in text that isn't
// part of a longer name or property chain.
func replaceWholeExpression(text, old, replacement string) string {
	var out strings.Builder
	for i := 0; i < len(text); {
		if c := text[i]; c == '\'' || c == '"' || c == '`' {
			end := skipCypherQuotedText(text, i, c)
			out.WriteString(text[i:end])
			i = end
			continue
		}
		if strings.HasPrefix(text[i:], old) && (i == 0 || !isIdentByte(text[i-1]) && text[i-1] != '.') &&
			(i+len(old) == len(text) || !isIdentByte(text[i+len(old)]) && text[i+len(old)] != '.') {
			out.WriteString(replacement)
			i += len(old)
			continue
		}
		out.WriteByte(text[i])
		i++
	}
	return out.String()
}

// expressionVariables lists the variables expression reads: names that
// aren't function names, property keys, map keys, keywords or generated.
func expressionVariables(expression string) []string {
	var names []string
	for _, word := range projectionWords(expression) {
		name := expression[word.start:word.end]
		next := skipASCIISpaces(expression, word.end, len(expression))
		if next < len(expression) && (expression[next] == '(' || expression[next] == ':' && (next+1 >= len(expression) || expression[next+1] != ':')) {
			continue // a function name, or a map key
		}
		if isGeneratedVariable(name) || isExpressionKeyword(word.upper) {
			continue
		}
		if !containsString(names, name) {
			names = append(names, name)
		}
	}
	return names
}

func isExpressionKeyword(upper string) bool {
	switch upper {
	case "AND", "OR", "XOR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "STARTS", "ENDS", "WITH", "CONTAINS", "CASE", "WHEN",
		"THEN", "ELSE", "END", "DISTINCT", "AS", "TYPED", "NORMALIZED", "NFC", "NFD", "NFKC", "NFKD":
		return true
	}
	return false
}

// sameStrings reports whether a and b hold the same strings, in any order.
func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, value := range a {
		if !containsString(b, value) {
			return false
		}
	}
	return true
}
