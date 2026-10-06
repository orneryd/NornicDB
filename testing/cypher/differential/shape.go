package differential

import (
	"regexp"
	"strings"
)

// shape is what a statement's last RETURN clause says about its columns.
type shape struct {
	// sortKeys are the columns the RETURN's ORDER BY sorts on, when every
	// sort key is a returned column; nil otherwise.
	sortKeys []int
	// tokenColumns are the columns that return labels(…) or keys(…).
	tokenColumns []int
}

var (
	returnKeyword = regexp.MustCompile(`(?i)(?:^|[^.\w$])RETURN\b`)
	returnEnd     = regexp.MustCompile(`(?i)\b(ORDER\s+BY|SKIP|OFFSET|LIMIT|UNION)\b`)
	orderByEnd    = regexp.MustCompile(`(?i)\b(SKIP|OFFSET|LIMIT|UNION)\b`)
	distinctWord  = regexp.MustCompile(`(?i)^\s*DISTINCT\b`)
	aliasKeyword  = regexp.MustCompile(`(?i)\bAS\b`)
	sortDirection = regexp.MustCompile(`(?i)\s+(ASC|ASCENDING|DESC|DESCENDING)\s*$`)
	tokenFunction = regexp.MustCompile(`(?i)^(labels|keys)\s*\(`)
)

// returnShape reads query's last top-level RETURN clause. Its items match
// columns by position; a RETURN * or an item count that isn't the column
// count gives an empty shape.
func returnShape(query string, columns []string) shape {
	query, masked := maskNested(query)
	returns := returnKeyword.FindAllStringIndex(masked, -1)
	if len(returns) == 0 {
		return shape{}
	}
	start := returns[len(returns)-1][1]
	end := len(masked)
	if found := returnEnd.FindStringIndex(masked[start:]); found != nil {
		end = start + found[0]
	}
	if found := distinctWord.FindStringIndex(masked[start:end]); found != nil {
		start += found[1]
	}
	items := splitTopLevel(query[start:end], masked[start:end])
	if len(items) != len(columns) {
		return shape{}
	}
	var result shape
	expressions := make([]string, len(items))
	names := make([]string, len(items))
	for index, item := range items {
		expression, name := item.text, item.text
		if aliases := aliasKeyword.FindAllStringIndex(item.masked, -1); len(aliases) > 0 {
			last := aliases[len(aliases)-1]
			expression, name = item.text[:last[0]], item.text[last[1]:]
		}
		expressions[index], names[index] = normalizeText(expression), normalizeText(name)
		if expressions[index] == "*" {
			return shape{}
		}
		if tokenFunction.MatchString(expressions[index]) {
			result.tokenColumns = append(result.tokenColumns, index)
		}
	}
	if found := returnEnd.FindStringIndex(masked[end:]); found == nil || !strings.HasPrefix(strings.ToUpper(masked[end+found[0]:]), "ORDER") {
		return result
	}
	orderStart := end + len(returnEnd.FindString(masked[end:]))
	orderEnd := len(masked)
	if found := orderByEnd.FindStringIndex(masked[orderStart:]); found != nil {
		orderEnd = orderStart + found[0]
	}
	var keys []int
	for _, item := range splitTopLevel(query[orderStart:orderEnd], masked[orderStart:orderEnd]) {
		key := normalizeText(sortDirection.ReplaceAllString(item.text, ""))
		column := -1
		for index := range items {
			if key == names[index] || key == expressions[index] {
				column = index
				break
			}
		}
		if column < 0 {
			return result
		}
		keys = append(keys, column)
	}
	result.sortKeys = keys
	return result
}

// part is a piece of a statement and the same piece of its mask.
type part struct{ text, masked string }

// splitTopLevel splits text at the commas its mask has kept.
func splitTopLevel(text, masked string) []part {
	var parts []part
	start := 0
	for index := 0; index <= len(masked); index++ {
		if index == len(masked) || masked[index] == ',' {
			parts = append(parts, part{text: text[start:index], masked: masked[start:index]})
			start = index + 1
		}
	}
	return parts
}

// maskNested returns query with its // comments blanked out (clean), and clean
// with every byte inside a quoted string, a quoted name or brackets replaced
// by '_' (masked), so a keyword or comma left in masked is one of the
// statement's own. Both are as long as query.
func maskNested(query string) (clean, masked string) {
	blanked := []byte(query)
	hidden := []byte(query)
	depth := 0
	var quote byte
	for index := 0; index < len(query); index++ {
		char := query[index]
		switch {
		case quote != 0:
			if char == '\\' && quote != '`' && index+1 < len(query) {
				hidden[index] = '_'
				index++
			} else if char == quote {
				quote = 0
			}
		case char == '\'' || char == '"' || char == '`':
			quote = char
		case char == '/' && index+1 < len(query) && query[index+1] == '/':
			for ; index < len(query) && query[index] != '\n'; index++ {
				blanked[index], hidden[index] = ' ', ' '
			}
			continue
		case char == '(' || char == '[' || char == '{':
			depth++
		case char == ')' || char == ']' || char == '}':
			depth--
			hidden[index] = '_'
			continue
		}
		if quote != 0 || depth > 0 {
			hidden[index] = '_'
		}
	}
	return string(blanked), string(hidden)
}

// normalizeText is text with its spacing collapsed and one pair of
// surrounding backticks removed.
func normalizeText(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) >= 2 && text[0] == '`' && text[len(text)-1] == '`' {
		text = text[1 : len(text)-1]
	}
	return text
}
