package cypher

import (
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateAggregatingProjectionScopes is Neo4j's rule for a WITH or RETURN
// with DISTINCT or an aggregation (#907): its WHERE and ORDER BY see only
// what the projection carries. A variable there must be one of the
// projection's names, or sit inside an expression the projection projects
// (RETURN x + 1 AS y, count(*) ORDER BY x + 1; RETURN count(*) AS c ORDER BY
// count(*)). Anything else read from before the clause (ORDER BY x after
// RETURN x + 1 AS y, count(*); ORDER BY sum(x) after RETURN count(*)) is a
// SyntaxError. A projection with * carries every variable.
func validateAggregatingProjectionScopes(cypher string) error {
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	for _, clause := range clauses {
		keyword := ""
		switch clause.kind {
		case pipelineClauseWith:
			keyword = "WITH"
		case pipelineClauseReturn:
			keyword = "RETURN"
		default:
			continue
		}
		if err := aggregatingProjectionScopeError(strings.TrimSpace(clause.text), keyword); err != nil {
			return err
		}
	}
	return nil
}

// aggregatingProjectionScopeError checks one WITH or RETURN clause
// (validateAggregatingProjectionScopes).
func aggregatingProjectionScopeError(clause, keyword string) error {
	body, tail := projectionSemanticBodyAndTail(clause, keyword)
	_, distinct := cutDistinct(clause[len(keyword):])
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return nil
	}
	items := splitTopLevelComma(body)
	aliases := make(map[string]struct{}, len(items)+1)
	aliases[projectedExpressionPlaceholder] = struct{}{}
	expressions := make([]string, 0, len(items))
	aggregates := false
	for _, item := range items {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(item))
		expression = strings.TrimSpace(expression)
		if expression == "*" {
			return nil
		}
		aggregates = aggregates || containsAggregateFunc(expression)
		aliases[normalizeProjectionColumnName(alias)] = struct{}{}
		expressions = append(expressions, strings.Join(strings.Fields(expression), " "))
	}
	if !aggregates && !distinct {
		return nil
	}
	// Longest first, so a projected x + 1 is masked whole before a
	// projected x inside it.
	sort.Slice(expressions, func(i, j int) bool { return len(expressions[i]) > len(expressions[j]) })
	// An aggregate call spelled otherwise (COUNT(distinct n) for a
	// projected count(DISTINCT n)) is the projected one.
	spellings := aggregateCallSpellings(expressions)
	for _, part := range aggregatingProjectionTailExpressions(tail) {
		masked := respellAggregateCalls(strings.Join(strings.Fields(part), " "), spellings)
		for _, expression := range expressions {
			masked = maskProjectedExpression(masked, expression)
		}
		for _, reference := range semanticFreeReferences(masked) {
			base := strings.SplitN(reference, ".", 2)[0]
			if _, projected := aliases[normalizeProjectionColumnName(base)]; projected {
				continue
			}
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UndefinedVariable",
				localization.CypherMatchingAggregationScopeVariable(base))
		}
	}
	return nil
}

// aggregatingProjectionTailExpressions are the expressions of a projection's
// WHERE and ORDER BY terms, in whichever order the clause has them (WITH x
// ORDER BY x LIMIT 1 WHERE x > 0); SKIP and LIMIT read no variables.
func aggregatingProjectionTailExpressions(tail string) []string {
	keywords := []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"}
	starts := make([]int, len(keywords))
	for i, keyword := range keywords {
		starts[i] = topLevelKeywordIndex(tail, keyword)
	}
	segment := func(i int) string {
		if starts[i] < 0 {
			return ""
		}
		begin := starts[i] + len(keywords[i])
		end := len(tail)
		for _, other := range starts {
			if other > starts[i] && other < end {
				end = other
			}
		}
		return tail[begin:end]
	}
	var parts []string
	if where := segment(0); strings.TrimSpace(where) != "" {
		parts = append(parts, where)
	}
	for _, term := range parseOrderByClause(segment(1)) {
		parts = append(parts, term.column)
	}
	return parts
}

// projectedExpressionPlaceholder stands for a projected expression in a
// masked WHERE or ORDER BY (maskProjectedExpression).
const projectedExpressionPlaceholder = "__nornic_projected"

// maskProjectedExpression replaces each whole occurrence of a projected
// expression in text, outside quotes, with projectedExpressionPlaceholder,
// a name the check counts as projected, so the variables inside it (and a
// property read on it: m.a after a projected m) don't count as read from
// before the projection. An occurrence
// is whole when the text around it doesn't continue an identifier or a
// property access (x is not inside n.x or xy).
func maskProjectedExpression(text, expression string) string {
	if expression == "" || !strings.Contains(text, expression) {
		return text
	}
	var out strings.Builder
	quote := byte(0)
	for index := 0; index < len(text); {
		c := text[index]
		if quote != 0 {
			out.WriteByte(c)
			if c == '\\' && quote != '`' && index+1 < len(text) {
				out.WriteByte(text[index+1])
				index += 2
				continue
			}
			if c == quote {
				quote = 0
			}
			index++
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
			out.WriteByte(c)
			index++
			continue
		}
		end := index + len(expression)
		if strings.HasPrefix(text[index:], expression) &&
			(index == 0 || !(isIdentCharByte(text[index-1]) || text[index-1] == '.' || text[index-1] == '$')) &&
			(end == len(text) || !isIdentCharByte(text[end])) {
			out.WriteString(projectedExpressionPlaceholder)
			index = end
			continue
		}
		out.WriteByte(c)
		index++
	}
	return out.String()
}
