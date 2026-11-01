// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateProjectionSemantics checks WITH projection aliases first, then
// Neo4j's rule for a WITH or RETURN
// with DISTINCT or an aggregation (#907): its WHERE and ORDER BY see only
// what the projection carries. A variable there must be one of the
// projection's names, or sit inside an expression the projection projects
// (RETURN x + 1 AS y, count(*) ORDER BY x + 1; RETURN count(*) AS c ORDER BY
// count(*)). Anything else read from before the clause (ORDER BY x after
// RETURN x + 1 AS y, count(*); ORDER BY sum(x) after RETURN count(*)) is a
// SyntaxError. A projection with * carries every variable.
func validateProjectionSemantics(ctx context.Context, cypher string) error {
	clauses, ok := splitPipelineTextClauses(ctx, cypher)
	if !ok {
		return ctx.Err()
	}
	// Preserve statement admission order: every WITH alias/aggregation check
	// precedes projection-tail scope checks, even on an earlier RETURN.
	for _, clause := range clauses {
		if clause.kind != pipelineClauseWith {
			continue
		}
		if err := validateReturnAggregationSemantics(ctx, clause); err != nil {
			return err
		}
		seen := make(map[string]struct{})
		for _, itemSource := range projectionItemsFromSyntax(clause.syntax) {
			item := itemSource.syntax.Text()
			if item == "" || item == "{}" {
				continue
			}
			expression, alias := itemSource.expression, itemSource.alias
			if err := validateExpressionOperandCompleteness(expression); err != nil {
				return err
			}
			explicitAlias := itemSource.as > 0
			// true, false and null are literals, not variables, even when a
			// variable of that name was declared (isLiteralKeyword).
			if !explicitAlias && expression != "*" && (simpleSemanticIdentifier(expression) == "" || isLiteralKeyword(expression)) {
				if containsMalformedCreateClauseToken(expression) {
					continue
				}
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"NoExpressionAlias",
					"expressions in WITH must be explicitly aliased",
				)
			}
			name := normalizeProjectionColumnName(alias)
			if name == "*" {
				continue
			}
			if _, duplicate := seen[name]; duplicate {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"ColumnNameConflict",
					"multiple WITH items project the same column name: "+name,
				)
			}
			seen[name] = struct{}{}
		}
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
		if err := aggregatingProjectionScopeError(ctx, clause, keyword); err != nil {
			return err
		}
	}
	return nil
}

// aggregatingProjectionScopeError checks one WITH or RETURN clause
// (validateProjectionSemantics).
func aggregatingProjectionScopeError(ctx context.Context, clause pipelineClause, keyword string) error {
	_, tail := projectionSemanticBodyAndTail(clause.text, keyword)
	_, distinct := cutDistinct(clause.text[len(keyword):])
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return nil
	}
	items := projectionItemsFromSyntax(clause.syntax)
	aliases := make(map[string]struct{}, len(items)+1)
	aliases[projectedExpressionPlaceholder] = struct{}{}
	expressions := make([]string, 0, len(items))
	aggregates := false
	for _, itemSource := range items {
		expression, alias := itemSource.expression, itemSource.alias
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
	var spellings map[string]string
	spelled := false
	for _, part := range aggregatingProjectionTailExpressions(tail) {
		masked := strings.Join(strings.Fields(part), " ")
		if containsAggregateFunc(masked) {
			if !spelled {
				spellings, spelled = aggregateCallSpellings(expressions), true
			}
			masked = respellAggregateCalls(masked, spellings)
		}
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
	for index := 0; index < len(text); {
		if end, quoted := skipCypherLexicalToken(text, index); quoted {
			out.WriteString(text[index:end])
			index = end
			continue
		}
		c := text[index]
		end := index + len(expression)
		if strings.HasPrefix(text[index:], expression) &&
			(index == 0 || !(cyphertext.NamePartAt(text, index-1) || text[index-1] == '.' || text[index-1] == '$')) &&
			(end == len(text) || !cyphertext.NamePartAt(text, end)) {
			out.WriteString(projectedExpressionPlaceholder)
			index = end
			continue
		}
		out.WriteByte(c)
		index++
	}
	return out.String()
}
