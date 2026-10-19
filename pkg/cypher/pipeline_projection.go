package cypher

// Pipeline projection owns WITH/RETURN plans, aggregation, ordering and result windows.

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// pipelineApplyWith drops / renames binding keys according to a WITH clause.
// Supports:
//   - plain variables:     `WITH a, b`            carries each forward
//   - map placeholder:     `WITH o, {}`           drops the {} projection
//   - variable → alias:    `WITH a AS b`          renames
//   - expression → alias:  `WITH a.name AS n`     evaluates a row expression
//   - aggregate pass-thru: `WITH count(*) AS c`   counts current rows
//
// Projection expressions use the same row-expression operator as WHERE,
// RETURN, and ORDER BY so list, map, property, and postfix operations cannot
// diverge between pipeline clauses.
func (e *StorageExecutor) pipelineApplyWith(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	return e.pipelineApplyWithSource(ctx, rows, clause, pipelineRowsSource(rows), false)
}

func (e *StorageExecutor) pipelineApplyWithSource(ctx context.Context, rows []pipelineRow, clause string, source pipelineRowSource, rowsValidated bool) ([]pipelineRow, bool) {
	if plan, ok := parsePipelineRowWith(clause); ok {
		out := make([]pipelineRow, 0, len(rows))
		for _, row := range rows {
			projected, accepted, resolved := e.pipelineProjectWithRow(ctx, row, plan, nil, nil)
			if !resolved {
				return nil, false
			}
			if accepted {
				out = append(out, projected)
			}
		}
		return out, true
	}
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (WITH with WHERE with = 3, #894).
	body := strings.TrimSpace(clause)
	orderTerms := parseOrderByTerms(body)
	withSkip, withLimit := 0, -1
	if skipIndex := topLevelKeywordIndex(body, "SKIP"); skipIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(body, "SKIP"), rows)
		if !ok {
			return nil, false
		}
		withSkip = value
	}
	if limitIndex := topLevelKeywordIndex(body, "LIMIT"); limitIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(body, "LIMIT"), rows)
		if !ok {
			return nil, false
		}
		withLimit = value
	}
	postWithWhere := ""
	if whereIdx := topLevelKeywordIndex(body, "WHERE"); whereIdx >= 0 {
		postWithWhere = strings.TrimSpace(body[whereIdx+len("WHERE"):])
		body = strings.TrimSpace(body[:whereIdx])
	}
	// WITH … [ORDER BY …] SKIP / LIMIT n WHERE p filters the rows SKIP /
	// LIMIT keep, as Neo4j does ("take the first n, then filter"). Without
	// SKIP / LIMIT the filter can run per row, before ordering: the result is
	// the same.
	windowedWhere := ""
	if postWithWhere != "" && (withSkip > 0 || withLimit >= 0) {
		windowedWhere, postWithWhere = postWithWhere, ""
	}
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	withDistinct := false
	body, withDistinct = cutDistinct(pipelineClauseBody(body, "WITH"))
	if strings.TrimSpace(body) == "*" {
		out := make([]pipelineRow, 0, len(rows))
		for _, row := range rows {
			projected := make(pipelineRow, len(row))
			for name, value := range row {
				projected[name] = value
			}
			out = append(out, projected)
		}
		if withDistinct {
			// WITH DISTINCT * keeps one row per distinct set of the
			// variables in scope (#883).
			out = deduplicatePipelineRows(out, pipelineRowColumns(out))
		}
		out = e.filterPipelineRows(ctx, out, postWithWhere)
		if !e.orderPipelineRows(ctx, out, orderTerms) {
			return nil, false
		}
		return e.filterPipelineRows(ctx, applyPipelineWindow(out, withSkip, withLimit), windowedWhere), true
	}
	items := splitTopLevelComma(body)
	if len(items) == 0 {
		return rows, true
	}
	if strings.TrimSpace(items[0]) == "*" {
		// WITH *, items: the * is every variable in scope (#883).
		items = starProjectionItems(pipelineWildcardColumns(rows), items[1:])
	}

	type withProjection struct {
		expr          string
		alias         string
		aggregate     bool
		aggregateName string
		aggregateExpr string
		distinct      bool
	}
	projections := make([]withProjection, 0, len(items))
	projectionAliases := make([]string, 0, len(items))
	hasAggregate := false
	for _, rawItem := range items {
		item := strings.TrimSpace(rawItem)
		if item == "" || item == "{}" {
			continue
		}
		expr, alias := parseProjectionExprAlias(item)
		if expr == "" {
			return nil, false
		}
		projection := withProjection{expr: expr, alias: alias}
		if aggregateName, aggregateExpr, distinct, aggregate := parsePipelineAggregate(expr); aggregate {
			if !strings.Contains(upperASCII(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateName = aggregateName
			projection.aggregateExpr = aggregateExpr
			projection.distinct = distinct
			hasAggregate = true
		} else if pipelineExpressionContainsAggregate(expr) {
			if !strings.Contains(upperASCII(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateExpr = expr
			hasAggregate = true
		}
		projections = append(projections, projection)
		projectionAliases = append(projectionAliases, alias)
	}
	if hasAggregate {
		aggregates := make([]returnProjection, len(projections))
		for index, projection := range projections {
			aggregates[index] = returnProjection{expr: projection.expr, alias: projection.alias, isAggr: projection.aggregate}
		}
		groups, ok := e.pipelineAggregateGroups(ctx, source, aggregates, rowsValidated)
		if !ok {
			return nil, false
		}
		if len(orderTerms) > 0 {
			expressions := make([]string, len(projections))
			for index, projection := range projections {
				expressions[index] = projection.expr
			}
			orderTerms = alignOrderTerms(orderTerms, expressions)
		}

		out := make([]pipelineRow, 0, len(groups))
		needsOrderScopes := len(orderTerms) > 0 || postWithWhere != "" || withDistinct || windowedWhere != ""
		var orderScopes []pipelineRow
		if needsOrderScopes {
			orderScopes = make([]pipelineRow, 0, len(groups))
		}
		for _, group := range groups {
			newRow := pipelineRow{}
			var projectedExpressions pipelineRow
			if needsOrderScopes {
				projectedExpressions = make(pipelineRow, len(projections))
			}
			for name, value := range group.first {
				if strings.HasPrefix(name, "$") {
					newRow[name] = value
				}
			}
			for index, projection := range projections {
				value, ok := group.value(ctx, e, index)
				if !ok {
					pipelineItemUnevaluable(ctx, projection.expr)
					return nil, false
				}
				newRow[projection.alias] = value
				if needsOrderScopes {
					projectedExpressions[projection.expr] = value
				}
			}
			if !needsOrderScopes {
				out = append(out, newRow)
				continue
			}
			orderScope := make(pipelineRow, len(group.first)+len(projectedExpressions)+len(newRow))
			for name, value := range group.first {
				orderScope[name] = value
			}
			for expression, value := range projectedExpressions {
				orderScope[expression] = value
			}
			for name, value := range newRow {
				orderScope[name] = value
			}
			if postWithWhere != "" && !e.evaluateWithWhereCondition(ctx, postWithWhere, orderScope) {
				continue
			}
			out = append(out, newRow)
			orderScopes = append(orderScopes, orderScope)
		}
		if withDistinct {
			out, orderScopes = deduplicatePipelineRowsWithScopes(out, orderScopes, projectionAliases)
		}
		if !e.orderPipelineRowsWithScopes(ctx, out, orderScopes, orderTerms) {
			return nil, false
		}
		return e.filterPipelineWindow(ctx, out, orderScopes, withSkip, withLimit, windowedWhere), true
	}

	orderedExpressions := orderedProjectionExpressions(orderTerms, len(projections), func(index int) (string, string) {
		return projections[index].expr, projections[index].alias
	})
	out := make([]pipelineRow, 0, len(rows))
	needsOrderScopes := len(orderTerms) > 0 || withDistinct || windowedWhere != ""
	var orderScopes []pipelineRow
	if needsOrderScopes {
		orderScopes = make([]pipelineRow, 0, len(rows))
	}
	rowPlan := pipelineRowWith{where: postWithWhere}
	for _, projection := range projections {
		rowPlan.projections = append(rowPlan.projections, pipelineRowProjection{projection.expr, projection.alias})
	}
	for _, row := range rows {
		var scope pipelineRow
		if postWithWhere != "" || needsOrderScopes {
			scope = make(pipelineRow, len(row)+len(projections))
		}
		newRow, accepted, ok := e.pipelineProjectWithRow(ctx, row, rowPlan, nil, scope)
		if !ok {
			return nil, false
		}
		if !accepted {
			continue
		}
		out = append(out, newRow)
		if needsOrderScopes {
			// ORDER BY may name a projected expression by its text; its value
			// is the projection's (orderedProjectionExpressions). A projected
			// name keeps its own value.
			for _, ordered := range orderedExpressions {
				if _, projected := newRow[ordered.expression]; !projected {
					scope[ordered.expression] = newRow[ordered.alias]
				}
			}
			orderScopes = append(orderScopes, scope)
		}
	}
	if withDistinct {
		out, orderScopes = deduplicatePipelineRowsWithScopes(out, orderScopes, projectionAliases)
	}
	if !e.orderPipelineRowsWithScopes(ctx, out, orderScopes, orderTerms) {
		return nil, false
	}
	return e.filterPipelineWindow(ctx, out, orderScopes, withSkip, withLimit, windowedWhere), true
}

// filterPipelineWindow applies SKIP / LIMIT to rows, then keeps the rows of
// the window whose scope (the row's projected and incoming values) satisfies
// whereClause: the WHERE after WITH … SKIP / LIMIT.
func (e *StorageExecutor) filterPipelineWindow(ctx context.Context, rows, scopes []pipelineRow, skip, limit int, whereClause string) []pipelineRow {
	if whereClause == "" {
		return applyPipelineWindow(rows, skip, limit)
	}
	start, end := 0, len(rows)
	if skip > 0 {
		start = skip
		if start > end {
			start = end
		}
	}
	if limit >= 0 && start+limit < end {
		end = start + limit
	}
	filtered := make([]pipelineRow, 0, end-start)
	for index := start; index < end; index++ {
		if e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(scopes[index])) {
			filtered = append(filtered, rows[index])
		}
	}
	return filtered
}

func pipelinePaginationExpression(body, keyword string) string {
	index := topLevelKeywordIndex(body, keyword)
	if index < 0 {
		return ""
	}
	expression := strings.TrimSpace(body[index+len(keyword):])
	end := len(expression)
	// WITH … SKIP / LIMIT n WHERE p: the WHERE isn't part of n.
	for _, nextKeyword := range []string{"SKIP", "LIMIT", "WHERE"} {
		if nextIndex := topLevelKeywordIndex(expression, nextKeyword); nextIndex >= 0 && nextIndex < end {
			end = nextIndex
		}
	}
	return strings.TrimSpace(expression[:end])
}

func (e *StorageExecutor) evaluatePipelinePagination(ctx context.Context, expression string, rows []pipelineRow) (int, bool) {
	// SKIP / LIMIT see the statement's parameters ($l) whatever the
	// projection kept in the row.
	values := e.parameterRow(ctx)
	if len(rows) > 0 {
		for name, value := range rows[0] {
			values[name] = value
		}
	}
	value, evaluated := e.evaluateRowExpressionWithContext(ctx, expression, values)
	if !evaluated {
		return 0, false
	}
	integer, valid := cypherIntegerValue(value)
	if !valid || integer < 0 || int64(int(integer)) != integer {
		return 0, false
	}
	return int(integer), true
}

// orderPipelineRows applies every ORDER BY term lexicographically. WITH has
// already materialized its projection at this point, so aliases and retained
// entity properties resolve from the same scope exposed to the next clause.
func (e *StorageExecutor) orderPipelineRows(ctx context.Context, rows []pipelineRow, terms []orderByTerm) bool {
	return e.orderPipelineRowsWithScopes(ctx, rows, rows, terms)
}

// orderedProjection is a projection expression an ORDER BY term repeats, with
// the alias of the projected column.
type orderedProjection struct {
	expression string
	alias      string
}

// orderedProjectionExpressions returns the projection items (count of them,
// read by item) whose expression an ORDER BY term repeats, as in
// RETURN size(n.s) AS n ORDER BY size(n.s). Neo4j orders such a term by the
// projected column even when an alias shadows a variable of the expression,
// so the order scope maps the expression text to the projected value, as the
// aggregating WITH does. Nil when no term repeats one.
func orderedProjectionExpressions(terms []orderByTerm, count int, item func(index int) (expression, alias string)) []orderedProjection {
	var ordered []orderedProjection
	for _, term := range terms {
		column := strings.TrimSpace(term.column)
		for index := 0; index < count; index++ {
			expression, alias := item(index)
			if expression != alias && strings.TrimSpace(expression) == column {
				ordered = append(ordered, orderedProjection{expression: column, alias: alias})
				break
			}
		}
	}
	return ordered
}

// alignOrderTerms respells the aggregate calls of ORDER BY terms as the
// projection expressions (expressions) spell them, and a term that is a
// projection expression up to that spelling and whitespace as that
// expression: COUNT(*) DESC after RETURN k, count(*) AS c reads count(*).
// The order scope maps a projection's text to its value, so the term then
// reads the projected value, as in Neo4j, where a function name's case and
// the whitespace between tokens don't change an expression. Terms are
// returned unchanged (not copied) when nothing differs.
func alignOrderTerms(terms []orderByTerm, expressions []string) []orderByTerm {
	spellings := aggregateCallSpellings(expressions)
	var aligned []orderByTerm
	for index, term := range terms {
		column := respellAggregateCalls(term.column, spellings)
		key := stripExpressionWhitespace(column)
		for _, expression := range expressions {
			if expression != column && stripExpressionWhitespace(expression) == key {
				column = expression
				break
			}
		}
		if column == term.column {
			continue
		}
		if aligned == nil {
			aligned = append([]orderByTerm(nil), terms...)
		}
		aligned[index].column = column
	}
	if aligned == nil {
		return terms
	}
	return aligned
}

// aggregateCallSpellings maps the key (aggregateCallKey) of each aggregate
// call in expressions to its text there. Nil when there is none.
func aggregateCallSpellings(expressions []string) map[string]string {
	var spellings map[string]string
	for _, expression := range expressions {
		for _, call := range semanticAggregateCalls(expression) {
			if spellings == nil {
				spellings = make(map[string]string)
			}
			spellings[aggregateCallKey(call)] = call
		}
	}
	return spellings
}

// respellAggregateCalls writes each aggregate call of text that spellings
// holds as spellings spells it: COUNT(distinct n) as count(DISTINCT n).
func respellAggregateCalls(text string, spellings map[string]string) string {
	for _, call := range semanticAggregateCalls(text) {
		if spelling, ok := spellings[aggregateCallKey(call)]; ok && spelling != call {
			text = strings.Replace(text, call, spelling, 1)
		}
	}
	return text
}

// aggregateCallKey is an aggregate call with its function name and a
// leading DISTINCT upper-cased and whitespace outside quotes removed: two
// calls with one key are one expression. call is a call semanticAggregateCalls
// found: a name, then its parenthesized arguments.
func aggregateCallKey(call string) string {
	open := strings.IndexByte(call, '(')
	arguments := strings.TrimSpace(call[open+1:])
	if len(arguments) > len("DISTINCT") && strings.EqualFold(arguments[:len("DISTINCT")], "DISTINCT") && !isIdentByte(arguments[len("DISTINCT")]) {
		arguments = "DISTINCT " + arguments[len("DISTINCT"):]
	}
	return upperASCII(strings.TrimSpace(call[:open])) + "(" + stripExpressionWhitespace(arguments)
}

// stripExpressionWhitespace removes the whitespace outside quoted text, but
// for one space between two identifier characters, so NOT a stays apart
// from NOTa.
func stripExpressionWhitespace(expression string) string {
	expression = strings.TrimSpace(expression)
	if strings.IndexAny(expression, " \t\n\r") < 0 {
		return expression
	}
	var stripped strings.Builder
	stripped.Grow(len(expression))
	var quote byte
	for index := 0; index < len(expression); index++ {
		current := expression[index]
		switch {
		case quote != 0:
			if current == '\\' && quote != '`' && index+1 < len(expression) {
				stripped.WriteByte(current)
				index++
				current = expression[index]
			} else if current == quote {
				quote = 0
			}
		case current == '\'' || current == '"' || current == '`':
			quote = current
		case isASCIIWhitespace(current):
			next := index + 1
			for next < len(expression) && isASCIIWhitespace(expression[next]) {
				next++
			}
			if isIdentByte(expression[index-1]) && isIdentByte(expression[next]) {
				stripped.WriteByte(' ')
			}
			index = next - 1
			continue
		}
		stripped.WriteByte(current)
	}
	return stripped.String()
}

// orderPipelineRowsWithScopes is orderPipelineRows with each row's terms
// evaluated in scopes[i]. A term the row evaluator can't resolve returns
// false; when an operator of it failed (1/0, a runtime TypeError) that failure
// is recorded as the statement's error, as for a projection item.
func (e *StorageExecutor) orderPipelineRowsWithScopes(ctx context.Context, rows, scopes []pipelineRow, terms []orderByTerm) bool {
	if len(terms) == 0 || len(rows) == 0 {
		return true
	}
	if len(rows) != len(scopes) {
		return false
	}
	type orderValue struct {
		values []interface{}
		row    pipelineRow
	}
	ordered := make([]orderValue, 0, len(rows))
	for index, row := range rows {
		values := make([]interface{}, len(terms))
		for termIndex, term := range terms {
			// The projections' evaluator: subquery values (COUNT { … }) and
			// recorded failures behave as in a RETURN item.
			value, ok := e.evaluateRowExpressionWithContext(ctx, term.column, scopes[index])
			if !ok {
				return false
			}
			values[termIndex] = value
		}
		ordered = append(ordered, orderValue{values: values, row: row})
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		for index, term := range terms {
			comparison := compareValuesForSort(ordered[left].values[index], ordered[right].values[index])
			if comparison == 0 {
				continue
			}
			if term.descending {
				return comparison > 0
			}
			return comparison < 0
		}
		return false
	})
	for index := range rows {
		rows[index] = ordered[index].row
	}
	return true
}

func applyPipelineWindow(rows []pipelineRow, skip, limit int) []pipelineRow {
	if skip >= len(rows) {
		return []pipelineRow{}
	}
	if skip > 0 {
		rows = rows[skip:]
	}
	if limit >= 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

func (e *StorageExecutor) filterPipelineRows(ctx context.Context, rows []pipelineRow, whereClause string) []pipelineRow {
	if whereClause == "" {
		return rows
	}
	filtered := rows[:0]
	for _, row := range rows {
		if e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(row)) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// pipelineRowColumns returns the variables bound in rows, sorted: the
// columns of WITH *.
func pipelineRowColumns(rows []pipelineRow) []string {
	seen := make(map[string]struct{})
	columns := make([]string, 0)
	for _, row := range rows {
		for name := range row {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				columns = append(columns, name)
			}
		}
	}
	sort.Strings(columns)
	return columns
}

func deduplicatePipelineRows(rows []pipelineRow, columns []string) []pipelineRow {
	unique, _ := deduplicatePipelineRowsWithScopes(rows, rows, columns)
	return unique
}

func deduplicatePipelineRowsWithScopes(rows, scopes []pipelineRow, columns []string) ([]pipelineRow, []pipelineRow) {
	seen := make(map[string]struct{}, len(rows))
	unique := make([]pipelineRow, 0, len(rows))
	uniqueScopes := make([]pipelineRow, 0, len(rows))
	keys := make([]string, len(columns))
	for rowIndex, row := range rows {
		for i, column := range columns {
			keys[i] = cypherValueKey(row[column], cypherEquivalenceContract)
		}
		key := strings.Join(keys, "\x1f")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, row)
		uniqueScopes = append(uniqueScopes, scopes[rowIndex])
	}
	return unique, uniqueScopes
}

// parsePipelineAggregate recognizes the standard Cypher aggregate functions
// and separates their input expression from an optional DISTINCT modifier.
func parsePipelineAggregate(expr string) (name, inner string, distinct, ok bool) {
	// An aggregate is a function call: without a parenthesis there is none.
	if strings.IndexByte(expr, '(') < 0 || !isAggregateFunc(expr) {
		return "", "", false, false
	}
	open := strings.Index(expr, "(")
	if open < 0 {
		return "", "", false, false
	}
	name = lowerASCII(strings.TrimSpace(expr[:open]))
	inner = strings.TrimSpace(extractFuncInner(expr))
	inner, distinct = cutDistinctArgument(inner)
	if inner == "" {
		return "", "", false, false
	}
	return name, inner, distinct, true
}

func pipelineExpressionContainsAggregate(expr string) bool {
	if strings.IndexByte(expr, '(') < 0 {
		return false
	}
	return len(findAggregateSpans(strings.TrimSpace(expr))) > 0
}

func (e *StorageExecutor) evaluatePipelineAggregateExpression(rows []pipelineRow, expr string) (interface{}, bool) {
	return e.evaluatePipelineAggregateExpressionWithContext(context.Background(), rows, expr)
}

func (e *StorageExecutor) evaluatePipelineAggregateExpressionWithContext(ctx context.Context, rows []pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if name, inner, distinct, ok := parsePipelineAggregate(expr); ok {
		return e.evaluatePipelineAggregateWithContext(ctx, rows, name, inner, distinct)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
		result := make(map[string]interface{})
		if inner == "" {
			return result, true
		}
		for _, pair := range splitTopLevelComma(inner) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 {
				return nil, false
			}
			key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
			value, ok := e.evaluatePipelineAggregateExpressionWithContext(ctx, rows, pair[separator+1:])
			if !ok {
				return nil, false
			}
			result[key] = value
		}
		return result, true
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		if inner == "" {
			return []interface{}{}, true
		}
		if _, _, _, _, comprehension := parseListComprehension(inner); !comprehension {
			result := make([]interface{}, 0)
			for _, item := range splitTopLevelComma(inner) {
				value, ok := e.evaluatePipelineAggregateExpressionWithContext(ctx, rows, item)
				if !ok {
					return nil, false
				}
				result = append(result, value)
			}
			return result, true
		}
	}
	if spans := findAggregateSpans(expr); len(spans) > 0 {
		values := make(pipelineRow, len(spans))
		// Mixed aggregate expressions are evaluated after isolating aggregate
		// calls, but their non-aggregate terms still resolve against the group's
		// grouping row. Preserve that scope in the shared row evaluator instead
		// of adding operator-specific aggregate paths.
		if len(rows) > 0 {
			values = make(pipelineRow, len(rows[0])+len(spans))
			for name, value := range rows[0] {
				values[name] = value
			}
		}
		var rewritten strings.Builder
		last := 0
		for index, span := range spans {
			name, inner, distinct, ok := parsePipelineAggregate(expr[span.start:span.end])
			if !ok {
				return nil, false
			}
			value, ok := e.evaluatePipelineAggregateWithContext(ctx, rows, name, inner, distinct)
			if !ok {
				return nil, false
			}
			placeholder := traversalAggPlaceholder(index)
			rewritten.WriteString(expr[last:span.start])
			rewritten.WriteString(placeholder)
			values[placeholder] = value
			last = span.end
		}
		rewritten.WriteString(expr[last:])
		return e.evaluateRowExpressionWithContext(ctx, rewritten.String(), values)
	}
	if len(rows) == 0 {
		return e.evaluateRowExpressionWithContext(ctx, expr, pipelineRow{})
	}
	return e.evaluateRowExpressionWithContext(ctx, expr, rows[0])
}

// evaluatePipelineAggregate applies an aggregate to one logical group. Null
// inputs are ignored by every standard aggregate, including collect().
func (e *StorageExecutor) evaluatePipelineAggregate(rows []pipelineRow, name, expr string, distinct bool) (interface{}, bool) {
	return e.evaluatePipelineAggregateWithContext(context.Background(), rows, name, expr, distinct)
}

func (e *StorageExecutor) evaluatePipelineAggregateWithContext(ctx context.Context, rows []pipelineRow, name, expr string, distinct bool) (interface{}, bool) {
	if name == "percentilecont" || name == "percentiledisc" {
		return e.evaluatePipelinePercentile(ctx, rows, name, expr, distinct)
	}
	state := pipelineAggregateState{name: name, expression: expr, distinct: distinct}
	for _, row := range rows {
		if !state.add(ctx, e, row) {
			return nil, false
		}
	}
	return state.result(ctx, e)
}

func pipelineAggregateNumber(value interface{}) (float64, int64, bool, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), int64(number), true, true
	case int8:
		return float64(number), int64(number), true, true
	case int16:
		return float64(number), int64(number), true, true
	case int32:
		return float64(number), int64(number), true, true
	case int64:
		return float64(number), number, true, true
	case uint:
		return float64(number), int64(number), true, true
	case uint8:
		return float64(number), int64(number), true, true
	case uint16:
		return float64(number), int64(number), true, true
	case uint32:
		return float64(number), int64(number), true, true
	case uint64:
		return float64(number), int64(number), true, true
	case float32:
		return float64(number), 0, false, true
	case float64:
		return number, 0, false, true
	default:
		return 0, 0, false, false
	}
}

// returnProjection is one RETURN item: its expression, its column name, and
// for an aggregating item the aggregate (aggregateName empty when the
// aggregate is nested in a larger expression, aggregateExpr then being the
// whole expression).
type returnProjection struct {
	expr          string
	alias         string
	isAggr        bool
	aggregateName string
	aggregateExpr string
	distinct      bool
}

// returnProjectionPlan is a RETURN clause parsed for pipelineApplyReturn:
// its items, columns, DISTINCT and trailing ORDER BY / SKIP / LIMIT. A plan
// depends only on the clause text, so it is parsed once per text
// (returnProjectionPlanFor). valid is false when the clause has no items.
type returnProjectionPlan struct {
	valid bool
	star  bool
	// starItems are the items after a leading * (RETURN *, x AS y, #883);
	// the * stands for every variable in scope, in name order.
	starItems    []string
	distinct     bool
	modifiers    string
	projections  []returnProjection
	columns      []string
	hasAggregate bool
}

// returnProjectionPlans caches parsed RETURN clauses by text. Plans are
// immutable once cached; the cache is cleared when it reaches
// returnProjectionPlanLimit entries, which bounds it for workloads with
// unbounded distinct query texts.
var returnProjectionPlans = struct {
	sync.RWMutex
	plans map[string]*returnProjectionPlan
}{plans: make(map[string]*returnProjectionPlan)}

const returnProjectionPlanLimit = 4096

func returnProjectionPlanFor(clause string) *returnProjectionPlan {
	returnProjectionPlans.RLock()
	plan, cached := returnProjectionPlans.plans[clause]
	returnProjectionPlans.RUnlock()
	if cached {
		return plan
	}
	plan = parseReturnProjectionPlan(clause)
	returnProjectionPlans.Lock()
	if len(returnProjectionPlans.plans) >= returnProjectionPlanLimit {
		returnProjectionPlans.plans = make(map[string]*returnProjectionPlan)
	}
	returnProjectionPlans.plans[clause] = plan
	returnProjectionPlans.Unlock()
	return plan
}

func parseReturnProjectionPlan(clause string) *returnProjectionPlan {
	if q := fabric.ReadQueryStructure(clause); q.Starts("RETURN") && len(q.Clauses) == 1 && q.Clauses[0].Projection != nil {
		p := q.Clauses[0].Projection
		plan := &returnProjectionPlan{modifiers: p.Modifiers, distinct: p.Distinct, star: p.Star}
		for _, item := range p.Items {
			alias := item.Column

			if p.Star {
				text := item.Expression
				if item.HasAlias {
					text += " AS `" + strings.ReplaceAll(item.Alias, "`", "``") + "`"
				}
				plan.starItems = append(plan.starItems, text)
			} else {
				plan.addProjection(item.Expression, alias)
			}
		}
		plan.valid = p.Star || len(plan.projections) > 0
		return plan
	}
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (RETURN by ORDER BY by, #894).
	body := strings.TrimSpace(clause)
	modifierStart := len(body)
	if cut := firstTopLevelModifierIndex(body); cut >= 0 {
		modifierStart = cut
	}
	plan := &returnProjectionPlan{modifiers: strings.TrimSpace(body[modifierStart:])}
	body, plan.distinct = cutDistinct(pipelineClauseBody(body[:modifierStart], "RETURN"))
	items := splitTopLevelComma(body)
	if len(items) > 0 && strings.TrimSpace(items[0]) == "*" {
		plan.valid, plan.star = true, true
		for _, item := range items[1:] {
			plan.starItems = append(plan.starItems, strings.TrimSpace(item))
		}
		return plan
	}
	for _, rawItem := range items {
		// Semantic validation rejects an empty item (RETURN 1,,2).
		// Same alias parsing as WITH (parseProjectionExprAlias, #547).
		expr, alias := parseProjectionExprAlias(strings.TrimSpace(rawItem))
		plan.addProjection(expr, alias)
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

// withStarExpanded is the plan of RETURN *, items with the * written out
// (starProjectionItems, #883).
func (plan *returnProjectionPlan) withStarExpanded(columns []string) *returnProjectionPlan {
	expanded := &returnProjectionPlan{valid: true, distinct: plan.distinct, modifiers: plan.modifiers}
	for _, item := range starProjectionItems(columns, plan.starItems) {
		expanded.addProjection(parseProjectionExprAlias(item))
	}
	return expanded
}

// pipelineOriginalReturnText is the text the statement gave the RETURN
// clause at idx, before rewrites, or "".
func pipelineOriginalReturnText(originalClauses []pipelineClause, idx int) string {
	if idx < len(originalClauses) && originalClauses[idx].kind == pipelineClauseReturn {
		return originalClauses[idx].text
	}
	return ""
}

// pipelineNameReturnColumns names final's columns. A RETURN * (with or
// without more items) that produced no rows lists the variables in scope, as
// Neo4j does, since no row carries them (#883). Otherwise the columns take
// the statement's own item texts (original), which rewrites may have changed.
func pipelineNameReturnColumns(final *ExecuteResult, clause, original string, scope map[string]struct{}) {
	if plan := returnProjectionPlanFor(clause); plan.star {
		if len(final.Rows) == 0 {
			final.Columns = plan.withStarExpanded(pipelineScopeColumns(scope)).columns
		}
		return
	}
	if original == "" {
		return
	}
	if columns := pipelineReturnSourceColumns(original); len(columns) == len(final.Columns) {
		final.Columns = columns
	}
}

// starProjectionItems writes out the * of `WITH *, items` or
// `RETURN *, items` (#883), as Neo4j orders the columns: each of columns, in
// the given (name) order, as a variable item, except a column one of items
// names (`RETURN *, a + 1 AS a` has the one column a), then items.
func starProjectionItems(columns, items []string) []string {
	named := make(map[string]bool, len(items))
	for _, item := range items {
		_, alias := parseProjectionExprAlias(item)
		named[alias] = true
	}
	out := make([]string, 0, len(columns)+len(items))
	for _, column := range columns {
		if !named[column] {
			variable := projectionVariableText(column)
			out = append(out, variable+" AS "+variable)
		}
	}
	return append(out, items...)
}

// projectionVariableText is a variable as an expression: backtick-quoted when
// it isn't a plain identifier.
func projectionVariableText(name string) string {
	if isSimpleIdentifier(name) && !strings.Contains(name, "`") {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (plan *returnProjectionPlan) addProjection(expr, alias string) {
	aggregateName, aggregateExpr, distinct, isAggr := parsePipelineAggregate(expr)
	if !isAggr && pipelineExpressionContainsAggregate(expr) {
		isAggr = true
		aggregateExpr = expr
	}
	plan.hasAggregate = plan.hasAggregate || isAggr
	plan.projections = append(plan.projections, returnProjection{expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct})
	plan.columns = append(plan.columns, alias)
}

func returnProjectionPlanFromItems(items []returnItem) *returnProjectionPlan {
	plan := &returnProjectionPlan{}
	for _, item := range items {
		plan.addProjection(item.expr, item.column())
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

// pipelineApplyReturn projects each binding row through the RETURN list.
// Supports:
//   - `count(*)` / `count(var)` (aggregate — collapses all rows to one)
//   - bare variable              (`RETURN node`)
//   - property access            (`RETURN m.name`)
//   - aliased forms              (`RETURN m.name AS probeName`)
//   - literal scalar             (`RETURN 42 AS answer`)
//
// Returns (nil, false) if any item can't be projected, so the caller falls
// back to the established RETURN projection.
func (e *StorageExecutor) pipelineApplyReturn(ctx context.Context, rows []pipelineRow, clause string) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnSource(ctx, rows, clause, pipelineRowsSource(rows), false)
}

func (e *StorageExecutor) pipelineApplyReturnSource(ctx context.Context, rows []pipelineRow, clause string, source pipelineRowSource, rowsValidated bool, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnPlan(ctx, rows, returnProjectionPlanFor(clause), source, rowsValidated, preparedGroups...)
}

func (e *StorageExecutor) pipelineApplyReturnPlan(ctx context.Context, rows []pipelineRow, plan *returnProjectionPlan, source pipelineRowSource, rowsValidated bool, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, bool) {
	if !plan.valid {
		return nil, false
	}
	if rows == nil && source != nil && (plan.star || (!plan.hasAggregate && (plan.modifiers != "" || plan.distinct))) {
		var materialized bool
		rows, materialized = materializePipelineSource(source)
		if !materialized {
			return nil, false
		}
	}
	modifiers, returnDistinct := plan.modifiers, plan.distinct
	if plan.star && len(plan.starItems) > 0 {
		return e.pipelineApplyReturnPlan(ctx, rows, plan.withStarExpanded(pipelineWildcardColumns(rows)), source, rowsValidated)
	}
	if plan.star {
		columns := pipelineWildcardColumns(rows)
		result := &ExecuteResult{Columns: columns, Rows: make([][]interface{}, 0, len(rows))}
		for _, row := range rows {
			projected := make([]interface{}, len(columns))
			for index, column := range columns {
				projected[index] = row[column]
			}
			result.Rows = append(result.Rows, projected)
		}
		if returnDistinct {
			result.Rows = deduplicatePipelineResultRows(result.Rows)
		}
		result, err := e.applyResultModifiers(ctx, result, modifiers)
		return result, err == nil
	}
	projs, hasAggregate := plan.projections, plan.hasAggregate
	result := &ExecuteResult{Columns: append([]string(nil), plan.columns...)}

	if hasAggregate {
		var groups []*pipelineAggregateGroup
		if len(preparedGroups) > 0 {
			groups = preparedGroups[0]
		} else {
			var ok bool
			groups, ok = e.pipelineAggregateGroups(ctx, source, projs, rowsValidated)
			if !ok {
				return nil, false
			}
		}

		// ORDER BY reads a group's grouping keys, its projected columns, and
		// a projection's expression text as its value (count(*) in
		// RETURN k, count(*) AS c ORDER BY count(*) DESC, k), as after an
		// aggregating WITH.
		orderTerms := parseOrderByTerms(modifiers)
		if len(orderTerms) > 0 {
			expressions := make([]string, len(projs))
			for index, projection := range projs {
				expressions[index] = projection.expr
			}
			orderTerms = alignOrderTerms(orderTerms, expressions)
		}
		var projectedRows, orderScopes []pipelineRow
		for _, group := range groups {
			outRow := make([]interface{}, 0, len(projs))
			for index, projection := range projs {
				value, ok := group.value(ctx, e, index)
				if !ok {
					pipelineItemUnevaluable(ctx, projection.expr)
					return nil, false
				}
				outRow = append(outRow, value)
			}
			if len(orderTerms) == 0 {
				result.Rows = append(result.Rows, outRow)
				continue
			}
			projected := make(pipelineRow, len(projs))
			scope := make(pipelineRow, len(group.first)+2*len(projs))
			for name, value := range group.first {
				scope[name] = value
			}
			for index, projection := range projs {
				scope[projection.expr] = outRow[index]
			}
			for index, column := range result.Columns {
				projected[column] = outRow[index]
				scope[column] = outRow[index]
			}
			projectedRows = append(projectedRows, projected)
			orderScopes = append(orderScopes, scope)
		}
		if len(orderTerms) > 0 {
			return e.finishPipelineReturn(ctx, result, projectedRows, orderScopes, orderTerms, modifiers, returnDistinct)
		}
		if returnDistinct {
			result.Rows = deduplicatePipelineResultRows(result.Rows)
		}
		result, err := e.applyResultModifiers(ctx, result, modifiers)
		return result, err == nil
	}

	// Without DISTINCT, ORDER BY, SKIP or LIMIT the projected values are the
	// result rows: no per-row map or ORDER BY scope is needed.
	if modifiers == "" && !returnDistinct {
		result.Rows = make([][]interface{}, 0, len(rows))
		projected, completed := e.pipelineProjectPlainReturn(ctx, projs, rows, source, &result.Rows, nil)
		if !completed {
			return nil, false
		}
		if !projected {
			if failure := getExpressionFailure(ctx); failure == nil || newPipelineDispatchOutcome(nil, true, failure).state == pipelineDispatchParseRejected {
				return nil, false
			}
			return result, false
		}
		return result, true
	}

	// ORDER BY and DISTINCT see the incoming row with the projection over
	// it; the merged scope is built only when one of them needs it.
	orderTerms := parseOrderByTerms(modifiers)
	orderedExpressions := orderedProjectionExpressions(orderTerms, len(projs), func(index int) (string, string) {
		return projs[index].expr, projs[index].alias
	})
	needsOrderScopes := len(orderTerms) > 0 || returnDistinct
	projectedRows := make([]pipelineRow, 0, len(rows))
	var orderScopes []pipelineRow
	if needsOrderScopes {
		orderScopes = make([]pipelineRow, 0, len(rows))
	}
	for _, row := range rows {
		projected := make(pipelineRow, len(projs))
		for _, p := range projs {
			val, ok := e.evaluateRowExpressionWithContext(ctx, p.expr, row)
			if !ok {
				pipelineItemUnevaluable(ctx, p.expr)
				return nil, false
			}
			projected[p.alias] = val
		}
		projectedRows = append(projectedRows, projected)
		if !needsOrderScopes {
			continue
		}
		scope := make(pipelineRow, len(row)+len(projected)+len(orderedExpressions))
		for name, value := range row {
			scope[name] = value
		}
		for _, ordered := range orderedExpressions {
			scope[ordered.expression] = projected[ordered.alias]
		}
		for name, value := range projected {
			scope[name] = value
		}
		orderScopes = append(orderScopes, scope)
	}
	return e.finishPipelineReturn(ctx, result, projectedRows, orderScopes, orderTerms, modifiers, returnDistinct)
}

// finishPipelineReturn applies DISTINCT, ORDER BY (each row's terms read in
// orderScopes[i]), SKIP and LIMIT to a RETURN's projected rows and writes
// them to result in its column order.
func (e *StorageExecutor) finishPipelineReturn(ctx context.Context, result *ExecuteResult, projectedRows, orderScopes []pipelineRow, orderTerms []orderByTerm, modifiers string, returnDistinct bool) (*ExecuteResult, bool) {
	if returnDistinct {
		projectedRows, orderScopes = deduplicatePipelineRowsWithScopes(projectedRows, orderScopes, result.Columns)
	}
	if !e.orderPipelineRowsWithScopes(ctx, projectedRows, orderScopes, orderTerms) {
		return nil, false
	}
	skip := 0
	if value, ok := e.parseIntModifier(ctx, modifiers, "SKIP"); ok {
		skip = value
	}
	limit := -1
	if value, ok := e.parseIntModifier(ctx, modifiers, "LIMIT"); ok {
		limit = value
	}
	projectedRows = applyPipelineWindow(projectedRows, skip, limit)
	result.Rows = make([][]interface{}, 0, len(projectedRows))
	for _, projected := range projectedRows {
		outRow := make([]interface{}, len(result.Columns))
		for index, column := range result.Columns {
			outRow[index] = projected[column]
		}
		result.Rows = append(result.Rows, outRow)
	}
	return result, true
}

// pipelineProjectPlainReturn projects the rows of a RETURN without DISTINCT,
// ORDER BY, SKIP, LIMIT or aggregation: rows, or source's rows when rows is
// nil. Each projected row is appended to *out, or handed to emit when emit
// is set, which stops the projection by returning false. projected is false
// when a row could not be projected (the expression error, if any, is
// recorded); completed is false when source declined its shape.
func (e *StorageExecutor) pipelineProjectPlainReturn(ctx context.Context, projs []returnProjection, rows []pipelineRow, source pipelineRowSource, out *[][]interface{}, emit func([]interface{}) bool) (projected, completed bool) {
	if rows == nil && source != nil {
		// A local the closure captures, so the rows path below doesn't pay
		// for a captured result variable.
		sourceProjected := true
		sourceCompleted := source(func(row pipelineRow) bool {
			values, evaluated := e.pipelineProjectReturnRow(ctx, projs, row)
			if !evaluated {
				sourceProjected = false
				return false
			}
			if emit != nil {
				return emit(values)
			}
			*out = append(*out, values)
			return true
		})
		return sourceProjected, sourceCompleted
	}
	for _, row := range rows {
		values, evaluated := e.pipelineProjectReturnRow(ctx, projs, row)
		if !evaluated {
			return false, true
		}
		if emit == nil {
			*out = append(*out, values)
		} else if !emit(values) {
			break
		}
	}
	return true, true
}

// pipelineStreamReturn runs the statement's RETURN into stream (#939), for
// a RETURN streamsReturn takes: its rows go to the stream as they are
// projected. A RETURN that ends before the stream started returns its
// rows, as usual (nil after streaming). Its errors and declines are the
// usual RETURN's until the stream started (a decline is handled false and
// no error, pipelineDecline); after, a decline is an error, since the
// client already has rows and no other route may run the statement again.
func (e *StorageExecutor) pipelineStreamReturn(ctx context.Context, stream *ResultStream, rows []pipelineRow, source pipelineRowSource, clauses, originalClauses []pipelineClause, idx int, scope map[string]struct{}, wrote bool) (columns []string, out [][]interface{}, handled bool, err error) {
	clause := clauses[idx]
	plan := returnProjectionPlanFor(clause.text)
	// The statement only reads, so no row holds an entity it deleted
	// (validateDeletedEntityReads), and the RETURN doesn't aggregate
	// (validatePipelinePercentileArguments). The argument checks run on each
	// row as it arrives, as WITH's do: a row that fails them fails the
	// statement where the row is, as in Neo4j.
	if err := e.validatePipelineProjectionValues(rows, clause.text, "RETURN"); err != nil {
		return nil, nil, true, err
	}
	var validationErr error
	if rows == nil && source != nil && strings.ContainsAny(clause.text, "([") {
		input := source
		source = func(yield func(pipelineRow) bool) bool {
			return input(func(row pipelineRow) bool {
				if err := e.validatePipelineProjectionValues([]pipelineRow{row}, clause.text, "RETURN"); err != nil {
					validationErr = err
					return false
				}
				return yield(row)
			})
		}
	}
	named := &ExecuteResult{Columns: append([]string(nil), plan.columns...)}
	pipelineNameReturnColumns(named, clause.text, pipelineOriginalReturnText(originalClauses, idx), scope)
	stream.begin(named.Columns)
	stopped := false
	projected, completed := e.pipelineProjectPlainReturn(ctx, plan.projections, rows, source, nil, func(values []interface{}) bool {
		if !stream.emit(ctx, e, values) {
			stopped = true
			return false
		}
		return true
	})
	failure := validationErr
	if failure == nil && !stopped && !(projected && completed) {
		failure = getExpressionFailure(ctx)
	}
	if failure != nil {
		stream.fail()
	}
	buffered, streamed := stream.end()
	switch {
	case stopped:
		return nil, nil, true, ctx.Err()
	case validationErr != nil:
		stream.discard()
		return nil, nil, true, validationErr
	case projected && completed:
		if streamed {
			return named.Columns, nil, true, nil
		}
		if buffered == nil {
			buffered = [][]interface{}{}
		}
		return named.Columns, buffered, true, nil
	case !streamed:
		stream.discard()
		_, handled, err := pipelineDecline(ctx, wrote, clause.text)
		return nil, nil, handled, err
	}
	if failure != nil {
		return nil, nil, true, failure
	}
	return nil, nil, true, localizedError(localization.CypherInvariantsPipelineDeclinedAfterStreaming(clause.text), nil)
}

// streamableReturnPlan reports whether a RETURN's rows can go to a result
// stream as they are projected: no DISTINCT, ORDER BY, SKIP, LIMIT,
// aggregation or *.
func streamableReturnPlan(plan *returnProjectionPlan) bool {
	return plan.valid && !plan.star && !plan.hasAggregate && !plan.distinct && plan.modifiers == ""
}

// streamsReturn reports whether clauses[idx] is a RETURN the statement's
// result stream takes (pipelineStreamReturn), so its input may stay a row
// source.
func streamsReturn(ctx context.Context, clauses []pipelineClause, idx int) bool {
	return clauses[idx].kind == pipelineClauseReturn && boundResultStream(ctx, &clauses[idx]) != nil &&
		streamableReturnPlan(returnProjectionPlanFor(clauses[idx].text))
}

func deduplicatePipelineResultRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]struct{}, len(rows))
	unique := make([][]interface{}, 0, len(rows))
	keys := make([]string, 0)
	for _, row := range rows {
		if cap(keys) < len(row) {
			keys = make([]string, len(row))
		} else {
			keys = keys[:len(row)]
		}
		for index, value := range row {
			keys[index] = cypherValueKey(value, cypherEquivalenceContract)
		}
		key := strings.Join(keys, "\x1f")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, row)
	}
	return unique
}

func pipelineWildcardColumns(rows []pipelineRow) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for column := range row {
			if strings.HasPrefix(column, "$") || isGeneratedVariable(column) {
				continue
			}
			seen[column] = struct{}{}
		}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func pipelineScopeColumns(scope map[string]struct{}) []string {
	columns := make([]string, 0, len(scope))
	for column := range scope {
		if !strings.HasPrefix(column, "$") && !isGeneratedVariable(column) {
			columns = append(columns, column)
		}
	}
	sort.Strings(columns)
	return columns
}

// projectFromRow resolves a RETURN / WITH expression against a single
// binding row. Returns (value, true) on success, (nil, false) otherwise.
func projectFromRow(row pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if val, ok := row[expr]; ok {
		return val, true
	}
	upperExpr := upperASCII(expr)
	if strings.HasPrefix(upperExpr, "SIZE(") && strings.HasSuffix(expr, ")") {
		value, ok := projectFromRow(row, strings.TrimSpace(expr[len("size("):len(expr)-1]))
		if !ok {
			return nil, false
		}
		return int64(len(toAnySlice(value))), true
	}
	if dot := strings.Index(expr, "."); dot > 0 {
		base := strings.TrimSpace(expr[:dot])
		field := strings.TrimSpace(expr[dot+1:])
		if baseVal, ok := row[base]; ok {
			if node, isNode := baseVal.(*storage.Node); isNode && node != nil {
				return node.Properties[field], true
			}
			if edge, isEdge := baseVal.(*storage.Edge); isEdge && edge != nil {
				return edge.Properties[field], true
			}
			if m, isMap := toStringAnyMap(baseVal); isMap {
				return m[field], true
			}
		}
	}
	if v, ok := parseLiteralScalarForPipeline(expr); ok {
		return v, true
	}
	return nil, false
}
