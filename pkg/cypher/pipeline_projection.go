package cypher

// Pipeline projection owns WITH/RETURN plans, aggregation, ordering and result windows.

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"sort"
	"strings"

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
//
// A nil source means the rows are already materialized, so the row source is
// derived from them; a caller with a replayable source passes it explicitly.
func (e *StorageExecutor) pipelineApplyWith(ctx context.Context, rows []pipelineRow, clause braceView, source pipelineRowSource, rowsValidated bool) ([]pipelineRow, bool) {
	if source == nil {
		source = pipelineRowsSource(rows)
	}
	if plan, ok := parsePipelineRowWith(ctx, clause); ok {
		plan.retainRows, plan.skipValidation = true, true
		out := make([]pipelineRow, 0, len(rows))
		if !e.pipelineWithRowSource(ctx, pipelineRowsSource(rows), plan)(func(projected pipelineRow) bool {
			out = append(out, projected)
			return true
		}) {
			return nil, false
		}

		return out, true
	}
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (WITH with WHERE with = 3, #894).
	bodyView := clause.trim()
	body := bodyView.text()
	orderTerms := parseOrderByTerms(bodyView)
	withSkip, withLimit := 0, -1
	if skipIndex := topLevelKeywordIndex(bodyView, "SKIP"); skipIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(bodyView, "SKIP"), rows)
		if !ok {
			return nil, false
		}
		withSkip = value
	}
	if limitIndex := topLevelKeywordIndex(bodyView, "LIMIT"); limitIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(bodyView, "LIMIT"), rows)
		if !ok {
			return nil, false
		}
		withLimit = value
	}
	postWithWhere := bodyView.window(0, 0)
	if whereIdx := topLevelKeywordIndex(bodyView, "WHERE"); whereIdx >= 0 {
		postWithWhere = bodyView.window(whereIdx+len("WHERE"), len(body)).trim()
		bodyView = bodyView.window(0, whereIdx).trim()
		body = bodyView.text()
	}
	// WITH … [ORDER BY …] SKIP / LIMIT n WHERE p filters the rows SKIP /
	// LIMIT keep, as Neo4j does ("take the first n, then filter"). Without
	// SKIP / LIMIT the filter can run per row, before ordering: the result is
	// the same.
	windowedWhere := bodyView.window(0, 0)
	if !postWithWhere.empty() && (withSkip > 0 || withLimit >= 0) {
		windowedWhere, postWithWhere = postWithWhere, bodyView.window(0, 0)
	}
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(bodyView, keyword); index >= 0 {
			bodyView = bodyView.window(0, index).trim()
			body = bodyView.text()
		}
	}
	withDistinct := false
	bodyView, withDistinct = cutDistinct(bodyView.clauseBody("WITH"))
	body = bodyView.text()
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
			out = deduplicatePipelineRows(memoryScopeFromContext(ctx), out, pipelineWildcardColumns(out))
		}
		out = e.filterPipelineRows(ctx, out, postWithWhere)
		if !e.orderPipelineRows(ctx, out, orderTerms) {
			return nil, false
		}
		return e.filterPipelineRows(ctx, applyPipelineWindow(out, withSkip, withLimit), windowedWhere), true
	}
	items := bodyView.commaViews()
	if len(items) == 0 {
		return rows, true
	}
	if items[0].text() == "*" {
		// WITH *, items: the * is every variable in scope (#883).
		items = starProjectionItems(ctx, bodyView, pipelineWildcardColumns(rows), items[1:])
	}

	type withProjection struct {
		view, aggregateView braceView
		expr                string
		alias               string
		aggregate           bool
		aggregateName       string
		aggregateExpr       string
		distinct            bool
	}
	projections := make([]withProjection, 0, len(items))
	projectionAliases := make([]string, 0, len(items))
	hasAggregate := false
	for _, rawItem := range items {
		item := rawItem.text()
		if item == "" || item == "{}" {
			continue
		}
		expressionView, alias, valid := projectionItemView(ctx, rawItem)
		if !valid {
			return nil, false
		}
		expr := expressionView.text()
		if expr == "" {
			return nil, false
		}
		projection := withProjection{expr: expr, alias: alias, view: expressionView}
		if aggregateName, aggregateExpr, distinct, aggregate := parsePipelineAggregate(expressionView); aggregate {
			if !strings.Contains(upperASCII(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateName = aggregateName
			projection.aggregateExpr = aggregateExpr.text()
			projection.aggregateView = aggregateExpr
			projection.distinct = distinct
			hasAggregate = true
		} else if pipelineExpressionContainsAggregate(expressionView) {
			if !strings.Contains(upperASCII(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateExpr = expr
			projection.aggregateView = expressionView
			hasAggregate = true
		}
		projections = append(projections, projection)
		projectionAliases = append(projectionAliases, alias)
	}
	if hasAggregate {
		aggregates := make([]returnProjection, len(projections))
		for index, projection := range projections {
			aggregates[index] = returnProjection{view: projection.view, aggregateView: projection.aggregateView, expr: projection.expr, alias: projection.alias, isAggr: projection.aggregate, aggregateName: projection.aggregateName, aggregateExpr: projection.aggregateExpr, distinct: projection.distinct}
		}
		groups, ok := e.pipelineAggregateGroups(ctx, source, aggregates, rowsValidated)
		if !ok {
			return nil, false
		}
		if len(orderTerms) > 0 {
			expressions := make([]braceView, len(projections))
			for index, projection := range projections {
				expressions[index] = projection.view
			}
			orderTerms = alignOrderTerms(clause, orderTerms, expressions)
		}

		out := make([]pipelineRow, 0, len(groups))
		needsOrderScopes := len(orderTerms) > 0 || !postWithWhere.empty() || withDistinct || !windowedWhere.empty()
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
			carryPipelineRowImports(newRow, group.first, pipelineImports(ctx))
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
			if !postWithWhere.empty() && !e.evaluateWithWhereCondition(ctx, postWithWhere, orderScope) {
				continue
			}
			out = append(out, newRow)
			orderScopes = append(orderScopes, orderScope)
		}
		if withDistinct {
			out, orderScopes = deduplicatePipelineRowsWithScopes(memoryScopeFromContext(ctx), out, orderScopes, projectionAliases)
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
	needsOrderScopes := len(orderTerms) > 0 || withDistinct || !windowedWhere.empty()
	var orderScopes []pipelineRow
	if needsOrderScopes {
		orderScopes = make([]pipelineRow, 0, len(rows))
	}
	rowPlan := pipelineRowWith{where: postWithWhere.text(), whereView: postWithWhere, view: clause, imports: pipelineImports(ctx)}
	for _, projection := range projections {
		rowPlan.projections = append(rowPlan.projections, pipelineRowProjection{expression: projection.expr, alias: projection.alias, view: projection.view})
	}
	rowPlan.retainRows, rowPlan.skipValidation = true, true
	if needsOrderScopes {
		rowPlan.observeScope = func(newRow, scope pipelineRow) {
			// ORDER BY expression names refer to their projected values while
			// explicit projected names retain their own values.
			for _, ordered := range orderedExpressions {
				if _, projected := newRow[ordered.expression]; !projected {
					scope[ordered.expression] = newRow[ordered.alias]
				}
			}
			orderScopes = append(orderScopes, scope)
		}
	}
	if !e.pipelineWithRowSource(ctx, pipelineRowsSource(rows), rowPlan)(func(newRow pipelineRow) bool {
		out = append(out, newRow)
		return true
	}) {
		return nil, false
	}

	if withDistinct {
		out, orderScopes = deduplicatePipelineRowsWithScopes(memoryScopeFromContext(ctx), out, orderScopes, projectionAliases)
	}
	if !e.orderPipelineRowsWithScopes(ctx, out, orderScopes, orderTerms) {
		return nil, false
	}
	return e.filterPipelineWindow(ctx, out, orderScopes, withSkip, withLimit, windowedWhere), true
}

// filterPipelineWindow applies SKIP / LIMIT to rows, then keeps the rows of
// the window whose scope (the row's projected and incoming values) satisfies
// whereClause: the WHERE after WITH … SKIP / LIMIT.
func (e *StorageExecutor) filterPipelineWindow(ctx context.Context, rows, scopes []pipelineRow, skip, limit int, whereClause braceView) []pipelineRow {
	if whereClause.empty() {
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

func pipelinePaginationExpression(view braceView, keyword string) braceView {
	body := view.text()
	index := topLevelKeywordIndex(view, keyword)
	if index < 0 {
		return view.window(0, 0)
	}
	expressionView := view.window(index+len(keyword), len(body)).trim()
	expression := expressionView.text()
	end := len(expression)
	// WITH … SKIP / LIMIT n WHERE p: the WHERE isn't part of n.
	for _, nextKeyword := range []string{"SKIP", "LIMIT", "WHERE"} {
		if nextIndex := topLevelKeywordIndex(expressionView, nextKeyword); nextIndex >= 0 && nextIndex < end {
			end = nextIndex
		}
	}
	return expressionView.window(0, end).trim()
}

func (e *StorageExecutor) evaluatePipelinePagination(ctx context.Context, expression braceView, rows []pipelineRow) (int, bool) {
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
func alignOrderTerms(base braceView, terms []orderByTerm, expressions []braceView) []orderByTerm {
	retained := map[string]braceView{}
	spellings := aggregateCallSpellings(expressions, func(index int, span aggregateSpan, key string) {
		retained[key] = expressions[index].window(span.start, span.end).trim()
	})
	var aligned []orderByTerm
	for index, term := range terms {
		view := term.view
		column := respellAggregateCalls(term.view, spellings, func(start, end int, key string) {
			view = base.assembleCode([]braceView{view.window(0, start), retained[key], view.window(end, len(view.text()))}, "")
		})
		key := stripExpressionWhitespace(column)
		for offset, expressionView := range expressions {
			expression := expressionView.text()
			if expression != column && stripExpressionWhitespace(expression) == key {
				column = expression
				view = expressions[offset]
				break
			}
		}
		if column == term.column {
			continue
		}
		if view.text() != column {
			panic("cypher: sort spelling producer lost its retained output pieces")
		}
		if aligned == nil {
			aligned = append([]orderByTerm(nil), terms...)
		}
		aligned[index].column, aligned[index].view = column, view
	}
	if aligned == nil {
		return terms
	}
	return aligned
}

// aggregateCallSpellings maps the key (aggregateCallKey) of each aggregate
// call in expressions to its text there. Nil when there is none.
func aggregateCallSpellings(expressionViews []braceView, observe ...func(int, aggregateSpan, string)) map[string]string {
	var spellings map[string]string
	for index, view := range expressionViews {
		expression := view.text()
		for _, span := range findAggregateSpans(view) {
			call := strings.TrimSpace(expression[span.start:span.end])
			if spellings == nil {
				spellings = make(map[string]string)
			}
			key := aggregateCallKey(call)
			spellings[key] = call
			for _, visit := range observe {
				visit(index, span, key)
			}
		}
	}
	return spellings
}

// respellAggregateCalls writes each aggregate call of text that spellings
// holds as spellings spells it: COUNT(distinct n) as count(DISTINCT n).
func respellAggregateCalls(view braceView, spellings map[string]string, observe ...func(int, int, string)) string {
	text := view.text()
	for _, call := range semanticAggregateCalls(view) {
		key := aggregateCallKey(call)
		if spelling, ok := spellings[key]; ok && spelling != call {
			at := strings.Index(text, call)
			if at < 0 {
				continue
			}
			for _, visit := range observe {
				visit(at, at+len(call), key)
			}
			text = text[:at] + spelling + text[at+len(call):]
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
	if len(arguments) > len("DISTINCT") && strings.EqualFold(arguments[:len("DISTINCT")], "DISTINCT") && !cyphertext.NamePartAt(arguments, len("DISTINCT")) {
		arguments = "DISTINCT " + arguments[len("DISTINCT"):]
	}
	return upperASCII(strings.TrimSpace(call[:open])) + "(" + stripExpressionWhitespace(arguments)
}

// stripExpressionWhitespace removes the whitespace outside quoted text, but
// for one space between two identifier characters, so NOT a stays apart
// from NOTa.
func stripExpressionWhitespace(expression string) string {
	expression = strings.TrimSpace(expression)
	if strings.IndexFunc(expression, cyphertext.IsWhitespace) < 0 {
		return expression
	}
	var stripped strings.Builder
	stripped.Grow(len(expression))
	for index := 0; index < len(expression); index++ {
		if end, quoted := skipCypherLexicalToken(expression, index); quoted {
			stripped.WriteString(expression[index:end])
			index = end - 1
			continue
		}
		current := expression[index]
		switch {
		case cyphertext.IsWhitespaceAt(expression, index):
			next := index + 1
			for next < len(expression) && cyphertext.IsWhitespaceAt(expression, next) {
				next++
			}
			if cyphertext.NamePartAt(expression, index-1) && cyphertext.NamePartAt(expression, next) {
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
	// The sort keys count against the statement's memory limit (#1343).
	if memoryScopeFromContext(ctx).grow(int64(len(rows))*(sliceHeaderBytes+int64(len(terms))*valueSlotBytes)) != nil {
		return false
	}
	for index, row := range rows {
		values := make([]interface{}, len(terms))
		for termIndex, term := range terms {
			// The projections' evaluator: subquery values (COUNT { … }) and
			// recorded failures behave as in a RETURN item.
			value, ok := e.evaluateRowExpressionWithContext(ctx, term.view, scopes[index])
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

func (e *StorageExecutor) filterPipelineRows(ctx context.Context, rows []pipelineRow, whereClause braceView) []pipelineRow {
	if whereClause.empty() {
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

func deduplicatePipelineRows(memory *memoryScope, rows []pipelineRow, columns []string) []pipelineRow {
	unique, _ := deduplicatePipelineRowsWithScopes(memory, rows, rows, columns)
	return unique
}

// deduplicatePipelineRowsWithScopes keeps the first row of each distinct
// tuple of columns; each kept tuple counts against memory's limit, and going
// over stops the deduplication (the statement fails with the limit's
// error, #1343).
func deduplicatePipelineRowsWithScopes(memory *memoryScope, rows, scopes []pipelineRow, columns []string) ([]pipelineRow, []pipelineRow) {
	account := memoryAccount{scope: memory}
	defer account.close()
	var seen cypherValueIndex
	unique := make([]pipelineRow, 0, len(rows))
	uniqueScopes := make([]pipelineRow, 0, len(rows))
	values := make([]interface{}, len(columns))
	for rowIndex, row := range rows {
		for i, column := range columns {
			values[i] = row[column]
		}
		if _, exists := seen.addTupleByContract(values, cypherEquivalenceContract); exists {
			continue
		}
		if account.growTuple(values, mapEntryBytes) != nil {
			break
		}
		unique = append(unique, row)
		uniqueScopes = append(uniqueScopes, scopes[rowIndex])
	}
	return unique, uniqueScopes
}

// parsePipelineAggregate recognizes the standard Cypher aggregate functions
// and separates their input expression from an optional DISTINCT modifier.
func parsePipelineAggregate(view braceView) (name string, inner braceView, distinct, ok bool) {
	view = view.trim()
	expr := view.text()
	// An aggregate is a function call: without a parenthesis there is none.
	if strings.IndexByte(expr, '(') < 0 || !isAggregateFunc(expr) {
		return "", braceView{}, false, false
	}
	open := strings.Index(expr, "(")
	if open < 0 {
		return "", braceView{}, false, false
	}
	name = lowerASCII(strings.TrimSpace(expr[:open]))
	close := findMatchingCallParen(expr, open)
	if close < 0 {
		return "", braceView{}, false, false
	}
	inner = view.window(open+1, close).trim()
	var modifier cyphertext.FunctionArgumentModifier
	inner, modifier = inner.argumentSource(cyphertext.DistinctArgumentModifier)
	distinct = modifier == cyphertext.DistinctArgumentModifier
	if inner.empty() {
		return "", braceView{}, false, false
	}
	return name, inner, distinct, true
}

func pipelineExpressionContainsAggregate(view braceView) bool {
	if strings.IndexByte(view.text(), '(') < 0 {
		return false
	}
	return len(findAggregateSpans(view.trim())) > 0
}

func (e *StorageExecutor) evaluatePipelineAggregateExpression(rows []pipelineRow, view braceView) (interface{}, bool) {
	return e.evaluatePipelineAggregateExpressionWithContext(withBraceView(context.Background(), view), rows, view)
}

func (e *StorageExecutor) evaluatePipelineAggregateExpressionWithContext(ctx context.Context, rows []pipelineRow, view braceView) (interface{}, bool) {
	view = view.trim()
	expr := view.text()
	if name, inner, distinct, ok := parsePipelineAggregate(view); ok {
		return e.evaluatePipelineAggregateWithContext(ctx, rows, name, inner, distinct)
	}
	if entries, valid := view.keyedEntries(); valid {
		result := make(map[string]interface{})
		resolved := true
		for _, entry := range entries {
			value, ok := e.evaluatePipelineAggregateExpressionWithContext(ctx, rows, entry.value)
			resolved = resolved && ok
			result[entry.key()] = value
		}
		if !resolved {
			return nil, false
		}
		return result, true
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		if inner == "" {
			return []interface{}{}, true
		}
		if _, _, _, _, comprehension := parseListComprehension(view.window(1, len(expr)-1).trim()); !comprehension {
			result := make([]interface{}, 0)
			for _, item := range view.window(1, len(expr)-1).commaViews() {
				value, ok := e.evaluatePipelineAggregateExpressionWithContext(ctx, rows, item)
				if !ok {
					return nil, false
				}
				result = append(result, value)
			}
			return result, true
		}
	}
	if spans := findAggregateSpans(view); len(spans) > 0 {
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
		rewritten := view
		delta := 0
		for index, span := range spans {
			name, inner, distinct, ok := parsePipelineAggregate(view.window(span.start, span.end))
			if !ok {
				return nil, false
			}
			value, ok := e.evaluatePipelineAggregateWithContext(ctx, rows, name, inner, distinct)
			if !ok {
				return nil, false
			}
			placeholder := traversalAggPlaceholder(index)
			rewritten = rewritten.replace(span.start+delta, span.end+delta, placeholder)
			delta += len(placeholder) - (span.end - span.start)
			values[placeholder] = value
		}
		return e.evaluateRowExpressionWithContext(ctx, rewritten, values)
	}
	if len(rows) == 0 {
		return e.evaluateRowExpressionWithContext(ctx, view, pipelineRow{})
	}
	return e.evaluateRowExpressionWithContext(ctx, view, rows[0])
}

// evaluatePipelineAggregate applies an aggregate to one logical group. Null
// inputs are ignored by every standard aggregate, including collect().
func (e *StorageExecutor) evaluatePipelineAggregate(rows []pipelineRow, name string, expr braceView, distinct bool) (interface{}, bool) {
	return e.evaluatePipelineAggregateWithContext(context.Background(), rows, name, expr, distinct)
}

func (e *StorageExecutor) evaluatePipelineAggregateWithContext(ctx context.Context, rows []pipelineRow, name string, expr braceView, distinct bool) (interface{}, bool) {
	if name == "percentilecont" || name == "percentiledisc" {
		return e.evaluatePipelinePercentile(ctx, rows, name, expr, distinct)
	}
	state := pipelineAggregateState{name: name, expression: expr.text(), view: expr, distinct: distinct, memory: memoryAccount{scope: memoryScopeFromContext(ctx)}}
	for _, row := range rows {
		if !state.add(ctx, e, row) {
			return nil, false
		}
	}
	return state.result(ctx, e)
}

func pipelineAggregateNumber(value interface{}) (float64, int64, bool, bool) {
	number, ok := valueconvert.ReadNumber(value)
	if !ok {
		return 0, 0, false, false
	}
	integer, exact := number.Int64(valueconvert.IntegerOnly)
	return number.Float64(), integer, exact, true
}

// pipelineOriginalReturnText is the text the statement gave the RETURN
// clause at idx, before rewrites, or "".
func pipelineOriginalReturnText(originalClauses []pipelineClause, idx int) string {
	if idx < len(originalClauses) && originalClauses[idx].kind == pipelineClauseReturn {
		return originalClauses[idx].text
	}
	return ""
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
func (e *StorageExecutor) pipelineApplyReturn(ctx context.Context, rows []pipelineRow, clause braceView) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnSource(ctx, rows, clause, pipelineRowsSource(rows), false)
}

func (e *StorageExecutor) pipelineApplyReturnSource(ctx context.Context, rows []pipelineRow, clause braceView, source pipelineRowSource, rowsValidated bool, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnPlan(ctx, rows, returnProjectionPlanFor(ctx, clause), source, rowsValidated, preparedGroups...)
}

func (e *StorageExecutor) pipelineApplyReturnPlan(ctx context.Context, rows []pipelineRow, plan *returnProjectionPlan, source pipelineRowSource, rowsValidated bool, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, bool) {
	if !plan.valid {
		return nil, false
	}
	if rows == nil && source != nil && (plan.star || (!plan.hasAggregate && (plan.modifiers != "" || plan.distinct))) {
		var materialized bool
		rows, materialized = materializePipelineSource(ctx, source)
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
		result, err := e.applyResultModifiers(ctx, result, plan.modifiersView)
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
		orderTerms := parseOrderByTerms(plan.modifiersView)
		if len(orderTerms) > 0 {
			expressions := make([]braceView, len(projs))
			for index, projection := range projs {
				expressions[index] = projection.view
			}
			orderTerms = alignOrderTerms(plan.view, orderTerms, expressions)
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
			return e.finishPipelineReturn(ctx, result, projectedRows, orderScopes, orderTerms, plan.modifiersView, returnDistinct)
		}
		if returnDistinct {
			result.Rows = deduplicatePipelineResultRows(result.Rows)
		}
		result, err := e.applyResultModifiers(ctx, result, plan.modifiersView)
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
	orderTerms := parseOrderByTerms(plan.modifiersView)
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
			val, ok := e.evaluateRowExpressionWithContext(ctx, p.view, row)
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
	return e.finishPipelineReturn(ctx, result, projectedRows, orderScopes, orderTerms, plan.modifiersView, returnDistinct)
}

// finishPipelineReturn applies DISTINCT, ORDER BY (each row's terms read in
// orderScopes[i]), SKIP and LIMIT to a RETURN's projected rows and writes
// them to result in its column order.
func (e *StorageExecutor) finishPipelineReturn(ctx context.Context, result *ExecuteResult, projectedRows, orderScopes []pipelineRow, orderTerms []orderByTerm, modifiers braceView, returnDistinct bool) (*ExecuteResult, bool) {
	if returnDistinct {
		projectedRows, orderScopes = deduplicatePipelineRowsWithScopes(memoryScopeFromContext(ctx), projectedRows, orderScopes, result.Columns)
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
// an admitted RETURN: its rows go to the stream as they are
// projected. A RETURN that ends before the stream started returns its
// rows, as usual (nil after streaming). Its errors and declines are the
// usual RETURN's until the stream started (a decline is handled false and
// no error, pipelineDecline); after, a decline is an error, since the
// client already has rows and no other route may run the statement again.
func (e *StorageExecutor) pipelineStreamReturn(ctx context.Context, stream *ResultStream, rows []pipelineRow, source pipelineRowSource, clauses, originalClauses []pipelineClause, idx int, scope map[string]struct{}, wrote bool) (columns []string, out [][]interface{}, handled bool, err error) {
	clause := clauses[idx]
	original := clause
	if idx < len(originalClauses) {
		original = originalClauses[idx]
	}
	plan := returnProjectionPlanFor(ctx, original.input(ctx), original)
	if plan == nil {
		return nil, nil, true, unresolvedStatementError()
	}
	// The statement only reads, so no row holds an entity it deleted
	// (validateDeletedEntityReads), and the RETURN doesn't aggregate
	// (validatePipelinePercentileArguments). The argument checks run on each
	// row as it arrives, as WITH's do: a row that fails them fails the
	// statement where the row is, as in Neo4j.
	if err := e.validatePipelineProjectionValues(ctx, rows, clause.input(ctx), "RETURN"); err != nil {
		return nil, nil, true, err
	}
	var validationErr error
	if rows == nil && source != nil && strings.ContainsAny(clause.text, "([") {
		input := source
		source = func(yield func(pipelineRow) bool) bool {
			return input(func(row pipelineRow) bool {
				if err := e.validatePipelineProjectionValues(ctx, []pipelineRow{row}, clause.input(ctx), "RETURN"); err != nil {
					validationErr = err
					return false
				}
				return yield(row)
			})
		}
	}
	named := &ExecuteResult{Columns: append([]string(nil), plan.columns...)}

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

func deduplicatePipelineResultRows(rows [][]interface{}) [][]interface{} {
	var seen cypherValueIndex
	unique := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if _, exists := seen.addTupleByContract(row, cypherEquivalenceContract); exists {
			continue
		}
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
