package cypher

import (
	"context"
	"strings"
)

// pipelineProcedureCallsAreClauses reports whether every CALL in the statement
// is a top-level procedure call. Unknown procedures are admitted so execution
// can report their classified lookup error even when no rows reach the call.
func pipelineProcedureCallsAreClauses(ctx context.Context, cypher string) bool {
	if !containsFold(cypher, "CALL") || hasCallSubqueryPattern(ctx, cypher) {
		return false
	}
	ensureBuiltInProceduresRegistered()
	view := statementBraceView(ctx, cypher)
	for _, position := range findAllTopLevelPipelineKeywordPositions(view, "CALL") {
		procedure, found := globalProcedureRegistry.Get(extractProcedureName(ctx, cypher[position:]))
		if found && procedure.Spec.Mode != ProcedureModeRead && !procedure.Spec.writes() && procedure.Spec.Mode != ProcedureModeDBMS && !(procedure.User && procedure.Spec.Mode == "") {
			return false
		}
	}
	topLevel := len(findAllTopLevelPipelineKeywordPositions(view, "CALL"))
	total := 0
	for offset := 0; offset < len(cypher); {
		index := findKeywordIndex(view.window(offset, len(cypher)), "CALL")
		if index < 0 {
			break
		}
		total++
		offset += index + len("CALL")
	}
	return topLevel > 0 && topLevel == total
}

// pipelineApplyProcedureCall runs CALL proc(args) YIELD items [WHERE p] for
// every input row, as Neo4j does: the arguments are evaluated in the row's
// scope, each yielded record extends a copy of the row with the yielded
// (aliased) columns, and the YIELD's WHERE filters the extended rows, so it
// can compare yielded columns with the row's variables. A row whose call
// yields nothing produces no row. The clauses after the call then see every
// row at once, so aggregation, ORDER BY and SKIP / LIMIT apply to all of
// them.
//
// Void calls without YIELD preserve input rows; non-void calls require YIELD.
// Explicit arguments are evaluated as typed values in the canonical invocation.
func (e *StorageExecutor) pipelineApplyProcedureCall(ctx context.Context, rows []pipelineRow, clauseView braceView, totals *QueryStats) ([]pipelineRow, []string, bool, error) {
	clause := clauseView.text()
	yieldIndex := findKeywordIndexInContext(clauseView, "YIELD")
	invocationView := clauseView.trim()
	if yieldIndex >= 0 {
		invocationView = clauseView.window(0, yieldIndex).trim()
	}
	invocation := invocationView.text()
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(ctx, invocation))
	if !found {
		return nil, nil, true, newSemanticError("Neo.ClientError.Procedure.ProcedureNotFound", "ProcedureNotFound", "There is no procedure with the name "+extractProcedureName(ctx, invocation)+" registered")
	}
	if yieldIndex < 0 && len(procedure.Spec.Returns) > 0 {
		return nil, nil, true, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", "procedure calls inside a query must name results explicitly using YIELD")
	}
	if err := validateProcedureCallArguments(invocationView); err != nil {
		return nil, nil, true, err
	}
	yieldBody := clauseView.window(0, 0)
	if yieldIndex >= 0 {
		yieldBody = clauseView.window(yieldIndex+len("YIELD"), len(clause)).trim()
	}
	where := clauseView.window(0, 0)
	if whereIndex := topLevelKeywordIndex(yieldBody, "WHERE"); whereIndex >= 0 {
		where = yieldBody.window(whereIndex+len("WHERE"), len(yieldBody.text())).trim()
		yieldBody = yieldBody.window(0, whereIndex).trim()
	}
	callView := invocationView.wrap("", " ")
	if yieldIndex >= 0 {
		callView = clauseView.assembleCode([]braceView{invocationView, yieldBody.wrap("YIELD ", "")}, " ")
	}

	arguments := explicitProcedureArgumentViews(ctx, invocationView)
	if arguments != nil {
		if err := validateProcedureArgumentCount(procedure.Spec, len(arguments)); err != nil {
			return nil, nil, true, err
		}
	}
	rowDependent := false
	for _, argument := range arguments {
		if len(rows) > 0 && argumentUsesRowVariable(argument, rows[0]) {
			rowDependent = true
			break
		}
	}

	var yielded []string
	if yieldIndex >= 0 {
		yield := parseYieldClause(ctx, clauseView)
		if err := validateProcedureYieldBindings(yield, true); err != nil {
			return nil, nil, true, err
		}
		if err := e.validateYieldModifiers(ctx, yield, true); err != nil {
			return nil, nil, true, err
		}
		for _, item := range yield.items {
			yielded = append(yielded, item.column())
		}
		if len(procedure.Spec.Returns) > 0 {
			columns := make([]string, len(procedure.Spec.Returns))
			for index, column := range procedure.Spec.Returns {
				columns[index] = column.Name
			}
			if _, err := e.applyYieldFilter(ctx, &ExecuteResult{Columns: columns, Rows: [][]interface{}{}}, &yieldClause{items: yield.items}); err != nil {
				return nil, nil, true, err
			}
		}
	}
	var shared *ExecuteResult
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, nil, true, err
		}
		result := shared
		if result == nil {
			var err error
			result, err = e.executeProcedureCall(withBraceView(withValueBindings(ctx, row), callView), callView.text(), true)
			if err != nil {
				return nil, nil, true, err
			}
			// Count executions, not yielded rows or shared result reuse.
			if result != nil {
				addQueryStats(totals, result.Stats)
			}
			if !rowDependent && procedure.Spec.Mode == ProcedureModeRead {
				shared = result
			}
		}
		if yieldIndex < 0 {
			out = append(out, row)
			continue
		}
		if result == nil {
			continue
		}
		if yielded == nil {
			yielded = append([]string(nil), result.Columns...)
		}
		extended := make([]pipelineRow, 0, len(result.Rows))
		for _, record := range result.Rows {
			next := make(pipelineRow, len(row)+len(result.Columns))
			for key, value := range row {
				next[key] = value
			}
			for i, column := range result.Columns {
				if i < len(record) {
					next[column] = record[i]
				}
			}
			extended = append(extended, next)
		}
		if !where.empty() {
			extended = e.filterPipelineRows(ctx, extended, where)
		}
		out = append(out, extended...)
	}
	return out, yielded, true, nil
}

// argumentUsesRowVariable reports whether a procedure argument refers to a
// variable bound in the row (parameters are already substituted).
func argumentUsesRowVariable(argument braceView, row pipelineRow) bool {
	for _, reference := range semanticExpressionReferences(argument) {
		base := strings.SplitN(reference, ".", 2)[0]
		if _, bound := row[base]; bound {
			return true
		}
	}
	return false
}
