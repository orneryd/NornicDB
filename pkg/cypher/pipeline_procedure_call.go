package cypher

import (
	"context"
	"strings"
)

// pipelineProcedureCallsAreClauses reports whether every CALL in the statement
// is a top-level procedure call. Unknown procedures are admitted so execution
// can report their classified lookup error even when no rows reach the call.
func pipelineProcedureCallsAreClauses(cypher string) bool {
	if !containsFold(cypher, "CALL") || hasCallSubqueryPattern(cypher) {
		return false
	}
	ensureBuiltInProceduresRegistered()
	for _, position := range findAllTopLevelPipelineKeywordPositions(cypher, "CALL") {
		procedure, found := globalProcedureRegistry.Get(extractProcedureName(cypher[position:]))
		if found && procedure.Spec.Mode != ProcedureModeRead && !procedure.Spec.writes() && procedure.Spec.Mode != ProcedureModeDBMS && !(procedure.User && procedure.Spec.Mode == "") {
			return false
		}
	}
	topLevel := len(findAllTopLevelPipelineKeywordPositions(cypher, "CALL"))
	total := 0
	for offset := 0; offset < len(cypher); {
		index := findKeywordIndex(cypher[offset:], "CALL")
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
func (e *StorageExecutor) pipelineApplyProcedureCall(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, []string, bool, error) {
	yieldIndex := findKeywordIndexInContext(clause, "YIELD")
	invocation := strings.TrimSpace(clause)
	if yieldIndex >= 0 {
		invocation = strings.TrimSpace(clause[:yieldIndex])
	}
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(invocation))
	if !found {
		return nil, nil, true, newSemanticError("Neo.ClientError.Procedure.ProcedureNotFound", "ProcedureNotFound", "There is no procedure with the name "+extractProcedureName(invocation)+" registered")
	}
	if yieldIndex < 0 && len(procedure.Spec.Returns) > 0 {
		return nil, nil, true, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", "procedure calls inside a query must name results explicitly using YIELD")
	}
	if err := validateProcedureCallArguments(invocation); err != nil {
		return nil, nil, true, err
	}
	yieldBody := ""
	if yieldIndex >= 0 {
		yieldBody = strings.TrimSpace(clause[yieldIndex+len("YIELD"):])
	}
	where := ""
	if whereIndex := topLevelKeywordIndex(yieldBody, "WHERE"); whereIndex >= 0 {
		where = strings.TrimSpace(yieldBody[whereIndex+len("WHERE"):])
		yieldBody = strings.TrimSpace(yieldBody[:whereIndex])
	}
	yieldText := ""
	if yieldIndex >= 0 {
		yieldText = "YIELD " + yieldBody
	}

	arguments := explicitProcedureArgumentTexts(invocation)
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
		yield := parseYieldClause(clause)
		if err := validateProcedureYieldBindings(yield, true); err != nil {
			return nil, nil, true, err
		}
		if err := e.validateYieldModifiers(yield, true); err != nil {
			return nil, nil, true, err
		}
		for _, item := range yield.items {
			name := item.name
			if item.alias != "" {
				name = item.alias
			}
			yielded = append(yielded, name)
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
			result, err = e.executeProcedureCall(withValueBindings(ctx, row), invocation+" "+yieldText, true)
			if err != nil {
				return nil, nil, true, err
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
		if where != "" {
			extended = e.filterPipelineRows(ctx, extended, where)
		}
		out = append(out, extended...)
	}
	return out, yielded, true, nil
}

// argumentUsesRowVariable reports whether a procedure argument refers to a
// variable bound in the row (parameters are already substituted).
func argumentUsesRowVariable(argument string, row pipelineRow) bool {
	for _, reference := range semanticExpressionReferences(argument) {
		base := strings.SplitN(reference, ".", 2)[0]
		if _, bound := row[base]; bound {
			return true
		}
	}
	return false
}
