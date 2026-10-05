package cypher

import (
	"context"
	"strings"
)

// pipelineProcedureCallsAreClauses reports whether every CALL in the statement
// is a top-level call of a registered read or write procedure. CALL subqueries,
// nested calls, and unregistered procedures are not procedure clauses.
func pipelineProcedureCallsAreClauses(cypher string) bool {
	if !containsFold(cypher, "CALL") || hasCallSubqueryPattern(cypher) {
		return false
	}
	ensureBuiltInProceduresRegistered()
	for _, position := range findAllTopLevelPipelineKeywordPositions(cypher, "CALL") {
		procedure, found := globalProcedureRegistry.Get(extractProcedureName(cypher[position:]))
		if !found || (procedure.Spec.Mode != ProcedureModeRead && procedure.Spec.Mode != ProcedureModeWrite) {
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
// Write calls without YIELD preserve input rows. Read calls require YIELD.
// Explicit arguments are evaluated as typed values in the canonical invocation.
func (e *StorageExecutor) pipelineApplyProcedureCall(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, []string, bool, error) {
	yieldIndex := findKeywordIndexInContext(clause, "YIELD")
	invocation := strings.TrimSpace(clause)
	if yieldIndex >= 0 {
		invocation = strings.TrimSpace(clause[:yieldIndex])
	}
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(invocation))
	if !found || (yieldIndex < 0 && procedure.Spec.Mode != ProcedureModeWrite) {
		return nil, nil, false, nil
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

	_, arguments, _ := splitProcedureInvocationArguments(invocation)
	rowDependent := false
	for _, argument := range arguments {
		if len(rows) > 0 && argumentUsesRowVariable(argument, rows[0]) {
			rowDependent = true
			break
		}
	}

	var yielded []string
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
			if !rowDependent && procedure.Spec.Mode != ProcedureModeWrite {
				shared = result
			}
		}
		if yieldIndex < 0 {
			out = append(out, row)
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

// splitProcedureInvocationArguments splits "CALL name(a, b)" into the call
// target ("CALL name") and its argument expressions. hasArguments is false
// when the call has no parenthesized argument list.
func splitProcedureInvocationArguments(invocation string) (name string, arguments []string, hasArguments bool) {
	open := strings.IndexByte(invocation, '(')
	if open < 0 {
		return invocation, nil, false
	}
	closing := findMatchingDelimiter(invocation, open, '(', ')')
	if closing < 0 {
		return invocation, nil, false
	}
	name = strings.TrimSpace(invocation[:open])
	inner := strings.TrimSpace(invocation[open+1 : closing])
	if inner == "" {
		return name, nil, true
	}
	for _, argument := range splitTopLevelComma(inner) {
		arguments = append(arguments, strings.TrimSpace(argument))
	}
	return name, arguments, true
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
