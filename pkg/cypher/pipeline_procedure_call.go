package cypher

import (
	"context"
	"strings"
)

// pipelineApplyProcedureCall runs CALL proc(args) YIELD items [WHERE p] for
// every input row, as Neo4j does: the arguments are evaluated in the row's
// scope, each yielded record extends a copy of the row with the yielded
// (aliased) columns, and the YIELD's WHERE filters the extended rows, so it
// can compare yielded columns with the row's variables. A row whose call
// yields nothing produces no row. The clauses after the call then see every
// row at once, so aggregation, ORDER BY and SKIP / LIMIT apply to all of
// them.
//
// The procedure, its arguments and its YIELD are the ones the statement
// compiler admitted for this clause. Void calls without YIELD preserve input
// rows; non-void calls require YIELD (decided at admission).
func (e *StorageExecutor) pipelineApplyProcedureCall(ctx context.Context, rows []pipelineRow, clause pipelineClause, totals *QueryStats) ([]pipelineRow, []string, bool, error) {
	call, err := executionProcedureCall(ctx, clause)
	if err != nil {
		return nil, nil, true, err
	}
	// The WHERE reads the row's variables, so it filters the extended rows
	// below; items, aliases and modifiers apply to each call's own result.
	where := ""
	if call.yield != nil {
		yield := *call.yield
		where, yield.where = yield.where, ""
		call.yield = &yield
	}
	rowDependent := false
	for _, argument := range call.arguments {
		if len(rows) > 0 && argumentUsesRowVariable(argument, rows[0]) {
			rowDependent = true
			break
		}
	}
	var yielded []string
	if call.yield != nil {
		for _, item := range call.yield.items {
			yielded = append(yielded, item.column())
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
			result, err = e.invokeProcedure(withValueBindings(ctx, row), call)
			if err != nil {
				return nil, nil, true, err
			}
			// Count executions, not yielded rows or shared result reuse.
			if result != nil {
				addQueryStats(totals, result.Stats)
			}
			if !rowDependent && call.procedure.Spec.Mode == ProcedureModeRead {
				shared = result
			}
		}
		if call.yield == nil {
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
