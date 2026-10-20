package cypher

import (
	"context"
	"slices"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/txphase"
)

func (e *StorageExecutor) pipelineApplyCallSubquery(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, *QueryStats, bool, error) {
	return e.pipelineApplyCallSubqueryWithMetadata(ctx, rows, clause, nil)
}

type pipelineCallMetadata struct {
	scope   map[string]struct{}
	columns []string
}

func (e *StorageExecutor) pipelineApplyCallSubqueryWithMetadata(ctx context.Context, rows []pipelineRow, clause string, metadata *pipelineCallMetadata) ([]pipelineRow, *QueryStats, bool, error) {
	// The statement parses the clause and its body once (pipelineClausePlans),
	// however many outer rows run the operator.
	ctx = withPipelineClausePlans(ctx)
	parsed := e.callSubqueryParseFor(ctx, clause)
	body, afterCall, inTransactions, batchSize := parsed.body, parsed.afterCall, parsed.inTransactions, parsed.batchSize
	if body == "" {
		return nil, nil, true, localizedError(localization.CypherSubqueriesCallBodyExpected(), nil)
	}
	if strings.TrimSpace(afterCall) != "" {
		return nil, nil, false, nil
	}
	scoped := parsed.scoped
	scopedImports := parsed.scopedImports
	branches, unionAll, _, isUnion := parseTopLevelUnionBranchesFor(ctx, body)
	if !isUnion {
		branches = []string{body}
	}
	if metadata != nil {
		for branchIndex, branch := range branches {
			plan, err := planCallBranchBindings(branch, metadata.scope, scoped, scopedImports, branchIndex)
			if err != nil {
				return nil, nil, true, err
			}
			if _, _, missing := callBranchBindingValues(plan, metadata.scope); missing != "" {
				return nil, nil, true, createUndefinedVariableError(missing)
			}
		}
	}
	if len(rows) == 0 {
		if metadata != nil {
			metadata.columns = slices.Clone(e.inferTopLevelReturnColumnsFor(ctx, body))
			for _, name := range metadata.columns {
				metadata.scope[name] = struct{}{}
			}
		}
		return []pipelineRow{}, &QueryStats{}, true, nil
	}

	targetExec := e
	if use, remaining, hasUse, err := parseUseClause(body, true); hasUse || err != nil {
		if err != nil {
			return nil, nil, true, err
		}
		if err := e.dynamicUseError(use); err != nil {
			return nil, nil, true, err
		}
		scoped, scopedCtx, err := e.useDatabase(ctx, use.Name, remaining)
		if err != nil {
			return nil, nil, true, err
		}
		targetExec = scoped
		ctx = scopedCtx
		body = remaining
	}

	branches, unionAll, _, isUnion = parseTopLevelUnionBranchesFor(ctx, body)
	if !isUnion {
		branches = []string{body}
	}
	bindings := make(map[string]callBranchBindingPlan, len(branches))
	independentRead := !callSubqueryQueryIsWriteFor(ctx, body)
	for branchIndex, branch := range branches {
		plan, err := planCallBranchBindings(branch, rows[0], scoped, scopedImports, branchIndex)
		if err != nil {
			return nil, nil, true, err
		}
		bindings[strings.TrimSpace(branch)] = plan
		independentRead = independentRead && len(plan.inputs) == 0
	}

	run := func(runExec *StorageExecutor, runCtx context.Context, outerRows []pipelineRow) ([]pipelineRow, *QueryStats, bool, error) {
		clauses, ok := pipelineClausesFor(body)
		if !ok && !isUnion {
			return nil, nil, false, nil
		}
		stats := &QueryStats{}
		out := make([]pipelineRow, 0, len(outerRows))
		hasReturn := isUnion || pipelineHasClauseKind(clauses, pipelineClauseReturn)
		// What each outer row's run of the body takes goes back when the run
		// ends; the rows kept from it count below (#1343).
		memory := memoryScopeFromContext(runCtx)
		retention := newMemoryRetention(runCtx)
		defer retention.close()
		for _, outer := range outerRows {
			rowMark := memory.mark()
			runBranch := func(query string) (*ExecuteResult, error) {
				branchClauses := clauses
				if isUnion {
					var planned bool
					branchClauses, planned = pipelineClausesFor(query)
					if !planned {
						return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "CALL branch could not be planned as a clause pipeline")
					}
				}
				branchInput, carried, missing := callBranchBindingValues(bindings[strings.TrimSpace(query)], outer)
				if missing != "" {
					return nil, localizedError(localization.CypherSubqueriesWithImportUnknownVariable(missing), nil)
				}
				scope := make(map[string]struct{}, len(branchInput))
				for name := range branchInput {
					scope[name] = struct{}{}
				}
				bindParameterRow(runCtx, branchInput)
				branchCtx := withPipelineImports(withValueBindings(runCtx, branchInput), carried)

				inner, handled, execErr := runExec.runPipelineClauseRows(branchCtx, []pipelineRow{branchInput}, scope, branchClauses, branchClauses, &pipelineRowOutput{})
				if execErr == nil {
					execErr = getExpressionFailure(runCtx)
				}
				if !handled && execErr == nil {
					execErr = newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "CALL branch could not be executed as a clause pipeline")
				}
				return inner, execErr
			}
			var inner *ExecuteResult
			var execErr error
			if isUnion {
				inner, execErr = runExec.executeUnionBranches(runCtx, body, unionAll, runBranch)
			} else {
				inner, execErr = runBranch(body)
			}
			memory.releaseSince(rowMark)
			// The rows kept from the run count before they are built: the
			// outer row's values once, each returned row's in full (#1343).
			if retention.tracking() && execErr == nil {
				if !hasReturn {
					execErr = retention.reserveExtendedRow(outer, nil, nil)
				} else if inner != nil {
					for _, values := range inner.Rows {
						if execErr = retention.reserveExtendedRow(outer, inner.Columns, values); execErr != nil {
							break
						}
					}
				}
			}
			if inner != nil {
				addQueryStats(stats, inner.Stats)
				if len(inner.Columns) > 0 {
					if metadata != nil {
						metadata.columns = append(metadata.columns[:0], inner.Columns...)
						for _, name := range inner.Columns {
							metadata.scope[name] = struct{}{}
						}
					}
				}
			}
			if execErr != nil {
				if hasReturn && inner != nil {
					out = appendCallReturnRows(out, outer, inner.Columns, inner.Rows)
				}
				return out, stats, true, execErr
			}
			if hasReturn {
				if inner != nil {
					out = appendCallReturnRows(out, outer, inner.Columns, inner.Rows)
				}
			} else {
				retained := make(pipelineRow, len(outer))
				for name, value := range outer {
					retained[name] = value
				}
				out = append(out, retained)
			}
		}
		// Every row kept is charged in full: the rows they replace give
		// theirs back (rowGenerations).
		return retention.owned(out), stats, true, nil
	}

	if !inTransactions {
		if independentRead {
			innerRows, stats, ok, err := run(targetExec, ctx, []pipelineRow{{}})
			if err != nil || !ok {
				return innerRows, stats, ok, err
			}
			// The product counts against the statement's memory limit
			// before it is built: its slots, then each merged row (#1343).
			retention := newMemoryRetention(ctx)
			defer retention.close()
			if err := retention.reserveSlots(len(rows)*len(innerRows), valueSlotBytes); err != nil {
				return nil, stats, true, err
			}
			joined := make([]pipelineRow, 0, len(rows)*len(innerRows))
			for _, outer := range rows {
				for _, inner := range innerRows {
					if err := retention.reserveMergedRow(outer, inner); err != nil {
						return nil, stats, true, err
					}
					joined = append(joined, mergeCallPipelineRows(outer, inner))
				}
			}
			return retention.owned(joined), stats, true, nil
		}
		return run(targetExec, ctx, rows)
	}

	if err := targetExec.rejectCallInTransactionsInExplicitTx(); err != nil {
		return nil, nil, true, err
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	combined := make([]pipelineRow, 0, len(rows))
	stats := &QueryStats{}
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		var batchRows []pipelineRow
		var batchStats *QueryStats
		_, err := targetExec.executeWithImplicitTransactionCallback(txphase.Statements(ctx), clause, upperASCII(clause), func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
			var handled bool
			var runErr error
			batchRows, batchStats, handled, runErr = run(txExec, txCtx, batch)
			if !handled && runErr == nil {
				runErr = localizedError(localization.CypherSubqueriesTransactionBodyEmpty("CALL"), nil)
			}
			return &ExecuteResult{Stats: batchStats}, runErr
		})
		if err != nil {
			return combined, stats, true, err
		}
		combined = append(combined, batchRows...)
		addQueryStats(stats, batchStats)
	}
	return combined, stats, true, nil
}

func appendCallReturnRows(out []pipelineRow, outer pipelineRow, columns []string, rows [][]interface{}) []pipelineRow {
	for _, values := range rows {
		joined := make(pipelineRow, len(outer)+len(columns))
		for name, value := range outer {
			joined[name] = value
		}
		for index, name := range columns {
			if index < len(values) {
				joined[name] = values[index]
			} else {
				joined[name] = nil
			}
		}
		out = append(out, joined)
	}
	return out
}

func mergeCallPipelineRows(outer, inner pipelineRow) pipelineRow {
	joined := make(pipelineRow, len(outer)+len(inner))
	for name, value := range outer {
		joined[name] = value
	}
	for name, value := range inner {
		joined[name] = value
	}
	return joined
}

func callPipelineRowsFromResult(ctx context.Context, result *ExecuteResult) []pipelineRow {
	if result == nil {
		row := pipelineRow{}
		bindParameterRow(ctx, row)
		return []pipelineRow{row}
	}
	if len(result.Rows) == 0 {
		return []pipelineRow{}
	}
	rows := make([]pipelineRow, 0, len(result.Rows))
	params := getParamsFromContext(ctx)
	for _, values := range result.Rows {
		row := make(pipelineRow, len(result.Columns)+len(params))
		for index, name := range result.Columns {
			if index < len(values) {
				row[name] = values[index]
			} else {
				row[name] = nil
			}
		}
		bindParameterRow(ctx, row)
		rows = append(rows, row)
	}
	return rows
}

func callPipelineResultFromRows(rows []pipelineRow, seed *ExecuteResult, stats *QueryStats, callColumns []string) *ExecuteResult {
	columns := make([]string, 0)
	seen := make(map[string]struct{})
	if seed != nil {
		for _, name := range seed.Columns {
			if _, exists := seen[name]; !exists {
				seen[name] = struct{}{}
				columns = append(columns, name)
			}
		}
	}
	for _, name := range callColumns {
		if _, exists := seen[name]; !exists {
			seen[name] = struct{}{}
			columns = append(columns, name)
		}
	}
	additional := make(map[string]struct{})
	for _, row := range rows {
		for name := range row {
			if !strings.HasPrefix(name, "$") {
				if _, exists := seen[name]; !exists {
					additional[name] = struct{}{}
				}
			}
		}
	}
	sorted := make([]string, 0, len(additional))
	for name := range additional {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	columns = append(columns, sorted...)
	result := &ExecuteResult{Columns: columns, Rows: make([][]interface{}, 0, len(rows)), Stats: mergeQueryStats(seedStats(seed), stats)}
	for _, row := range rows {
		values := make([]interface{}, len(columns))
		for index, name := range columns {
			values[index] = row[name]
		}
		result.Rows = append(result.Rows, values)
	}
	return result
}

func seedStats(seed *ExecuteResult) *QueryStats {
	if seed == nil {
		return nil
	}
	return seed.Stats
}
