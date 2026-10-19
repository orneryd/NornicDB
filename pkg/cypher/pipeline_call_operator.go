package cypher

import (
	"context"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func (e *StorageExecutor) pipelineApplyCallSubquery(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, *QueryStats, bool, error) {
	return e.pipelineApplyCallSubqueryWithMetadata(ctx, rows, clause, nil)
}

type pipelineCallMetadata struct {
	scope   map[string]struct{}
	columns []string
}

func (e *StorageExecutor) pipelineApplyCallSubqueryWithMetadata(ctx context.Context, rows []pipelineRow, clause string, metadata *pipelineCallMetadata) ([]pipelineRow, *QueryStats, bool, error) {
	ctx = withPipelineCancellation(ctx)
	// The statement parses the clause and its body once (pipelineClausePlans),
	// however many outer rows run the operator.
	ctx = withPipelineClausePlans(ctx)
	preparationProbe := pipelineCancellationForContext(ctx)
	parsed := e.callSubqueryParseFor(ctx, clause, preparationProbe)
	if preparationProbe.err != nil {
		return nil, nil, true, preparationProbe.err
	}
	body, afterCall, inTransactions, batchSize := parsed.body, parsed.afterCall, parsed.inTransactions, parsed.batchSize
	if body == "" {
		return nil, nil, true, localizedError(localization.CypherSubqueriesCallBodyExpected(), nil)
	}
	if strings.TrimSpace(afterCall) != "" {
		return nil, nil, false, nil
	}
	// An unscoped body imports through its branches' leading WITHs
	// (unscopedCallImports, #907).
	scoped := parsed.scoped
	if metadata != nil {
		declared := parsed.scopedImports
		if declared == nil && !scoped {
			var importErr error
			declared, _, importErr = unscopedCallImports(body, func(name string) bool {
				_, exists := metadata.scope[name]
				return exists
			}, true)
			if importErr != nil {
				return nil, nil, true, importErr
			}
		}
		for _, name := range declared {
			if _, exists := metadata.scope[name]; !exists {
				return nil, nil, true, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UndefinedVariable", "variable "+name+" is not defined")
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

	scopedImports := parsed.scopedImports
	hasLegacyImports := false
	if !scoped {
		var importErr error
		_, hasLegacyImports, importErr = unscopedCallImports(body, func(name string) bool {
			_, exists := rows[0][name]
			return exists && !strings.HasPrefix(name, "$")
		}, false)
		if importErr != nil {
			return nil, nil, true, importErr
		}
	}
	// CALL (*) imports every outer variable, including for WITH/RETURN *.
	// An unscoped body without an importing WITH implicitly imports the
	// outer variables it reads: a NornicDB extension.
	// Neo4j 5.26 rejects the outer variable there ("Variable `n` not
	// defined"); NornicDB keeps running such statements as before, since
	// the import is unambiguous (#907, kept at the owner's direction).
	imports := scopedImports
	if scopedImports == nil && (scoped || !hasLegacyImports) && len(rows) > 0 {
		for name := range rows[0] {
			if !strings.HasPrefix(name, "$") && (scoped || isIdentifierReferenced(body, name)) {
				imports = append(imports, name)
			}
		}
	}
	write := callSubqueryQueryIsWriteFor(ctx, body)
	independentRead := len(scopedImports) == 0 && !hasLegacyImports && len(imports) == 0 && !write
	unionBranches, unionAll, _, isUnion := parseTopLevelUnionBranchesFor(ctx, body)

	// A branch of an unscoped body that declares an outer variable's name
	// itself (MATCH (n)-->(m)) gets a new variable, as in Cypher, not the
	// implicitly imported outer value (unscopedBranchLocals, #907).
	var branchLocals map[string][]string
	if !scoped && len(rows) > 0 {
		var outerNames []string
		for name := range rows[0] {
			if !strings.HasPrefix(name, "$") {
				outerNames = append(outerNames, name)
			}
		}
		branchLocals = map[string][]string{}
		branches := []string{body}
		if isUnion {
			branches = unionBranches
		}
		for _, branch := range branches {
			if locals := unscopedBranchLocals(branch, outerNames); len(locals) > 0 {
				branchLocals[strings.TrimSpace(branch)] = locals
			}
		}
	}

	run := func(runExec *StorageExecutor, runCtx context.Context, outerRows []pipelineRow) ([]pipelineRow, *QueryStats, bool, error) {
		clauses, ok := pipelineClausesFor(body)
		if !ok && !isUnion {
			return nil, nil, false, nil
		}
		runCtx = withPipelineCancellation(runCtx)
		cancellation := pipelineCancellationForContext(runCtx)
		stats := &QueryStats{}
		out := make([]pipelineRow, 0, len(outerRows))
		hasReturn := isUnion || pipelineHasClauseKind(clauses, pipelineClauseReturn)
		// What each outer row's run of the body takes goes back when the run
		// ends; the rows kept from it count below (#1343).
		memory := memoryScopeFromContext(runCtx)
		retention := newMemoryRetention(runCtx)
		defer retention.close()
		for _, outer := range outerRows {
			if !cancellation.check() {
				return nil, stats, true, cancellation.err
			}
			rowMark := memory.mark()
			input := pipelineRow{}
			bindParameterRow(runCtx, input)
			if hasLegacyImports {
				for name, value := range outer {
					if !cancellation.check() {
						return nil, stats, true, cancellation.err
					}
					input[name] = value
				}
			} else {
				for _, name := range imports {
					if !cancellation.check() {
						return nil, stats, true, cancellation.err
					}
					value, exists := outer[name]
					if !exists {
						return out, stats, true, localizedError(localization.CypherSubqueriesWithImportUnknownVariable(name), nil)
					}
					input[name] = value
				}
			}
			runBranch := func(query string) (*ExecuteResult, error) {
				branchClauses := clauses
				if isUnion {
					var planned bool
					branchClauses, planned = pipelineClausesFor(query)
					if !planned {
						return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "CALL branch could not be planned as a clause pipeline")
					}
				}
				branchInput := make(pipelineRow, len(input))
				scope := make(map[string]struct{}, len(input))
				for name, value := range input {
					if !cancellation.check() {
						return nil, cancellation.err
					}
					branchInput[name] = value
					if !strings.HasPrefix(name, "$") {
						scope[name] = struct{}{}
					}
				}
				for _, name := range branchLocals[strings.TrimSpace(query)] {
					if !cancellation.check() {
						return nil, cancellation.err
					}
					delete(branchInput, name)
					delete(scope, name)
				}
				var carried pipelineRow
				if scoped {
					carried = make(pipelineRow, len(imports))
					for _, name := range imports {
						carried[name] = branchInput[name]
					}
				}
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
						if cancellation.stopped() {
							return nil, nil, false, cancellation.failed()
						}
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
						metadata.columns = appendPipelineItems(metadata.columns[:0], inner.Columns, cancellation)
						if cancellation.err != nil {
							return nil, stats, true, cancellation.err
						}
						for _, name := range inner.Columns {
							if !cancellation.check() {
								return nil, stats, true, cancellation.err
							}
							metadata.scope[name] = struct{}{}
						}
					}
				}
			}
			if execErr != nil {
				if hasReturn && inner != nil {
					out = appendCallReturnRows(runCtx, out, outer, inner.Columns, inner.Rows)
				}
				return out, stats, true, execErr
			}
			if hasReturn {
				if inner != nil {
					out = appendCallReturnRows(runCtx, out, outer, inner.Columns, inner.Rows)
				}
			} else {
				retained := make(pipelineRow, len(outer))
				for name, value := range outer {
					if !cancellation.check() {
						return nil, stats, true, cancellation.err
					}
					retained[name] = value
				}
				out = appendPipelineItem(out, retained, cancellation)
				if cancellation.err != nil {
					return nil, stats, true, cancellation.err
				}
			}
		}
		if failure := getExpressionFailure(runCtx); failure != nil {
			return nil, stats, true, failure
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
			ctx = withPipelineCancellation(ctx)
			probe := pipelineCancellationForContext(ctx)
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
					if !probe.check() {
						return nil, stats, true, probe.err
					}
					if err := retention.reserveMergedRow(outer, inner); err != nil {
						return nil, stats, true, err
					}
					merged := mergeCallPipelineRowsWithCancellation(outer, inner, probe)
					if probe.err != nil {
						return nil, stats, true, probe.err
					}
					joined = appendPipelineItem(joined, merged, probe)
					if probe.err != nil {
						return nil, stats, true, probe.err
					}
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
		_, err := targetExec.executeWithImplicitTransactionCallback(ctx, clause, upperASCII(clause), func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
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

func appendCallReturnRows(ctx context.Context, out []pipelineRow, outer pipelineRow, columns []string, rows [][]interface{}) []pipelineRow {
	ctx = withPipelineCancellation(ctx)
	probe := pipelineCancellationForContext(ctx)
	for _, values := range rows {
		if !probe.check() {
			return nil
		}
		joined := make(pipelineRow, len(outer)+len(columns))
		for name, value := range outer {
			if !probe.check() {
				return nil
			}
			joined[name] = value
		}
		for index, name := range columns {
			if !probe.check() {
				return nil
			}
			if index < len(values) {
				joined[name] = values[index]
			} else {
				joined[name] = nil
			}
		}
		out = appendPipelineItem(out, joined, probe)
		if probe.err != nil {
			return nil
		}
	}
	return out
}

func mergeCallPipelineRows(outer, inner pipelineRow) pipelineRow {
	return mergeCallPipelineRowsWithCancellation(outer, inner, nil)
}
func mergeCallPipelineRowsWithCancellation(outer, inner pipelineRow, probe *pipelineCancellation) pipelineRow {
	if probe != nil && !probe.check() {
		return nil
	}
	joined := make(pipelineRow, len(outer)+len(inner))
	for name, value := range outer {
		if probe != nil && !probe.check() {
			return nil
		}
		joined[name] = value
	}
	for name, value := range inner {
		if probe != nil && !probe.check() {
			return nil
		}
		joined[name] = value
	}
	return joined
}

func callPipelineRowsFromResult(ctx context.Context, result *ExecuteResult) []pipelineRow {
	ctx = withPipelineCancellation(ctx)
	probe := pipelineCancellationForContext(ctx)
	if !probe.check() {
		return nil
	}
	if result == nil {
		row := pipelineRow{}
		bindParameterRow(ctx, row)
		return []pipelineRow{row}
	}
	if len(result.Rows) == 0 {
		return []pipelineRow{}
	}
	rows := make([]pipelineRow, 0, pipelineInitialCapacity(len(result.Rows), probe))
	params := getParamsFromContext(ctx)
	for _, values := range result.Rows {
		if !probe.check() {
			return nil
		}
		row := make(pipelineRow, len(result.Columns)+len(params))
		for index, name := range result.Columns {
			if !probe.check() {
				return nil
			}
			if index < len(values) {
				row[name] = values[index]
			} else {
				row[name] = nil
			}
		}
		bindParameterRow(ctx, row)
		if probe.err != nil {
			return nil
		}
		rows = appendPipelineItem(rows, row, probe)
		if probe.err != nil {
			return nil
		}
	}
	return rows
}

func callPipelineResultFromRows(ctx context.Context, rows []pipelineRow, seed *ExecuteResult, stats *QueryStats, callColumns []string) *ExecuteResult {
	ctx = withPipelineCancellation(ctx)
	probe := pipelineCancellationForContext(ctx)
	if !probe.check() {
		return nil
	}
	columns := make([]string, 0)
	seen := pipelineStringSet{probe: probe}
	if seed != nil {
		for _, name := range seed.Columns {
			if !probe.check() {
				return nil
			}
			if !seen.add(name) {
				columns = appendPipelineItem(columns, name, probe)
			}
			if probe.err != nil {
				return nil
			}
		}
	}
	for _, name := range callColumns {
		if !probe.check() {
			return nil
		}
		if !seen.add(name) {
			columns = appendPipelineItem(columns, name, probe)
		}
		if probe.err != nil {
			return nil
		}
	}
	additional := pipelineStringSet{probe: probe}
	var sorted []string
	for _, row := range rows {
		if !probe.check() {
			return nil
		}
		for name := range row {
			if !probe.check() {
				return nil
			}
			if !strings.HasPrefix(name, "$") {
				if !seen.contains(name) && !additional.add(name) {
					sorted = appendPipelineItem(sorted, name, probe)
				}
				if probe.err != nil {
					return nil
				}
			}
		}
	}
	if !probe.sort(sorted, func(i, j int) bool { return comparePipelineStrings(sorted[i], sorted[j], probe) < 0 }, false) {
		return nil
	}
	columns = appendPipelineItems(columns, sorted, probe)
	if probe.err != nil {
		return nil
	}
	result := &ExecuteResult{Columns: columns, Rows: make([][]interface{}, 0, pipelineInitialCapacity(len(rows), probe)), Stats: mergeQueryStats(seedStats(seed), stats)}
	for _, row := range rows {
		if !probe.check() {
			return nil
		}
		values := make([]interface{}, len(columns))
		for index, name := range columns {
			if !probe.check() {
				return nil
			}
			values[index] = row[name]
		}
		result.Rows = appendPipelineItem(result.Rows, values, probe)
		if probe.err != nil {
			return nil
		}
	}
	return result
}

func seedStats(seed *ExecuteResult) *QueryStats {
	if seed == nil {
		return nil
	}
	return seed.Stats
}
