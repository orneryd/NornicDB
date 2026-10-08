package cypher

import (
	"context"
	"sort"
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
	body, afterCall, inTransactions, batchSize := e.parseCallSubquery(clause)
	if body == "" {
		return nil, nil, true, localizedError(localization.CypherSubqueriesCallBodyExpected(), nil)
	}
	if strings.TrimSpace(afterCall) != "" {
		return nil, nil, false, nil
	}
	// An unscoped body imports through its branches' leading WITHs
	// (unscopedCallImports, #907).
	scoped := callSubqueryHasScopeClause(clause)
	if metadata != nil {
		declared := parseCallSubqueryImportVariables(clause)
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
			metadata.columns = e.inferTopLevelReturnColumns(body)
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
		if err := e.authorizeSelectedDatabase(ctx, use.Name); err != nil {
			return nil, nil, true, err
		}
		scoped, database, err := e.scopedExecutorForUse(use.Name, GetAuthTokenFromContext(ctx))
		if err != nil {
			return nil, nil, true, localizedError(localization.CypherSubqueriesUseDatabaseFailed(use.Name, err), err)
		}
		targetExec = scoped
		ctx = withExecutionDatabase(ctx, database)
		body = remaining
	}

	scopedImports := parseCallSubqueryImportVariables(clause)
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
	// CALL (*) imports every outer variable the body reads. So does an
	// unscoped subquery without an importing WITH: a NornicDB extension.
	// Neo4j 5.26 rejects the outer variable there ("Variable `n` not
	// defined"); NornicDB keeps running such statements as before, since
	// the import is unambiguous (#907, kept at the owner's direction).
	imports := scopedImports
	if scopedImports == nil && (scoped || !hasLegacyImports) && len(rows) > 0 {
		for name := range rows[0] {
			if !strings.HasPrefix(name, "$") && isIdentifierReferenced(body, name) {
				imports = append(imports, name)
			}
		}
	}
	write := callSubqueryQueryIsWrite(body)
	independentRead := len(scopedImports) == 0 && !hasLegacyImports && len(imports) == 0 && !write
	_, unionAll, _, isUnion := parseTopLevelUnionBranches(body)

	run := func(runExec *StorageExecutor, runCtx context.Context, outerRows []pipelineRow) ([]pipelineRow, *QueryStats, bool, error) {
		clauses, ok := pipelineClausesFor(body)
		if !ok && !isUnion {
			return nil, nil, false, nil
		}
		stats := &QueryStats{}
		out := make([]pipelineRow, 0, len(outerRows))
		hasReturn := isUnion || pipelineHasClauseKind(clauses, pipelineClauseReturn)
		for _, outer := range outerRows {
			input := pipelineRow{}
			bindParameterRow(runCtx, input)
			if hasLegacyImports {
				for name, value := range outer {
					input[name] = value
				}
			} else {
				for _, name := range imports {
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
					branchInput[name] = value
					if !strings.HasPrefix(name, "$") {
						scope[name] = struct{}{}
					}
				}
				inner, handled, execErr := runExec.runPipelineClauseRows(withValueBindings(runCtx, branchInput), []pipelineRow{branchInput}, scope, branchClauses, branchClauses, &pipelineRowOutput{})
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
				inner, execErr = runExec.executeUnionBranches(body, unionAll, runBranch)
			} else {
				inner, execErr = runBranch(body)
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
		return out, stats, true, nil
	}

	if !inTransactions {
		if independentRead {
			innerRows, stats, ok, err := run(targetExec, ctx, []pipelineRow{{}})
			if err != nil || !ok {
				return innerRows, stats, ok, err
			}
			joined := make([]pipelineRow, 0, len(rows)*len(innerRows))
			for _, outer := range rows {
				for _, inner := range innerRows {
					joined = append(joined, mergeCallPipelineRows(outer, inner))
				}
			}
			return joined, stats, true, nil
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
