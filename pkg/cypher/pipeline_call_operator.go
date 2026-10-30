package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/callinputtrace"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type pipelineCallMetadata struct {
	scope   map[string]struct{}
	columns []string
	batch   *boundTransactionBatch
	call    *resultmetadata.Call
}

func (e *StorageExecutor) pipelineApplyCallSubqueryWithMetadata(ctx context.Context, rows []pipelineRow, clause string, metadata *pipelineCallMetadata) ([]pipelineRow, *QueryStats, bool, error) {
	body, afterCall, inTransactions, batchExpression := e.parseCallSubquery(ctx, clause)
	if err := validateCallSubqueryBody(body); err != nil {
		return nil, nil, true, err
	}
	if strings.TrimSpace(afterCall) != "" {
		return nil, nil, false, nil
	}
	var batch *boundTransactionBatch
	if inTransactions {
		if metadata != nil {
			batch = metadata.batch
		}
		size, err := e.resolveCallBatchSize(ctx, batchExpression, batch)
		if err != nil {
			return nil, nil, true, err
		}
		if batch == nil {
			batch = &boundTransactionBatch{size: size}
		}
	}
	if metadata == nil || metadata.call == nil || metadata.call.Statement == nil {
		return nil, nil, true, unresolvedStatementError()
	}
	call := metadata.call
	imports := call.Imports
	targetExec := e
	if use, remaining, hasUse, err := parseUseClause(ctx, body, true, cyphertext.AcceptedSource); hasUse || err != nil {
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

	if len(rows) == 0 {
		var failure error
		call.Body.Visit(func(columns []string) { metadata.columns = columns }, func() { failure = unresolvedStatementError() })
		if failure != nil {
			return nil, nil, true, failure
		}
		for _, name := range metadata.columns {
			metadata.scope[name] = struct{}{}
		}
		return []pipelineRow{}, &QueryStats{}, true, nil
	}

	write := callSubqueryQueryIsWrite(body)
	independentRead := len(imports) == 0 && !write
	branches, unionAll, _, isUnion := parseTopLevelUnionBranches(ctx, body)
	if !isUnion {
		branches = []string{body}
	}

	// Execution consumes the compiler's branch inputs. No row or text reader
	// reconstructs imports, local-shadowing decisions, or returning/unit kind.
	branchFacts := call.Statement.Branches
	if len(branchFacts) == 0 {
		branchFacts = []resultmetadata.Statement{*call.Statement}
	}
	if len(branchFacts) != len(branches) {
		return nil, nil, true, unresolvedStatementError()
	}
	branchInputs := map[string]map[string]struct{}{}
	branchRetained := map[string][]string{}
	for index, query := range branches {
		var allowed map[string]struct{}
		branchFacts[index].Incoming.Visit(func(names []string) {
			allowed = map[string]struct{}{}
			for _, name := range names {
				allowed[name] = struct{}{}
			}
		}, func() {})
		if allowed == nil {
			return nil, nil, true, unresolvedStatementError()
		}
		branchInputs[strings.TrimSpace(query)] = allowed
		var retained []string
		known := false
		branchFacts[index].Retained.Visit(func(names []string) { retained, known = names, true }, func() {})
		if !known {
			return nil, nil, true, unresolvedStatementError()
		}
		branchRetained[strings.TrimSpace(query)] = retained
	}

	run := func(runExec *StorageExecutor, runCtx context.Context, outerRows []pipelineRow) ([]pipelineRow, *QueryStats, bool, error) {
		clauses, ok := pipelineClausesFor(runCtx, body)
		if !ok && !isUnion {
			return nil, nil, false, nil
		}
		stats := &QueryStats{}
		out := make([]pipelineRow, 0, len(outerRows))
		hasReturn := call.Kind == resultmetadata.CallReturning
		// What each outer row's run of the body takes goes back when the run
		// ends; the rows kept from it count below (#1343).
		memory := memoryScopeFromContext(runCtx)
		kept := memoryAccount{scope: memory}
		keptRows := rowEstimator{known: memory.knownLists()}
		defer kept.close()
		for _, outer := range outerRows {
			rowMark := memory.mark()
			input := pipelineRow{}
			bindParameterRow(runCtx, input)
			for _, name := range imports {
				value, exists := outer[name]
				if !exists {
					return out, stats, true, localizedError(localization.CypherSubqueriesWithImportUnknownVariable(name), nil)
				}
				input[name] = value
			}
			runBranch := func(query string) (*ExecuteResult, error) {
				branchClauses := clauses
				if isUnion {
					var planned bool
					branchClauses, planned = pipelineClausesFor(runCtx, query)
					if !planned {
						return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "CALL branch could not be planned as a clause pipeline")
					}
				}
				branchInput := make(pipelineRow, len(input))
				scope := make(map[string]struct{}, len(input))
				for name, value := range input {
					if _, allowed := branchInputs[strings.TrimSpace(query)][name]; !allowed && !strings.HasPrefix(name, "$") {
						continue
					}
					branchInput[name] = value
					if !strings.HasPrefix(name, "$") {
						scope[name] = struct{}{}
					}
				}
				if callinputtrace.Enabled(runCtx) {
					var inputNames []string
					for name := range scope {
						inputNames = append(inputNames, name)
					}
					for index, candidate := range branches {
						if strings.TrimSpace(candidate) == strings.TrimSpace(query) {
							callinputtrace.Observe(runCtx, "local-execution", branchFacts[index], inputNames)
							break
						}
					}

				}
				var carried pipelineRow
				if retained := branchRetained[strings.TrimSpace(query)]; len(retained) > 0 {
					carried = make(pipelineRow, len(retained))
					for _, name := range retained {
						carried[name] = branchInput[name]
					}
				}
				branchCtx := withPipelineImports(withValueBindings(runCtx, branchInput), carried)
				for index, candidate := range branches {
					if strings.TrimSpace(candidate) == strings.TrimSpace(query) {
						branchCtx = withSemanticBodyClause(branchCtx, executionBranchFrame(runCtx, branchFacts[index].Start))
						break
					}
				}

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
				inner, execErr = runExec.executeUnion(runCtx, body, unionAll, runBranch)
			} else {
				inner, execErr = runBranch(body)
			}
			memory.releaseSince(rowMark)
			if memory != nil && execErr == nil && hasReturn && inner != nil {
				outerBytes := keptRows.bytes(outer)
				for _, values := range inner.Rows {
					if execErr = kept.grow(estimateResultRowBytes(values, keptRows.known) + outerBytes); execErr != nil {
						break
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

	if err := targetExec.validateTransactionalCallInput(len(rows)); err != nil {
		return nil, nil, true, err
	}
	combined := make([]pipelineRow, 0, len(rows))
	stats := &QueryStats{}
	for start := 0; start < len(rows); start += batch.size {
		end := start + batch.size
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		var batchRows []pipelineRow
		var batchStats *QueryStats
		_, err := targetExec.executeTransactionBatch(ctx, clause, func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
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
