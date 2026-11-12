package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type pipelineCallMetadata struct {
	scope   map[string]struct{}
	columns []string
	batch   *boundTransactionBatch
}

func (e *StorageExecutor) pipelineApplyCallSubqueryWithMetadata(ctx context.Context, rows []pipelineRow, clause pipelineClause, metadata *pipelineCallMetadata) ([]pipelineRow, *QueryStats, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, true, err
	}
	call := cypherantlr.DescribeCall(clause.syntax)
	inTransactions, batchExpression := call.Batch != nil, call.Batch
	if err := validateCallSubqueryBody(strings.TrimSpace(queryBodyWindow(clause))); err != nil {
		return nil, nil, true, err
	}
	body, retainedBody := callBodyFromSyntax(clause)
	if !retainedBody {
		return nil, nil, true, unresolvedStatementError()
	}
	// CALL's body text, projected from the retained product, serves the owners
	// that still take source (USE admission, write detection, UNION execution
	// and transaction batches). Imports and branch locals read the body itself:
	// the retained clauses of the text those owners run.
	bodyText := ""
	bodyTextLoaded := false
	text := func() string {
		if !bodyTextLoaded {
			bodyText, bodyTextLoaded = queryBodyText(body), true
		}
		return bodyText
	}
	importBody := body
	// A transactional body keeps the text admission: its batch owner reads the
	// CALL's text, and each batch runs the body's clauses from that text.
	useBody := inTransactions
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
	// An unscoped body imports through its branches' leading WITHs
	// (unscopedCallImports, #907).
	program, err := boundClauseProgram(ctx, clause)
	if err != nil {
		return nil, nil, true, err
	}
	compiled, found := program.calls[clause.syntax.ID()]
	if !found {
		return nil, nil, true, unresolvedStatementError()
	}
	compiledCall := compiled.call
	callScope := scopeClauseFromSyntax(clause.syntax)
	scopedImports, scoped := callScope.names, callScope.scoped

	targetExec := e
	if body.branches[0].useStart >= 0 {
		// A USE-bearing body keeps the explicit text admission: USE selects the
		// graph its remaining clauses run on, and that remaining text stays the
		// body the pipeline reads for this one form.
		use, remaining, hasUse, err := parseUseClause(ctx, text(), true, cyphertext.AcceptedSource)
		if err != nil {
			return nil, nil, true, err
		}
		if hasUse {
			if err := e.dynamicUseError(use); err != nil {
				return nil, nil, true, err
			}
			scoped, scopedCtx, err := e.useDatabase(ctx, use.Name, remaining)
			if err != nil {
				return nil, nil, true, err
			}
			targetExec = scoped
			ctx = scopedCtx
			bodyText, bodyTextLoaded, useBody = remaining, true, true
			// The remaining source is the body those clauses now are: it is admitted
			// once, here, so the post-USE imports and branch-local keys describe the
			// text that runs rather than the original USE-bearing statement.
			importBody = readQueryBody(ctx, remaining, 0, len(remaining))
		}
	}

	if len(rows) == 0 {
		if metadata != nil {
			columns, err := e.retainedClauseColumns(ctx, clause, metadata.scope)
			if err != nil {
				return nil, nil, true, err
			}
			metadata.columns = columns
			for _, name := range metadata.columns {
				metadata.scope[name] = struct{}{}
			}
		}
		return []pipelineRow{}, &QueryStats{}, true, nil
	}

	hasLegacyImports := false
	if !scoped {
		var importErr error
		_, hasLegacyImports, importErr = unscopedCallImports(ctx, importBody, func(name string) bool {
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
	imports := compiledCall.Imports

	// The body's effect is the statement-effect owner's answer for the text that
	// runs: a registered WRITE procedure is a write of an uncorrelated body, so
	// the body runs where the input rows are instead of once for all of them.
	bodyInfo, bodyInfoErr := e.analyzer.Analyze(ctx, text())
	if bodyInfoErr != nil {
		return nil, nil, true, bodyInfoErr
	}
	write := bodyInfo.Writes()
	independentRead := len(scopedImports) == 0 && !hasLegacyImports && len(imports) == 0 && !write
	_, unionAll, _, isUnion := parseTopLevelUnionBranches(ctx, text())

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
		// Each retained branch is keyed by the text it runs as: its own projected
		// bytes, which is the branch the UNION executor selects and, without
		// UNION, the body itself.
		branchTexts := queryBranchTexts(importBody)
		for index, branch := range importBody.branches {
			locals := unscopedBranchLocals(ctx, branch, outerNames)
			if len(locals) == 0 || index >= len(branchTexts) {
				continue
			}
			branchLocals[branchTexts[index]] = locals
		}
	}

	// Names.Visit protects compiled metadata by copying its slice. Resolve the
	// immutable selection once per call, rather than allocating once per row.
	var retainedNames []string
	compiledCall.Retained.Visit(func(names []string) { retainedNames = names }, func() {})
	var branchRetainedNames [][]string
	if isUnion {
		branchRetainedNames = make([][]string, len(compiledCall.Statement.Branches))
		for index, branch := range compiledCall.Statement.Branches {
			branch.Retained.Visit(func(names []string) { branchRetainedNames[index] = names }, func() {})
		}
	}

	run := func(runExec *StorageExecutor, runCtx context.Context, outerRows []pipelineRow) ([]pipelineRow, *QueryStats, bool, error) {
		if useBody {
			// USE/batch admission changes the source window, not its binding frame.
			// A private clone carries that immutable compile mode; row values remain
			// execution inputs and never enter a program-cache key.
			runExec = runExec.cloneWithStorage(runExec.getStorage(runCtx))
			runExec.inputBindingRole = boundImplicitCallBody
			if scoped {
				runExec.inputBindingRole = boundScopedCallBody
			}
		}
		clauses, ok := pipelineClausesForBody(runCtx, body)
		if useBody {
			clauses, ok = pipelineClausesFor(runCtx, text())
		}
		if !ok && !isUnion {
			return nil, nil, false, nil
		}
		stats := &QueryStats{}
		out := make([]pipelineRow, 0, len(outerRows))
		hasReturn := compiledCall.Kind == resultmetadata.CallReturning
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
			branchOrdinal := 0
			runBranch := func(runCtx context.Context, query string) (*ExecuteResult, error) {
				branchClauses := clauses
				if isUnion {
					if branchOrdinal >= len(importBody.branches) {
						return nil, unresolvedStatementError()
					}
					// Preserve the body's admitted source and binding identities.
					// Reparsing the branch as a root made readable imports into
					// wildcard columns and discarded the enclosing scope contract.
					split := cachedPipelineClausesBranch(runCtx, importBody.branches[branchOrdinal], true)
					branchOrdinal++
					branchClauses = split.clauses
					planned := split.ok
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
				for _, name := range branchLocals[strings.TrimSpace(query)] {
					delete(branchInput, name)
					delete(scope, name)
				}
				carried := pipelineRow{}
				names := retainedNames
				if isUnion && branchOrdinal > 0 {
					names = branchRetainedNames[branchOrdinal-1]
				}
				for _, name := range names {
					carried[name] = branchInput[name]
				}

				branchCtx := withPipelineImports(withValueBindings(runCtx, branchInput), carried)

				inner, handled, execErr := runExec.runPipelineClauseRows(branchCtx, []pipelineRow{branchInput}, scope, branchClauses, branchClauses, &pipelineRowOutput{}, nil)
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
			// A UNION body is executed branch by branch on its text; every other
			// body runs the retained clauses. Only an unscoped body's branch-local
			// filter is keyed by the branch text.
			branchText := ""
			if isUnion || len(branchLocals) > 0 {
				branchText = text()
			}
			if isUnion {
				inner, execErr = runExec.executeUnion(runCtx, branchText, unionAll, runBranch)
			} else {
				inner, execErr = runBranch(runCtx, branchText)
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
		_, err := targetExec.executeTransactionBatch(ctx, clause.text, func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
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
