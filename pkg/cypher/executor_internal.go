// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// executeInternal executes a Cypher fragment as part of a larger execution flow.
//
// Unlike Execute(), this does NOT:
//   - apply query limits / rate limiting
//   - consult or populate result caches
//   - start implicit transactions for write queries
//
// The key property is that internal subqueries/procedure bodies participate in
// the caller's transaction context (explicit tx or implicit tx wrapper carried
// on ctx), avoiding nested implicit transactions and misrouting.
// executeUserStatement runs statement text a procedure was given to run
// (apoc.cypher.run / runMany, apoc.periodic.iterate / commit,
// apoc.export.*.query): a user's statement, which may not call NornicDB's
// internal functions (#1105). It runs inside the caller's statement, a data
// statement, so an administration command there is refused as after any
// data statement (ForbiddenDueToTransactionType), in auto-commit as in an
// explicit transaction.
func (e *StorageExecutor) executeUserStatement(ctx context.Context, statement string, params map[string]interface{}) (result *ExecuteResult, err error) {
	if totals, ok := ctx.Value(procedureStatementStatsKey{}).(*procedureStatementStats); ok && totals != nil {
		// Every nested procedure installs its own invocation collector. Its
		// completed summary enters this collector only through this result.
		defer func() {
			if err == nil && result != nil {
				totals.record(result.Stats)
			}
		}()
	}
	if err := internalFunctionCallError(statement); err != nil {
		// Refused like any statement of the caller's transaction: it fails
		// an explicit transaction (executeInternal fails it for the rest).
		return nil, e.failTransaction(ctx, err)
	}
	if StatementDatabase("", statement) == "system" {
		var transaction any
		if wrapper, ok := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); ok {
			transaction = wrapper.tx
		} else if e.txContext != nil && e.txContext.active {
			transaction = e.txContext.tx
		}
		return nil, e.failTransaction(ctx, statementTypeError(ctx, statement, "Administration", "Data", transaction))
	}
	return e.executeInternal(ctx, statement, params)
}

func (e *StorageExecutor) executeInternal(ctx context.Context, cypher string, params map[string]interface{}) (result *ExecuteResult, retErr error) {
	member := e.membership(ctx)
	if member.storage != nil {
		ctx = context.WithValue(ctx, ctxKeyTxStorage, member.storage)
	}
	if member.statement != nil {
		ctx = &compositeContext{Context: ctx, statement: member.statement, storage: member.storage}
	}

	if e.txContext != nil && e.txContext.active {
		defer func() { e.failTransaction(ctx, retErr) }()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, source := cyphertext.WithSource(ctx, cypher)
	if source.Err != nil {
		if invalid, ok := source.Err.(*cyphertext.UnicodeSourceError); ok {
			return nil, invalidUnicodeLiteralError(invalid.Offset)
		}
		return nil, source.Err
	}
	if cypher != source.Text {
		normalized := sourceQueryRewrite(source)
		defer func() { result, retErr = (resultRestoration{source: normalized}).restore(result, retErr, nil) }()
	}
	cypher = source.Text
	if config.IsCypherQueryNormalizationEnabled() {
		var err error
		var rewrite *queryRewrite
		cypher, rewrite, err = normalizeQueryText(ctx, cypher)
		ctx = withSourceRewrite(ctx, rewrite)
		if err != nil {
			return nil, err
		}
	}
	if canonical, rewrite := canonicalizeNumericLiterals(cypher); rewrite != nil {
		cypher = canonical
		ctx = withSourceRewrite(ctx, rewrite)
	}
	if canonical, rewrite := canonicalizeFunctionAliases(cypher); rewrite != nil {
		cypher = canonical
		ctx = withSourceRewrite(ctx, rewrite)
	}
	if cypherVersionFromContext(ctx) == "25" {
		if canonical, rewrite := separateCypher25Parameters(cypher); rewrite != nil {
			cypher = canonical
			ctx = withSourceRewrite(ctx, rewrite)
		}
	}
	if canonical, rewrite := desugarStandaloneOrderClauses(cypher); rewrite != nil {
		cypher = canonical
		ctx = withSourceRewrite(ctx, rewrite)
	}
	ctx, cypher = trimSourceText(ctx, cypher, trimSourceTerminator)
	if err := e.validateStatementFraming(ctx, cypher); err != nil {
		return nil, err
	}
	finishTerminated := false
	if stripped, ok := stripUnionBranchFinishes(ctx, cypher); ok {
		cypher = strings.TrimSpace(stripped)
		finishTerminated = true
		ctx = withFinishedStatement(ctx, cypher)
	}
	if cypher == "" {
		if finishTerminated {
			return &ExecuteResult{}, nil
		}
		return nil, localizedError(localization.CypherCoreEmptyQuery(), nil)
	}
	if finishTerminated {
		defer func() {
			completeFinishedResult(result)
		}()
	}

	if use, remaining, hasUse, err := parseUseClause(ctx, cypher, false, cyphertext.AcceptedSource); hasUse || err != nil {
		if err != nil {
			return nil, err
		}
		if err := e.dynamicUseError(use); err != nil {
			return nil, err
		}
		return e.executeOnDatabase(ctx, use.Name, remaining, params, true)
	}
	if _, err := e.admitTransactionStatement(ctx, cypher); err != nil {
		return nil, err
	}
	routed, err := e.routeAdministrationStatement(ctx, cypher)
	if err != nil {
		return nil, err
	}
	if routed != e {
		return routed.executeInternal(ctx, cypher, params)
	}
	if err := e.authorizeSelectedDatabase(ctx, "", cypher); err != nil {
		return nil, err
	}
	e.statementAdmitted(ctx)
	if err := e.systemDatabaseWriteError(ctx, cypher); err != nil {
		return nil, err
	}

	// Local fragments own their lowering and restore generated names before
	// returning to Fabric or their containing expression.
	desugared, labelRewrite, err := desugarLabelExpressions(cypher, params, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return nil, err
	}
	if labelRewrite != nil {
		cypher = desugared
		defer func() {
			result, retErr = (resultRestoration{source: labelRewrite}).restore(withoutGeneratedColumns(result), retErr, nil)
		}()
	}

	// Basic syntax validation to preserve existing error behavior.
	if err := e.validateSyntax(ctx, cypher, cypherVersionFromContext(ctx) == "25", syntaxStatement); err != nil {
		return nil, err
	}
	if err := e.validateTransactionalCalls(ctx, cypher, nil); err != nil {
		return nil, err
	}
	if err := e.validateDuplicateReturnColumnName(ctx, readQueryBodyText(ctx, cypher, 0, len(cypher)), quotedVariableNamesFor(ctx, cypher)); err != nil {
		return nil, err
	}
	if err := e.statementParametersError(ctx, cypher, params); err != nil {
		return nil, err
	}

	params = mergeInheritedQueryParams(ctx, params)
	params = normalizeQueryParameters(params)
	ctx = withQueryParams(ctx, params)
	if err := e.validateBoundParameterExpressions(ctx, cypher, params); err != nil {
		return nil, err
	}
	upper := e.cachedUpperQuery(cypher)
	// This is executable, normalized statement text with its actual parameter
	// values installed. Nested dynamic handlers return through this same gate;
	// no admission parser attempts to reconstruct their arguments.
	if err := e.admitResolvedStatementWrite(ctx, cypher); err != nil {
		return nil, err
	}

	// If we're in an explicit transaction, execute within it.
	if e.txContext != nil && e.txContext.active {
		return e.executeInTransaction(ctx, cypher, upper)
	}

	// Otherwise, stay on the caller's execution path (no implicit tx starts here).
	result, retErr = e.executeWithoutTransaction(ctx, cypher, upper)
	if retErr == nil && !e.inTransaction(ctx) {
		// An opaque procedure argument can contain a write even when its
		// caller opened no implicit transaction. Those effects apply directly.
		// A Fabric fragment instead belongs to the coordinator's commit.
		if _, pending := fabric.FabricTransactionFromContext(ctx); !pending {
			e.publishDirectStatementCommit(ctx, cypher)
		}
	}
	return result, retErr
}

func (e *StorageExecutor) validateBoundParameterExpressions(ctx context.Context, cypher string, params map[string]interface{}) error {
	if err := e.validateTransactionalCalls(ctx, cypher, params); err != nil {
		return err
	}
	// Only parse parameterized arithmetic when an operand's actual type can
	// be rejected; ordinary numeric hot paths keep their single cheap scan.
	if len(params) > 0 && parameterMayMismatchOperator(cypher, params) {
		if clauses, ok := splitPipelineTextClauses(ctx, cypher); ok {
			for _, clause := range clauses {
				if err := e.validateStaticOperatorTypes(ctx, clause, staticTypeScope{cypher25: cypherVersionFromContext(ctx) == "25"}, nil, params); err != nil {
					return err
				}
			}
		}
	}
	if err := validateStaticPropertyAccessParameters(ctx, cypher, params); err != nil {
		return err
	}
	if err := validateStaticFunctionParameters(cypher, params, cypherVersionFromContext(ctx) == "25"); err != nil {
		return err
	}
	// A transactional CALL commits independently before later runtime
	// pagination opens. Static pagination admission still runs at compilation;
	// its runtime values are checked by the owning pipeline operator.
	if !hasCallInTransactions(ctx, cypher) {
		if err := e.validateRuntimePaginationExpressions(ctx, extractPaginationExpressions(ctx, cypher)...); err != nil {
			return err
		}
	}
	return validateListOperands(cypher, params)
}
