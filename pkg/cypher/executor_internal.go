package cypher

import (
	"context"
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
		return nil, e.failTransaction(ctx, statementTypeError(statement, "Administration", "Data", transaction))
	}
	return e.executeInternal(ctx, statement, params)
}

func (e *StorageExecutor) executeInternal(ctx context.Context, cypher string, params map[string]interface{}) (result *ExecuteResult, retErr error) {
	if e.txContext != nil && e.txContext.active {
		defer func() { e.failTransaction(ctx, retErr) }()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cypher = normalizeCypherSyntaxConfusables(cypher)
	if config.IsCypherQueryNormalizationEnabled() {
		var err error
		cypher, _, err = normalizeQueryText(cypher)
		if err != nil {
			return nil, err
		}
	}
	cypher, _ = canonicalizeNumericLiterals(cypher)
	cypher, _ = canonicalizeFunctionAliases(cypher)
	if cypherVersionFromContext(ctx) == "25" {
		cypher, _ = separateCypher25Parameters(cypher)
	}
	cypher, _ = desugarStandaloneOrderClauses(cypher)
	cypher = strings.TrimSpace(cypher)
	cypher = trimTrailingStatementDelimiters(cypher)
	if err := e.validateStatementFraming(cypher); err != nil {
		return nil, err
	}
	finishTerminated := false
	if stripped, ok := stripUnionBranchFinishes(cypher); ok {
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

	if use, remaining, hasUse, err := parseUseClause(cypher, false); hasUse || err != nil {
		if err != nil {
			return nil, err
		}
		if err := e.dynamicUseError(use); err != nil {
			return nil, err
		}
		return e.executeOnDatabase(ctx, use.Name, remaining, params, true)
	}
	if err := e.admitTransactionStatement(cypher); err != nil {
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
	if err := e.systemDatabaseWriteError(cypher); err != nil {
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
		defer func() { result, retErr = labelRewrite.restore(withoutGeneratedColumns(result), retErr) }()
	}

	// Basic syntax validation to preserve existing error behavior.
	if err := e.validateSyntax(ctx, cypher, cypherVersionFromContext(ctx) == "25"); err != nil {
		return nil, err
	}
	if err := e.validateTransactionalCalls(ctx, cypher, nil); err != nil {
		return nil, err
	}
	if err := e.validateDuplicateReturnColumnName(cypher, quotedVariableNamesFor(ctx, cypher)); err != nil {
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
	if err := e.admitResolvedStatementWrite(ctx,nil,cypher); err != nil { return nil,err }

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
	if err := e.validateStaticOperatorParameters(cypher, params, cypherVersionFromContext(ctx) == "25"); err != nil {
		return err
	}
	if err := validateStaticPropertyAccessParameters(cypher, params); err != nil {
		return err
	}
	if err := validateStaticFunctionParameters(cypher, params, cypherVersionFromContext(ctx) == "25"); err != nil {
		return err
	}
	// A transactional CALL commits independently before later runtime
	// pagination opens. Static pagination admission still runs at compilation;
	// its runtime values are checked by the owning pipeline operator.
	if !hasCallInTransactions(cypher) {
		if err := e.validateRuntimePaginationExpressions(ctx, cypher); err != nil {
			return err
		}
	}
	return validateListOperands(cypher, params)
}
