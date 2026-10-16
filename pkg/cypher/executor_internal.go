package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config"
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
func (e *StorageExecutor) executeUserStatement(ctx context.Context, statement string, params map[string]interface{}) (*ExecuteResult, error) {
	if err := internalFunctionCallError(statement); err != nil {
		// Refused like any statement of the caller's transaction: it fails
		// an explicit transaction (executeInternal fails it for the rest).
		return nil, e.failTransaction(err)
	}
	if StatementDatabase("", statement) == "system" {
		var transaction any
		if wrapper, ok := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); ok {
			transaction = wrapper.tx
		} else if e.txContext != nil && e.txContext.active {
			transaction = e.txContext.tx
		}
		return nil, e.failTransaction(statementTypeError(statement, "Administration", "Data", transaction))
	}
	return e.executeInternal(withStatementText(ctx, statement), statement, params)
}

func (e *StorageExecutor) executeInternal(ctx context.Context, cypher string, params map[string]interface{}) (result *ExecuteResult, retErr error) {
	if e.txContext != nil && e.txContext.active {
		defer func() { e.failTransaction(retErr) }()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx = withPipelineCancellation(withExpressionFailureSlot(ctx))
	preparationProbe := parameterCancellationForContext(ctx)
	cypher = normalizeCypherSyntaxConfusablesWithCancellation(cypher, preparationProbe)
	if preparationProbe != nil && preparationProbe.err != nil {
		return nil, preparationProbe.err
	}
	if config.IsCypherQueryNormalizationEnabled() {
		cypher, _ = canonicalizeQueryTextWithCancellation(cypher, preparationProbe)
		if preparationProbe != nil && preparationProbe.err != nil {
			return nil, preparationProbe.err
		}
	}
	cypher, _ = canonicalizeNumericLiteralsWithCancellation(cypher, preparationProbe)
	if preparationProbe != nil && preparationProbe.err != nil {
		return nil, preparationProbe.err
	}
	cypher, _ = canonicalizeFunctionAliasesWithCancellation(cypher, preparationProbe)
	if preparationProbe != nil && preparationProbe.err != nil {
		return nil, preparationProbe.err
	}
	if cypherVersionFromContext(ctx) == "25" {
		cypher, _ = separateCypher25ParametersWithCancellation(cypher, preparationProbe)
		if preparationProbe != nil && preparationProbe.err != nil {
			return nil, preparationProbe.err
		}
	}
	cypher, _, err := desugarLabelExpressions(cypher, params, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return nil, err
	}
	cypher, _ = desugarStandaloneOrderClauses(cypher)
	cypher = strings.TrimSpace(cypher)
	cypher = trimTrailingStatementDelimitersWithCancellation(cypher, preparationProbe)
	if err := e.validateStatementFramingWithCancellation(cypher, preparationProbe); err != nil {
		return nil, err
	}
	finishTerminated := false
	if stripped, ok := stripUnionBranchFinishesWithCancellation(cypher, preparationProbe); ok {
		cypher = trimSpaceWithCancellation(stripped, preparationProbe)
		finishTerminated = true
		ctx = withFinishedStatement(ctx, cypher)
	}
	if preparationProbe != nil && preparationProbe.err != nil {
		return nil, preparationProbe.err
	}
	if cypher == "" {
		if finishTerminated {
			return &ExecuteResult{}, nil
		}
		return nil, localizedError(localization.CypherCoreEmptyQuery(), nil)
	}
	if finishTerminated {
		defer func() {
			if result != nil {
				result.Columns = nil
				result.Rows = nil
			}
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
	if err := AuthorizeQuery(ctx, cypher); err != nil {
		return nil, err
	}
	if err := e.systemDatabaseWriteError(cypher); err != nil {
		return nil, err
	}

	// Basic syntax validation to preserve existing error behavior.
	if err := e.validateSyntaxWithCancellation(cypher, cypherVersionFromContext(ctx) == "25", parameterCancellationForContext(ctx)); err != nil {
		return nil, err
	}
	if err := e.validateDuplicateReturnColumnName(cypher, quotedVariableNamesFor(ctx, cypher)); err != nil {
		return nil, err
	}
	if err := e.statementParametersError(ctx, cypher, params); err != nil {
		return nil, err
	}

	params = mergeInheritedQueryParams(ctx, params)
	if preparationProbe != nil && preparationProbe.err != nil {
		return nil, preparationProbe.err
	}
	params = normalizeQueryParametersWithCancellation(params, parameterCancellationForContext(ctx))
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, failure
	}
	ctx = withQueryParams(ctx, params)
	if err := e.validateBoundParameterExpressions(ctx, cypher, params); err != nil {
		return nil, err
	}
	upper := e.cachedUpperQueryWithCancellation(cypher, preparationProbe)
	if preparationProbe != nil && preparationProbe.err != nil {
		return nil, preparationProbe.err
	}

	// If we're in an explicit transaction, execute within it.
	if e.txContext != nil && e.txContext.active {
		return e.executeInTransaction(ctx, cypher, upper)
	}

	// Otherwise, stay on the caller's execution path (no implicit tx starts here).
	return e.executeWithoutTransaction(ctx, cypher, upper)
}

func (e *StorageExecutor) validateBoundParameterExpressions(ctx context.Context, cypher string, params map[string]interface{}) error {
	if err := e.validateStaticOperatorParameters(cypher, params, cypherVersionFromContext(ctx) == "25"); err != nil {
		return err
	}
	if err := validateStaticPropertyAccessParameters(cypher, params); err != nil {
		return err
	}
	if err := validateStaticFunctionParameters(cypher, params, cypherVersionFromContext(ctx) == "25"); err != nil {
		return err
	}
	if err := e.validateRuntimePaginationExpressions(ctx, cypher); err != nil {
		return err
	}
	return validateListOperands(cypher, params)
}
