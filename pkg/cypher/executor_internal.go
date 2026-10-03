package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
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
func (e *StorageExecutor) executeInternal(ctx context.Context, cypher string, params map[string]interface{}) (result *ExecuteResult, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cypher = normalizeCypherSyntaxConfusables(cypher)
	if config.IsCypherQueryNormalizationEnabled() {
		cypher, _ = canonicalizeQueryText(cypher)
	}
	cypher = strings.TrimSpace(cypher)
	cypher = trimTrailingStatementDelimiters(cypher)
	if err := e.validateStatementFraming(cypher); err != nil {
		return nil, err
	}
	finishTerminated := false
	if stripped, ok := stripUnionBranchFinishes(cypher); ok {
		cypher = strings.TrimSpace(stripped)
		finishTerminated = true
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
		if err := e.authorizeSelectedDatabase(ctx, use.Name); err != nil {
			return nil, err
		}
		scopedExec, resolvedDB, err := e.scopedExecutorForUse(use.Name, GetAuthTokenFromContext(ctx))
		if err != nil {
			return nil, err
		}
		ctx = withExecutionDatabase(ctx, resolvedDB)
		return scopedExec.executeInternal(ctx, remaining, params)
	}
	if err := AuthorizeQuery(ctx, cypher); err != nil {
		return nil, err
	}

	// Basic syntax validation to preserve existing error behavior.
	if err := e.validateSyntax(cypher); err != nil {
		return nil, err
	}
	if err := e.validateDuplicateReturnColumnName(cypher, quotedVariableNamesFor(ctx, cypher)); err != nil {
		return nil, err
	}
	if err := e.statementParametersError(ctx, cypher, params); err != nil {
		return nil, err
	}

	if inheritedParams := getParamsFromContext(ctx); inheritedParams != nil {
		if params == nil {
			params = inheritedParams
		} else {
			mergedParams := make(map[string]interface{}, util.SafePreallocSum(len(inheritedParams), len(params)))
			for key, value := range inheritedParams {
				mergedParams[key] = value
			}
			for key, value := range params {
				mergedParams[key] = value
			}
			params = mergedParams
		}
	}
	params = normalizeQueryParameters(params)
	ctx = withQueryParams(ctx, params)
	if err := e.validateBoundParameterExpressions(ctx, cypher, params); err != nil {
		return nil, err
	}
	upper := e.cachedUpperQuery(cypher)

	// If we're in an explicit transaction, execute within it.
	if e.txContext != nil && e.txContext.active {
		return e.executeInTransaction(ctx, cypher, upper)
	}

	// Otherwise, stay on the caller's execution path (no implicit tx starts here).
	return e.executeWithoutTransaction(ctx, cypher, upper)
}

func (e *StorageExecutor) validateBoundParameterExpressions(ctx context.Context, cypher string, params map[string]interface{}) error {
	if err := e.validateStaticOperatorParameters(cypher, params); err != nil {
		return err
	}
	if err := validateStaticPropertyAccessParameters(cypher, params); err != nil {
		return err
	}
	if err := e.validateRuntimePaginationExpressions(ctx, cypher); err != nil {
		return err
	}
	return validateListOperands(cypher, params)
}
