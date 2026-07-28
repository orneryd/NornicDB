package cypher

import (
	"context"
	"fmt"
	"strings"

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
func (e *StorageExecutor) executeInternal(ctx context.Context, cypher string, params map[string]interface{}) (*ExecuteResult, error) {
	cypher = normalizeCypherSyntaxConfusables(cypher)
	cypher = strings.TrimSpace(cypher)
	cypher = trimTrailingStatementDelimiters(cypher)
	if cypher == "" {
		return nil, fmt.Errorf("empty query")
	}

	if useDB, remaining, hasUse, err := parseLeadingUseClause(cypher); hasUse || err != nil {
		if err != nil {
			return nil, err
		}
		scopedExec, resolvedDB, err := e.scopedExecutorForUse(useDB, GetAuthTokenFromContext(ctx))
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, ctxKeyUseDatabase, resolvedDB)
		if strings.TrimSpace(remaining) == "" {
			return &ExecuteResult{Columns: []string{"database"}, Rows: [][]interface{}{{resolvedDB}}}, nil
		}
		return scopedExec.executeInternal(ctx, remaining, params)
	}

	// Basic syntax validation to preserve existing error behavior.
	if err := e.validateSyntax(cypher); err != nil {
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
	ctx = context.WithValue(ctx, paramsKey, params)
	upper := e.cachedUpperQuery(cypher)

	// If we're in an explicit transaction, execute within it.
	if e.txContext != nil && e.txContext.active {
		return e.executeInTransaction(ctx, cypher, upper)
	}

	// Otherwise, stay on the caller's execution path (no implicit tx starts here).
	return e.executeWithoutTransaction(ctx, cypher, upper)
}
