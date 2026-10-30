// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"maps"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// prepareTopLevelUnwind parses and evaluates the leading UNWIND exactly once
// for both optimized physical operators and the general row pipeline.
func (e *StorageExecutor) prepareTopLevelUnwind(ctx context.Context, cypher string) (topLevelUnwindPlan, bool, error) {
	var absent topLevelUnwindPlan
	if err := ctx.Err(); err != nil {
		return absent, false, err
	}
	head := queryGapEnd(cypher, 0)
	if !startsWithKeywordFold(cypher[head:], "UNWIND") {
		return absent, false, localizedError(localization.CypherResidualUnwindClauseNotFound(truncateQuery(cypher, 80)), nil)
	}
	query := readQueryBodyText(ctx, cypher, 0, len(cypher))
	if query.status != queryBodyRead || len(query.branches) != 1 || len(query.branches[0].clauses) == 0 || query.branches[0].clauses[0].kind != pipelineClauseUnwind {
		return absent, false, localizedError(localization.CypherMutationsUnwindASRequired(), nil)
	}
	unwind := query.branches[0].clauses[0].syntax
	if unwind.ChildRule(cypherantlr.CypherParserRULE_where).Valid() {
		// Batch operators lower a bare iteration, not an attached predicate.
		// Keep that admitted occurrence in the general clause pipeline.
		return absent, false, nil
	}
	plan := unwindPlanFromSyntax(query)
	listExpr := plan.expression

	params := getParamsFromContext(ctx)
	var list interface{}
	if strings.HasPrefix(strings.TrimSpace(listExpr), "$") {
		plan.parameterName = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(listExpr), "$"))
		if plan.parameterName == "" {
			return absent, false, localizedError(localization.CypherMutationsUnwindParameterNameRequired(), nil)
		}
		if params == nil {
			return absent, false, localizedError(localization.CypherMutationsUnwindParametersRequired(plan.parameterName), nil)
		}
		var exists bool
		list, exists = params[plan.parameterName]
		if !exists {
			return absent, false, localizedError(localization.CypherMutationsUnwindParameterNotFound(plan.parameterName), nil)
		}
	} else {
		list = e.evaluateExpressionWithContext(ctx, listExpr, map[string]*storage.Node{}, map[string]*storage.Edge{})
	}

	// The one UNWIND coercion (#693): null is no rows, a list its elements,
	// any other value one row.
	plan.items = coerceToUnwindItems(list)
	if plan.items == nil {
		plan.items = []interface{}{}
	}
	return plan, true, nil
}

// executeUnwindBatchOperator chooses an optimized physical operator without
// changing clause semantics. Unsupported shapes return handled=false and are
// executed by the same pipeline's general row operators.
func (e *StorageExecutor) executeUnwindBatchOperator(ctx context.Context, plan topLevelUnwindPlan) (*ExecuteResult, bool, error) {
	remainder := plan.body.executionText
	if remainder == "" {
		return nil, false, nil
	}
	upperRest := upperASCII(strings.TrimSpace(remainder))
	matchMutation := (strings.HasPrefix(upperRest, "MATCH") || strings.HasPrefix(upperRest, "OPTIONAL MATCH")) &&
		(findKeywordIndexInContext(remainder, "MERGE") >= 0 || findKeywordIndexInContext(remainder, "CREATE") >= 0 || findKeywordIndexInContext(remainder, "SET") >= 0)
	if matchMutation {
		if strings.HasPrefix(upperRest, "MATCH") {
			if result, handled, err := e.executeUnwindRelationshipMergeBatch(ctx, plan.variable, plan.items, plan.body); handled || err != nil {
				return result, true, err
			}
			if result, handled, err := e.executeUnwindMultiMatchCreateBatch(ctx, plan.variable, plan.items, plan.body); handled || err != nil {
				return result, true, err
			}
			if result, handled, err := e.executeUnwindFixedChainLinkBatch(ctx, plan.variable, plan.items, plan.body); handled || err != nil {
				return result, true, err
			}
		}
		if result, handled, err := e.executeUnwindMergeChainBatch(ctx, plan.variable, plan.items, plan.body); handled || err != nil {
			return result, true, err
		}
		if result, handled, err := e.executeUnwindCompoundMutationBatch(ctx, plan.variable, plan.parameterName, plan.items, plan.body); handled || err != nil {
			return result, true, err
		}
		if result, handled, err := e.executeSetBasedUnwindCreateOperator(ctx, plan); handled || err != nil {
			return result, handled, err
		}
	}
	if strings.HasPrefix(upperRest, "MERGE") || strings.HasPrefix(upperRest, "OPTIONAL MATCH") {
		if result, handled, err := e.executeUnwindMergeChainBatch(ctx, plan.variable, plan.items, plan.body); handled || err != nil {
			return result, true, err
		}
		if result, handled, err := e.executeUnwindCompoundMutationBatch(ctx, plan.variable, plan.parameterName, plan.items, plan.body); handled || err != nil {
			return result, true, err
		}
	}
	if strings.HasPrefix(upperRest, "MATCH") {
		normalized := normalizeMultiMatchWhereClauses(remainder)
		if canApplySetBasedUnwindRewrite(normalized, plan.items) {
			if rewritten, ok := rewriteUnwindCorrelationToIn(normalized, plan.variable, "__unwind_items"); ok {
				rewritten = rewriteTopLevelMultiMatchToCartesianMatch(rewritten)
				params := make(map[string]interface{}, len(getParamsFromContext(ctx))+1)
				for key, value := range getParamsFromContext(ctx) {
					params[key] = value
				}
				params["__unwind_items"] = plan.items
				result, err := e.executeRequiredPipeline(withQueryParams(ctx, params), rewritten)
				return result, true, err
			}
		}
	}
	// Pure UNWIND + CREATE: every row creates independent new entities —
	// the stretch reads nothing but the unwind binding — so all rows are
	// planned into one createPlan and written with one BulkCreateNodes /
	// one BulkCreateEdges instead of one statement per row (#1043
	// write-path measurement 6: +27–39% at 16 writers). Any clause that
	// could read what another row writes (MERGE, SET, MATCH, aggregation
	// via WITH) keeps the row-by-row route.
	if strings.HasPrefix(upperRest, "CREATE") &&
		findKeywordIndexInContext(remainder, "MERGE") < 0 &&
		findKeywordIndexInContext(remainder, "SET") < 0 &&
		findKeywordIndexInContext(remainder, "DELETE") < 0 &&
		findKeywordIndexInContext(remainder, "REMOVE") < 0 &&
		findKeywordIndexInContext(remainder, "FOREACH") < 0 &&
		findKeywordIndexInContext(remainder, "MATCH") < 0 &&
		findKeywordIndexInContext(remainder, "WITH") < 0 &&
		findKeywordIndexInContext(remainder, "CALL") < 0 &&
		findKeywordIndexInContext(remainder, "LOAD CSV") < 0 {
		ctx = context.WithValue(ctx, pipelineIndependentCreateBatchKey{}, true)
		return e.executeUnwindRowsPipeline(ctx, plan.variable, plan.items, plan.body)
	}
	return nil, false, nil
}

func (e *StorageExecutor) executeUnwindRowsPipeline(ctx context.Context, variable string, items []interface{}, query queryBody) (*ExecuteResult, bool, error) {
	split := parsePipelineClauses(ctx, query)
	clauses, ok := split.clauses, split.ok
	if !ok {
		return nil, false, nil
	}
	originalClauses := clauses
	ctx = withExpressionFailureSlot(ctx)
	scope := map[string]struct{}{variable: {}}
	base := pipelineRow{}
	for name, value := range e.fabricRecordBindings {
		base[name] = value
		scope[name] = struct{}{}
	}
	for name, value := range valueBindingsFromContext(ctx) {
		base[name] = value
		scope[name] = struct{}{}
	}
	bindParameterRow(ctx, base)
	rows := make([]pipelineRow, 0, len(items))
	for _, item := range items {
		row := maps.Clone(base)
		row[variable] = item
		rows = append(rows, row)
	}
	result, handled, err := e.runPipelineClauseRows(ctx, rows, scope, clauses, originalClauses, nil)
	if failure := getExpressionFailure(ctx); failure != nil && err == nil {
		return nil, true, failure
	}
	return result, handled, err
}

func (e *StorageExecutor) executeSetBasedUnwindCreateOperator(ctx context.Context, plan topLevelUnwindPlan) (*ExecuteResult, bool, error) {
	query := normalizeMultiMatchWhereClauses(plan.body.executionText)
	if findKeywordIndexInContext(query, "CREATE") < 0 ||
		findKeywordIndexInContext(query, "MERGE") >= 0 ||
		findKeywordIndexInContext(query, "SET") >= 0 ||
		findKeywordIndexInContext(query, "DELETE") >= 0 ||
		findKeywordIndexInContext(query, "REMOVE") >= 0 ||
		!unwindItemsAreDistinctComparable(plan.items) {
		return nil, false, nil
	}
	returnIndex := findKeywordIndexInContext(query, "RETURN")
	if returnIndex < 0 {
		return nil, false, nil
	}
	if _, ok := parseUnwindBatchCountReturn(projectionPlanAt(ctx, query, returnIndex)); !ok {
		return nil, false, nil
	}
	rewritten, ok := rewriteUnwindCorrelationToIn(query, plan.variable, "__unwind_items")
	if !ok {
		return nil, false, nil
	}
	rewritten = rewriteTopLevelMultiMatchToCartesianMatch(rewritten)
	params := make(map[string]interface{}, len(getParamsFromContext(ctx))+1)
	for key, value := range getParamsFromContext(ctx) {
		params[key] = value
	}
	params["__unwind_items"] = plan.items
	result, err := e.executeRequiredPipeline(withQueryParams(ctx, params), rewritten)
	return result, true, err
}
