package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"maps"
	"strings"
)

type topLevelUnwindPlan struct {
	variable      string
	parameterName string
	items         []interface{}
	remainder     string
}

// prepareTopLevelUnwind parses and evaluates the leading UNWIND exactly once
// for both optimized physical operators and the general row pipeline.
func (e *StorageExecutor) prepareTopLevelUnwind(ctx context.Context, cypher string) (topLevelUnwindPlan, error) {
	clauses, err := statementPhysicalClauses(cypher)
	if err != nil {
		return topLevelUnwindPlan{}, err
	}
	var clause *pipelineClause
	for index := range clauses {
		if clauses[index].kind == pipelineClauseUnwind {
			clause = &clauses[index]
			break
		}
	}
	if clause == nil {
		return topLevelUnwindPlan{}, localizedError(localization.CypherResidualUnwindClauseNotFound(truncateQuery(cypher, 80)), nil)
	}
	listExpr := clause.iterationExpression
	_, end := statementClauseRange(cypher, clause, 0)
	if end < 0 {
		return topLevelUnwindPlan{}, localizedError(localization.CypherResidualUnwindClauseNotFound(truncateQuery(cypher, 80)), nil)
	}
	plan := topLevelUnwindPlan{variable: clause.iterationAlias, remainder: strings.TrimSpace(cypher[end:])}

	params := getParamsFromContext(ctx)
	var list interface{}
	if strings.HasPrefix(strings.TrimSpace(listExpr), "$") {
		plan.parameterName = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(listExpr), "$"))
		if plan.parameterName == "" {
			return topLevelUnwindPlan{}, localizedError(localization.CypherMutationsUnwindParameterNameRequired(), nil)
		}
		if params == nil {
			return topLevelUnwindPlan{}, localizedError(localization.CypherMutationsUnwindParametersRequired(plan.parameterName), nil)
		}
		var exists bool
		list, exists = params[plan.parameterName]
		if !exists {
			return topLevelUnwindPlan{}, localizedError(localization.CypherMutationsUnwindParameterNotFound(plan.parameterName), nil)
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
	return plan, nil
}

// executeUnwindBatchOperator chooses an optimized physical operator without
// changing clause semantics. Unsupported shapes return handled=false and are
// executed by the same pipeline's general row operators.
func (e *StorageExecutor) executeUnwindBatchOperator(ctx context.Context, plan topLevelUnwindPlan) (*ExecuteResult, bool, error) {
	if plan.remainder == "" {
		return nil, false, nil
	}
	upperRest := upperASCII(strings.TrimSpace(plan.remainder))
	matchMutation := (strings.HasPrefix(upperRest, "MATCH") || strings.HasPrefix(upperRest, "OPTIONAL MATCH")) &&
		(findKeywordIndexInContext(plan.remainder, "MERGE") >= 0 || findKeywordIndexInContext(plan.remainder, "CREATE") >= 0 || findKeywordIndexInContext(plan.remainder, "SET") >= 0)
	if matchMutation {
		if strings.HasPrefix(upperRest, "MATCH") {
			if result, handled, err := e.executeUnwindRelationshipMergeBatch(ctx, plan.variable, plan.items, plan.remainder); handled || err != nil {
				return result, true, err
			}
			if result, handled, err := e.executeUnwindMultiMatchCreateBatch(ctx, plan.variable, plan.items, plan.remainder); handled || err != nil {
				return result, true, err
			}
			if result, handled, err := e.executeUnwindFixedChainLinkBatch(ctx, plan.variable, plan.items, plan.remainder); handled || err != nil {
				return result, true, err
			}
		}
		returnIndex := findKeywordIndex(plan.remainder, "RETURN")
		mutationPart := plan.remainder
		returnPart := ""
		if returnIndex > 0 {
			mutationPart = strings.TrimSpace(plan.remainder[:returnIndex])
			returnPart = strings.TrimSpace(plan.remainder[returnIndex:])
		}
		if result, handled, err := e.executeUnwindMergeChainBatch(ctx, plan.variable, plan.items, mutationPart, returnPart); handled || err != nil {
			return result, true, err
		}
		if result, handled, err := e.executeUnwindCompoundMutationBatch(ctx, plan.variable, plan.parameterName, plan.items, plan.remainder); handled || err != nil {
			return result, true, err
		}
		if result, handled, err := e.executeSetBasedUnwindCreateOperator(ctx, plan); handled || err != nil {
			return result, handled, err
		}
	}
	if strings.HasPrefix(upperRest, "MERGE") || strings.HasPrefix(upperRest, "OPTIONAL MATCH") {
		returnIndex := findKeywordIndex(plan.remainder, "RETURN")
		mutationPart := plan.remainder
		returnPart := ""
		if returnIndex > 0 {
			mutationPart = strings.TrimSpace(plan.remainder[:returnIndex])
			returnPart = strings.TrimSpace(plan.remainder[returnIndex:])
		}
		if result, handled, err := e.executeUnwindMergeChainBatch(ctx, plan.variable, plan.items, mutationPart, returnPart); handled || err != nil {
			return result, true, err
		}
		if result, handled, err := e.executeUnwindCompoundMutationBatch(ctx, plan.variable, plan.parameterName, plan.items, plan.remainder); handled || err != nil {
			return result, true, err
		}
	}
	if strings.HasPrefix(upperRest, "MATCH") {
		normalized := normalizeMultiMatchWhereClauses(plan.remainder)
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
		findKeywordIndexInContext(plan.remainder, "MERGE") < 0 &&
		findKeywordIndexInContext(plan.remainder, "SET") < 0 &&
		findKeywordIndexInContext(plan.remainder, "DELETE") < 0 &&
		findKeywordIndexInContext(plan.remainder, "REMOVE") < 0 &&
		findKeywordIndexInContext(plan.remainder, "FOREACH") < 0 &&
		findKeywordIndexInContext(plan.remainder, "MATCH") < 0 &&
		findKeywordIndexInContext(plan.remainder, "WITH") < 0 &&
		findKeywordIndexInContext(plan.remainder, "CALL") < 0 &&
		findKeywordIndexInContext(plan.remainder, "LOAD CSV") < 0 {
		ctx = context.WithValue(ctx, pipelineIndependentCreateBatchKey{}, true)
		return e.executeUnwindRowsPipeline(ctx, plan.variable, plan.items, plan.remainder)
	}
	return nil, false, nil
}

func (e *StorageExecutor) executeUnwindRowsPipeline(ctx context.Context, variable string, items []interface{}, query string) (*ExecuteResult, bool, error) {
	clauses, ok, _ := parsePipelineClauses(query)
	if !ok {
		return nil, false, nil
	}
	originalClauses := clauses
	if independent, _ := ctx.Value(pipelineIndependentCreateBatchKey{}).(bool); independent {
		var patterns []string
		for _, clause := range clauses {
			if clause.kind != pipelineClauseMatch {
				break
			}
			patterns = append(patterns, pipelineClauseBody(clause.text, "MATCH"))
		}
		if len(patterns) > 1 {
			combined := "MATCH " + strings.Join(patterns, ", ")
			if e.compilePipelineNodeMatchSource(combined).supported {
				clauses = append([]pipelineClause{{kind: pipelineClauseMatch, text: combined}}, clauses[len(patterns):]...)
			}
		}
	}
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
	result, handled, err := e.runPipelineClauses(ctx, rows, scope, clauses, originalClauses)
	if failure := getExpressionFailure(ctx); failure != nil && err == nil {
		return nil, true, failure
	}
	return result, handled, err
}

func (e *StorageExecutor) executeSetBasedUnwindCreateOperator(ctx context.Context, plan topLevelUnwindPlan) (*ExecuteResult, bool, error) {
	query := normalizeMultiMatchWhereClauses(plan.remainder)
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
	if _, ok := parseUnwindBatchCountReturn(strings.TrimSpace(query[returnIndex:])); !ok {
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
