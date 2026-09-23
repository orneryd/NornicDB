package cypher

import (
	"context"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
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
	upper := strings.ToUpper(cypher)
	if strings.Contains(upper, "KEYS(") && strings.Contains(upper, "UNWIND") {
		return topLevelUnwindPlan{}, localizedError(localization.CypherMutationsUnwindKeysUnsupported(), nil)
	}
	unwindIdx := findKeywordIndex(cypher, "UNWIND")
	if unwindIdx == -1 {
		return topLevelUnwindPlan{}, localizedError(localization.CypherResidualUnwindClauseNotFound(truncateQuery(cypher, 80)), nil)
	}
	afterUnwind := cypher[unwindIdx+len("UNWIND"):]
	asRelativeIdx := findKeywordNotInBrackets(afterUnwind, " AS ")
	if asRelativeIdx == -1 {
		return topLevelUnwindPlan{}, localizedError(localization.CypherMutationsUnwindASRequired(), nil)
	}
	asIdx := unwindIdx + len("UNWIND") + asRelativeIdx
	listExpr := strings.TrimSpace(cypher[unwindIdx+len("UNWIND") : asIdx])
	remainderStart := asIdx + len("AS")
	for remainderStart < len(cypher) && isASCIISpace(cypher[remainderStart]) {
		remainderStart++
	}
	remainder := strings.TrimSpace(cypher[remainderStart:])
	spaceIdx := strings.IndexAny(remainder, " \t\r\n")
	plan := topLevelUnwindPlan{}
	if spaceIdx > 0 {
		plan.variable = strings.TrimSpace(remainder[:spaceIdx])
		plan.remainder = strings.TrimSpace(remainder[spaceIdx:])
	} else {
		plan.variable = strings.TrimSpace(remainder)
	}

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
		if params != nil {
			listExpr = e.substituteParams(listExpr, params)
		}
		list = e.evaluateExpressionWithContext(ctx, listExpr, map[string]*storage.Node{}, map[string]*storage.Edge{})
	}

	switch value := list.(type) {
	case nil:
		plan.items = []interface{}{}
	case []interface{}:
		plan.items = value
	case []string:
		plan.items = make([]interface{}, len(value))
		for index := range value {
			plan.items[index] = value[index]
		}
	case []int64:
		plan.items = make([]interface{}, len(value))
		for index := range value {
			plan.items[index] = value[index]
		}
	case []float64:
		plan.items = make([]interface{}, len(value))
		for index := range value {
			plan.items[index] = value[index]
		}
	case []map[string]interface{}:
		plan.items = make([]interface{}, len(value))
		for index := range value {
			plan.items[index] = value[index]
		}
	default:
		reflected := reflect.ValueOf(list)
		if reflected.IsValid() && (reflected.Kind() == reflect.Slice || reflected.Kind() == reflect.Array) {
			plan.items = make([]interface{}, reflected.Len())
			for index := 0; index < reflected.Len(); index++ {
				plan.items[index] = reflected.Index(index).Interface()
			}
		} else {
			plan.items = []interface{}{list}
		}
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
	upperRest := strings.ToUpper(strings.TrimSpace(plan.remainder))
	matchMutation := (strings.HasPrefix(upperRest, "MATCH") || strings.HasPrefix(upperRest, "OPTIONAL MATCH")) &&
		(findKeywordIndexInContext(plan.remainder, "MERGE") >= 0 || findKeywordIndexInContext(plan.remainder, "CREATE") >= 0 || findKeywordIndexInContext(plan.remainder, "SET") >= 0)
	if matchMutation {
		if strings.HasPrefix(upperRest, "MATCH") {
			if result, handled, err := e.executeUnwindRelationshipMergeBatch(ctx, plan.variable, plan.items, plan.remainder); handled {
				return result, true, err
			}
			if result, handled, err := e.executeUnwindMultiMatchCreateBatch(ctx, plan.variable, plan.items, plan.remainder); handled {
				return result, true, err
			}
			if result, handled, err := e.executeUnwindFixedChainLinkBatch(ctx, plan.variable, plan.items, plan.remainder); handled {
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
		if result, handled, err := e.executeUnwindMergeChainBatch(ctx, plan.variable, plan.items, mutationPart, returnPart); handled {
			return result, true, err
		}
		if result, handled, err := e.executeUnwindCompoundMutationBatch(ctx, plan.variable, plan.parameterName, plan.items, plan.remainder); handled {
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
		if result, handled, err := e.executeUnwindMergeChainBatch(ctx, plan.variable, plan.items, mutationPart, returnPart); handled {
			return result, true, err
		}
		if result, handled, err := e.executeUnwindCompoundMutationBatch(ctx, plan.variable, plan.parameterName, plan.items, plan.remainder); handled {
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
				result, err := e.Execute(ctx, rewritten, params)
				if err == nil {
					return result, true, nil
				}
			}
		}
	}
	return nil, false, nil
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
	result, err := e.Execute(ctx, rewritten, params)
	if err != nil {
		return nil, false, nil
	}
	return result, true, nil
}
