package cypher

import (
	"context"
	"maps"
	"strings"
	"sync/atomic"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type topLevelUnwindPlan struct {
	variable      string
	parameterName string
	items         []interface{}
	remainder     string
	// aliasStart and aliasEnd span the UNWIND alias in the statement source
	// and bodyOffset is where the trimmed remainder starts there. The
	// set-based rewrite proves the binding dead against the admitted statement
	// program, whose binding identities and reads address the same source.
	aliasStart int
	aliasEnd   int
	bodyOffset int
}

// prepareTopLevelUnwind parses and evaluates the leading UNWIND exactly once
// for both optimized physical operators and the general row pipeline.
func (e *StorageExecutor) prepareTopLevelUnwind(ctx context.Context, cypher string) (topLevelUnwindPlan, error) {
	head := queryGapEnd(cypher, 0)
	if !startsWithKeywordFold(cypher[head:], "UNWIND") {
		return topLevelUnwindPlan{}, localizedError(localization.CypherResidualUnwindClauseNotFound(truncateQuery(cypher, 80)), nil)
	}
	product, ok := splitUnwindBody(ctx, cypher)
	if !ok {
		return topLevelUnwindPlan{}, localizedError(localization.CypherMutationsUnwindASRequired(), nil)
	}
	listExpr := product.expression
	plan := topLevelUnwindPlan{variable: product.variableWritten, remainder: product.remainder, bodyOffset: -1}
	if aliasStart, aliasEnd, bodyOffset, located := unwindBodySpans(cypher, plan.remainder, plan.variable); located {
		plan.aliasStart, plan.aliasEnd, plan.bodyOffset = aliasStart, aliasEnd, bodyOffset
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
		// The same checked row owner evaluates both prepared and ordinary
		// iteration values; unresolved expressions must not become empty lists.
		row := entityRow(ctx, nil, nil)
		if profileRuntimeFromContext(ctx) != nil {
			if row == nil {
				row = make(pipelineRow)
			}
			bindRowExpressionOrigin(row, product.expressionStart)
		}
		var err error
		list, err = e.checkedPropertyExpression(ctx, listExpr, row)
		if err != nil {
			return topLevelUnwindPlan{}, err
		}
	}

	// The one UNWIND coercion (#693): null is no rows, a list its elements,
	// any other value one row.
	plan.items = coerceToUnwindItems(list)
	if plan.items == nil {
		plan.items = []interface{}{}
	}
	return plan, nil
}

// unwindBodySpans locates the UNWIND alias token and the trimmed body after it
// in the statement source. The body split keeps only the alias text, so both
// spans are re-derived from the source: the body is its trimmed suffix and the
// alias is the last token before it.
func unwindBodySpans(source, remainder, variable string) (aliasStart, aliasEnd, bodyOffset int, ok bool) {
	if remainder == "" || variable == "" {
		return 0, 0, 0, false
	}
	end := len(strings.TrimRightFunc(source, unicode.IsSpace))
	if end < len(remainder) || source[end-len(remainder):end] != remainder {
		return 0, 0, 0, false
	}
	bodyOffset = end - len(remainder)
	aliasEnd = len(strings.TrimRightFunc(source[:bodyOffset], unicode.IsSpace))
	aliasStart = aliasEnd - len(variable)
	if aliasStart < 0 || source[aliasStart:aliasEnd] != variable {
		return 0, 0, 0, false
	}
	return aliasStart, aliasEnd, bodyOffset, true
}

// rewriteUnwindCorrelations is the one owner of the set-based UNWIND rewrite
// decision. It replaces the conjunctive WHERE equalities whose right side is
// exactly a read of the UNWIND binding, and admits the result only when the
// admitted statement program proves those replacements are the binding's own
// reads and cover every read of it, including the reads a wildcard projection
// expands to. Without that proof it declines, and the caller runs the canonical
// row plan instead.
func (e *StorageExecutor) rewriteUnwindCorrelations(ctx context.Context, plan topLevelUnwindPlan) (string, bool) {
	program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	declaration, ok := unwindDeclaration(program, plan.aliasStart, plan.aliasEnd)
	if !ok {
		return "", false
	}
	product := readUnwindCorrelations(program.syntax, declaration, plan.remainder, plan.bodyOffset)
	if !product.eliminates(unwindBindingProof{program: program, declaration: declaration}) {
		return "", false
	}
	rewritten, ok := product.rewrite("__unwind_items")
	if !ok {
		return "", false
	}
	rewritten = normalizeMultiMatchWhereClauses(ctx, rewritten)
	return rewriteTopLevelMultiMatchToCartesianMatch(ctx, rewritten), true
}

// executeUnwindCorrelationRewrite runs one proven set-based rewrite with the
// UNWIND items bound as its IN-list parameter.
func (e *StorageExecutor) executeUnwindCorrelationRewrite(ctx context.Context, plan topLevelUnwindPlan, rewritten string) (*ExecuteResult, error) {
	// PROFILE: the rewrite runs the body as one set-based statement; the items
	// the leading UNWIND produced are still its own rows, and the rewritten
	// statement's clauses are reported under it.
	ctx = profileUnwindRouteOperator(ctx, len(plan.items))
	params := make(map[string]interface{}, len(getParamsFromContext(ctx))+1)
	for key, value := range getParamsFromContext(ctx) {
		params[key] = value
	}
	params["__unwind_items"] = plan.items
	return e.executeRequiredPipeline(withQueryParams(ctx, params), rewritten)
}

// unwindProfileRouteKey is the context slot holding this statement's leading
// UNWIND report from the routing owner to the body pipelines that run it. A
// statement nobody profiles never sets it.
type unwindProfileRouteKey struct{}

// unwindProfileRoute is one statement's leading UNWIND report: the clause text
// the routes report, and whether the collector already holds its record.
// reported is written by profileUnwindRouteOperator alone.
type unwindProfileRoute struct {
	source   string
	reported atomic.Bool
}

// withUnwindProfileRoute returns ctx carrying this statement's leading UNWIND
// report, or ctx itself when the statement has no collector.
func withUnwindProfileRoute(ctx context.Context, source string) context.Context {
	if profileRuntimeFromContext(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, unwindProfileRouteKey{}, &unwindProfileRoute{source: source})
}

// profileUnwindRouteOperator reports the statement's leading UNWIND operator to
// the PROFILE collector and returns the context its body runs under, so the
// body's clauses are reported as its children. The rows are the items the UNWIND
// actually produced, reported once per statement: a body a route attempts and
// then declines, and a compound route's several stages, all run the same
// leading operator instead of adding its rows again.
func profileUnwindRouteOperator(ctx context.Context, rows int) context.Context {
	route, _ := ctx.Value(unwindProfileRouteKey{}).(*unwindProfileRoute)
	if route == nil || route.reported.Swap(true) {
		return ctx
	}
	ctx, record := profileRuntimeInvocation(ctx, profileClauseOperator(pipelineClauseUnwind), route.source, 0)
	record.invocationRows(rows)
	return ctx
}

// executeUnwindBatchOperator chooses an optimized physical operator without
// changing clause semantics. Unsupported shapes return handled=false and are
// executed by the same pipeline's general row operators.
func (e *StorageExecutor) executeUnwindBatchOperator(ctx context.Context, plan topLevelUnwindPlan, source string) (*ExecuteResult, bool, error) {
	if plan.remainder == "" {
		return nil, false, nil
	}
	// PROFILE: the routes below run the leading UNWIND's rows themselves, so
	// they report the operator the statement wrote. Without a collector the
	// context is unchanged.
	ctx = withUnwindProfileRoute(ctx, source)
	upperRest := upperASCII(strings.TrimSpace(plan.remainder))
	matchMutation := (strings.HasPrefix(upperRest, "MATCH") || strings.HasPrefix(upperRest, "OPTIONAL MATCH")) &&
		(findKeywordIndexInContext(ctx, plan.remainder, "MERGE") >= 0 || findKeywordIndexInContext(ctx, plan.remainder, "CREATE") >= 0 || findKeywordIndexInContext(ctx, plan.remainder, "SET") >= 0)
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
		returnIndex := findKeywordIndex(ctx, plan.remainder, "RETURN")
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
		returnIndex := findKeywordIndex(ctx, plan.remainder, "RETURN")
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
		eligible, eligibilityErr := e.canApplySetBasedUnwindRewrite(ctx, plan.remainder, plan.items)
		if eligibilityErr != nil {
			return nil, true, eligibilityErr
		}
		if eligible {
			if rewritten, ok := e.rewriteUnwindCorrelations(ctx, plan); ok {
				result, err := e.executeUnwindCorrelationRewrite(ctx, plan, rewritten)

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
		!hasNonCreateMutationClause(ctx, plan.remainder) &&
		findKeywordIndexInContext(ctx, plan.remainder, "FOREACH") < 0 &&
		findKeywordIndexInContext(ctx, plan.remainder, "MATCH") < 0 &&
		findKeywordIndexInContext(ctx, plan.remainder, "WITH") < 0 &&
		findKeywordIndexInContext(ctx, plan.remainder, "CALL") < 0 &&
		findKeywordIndexInContext(ctx, plan.remainder, "LOAD CSV") < 0 {
		ctx = context.WithValue(ctx, pipelineIndependentCreateBatchKey{}, true)
		return e.executeUnwindRowsPipeline(ctx, plan.variable, plan.items, plan.remainder)
	}
	return nil, false, nil
}

func (e *StorageExecutor) executeUnwindRowsPipeline(ctx context.Context, variable string, items []interface{}, query string) (*ExecuteResult, bool, error) {
	split := parsePipelineClauses(ctx, query)
	clauses, ok := split.clauses, split.ok
	if !ok {
		return nil, false, nil
	}
	originalClauses := clauses
	// PROFILE: the statement's leading UNWIND is not one of the remainder's
	// clauses, so this route reports the rows the operator produced and the
	// body it runs below is reported under it.
	ctx = profileUnwindRouteOperator(ctx, len(items))
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
			if e.compilePipelineNodeMatchSource(ctx, combined).supported {
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
	result, handled, err := e.runPipelineClauseRows(ctx, rows, scope, clauses, originalClauses, nil, nil)
	if failure := getExpressionFailure(ctx); failure != nil && err == nil {
		return nil, true, failure
	}
	return result, handled, err
}

func (e *StorageExecutor) executeSetBasedUnwindCreateOperator(ctx context.Context, plan topLevelUnwindPlan) (*ExecuteResult, bool, error) {
	query := plan.remainder
	if findKeywordIndexInContext(ctx, query, "CREATE") < 0 ||
		hasNonCreateMutationClause(ctx, query) ||
		!unwindItemsAreDistinctComparable(plan.items) {
		return nil, false, nil
	}
	returnIndex := findKeywordIndexInContext(ctx, query, "RETURN")
	if returnIndex < 0 {
		return nil, false, nil
	}
	if _, ok := parseUnwindBatchCountReturn(ctx, strings.TrimSpace(query[returnIndex:])); !ok {
		return nil, false, nil
	}
	rewritten, ok := e.rewriteUnwindCorrelations(ctx, plan)
	if !ok {
		return nil, false, nil
	}
	result, err := e.executeUnwindCorrelationRewrite(ctx, plan, rewritten)
	return result, true, err
}

// hasNonCreateMutationClause is the shared CREATE-only batching restriction.
// It checks top-level clauses, preserving per-row semantics for other mutations.
// Read-only rewrite admission uses the complete statement-effect owner instead.
func hasNonCreateMutationClause(ctx context.Context, query string) bool {
	return findKeywordIndexInContext(ctx, query, "MERGE") >= 0 ||
		findKeywordIndexInContext(ctx, query, "SET") >= 0 ||
		findKeywordIndexInContext(ctx, query, "DELETE") >= 0 ||
		findKeywordIndexInContext(ctx, query, "REMOVE") >= 0
}
