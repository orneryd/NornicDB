package cypher

// Parsed projections are immutable. Query cancellation may abandon private
// preparation, but never publishes a partial plan or holds a cache lock while
// parsing an expression.
func returnProjectionPlanForWithCancellation(clause string, probe *pipelineCancellation) *returnProjectionPlan {
	if probe != nil && !probe.check() {
		return &returnProjectionPlan{}
	}
	if plan, cached := getQueryTextCache(&returnProjectionPlans.plans, &returnProjectionPlans.RWMutex, clause, probe); cached {
		return plan
	}
	if probe != nil && probe.err != nil {
		return &returnProjectionPlan{}
	}
	plan := parseReturnProjectionPlanWithCancellation(clause, probe)
	if probe != nil && probe.err != nil {
		return &returnProjectionPlan{}
	}
	if !lockQueryTextCache(&returnProjectionPlans.RWMutex, false, probe) {
		return &returnProjectionPlan{}
	}
	defer returnProjectionPlans.Unlock()
	// Preserve the existing clear-at-capacity policy. Dropping the root is
	// bounded; reclamation belongs to the runtime, not the query's copy loop.
	if returnProjectionPlans.plans.Len() >= returnProjectionPlanLimit {
		returnProjectionPlans.plans = pipelineStringMap[*returnProjectionPlan]{}
	}
	if !returnProjectionPlans.plans.SetWithCancellation(clause, plan, probe) {
		return &returnProjectionPlan{}
	}
	return plan
}

func parseReturnProjectionPlanWithCancellation(clause string, probe *pipelineCancellation) *returnProjectionPlan {
	plan := &returnProjectionPlan{}
	if probe != nil && !probe.check() {
		return plan
	}
	body := trimSpaceWithCancellation(clause, probe)
	modifierStart := len(body)
	if cut := firstTopLevelModifierIndexWithCancellation(body, probe); cut >= 0 {
		modifierStart = cut
	}
	if probe != nil && probe.err != nil {
		return plan
	}
	plan.modifiers = trimSpaceWithCancellation(body[modifierStart:], probe)
	body, plan.distinct = cutDistinctWithCancellation(pipelineClauseBody(body[:modifierStart], "RETURN"), probe)
	items := splitTopLevelCommaWithCancellation(body, probe)
	if probe != nil && probe.err != nil {
		return plan
	}
	if len(items) > 0 && trimSpaceWithCancellation(items[0], probe) == "*" {
		plan.valid, plan.star = true, true
		for _, item := range items[1:] {
			if probe != nil && !probe.check() {
				return &returnProjectionPlan{}
			}
			plan.starItems = appendPipelineItem(plan.starItems, trimSpaceWithCancellation(item, probe), probe)
		}
		if probe != nil && probe.err != nil {
			return &returnProjectionPlan{}
		}
		return plan
	}
	for _, rawItem := range items {
		if probe != nil && !probe.check() {
			return &returnProjectionPlan{}
		}
		expr, alias := parseProjectionExprAliasWithCancellation(trimSpaceWithCancellation(rawItem, probe), probe)
		if probe != nil && probe.err != nil {
			return &returnProjectionPlan{}
		}
		if !plan.addProjectionWithCancellation(expr, alias, probe) {
			return &returnProjectionPlan{}
		}
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

func (plan *returnProjectionPlan) addProjectionWithCancellation(expr, alias string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	aggregateName, aggregateExpr, distinct, isAggr := parsePipelineAggregateWithCancellation(expr, probe)
	if probe != nil && probe.err != nil {
		return false
	}
	if !isAggr && pipelineExpressionContainsAggregateWithCancellation(expr, probe) {
		isAggr = true
		aggregateExpr = expr
	}
	if probe != nil && probe.err != nil {
		return false
	}
	plan.hasAggregate = plan.hasAggregate || isAggr
	plan.projections = appendPipelineItem(plan.projections, returnProjection{expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct}, probe)
	plan.columns = appendPipelineItem(plan.columns, alias, probe)
	return probe == nil || probe.err == nil
}
