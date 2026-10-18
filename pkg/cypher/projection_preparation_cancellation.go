package cypher



func (plan *returnProjectionPlan) addProjectionWithCancellation(expr, alias string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	aggregateName, aggregateExpr, distinct, isAggr := parsePipelineAggregate(expr)
	if probe != nil && probe.err != nil {
		return false
	}
	if !isAggr && pipelineExpressionContainsAggregate(expr) {
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
