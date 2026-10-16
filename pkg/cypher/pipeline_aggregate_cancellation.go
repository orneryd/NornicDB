package cypher

// A group's first row and aggregate state templates can both be wide even when
// the source contains only one row. Copy them through the statement's budget.
func newPipelineAggregateGroup(row pipelineRow, templates []pipelineAggregateProjection, probe *pipelineCancellation) *pipelineAggregateGroup {
	if probe != nil && !probe.check() {
		return nil
	}
	group := &pipelineAggregateGroup{first: make(pipelineRow, len(row)), projections: make([]pipelineAggregateProjection, 0, pipelineInitialCapacity(len(templates), probe))}
	for name, value := range row {
		if probe != nil && !probe.check() {
			return nil
		}
		group.first[name] = value
	}
	for _, template := range templates {
		if probe != nil && !probe.check() {
			return nil
		}
		states := appendPipelineItems([]pipelineAggregateState(nil), template.states, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
		group.projections = appendPipelineItem(group.projections, pipelineAggregateProjection{expression: template.expression, states: states}, probe)
		if probe != nil && probe.err != nil {
			return nil
		}
	}
	return group
}
