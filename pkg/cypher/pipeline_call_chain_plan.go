package cypher

import "context"

type plannedCallChainOperator struct {
	index      int
	spec       *transactionBatchSpec
	pagination []paginationExpression
}

type pipelineCallChainPlan struct {
	end       int
	operators []plannedCallChainOperator
}

// planPipelineCallChain consumes the shared effect/dependency contract. No
// clause or expression representation has a separate planning path here.
func (e *StorageExecutor) planPipelineCallChain(ctx context.Context, clauses []pipelineClause, start, maximumRows int) (pipelineCallChainPlan, error) {
	plan := pipelineCallChainPlan{end: len(clauses)}
	var writes stretchTokens
	for index := start; index < len(clauses); index++ {
		effects, err := executionClauseEffects(ctx, clauses[index])
		if err != nil {
			return plan, err
		}
		// Pagination opens before this clause's scalar projections, but
		// after a sort/aggregation has consumed its input. Keep that ordering
		// in the same consumer-to-input binder as transactional CALL sizes.
		if !effects.materializes && (effects.skip != "" || effects.limit != "") {
			var pagination []paginationExpression
			if effects.skip != "" {
				pagination = append(pagination, paginationExpression{keyword: "SKIP", value: effects.skip})
			}
			if effects.limit != "" {
				pagination = append(pagination, paginationExpression{keyword: "LIMIT", value: effects.limit})
			}
			plan.operators = append(plan.operators, plannedCallChainOperator{index: index, pagination: pagination})
		}
		boundary, nextMaximum := e.pipelineClauseBoundary(ctx, effects, &writes, maximumRows)
		if boundary {
			plan.end = index
			return plan, nil
		}
		if effects.batch != nil {
			plan.operators = append(plan.operators, plannedCallChainOperator{index: index, spec: effects.batch})
		}
		writes.include(&effects.chainWrites)
		maximumRows = nextMaximum
	}
	return plan, nil
}
