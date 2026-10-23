package cypher

import "context"

type plannedTransactionalCall struct {
	index int
	spec  *transactionBatchSpec
}

type pipelineCallChainPlan struct {
	end   int
	calls []plannedTransactionalCall
}

// planPipelineCallChain consumes the shared effect/dependency contract. No
// clause or expression representation has a separate planning path here.
func (e *StorageExecutor) planPipelineCallChain(ctx context.Context, clauses []pipelineClause, start, maximumRows int) (pipelineCallChainPlan, error) {
	plan := pipelineCallChainPlan{end: len(clauses)}
	var writes stretchTokens
	for index := start; index < len(clauses); index++ {
		effects, err := pipelineClauseEffectsFor(ctx, clauses[index].text)
		if err != nil {
			return plan, err
		}
		boundary, nextMaximum := e.pipelineClauseBoundary(ctx, effects, &writes, maximumRows)
		if boundary {
			plan.end = index
			return plan, nil
		}
		if effects.batch != nil {
			plan.calls = append(plan.calls, plannedTransactionalCall{index: index, spec: effects.batch})
		}
		writes.include(&effects.chainWrites)
		maximumRows = nextMaximum
	}
	return plan, nil
}
