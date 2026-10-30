package cypher

import (
	"context"
)

// Both public callback reentry and internal statement execution enter here
// after normalization and parameter installation. Procedure handlers have
// already evaluated their arguments using the normal evaluator. Nested dynamic
// calls execute through the same gate, with no argument-shape prediction.
func (e *StorageExecutor) admitResolvedStatementWrite(ctx context.Context, statement string) error {
	state := e.resolvedStatementModeOf(ctx)
	if state == nil {
		return nil
	}
	contract, err := readStatementExecutionContract(ctx, statement)
	if err != nil {
		return err
	}
	if !contract.changesState() {
		return nil
	}
	if state != nil {
		state.write.Store(true)
	}

	return nil
}
