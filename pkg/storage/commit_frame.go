package storage

import "context"

// commitFrame couples immutable operations with their before/final entity states.
// Materialization and publication share it when they consume identical operations;
// canceled relationship writes require a separate physical frame.
type commitFrame struct {
	ops    []Operation
	states commitStates
}

// prepareCommitFrame folds one frame's operations into the frame its consumers
// read, once. ctx is the frame's own lifetime: a canceled preparation selects
// no images and folds nothing, exactly as the fold itself reports.
func prepareCommitFrame(ctx context.Context, operations []Operation) (commitFrame, error) {
	states, err := newCommitStates(ctx, operations)
	if err != nil {
		return commitFrame{}, err
	}
	return commitFrame{ops: operations, states: states}, nil
}

// physicalCommitFrame follows physicalOperationsLocked: only canceled edge writes
// filter the logical operations and therefore require another fold.
func (tx *BadgerTransaction) physicalCommitFrame(ctx context.Context, logical commitFrame, physicalOperations []Operation) (commitFrame, error) {
	if len(tx.cancelledEdgeWrites) == 0 {
		return logical, nil
	}
	return prepareCommitFrame(ctx, physicalOperations)
}
