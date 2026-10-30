package storage

import "context"

// EntityCommitRoute describes whether an outer owner must process submission.
// RequiresSubmission is false only for canonical native storage owners.
// EntityCommitAdmissionGate enforces wrapper policy before preparation/admission.
type EntityCommitAdmissionGate interface{ CheckEntityCommitAdmission() error }
type EntityDirectForwarder interface {
	ForwardEntityChanges(context.Context, []Operation, EntityCommitMode) error
}
type EntityCommitRoute struct {
	ForwardDirect      func(context.Context, []Operation, EntityCommitMode) error
	CheckAdmission     func() error
	Submitter          EntityCommitSubmitter
	RequiresSubmission bool
}

// EntityCommitSubmitterFor returns the first authoritative wrapper owner.
// A journal wrapper is a routing boundary even before it exposes the capability:
// discovering an inner native engine must never bypass that wrapper's journal.
func EntityCommitSubmitterFor(engine Engine) (EntityCommitRoute, bool) {
	route, found, _ := EntityCommitSubmitterForWithContext(context.Background(), engine)
	return route, found
}

// EntityCommitSubmitterForWithContext observes pre-acceptance route discovery
// through the same first-authoritative-owner and journal-boundary policy.
func EntityCommitSubmitterForWithContext(ctx context.Context, engine Engine) (EntityCommitRoute, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for engine != nil {
		if err := ctx.Err(); err != nil {
			return EntityCommitRoute{}, false, err
		}
		if submitter, ok := engine.(EntityCommitSubmitter); ok {
			native := false
			switch engine.(type) {
			case *BadgerEngine, *MemoryEngine:
				native = true
			}
			route := EntityCommitRoute{Submitter: submitter, RequiresSubmission: !native}
			if gate, ok := engine.(EntityCommitAdmissionGate); ok {
				route.CheckAdmission = gate.CheckEntityCommitAdmission
			}
			if forwarder, ok := engine.(EntityDirectForwarder); ok {
				route.ForwardDirect = forwarder.ForwardEntityChanges
			}
			return route, true, nil
		}
		if _, journalBoundary := engine.(*WALEngine); journalBoundary {
			return EntityCommitRoute{}, false, nil
		}
		inner, ok := engine.(EngineUnwrapper)
		if !ok {
			return EntityCommitRoute{}, false, nil
		}
		next := inner.GetInnerEngine()
		if next == engine {
			return EntityCommitRoute{}, false, nil
		}
		engine = next
	}
	return EntityCommitRoute{}, false, nil
}

// SubmitEntityCommit owns a prepared native commit when no outer replication or
// journal wrapper owns submission. Receipt cancellation never reaches Commit.
func (b *BadgerEngine) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	if prepared == nil || prepared.engine != b {
		return nil, ErrInvalidData
	}
	if ctx != nil && ctx.Err() != nil {
		prepared.Abort()
		return nil, ctx.Err()
	}
	receipt, resolve := NewCommitReceipt(settle)
	if err := prepared.startCommit(func() { resolve.PublishPhysical(CommitOutcome{State: CommitApplied}) }); err != nil {
		return nil, err
	}
	go func() { <-prepared.finished; _ = resolve.Resolve(prepared.terminal) }()
	return receipt, nil
}
