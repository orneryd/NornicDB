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
	for engine != nil {
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
			return route, true
		}
		if _, journalBoundary := engine.(*WALEngine); journalBoundary {
			return EntityCommitRoute{}, false
		}
		inner, ok := engine.(EngineUnwrapper)
		if !ok {
			return EntityCommitRoute{}, false
		}
		next := inner.GetInnerEngine()
		if next == engine {
			return EntityCommitRoute{}, false
		}
		engine = next
	}
	return EntityCommitRoute{}, false
}

// SubmitEntityCommit owns a prepared native commit when no outer replication or
// journal wrapper owns submission. Receipt cancellation never reaches Commit.
func (b *BadgerEngine) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	if prepared == nil || prepared.engine != b {
		return nil, ErrInvalidData
	}
	// Author: NOR-288 lane (Codex), 2026-10-26. A durable outer owner has
	// accepted this command. A native conflict cannot reject that decision.
	if prepared.accepted.Load() {
		return b.submitAcceptedEntityApplication(prepared, settle), nil
	}
	if ctx != nil && ctx.Err() != nil {
		prepared.Abort()
		return nil, ctx.Err()
	}
	receipt, resolve := NewCommitReceipt(settle)
	if err := DecidePublication(ctx, func() error {
		return prepared.startCommit(func() { resolve.PublishPhysical(CommitOutcome{State: CommitApplied}) })
	}); err != nil {
		return nil, err
	}
	go func() { <-prepared.finished; _ = resolve.Resolve(prepared.terminal) }()
	return receipt, nil
}
