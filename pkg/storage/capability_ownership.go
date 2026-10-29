package storage

import "context"

// OwnerPolicy names delegation edges that are independent of data transparency.
type OwnerPolicy uint8

const (
	MembershipOwnerPolicy OwnerPolicy = iota + 1
	CommitOwnerPolicy
	WrapperMetadataPolicy
)

// CapabilityOwnerSource explicitly delegates a particular ownership policy.
// An EngineUnwrapper or read-transparent declaration grants none of these edges.
type CapabilityOwnerSource interface {
	CapabilityOwnerSource(OwnerPolicy) (Engine, bool)
}

// MembershipStatus distinguishes no transaction from a retained boundary whose
// owner is ended or temporarily unbound. Only Unowned permits session fallback.
type MembershipStatus uint8

const (
	MembershipUnowned MembershipStatus = iota
	MembershipBound
	MembershipUnbound
	MembershipEnded
)

// MembershipKind describes batch participation, independently of terminal
// authority. In particular, an explicit native member folds batches while a
// statement owner permits independent batches; a coordinator member refuses.
type MembershipKind uint8

const (
	MembershipKindNone MembershipKind = iota
	MembershipImplicit
	MembershipNativeExplicit
	MembershipCoordinator
	MembershipStoreTransaction
)

// MembershipBinding contains references to real causal owners, never copies of
// their mutable state. Storage does not interpret Cypher's opaque owner tokens.
// Controller is supplied only by the lifecycle issuer; discovery grants no
// terminal authority to an executor reconstructed from the read view.
type MembershipBinding struct {
	Status                                                      MembershipStatus
	Kind                                                        MembershipKind
	Boundary, Owner, Adapter, Controller                        any
	PendingActions, SchemaPublication, StatementMode, ReadLease any
	Enforcement                                                 any
}

// ReadError admits data operations against this live binding. An ended or
// unbound member remains a boundary even when its issued view can answer
// without consulting a native transaction, such as an empty or container view.
func (binding MembershipBinding) ReadError() error {
	switch binding.Status {
	case MembershipEnded:
		return ErrTransactionClosed
	case MembershipUnbound:
		return ErrNotImplemented
	default:
		return nil
	}
}

// MembershipProvider is an owning boundary, including while its binding is
// empty or ended. StorageMembership reads its live status on every use.
type MembershipProvider interface{ StorageMembership() MembershipBinding }

// MembershipHandle binds topology once while reading the actual owner live.
type MembershipHandle struct{ provider MembershipProvider }

func BindMembership(engine any) MembershipHandle {
	provider, _ := engineCapability[MembershipProvider](engine, capabilityMembershipOwner, nil)
	return MembershipHandle{provider}
}
func (handle MembershipHandle) Current() MembershipBinding {
	if handle.provider == nil {
		return MembershipBinding{Status: MembershipUnowned}
	}
	binding := handle.provider.StorageMembership()
	if binding.Boundary == nil {
		binding.Boundary = handle.provider
		if binding.Status == MembershipUnowned {
			binding.Status = MembershipUnbound
		}
	}
	return binding
}

// MembershipForEngine selects the first explicitly carried owning boundary.
// The lookup shares the capability cursor and never unwraps a data view.
func MembershipForEngine(engine any) MembershipBinding {
	return BindMembership(engine).Current()
}

// TransactionCommitter is the exact managed admission/accounting terminal.
type TransactionCommitter interface {
	CommitTransaction(context.Context, *BadgerTransaction) error
}

// CommitOwnerForEngine binds the outermost committer through explicitly declared
// commit delegation. No raw tx.Commit fallback is authorized by failed lookup.
func CommitOwnerForEngine(engine Engine) (TransactionCommitter, bool) {
	return engineCapability[TransactionCommitter](engine, capabilityCommitOwner, nil)
}

func (t *TracedEngine) CapabilityOwnerSource(policy OwnerPolicy) (Engine, bool) {
	return t.Engine, true
}
func (w *WALEngine) CapabilityOwnerSource(policy OwnerPolicy) (Engine, bool) { return w.engine, true }
func (n *NamespacedEngine) CapabilityOwnerSource(policy OwnerPolicy) (Engine, bool) {
	// A namespace issues the same transaction/managed owner with a different
	// data view. This explicit edge does not grant access to its child's reads.
	return n.inner, true
}

// Native leaves declare their raw transaction terminal, so a missing managed
// adapter can never silently turn into an executor-owned raw commit.
func (b *BadgerEngine) CommitTransaction(ctx context.Context, tx *BadgerTransaction) error {
	if tx == nil || tx.engine != b {
		return ErrInvalidData
	}
	return tx.CommitContext(ctx)
}

// TransactionOwnerForEngine selects an explicit native transaction issuer.
// It follows only commit-owner edges, preserving the original managed engine
// separately as the eventual committer.
func TransactionOwnerForEngine[T any](engine Engine) (T, bool) {
	return engineCapability[T](engine, capabilityCommitOwner, nil)
}

// WithMembership binds a runner/statement owner while retaining the remote
// transport and schema. It grants no controller authority or new wire session.
func (r *RemoteEngine) WithMembership(provider MembershipProvider) *RemoteEngine {
	return &RemoteEngine{transport: r.transport, schema: r.schema, membershipProvider: provider}
}
func (r *RemoteEngine) StorageMembership() MembershipBinding {
	if r.membershipProvider == nil {
		return MembershipBinding{Status: MembershipUnowned, Boundary: r}
	}
	binding := r.membershipProvider.StorageMembership()
	if binding.Status == MembershipUnowned {
		binding.Status = MembershipUnbound
	}
	binding.Boundary = r
	return binding
}
