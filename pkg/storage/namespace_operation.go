package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// NamespaceDrop owns a namespace's prepare, publication and completion boundary.
// A prepared operation fences new publications without holding the physical
// engine's global barrier while consensus or catalog I/O is pending.
type NamespaceDrop struct {
	engine                  *BadgerEngine
	prefix                  string
	namespace               string
	phase                   atomic.Uint32
	journalPosition         atomic.Uint64
	catalogRevision         uint64
	catalogRevisionChosen   bool
	catalogMutation         *NamespaceCatalogMutation
	committedEntry          bool
	nativeSequence          uint64
	nativeWAL               *WAL
	once                    sync.Once
	done                    chan struct{}
	journal                 func() (bool, error)
	complete                func() error
	resolved                func()
	err                     error
	releasePublication      func()
	physicalDone            bool
	consensus               bool
	releaseTasks            func()
	releaseJournalAdmission func()
}

const (
	dropPreparing uint32 = iota
	dropPrepared
	dropProposed
	dropCommitted
	dropCompleted
	dropAborted
)

type namespaceDropKey struct{}

func NewNamespaceDrop(complete func() error) *NamespaceDrop {
	return &NamespaceDrop{done: make(chan struct{}), complete: complete}
}

func WithNamespaceDrop(ctx context.Context, op *NamespaceDrop) context.Context {
	return context.WithValue(ctx, namespaceDropKey{}, op)
}

func NamespaceDropFromContext(ctx context.Context) *NamespaceDrop {
	op, _ := ctx.Value(namespaceDropKey{}).(*NamespaceDrop)
	return op
}

// SetJournal binds publication to its journal owner before preparation starts.
// The callback runs under the namespace's physical publication barrier.
func (op *NamespaceDrop) SetJournal(journal func() (bool, error)) { op.journal = journal }

// OnResolved releases preparation receipts after either abort or completion.
// It runs outside publication locks and never performs committed completion.
func (op *NamespaceDrop) OnResolved(resolved func()) { op.resolved = resolved }

func (op *NamespaceDrop) SetJournalPosition(position uint64) { op.journalPosition.Store(position) }
func (op *NamespaceDrop) JournalPosition() uint64            { return op.journalPosition.Load() }
func (op *NamespaceDrop) SetCatalogRevision(revision uint64) {
	op.catalogRevision = revision
	op.catalogRevisionChosen = true
}
func (op *NamespaceDrop) CatalogRevision() uint64 { return op.catalogRevision }

// MarkCommittedEntry is for deterministic application of an admitted journal
// entry. The caller's wait can no longer cancel or revalidate its preparation.
func (op *NamespaceDrop) MarkCommittedEntry() { op.committedEntry = true }

func (op *NamespaceDrop) SetPublicationRelease(release func()) { op.releasePublication = release }
func (op *NamespaceDrop) HasPublicationLease() bool            { return op.releasePublication != nil }
func (op *NamespaceDrop) DeferConsensusCompletion()            { op.consensus = true }
func (op *NamespaceDrop) Done() <-chan struct{}                { return op.done }
func (op *NamespaceDrop) Complete(err error) error             { return op.finish(errors.Join(op.err, err)) }

type deferredDropCompletionKey struct{}

func (op *NamespaceDrop) physicalComplete(ctx context.Context, err error) error {
	if op.releasePublication != nil {
		op.releasePublication()
		op.releasePublication = nil
	}
	err = errors.Join(op.err, err, op.applyCatalogMutation())
	// Recovery may skip this journal entry only after intrinsic cleanup succeeds.
	// External completion callbacks run later and cannot undo that checkpoint.
	if err == nil && op.nativeSequence != 0 {
		err = op.engine.persistNamespaceCompletion(op.namespace, op.nativeSequence)
	}
	op.physicalDone, op.err = true, err
	if (op.consensus || ctx.Value(deferredDropCompletionKey{}) == true) && op.complete != nil {
		return op.Result(op.err)
	}
	return op.finish(op.err)
}

func (op *NamespaceDrop) Committed() bool {
	p := op.phase.Load()
	return p == dropCommitted || p == dropCompleted
}

// NamespaceDropError reports an irreversible or still-pending outcome while
// preserving errors.Is for a caller's cancellation/deadline.
type NamespaceDropError struct {
	Committed bool
	Aborted   bool
	Cause     error
}

func (e *NamespaceDropError) Error() string {
	if e.Aborted {
		return fmt.Sprintf("namespace preparation aborted: %v", e.Cause)
	}
	if e.Committed {
		return fmt.Sprintf("namespace operation committed; completion/acknowledgement: %v", e.Cause)
	}
	return fmt.Sprintf("namespace proposed; outcome pending: %v", e.Cause)
}
func (e *NamespaceDropError) Unwrap() error         { return e.Cause }
func (e *NamespaceDropError) MutationApplied() bool { return e.Committed }

func (op *NamespaceDrop) Result(err error) error {
	if err == nil {
		return nil
	}
	var outcome *NamespaceDropError
	if errors.As(err, &outcome) && outcome.Committed == op.Committed() {
		return err
	}
	if op.Committed() {
		return &NamespaceDropError{Committed: true, Cause: err}
	}
	if op.phase.Load() == dropProposed {
		return &NamespaceDropError{Cause: err}
	}
	return err
}

// PrepareNamespaceDrop forwards only this lifecycle capability, independently
// of count dispatch. WAL admission is cancellable and never owns a manager lock.
func PrepareNamespaceDrop(ctx context.Context, engine Engine, prefix string, op *NamespaceDrop) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch e := engine.(type) {
	case *NamespacedEngine:
		return PrepareNamespaceDrop(ctx, e.inner, e.namespace+e.separator+prefix, op)
	case *WALEngine:
		journalPrefix := prefix
		if e.journalNamespace != "" {
			journalPrefix = e.journalNamespace + ":" + prefix
		}
		if err := PrepareNamespaceJournal(ctx, e.engine, journalPrefix, op); err != nil {
			return err
		}
		release, err := e.admitNamespaceLifecycle(ctx)
		if err != nil {
			op.Abort()
			return err
		}
		defer release()
		return PrepareNamespaceDrop(ctx, e.engine, prefix, op)
	case interface {
		PrepareNamespaceDrop(context.Context, string, *NamespaceDrop) error
	}:
		return e.PrepareNamespaceDrop(ctx, prefix, op)
	case EngineUnwrapper:
		return PrepareNamespaceDrop(ctx, e.GetInnerEngine(), prefix, op)
	default:
		return ErrNotImplemented
	}
}

func (b *BadgerEngine) PrepareNamespaceDrop(ctx context.Context, prefix string, op *NamespaceDrop) error {
	namespace, whole := strings.CutSuffix(prefix, ":")
	if !whole || namespace == "" || strings.Contains(namespace, ":") {
		return ErrNotImplemented
	}
	if err := PrepareNamespaceJournal(ctx, b, prefix, op); err != nil {
		return err
	}
	prepared := false
	defer func() {
		if !prepared {
			op.Abort()
		}
	}()
	releaseTasks, err := b.admitNamespaceTasks(ctx, namespace)
	if err != nil {
		return err
	}
	defer func() {
		if !prepared {
			releaseTasks()
		}
	}()
	// Close physical ACK admission before capturing the accepted buffer frontier.
	// Replay uses its shared publication barrier and can still drain old ACKs.
	release, err := b.beginSchemaWriteContext(ctx)
	if err != nil {
		return err
	}
	err = func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.checkNamespaceOperation(namespace); err != nil {
			return err
		}
		op.engine, op.prefix, op.namespace = b, prefix, namespace
		if op.catalogMutation == nil {
			if err := op.captureCatalogRetirement(); err != nil {
				return err
			}
		}
		if err := op.validateCatalogMutation(); err != nil {
			return err
		}
		if op.fencesNamespace() {
			b.setNamespaceOperation(namespace, op)
		}
		return nil
	}()
	release()
	if err != nil {
		return err
	}
	release, err = b.beginPrefixDeletion(ctx, prefix)
	if err != nil {
		return err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return err
	}
	op.releaseTasks = releaseTasks
	priorCompletion := op.complete
	op.complete = func() error {
		var err error
		if op.dropsNamespace() {
			err = b.completeNamespaceRetirement(namespace)
		}
		if priorCompletion != nil {
			err = errors.Join(err, priorCompletion())
		}
		return err
	}
	op.phase.Store(dropPrepared)
	prepared = true
	return nil
}

// Propose orders consensus-log admission while the prepared namespace is
// fenced. Cancellation before this boundary appends nothing. The proposal is
// distinct from a committed consensus entry; only its application retires data.
func (op *NamespaceDrop) Propose(ctx context.Context, appendLog func() error) error {
	release, err := op.engine.beginSchemaWriteContext(ctx)
	if err != nil {
		op.Abort()
		return err
	}
	if err = ctx.Err(); err == nil {
		op.chooseCatalogRevision()
		err = appendLog()
	}
	if err == nil {
		op.phase.Store(dropProposed)
	}
	release()
	if err != nil {
		op.Abort()
	}
	return err
}

func (op *NamespaceDrop) Abort() {
	if op.Committed() {
		return
	}
	op.once.Do(func() {
		if op.engine != nil {
			op.engine.writeBarrier.Lock()
			op.engine.setNamespaceOperation(op.namespace, nil)
			op.engine.writeBarrier.Unlock()
		}
		if op.releasePublication != nil {
			op.releasePublication()
			op.releasePublication = nil
		}
		op.phase.Store(dropAborted)
		if op.releaseTasks != nil {
			op.releaseTasks()
			op.releaseTasks = nil
		}
		if op.resolved != nil {
			op.resolved()
		}
		if op.releaseJournalAdmission != nil {
			op.releaseJournalAdmission()
			op.releaseJournalAdmission = nil
		}
		close(op.done)
	})
}

// Called with the exclusive publication barrier held. Both journal admission
// and lifetime retirement precede release of that barrier and physical deletion.
func (op *NamespaceDrop) commitLocked(ctx context.Context) error {
	if op.phase.Load() != dropProposed {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	op.chooseCatalogRevision()
	if op.nativeSequence == 0 {
		wal := op.nativeWAL
		if wal == nil {
			wal = op.engine.bufferedCommitWAL
		}
		op.bindNativeJournal(wal)
	}
	if op.journal != nil {
		admitted, err := op.journal()
		if !admitted && err != nil {
			return err
		}
		op.err = err
	}
	if op.namespace != "" && op.dropsNamespace() {
		op.err = errors.Join(op.err, op.engine.retireNamespace(op.namespace, op.catalogRevision))
		op.engine.publishLogicalNamespaceRetirement(op.namespace)
	}
	if op.namespace != "" {
		op.engine.observeCatalogRevision(op.catalogRevision)
	}
	op.phase.Store(dropCommitted)
	return nil
}

func (op *NamespaceDrop) chooseCatalogRevision() {
	if !op.catalogRevisionChosen && op.namespace != "" {
		op.catalogRevision = op.engine.catalogRevision.Load()
		if op.dropsNamespace() {
			op.catalogRevision++
		}
		op.catalogRevisionChosen = true
	}
}

// finish runs after releasing the physical barrier. The target remains fenced
// until catalog persistence and cache retirement finish; peer namespaces and
// catalog readers need no lock owned by this operation.
func (op *NamespaceDrop) finish(err error) error {
	op.once.Do(func() {
		if op.complete != nil {
			err = errors.Join(err, op.complete())
		}
		if op.engine != nil {
			op.engine.writeBarrier.Lock()
			op.engine.setNamespaceOperation(op.namespace, nil)
			op.engine.writeBarrier.Unlock()
		}
		op.err = err
		op.phase.Store(dropCompleted)
		if op.releaseTasks != nil {
			op.releaseTasks()
			op.releaseTasks = nil
		}
		if op.resolved != nil {
			op.resolved()
		}
		if op.releaseJournalAdmission != nil {
			op.releaseJournalAdmission()
			op.releaseJournalAdmission = nil
		}
		close(op.done)
	})
	return op.Result(op.err)
}

func (b *BadgerEngine) checkNamespaceOperation(namespace string) error {
	if state := b.namespaceLifetimes.Load(); state != nil && state.active[namespace] != nil {
		return fmt.Errorf("%w: namespace %q has a lifecycle operation in progress", ErrConflict, namespace)
	}
	return nil
}

// All namespace-lifetime state has one writer: the exclusive publication owner.
func (b *BadgerEngine) setNamespaceOperation(namespace string, op *NamespaceDrop) {
	next := b.copyNamespaceLifetimes()
	if op == nil {
		delete(next.active, namespace)
	} else {
		next.active[namespace] = op
	}
	b.namespaceLifetimes.Store(next)
}
