package fabric

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrSecondWriteShard is returned when a distributed transaction attempts to write
// to a second shard. Neo4j Fabric enforces a many-read/one-write constraint per
// transaction — this error matches that contract with Neo4j's code,
// Neo.ClientError.Statement.AccessMode, and message (#738).
var ErrSecondWriteShard = fmt.Errorf("Neo.ClientError.Statement.AccessMode: " +
	"Writing to more than one database per transaction is not allowed")

// SubTransaction represents an open sub-transaction on a single shard.
type SubTransaction struct {
	// ShardName identifies the constituent (e.g. "nornic.tr").
	ShardName string

	// IsWrite is true if any write has been performed on this shard.
	IsWrite bool

	// State tracks the lifecycle: "open", "committed", "rolledback".
	State string
	// CommitOutcome distinguishes durable state from terminal cleanup.
	CommitOutcome string
	CleanupState  string
	outcomeFn     ParticipantOutcome

	// commitFn/rollbackFn are optional per-participant lifecycle handlers.
	// When present, Commit()/Rollback() use these callbacks if global callbacks
	// are not provided by the caller.
	commitFn   CommitCallback
	rollbackFn RollbackCallback
}

// FabricTransaction coordinates sub-transactions across participating shards
// within a single distributed transaction.
//
// Invariants enforced:
//   - Reads may span any number of shards.
//   - At most one shard may receive write operations per transaction.
//   - Attempting a write on a second shard is rejected deterministically.
//   - Commit commits all open sub-transactions; rollback rolls back all.
//
// This mirrors Neo4j's FabricTransaction.java.
type FabricTransaction struct {
	subTxns        map[string]*SubTransaction
	writeShard     string // name of the single shard that has writes, or ""
	mu             sync.Mutex
	state          string // "open", "committed", "rolledback"
	txID           string
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc
}

// NewFabricTransaction creates a new distributed transaction.
func NewFabricTransaction(txID string) *FabricTransaction {
	return &FabricTransaction{
		subTxns: make(map[string]*SubTransaction),
		state:   "open",
		txID:    txID,
	}
}

// BindContext anchors participant completion to the transaction owner's
// lifetime. Protocols supply their transaction owner independently of BEGIN's
// request; embedded BEGIN callers supply that owner directly. Otherwise the root
// Execute binds it before any branch or batch derives a shorter-lived context.
// Subsequent statements and fragments cannot replace that owner.
func (t *FabricTransaction) BindContext(ctx context.Context) context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lifetimeCtx == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		t.lifetimeCtx, t.lifetimeCancel = context.WithCancel(transactionLifetime(ctx))
	}
	return t.lifetimeCtx
}

// cancelLifetime is called with mu held, after participant completion.
func (t *FabricTransaction) cancelLifetime() {
	if t.lifetimeCancel != nil {
		t.lifetimeCancel()
	}
}

// TxID returns the transaction identifier.
func (t *FabricTransaction) TxID() string {
	return t.txID
}

// State returns the current transaction state.
func (t *FabricTransaction) State() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// WriteShard returns the name of the shard that holds the write lock,
// or empty string if no writes have been performed.
func (t *FabricTransaction) WriteShard() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writeShard
}

// GetOrOpen returns an existing sub-transaction for the shard, or opens a new one.
// If isWrite is true and a different shard already holds the write lock,
// ErrSecondWriteShard is returned.
func (t *FabricTransaction) GetOrOpen(shardName string, isWrite bool) (*SubTransaction, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "open" {
		return nil, fmt.Errorf("transaction is %s", t.state)
	}

	// Check write constraint before anything else.
	if isWrite && t.writeShard != "" && t.writeShard != shardName {
		return nil, ErrSecondWriteShard
	}

	sub, exists := t.subTxns[shardName]
	if !exists {
		sub = &SubTransaction{
			ShardName: shardName,
			IsWrite:   isWrite,
			State:     "open",
		}
		t.subTxns[shardName] = sub
	}

	// Upgrade read-only sub-transaction to write if needed.
	if isWrite && !sub.IsWrite {
		if t.writeShard != "" && t.writeShard != shardName {
			return nil, ErrSecondWriteShard
		}
		sub.IsWrite = true
	}

	// Track write shard.
	if isWrite {
		t.writeShard = shardName
	}

	return sub, nil
}

// Participants returns the names of all shards participating in this transaction.
func (t *FabricTransaction) Participants() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	names := make([]string, 0, len(t.subTxns))
	for name := range t.subTxns {
		names = append(names, name)
	}
	return names
}

// SubTransactions returns a snapshot of all sub-transactions.
func (t *FabricTransaction) SubTransactions() map[string]*SubTransaction {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string]*SubTransaction, len(t.subTxns))
	for k, v := range t.subTxns {
		copy := *v
		result[k] = &copy
	}
	return result
}

// ParticipantCallbacks binds a participant's terminal operations and outcome
// observer together. Outcome is read only after a failed or panicking commit.
// Without an observer, an unmarked returned error means no commit; a panic or
// ErrCommitOutcomeUnknown means the write outcome is unknown.
type ParticipantCallbacks struct {
	Commit   CommitCallback
	Rollback RollbackCallback
	Outcome  ParticipantOutcome
}

// BindParticipantCallbacks installs the participant's complete terminal contract.
// Callers bind it before executing work on the participant.
func (t *FabricTransaction) BindParticipantCallbacks(shardName string, callbacks ParticipantCallbacks) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != "open" {
		return fmt.Errorf("cannot bind participant: transaction is %s", t.state)
	}
	sub, ok := t.subTxns[shardName]
	if !ok {
		return fmt.Errorf("sub-transaction '%s' not found", shardName)
	}
	if sub.State != "open" {
		return fmt.Errorf("cannot bind participant '%s': sub-transaction is %s", shardName, sub.State)
	}
	sub.commitFn, sub.rollbackFn, sub.outcomeFn = callbacks.Commit, callbacks.Rollback, callbacks.Outcome
	return nil
}

// CommitCallback is called for each sub-transaction during commit.
// The callback should perform the actual commit on the shard.
// If any callback returns an error, commit halts and the remaining
// sub-transactions are rolled back via RollbackCallback.
type CommitCallback func(sub *SubTransaction) error

// RollbackCallback is called for each sub-transaction during rollback.
type RollbackCallback func(sub *SubTransaction) error

const (
	// StateUnknown: the commit was sent but its outcome is not known: the
	// remote's acknowledgement was lost, or the commit panicked before its
	// participant could tell.
	StateUnknown = "unknown"
	// StateCleanupFailed: the rollback failed or panicked. Nothing the
	// sub-transaction wrote was committed, but what it holds (a remote
	// session, MERGE key locks) may not be released.
	StateCleanupFailed = "cleanup_failed"
)

var (
	// ErrCommitKeptNothing marks a commit that failed before the write
	// participant committed: nothing the transaction wrote is kept.
	ErrCommitKeptNothing = errors.New("nothing the transaction wrote was committed")
	// ErrCommitOutcomeUnknown marks a commit whose write participant's
	// outcome is not known: its writes may or may not be committed.
	ErrCommitOutcomeUnknown = errors.New("the outcome of the write database's commit is unknown")
)

// ParticipantOutcome reports, once a participant's commit failed or
// panicked, whether its transaction is committed ("committed"), not
// ("rolledback"), or not known (StateUnknown). A participant without one is
// not committed when its commit returns an error not marked
// ErrCommitOutcomeUnknown, and not known when its commit panics.
type ParticipantOutcome func() string

// participantPanic is a panic a participant's end raised: it goes on once
// every other participant has ended.
type participantPanic struct{ value any }

// endOne runs a participant's end (fn) as a finalizer: it always returns. A
// panic is recorded in *panicked (the first one) and returned as an error.
func endOne(fn func(*SubTransaction) error, sub *SubTransaction, panicked **participantPanic) (didPanic bool, err error) {
	if fn == nil {
		return false, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if *panicked == nil {
				*panicked = &participantPanic{value: recovered}
			}
			didPanic, err = true, fmt.Errorf("participant '%s' panicked: %v", sub.ShardName, recovered)
		}
	}()
	return false, fn(sub)
}

// Commit ends every open sub-transaction. At most one of them holds writes
// (GetOrOpen refuses a second writer), so the read-only ones end first and the
// write participant commits last: the transaction's outcome is the writer's.
//   - A failure before the writer's commit rolls every participant back, the
//     writer included: nothing is committed (ErrCommitKeptNothing).
//   - The writer's commit that fails is not committed (ErrCommitKeptNothing),
//     unless its outcome is not known, a lost remote acknowledgement or a
//     panic (ErrCommitOutcomeUnknown; the transaction ends StateUnknown).
//   - A committed write is never reported as an error.
//
// Every participant ends however another's end ends, a panic included: each
// end runs as a finalizer (endOne), and once all have ended the first panic
// goes on.
func (t *FabricTransaction) Commit(commitFn CommitCallback, rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer t.cancelLifetime()

	if t.state != "open" {
		return fmt.Errorf("cannot commit: transaction is %s", t.state)
	}
	var panicked *participantPanic
	defer func() {
		if panicked != nil {
			panic(panicked.value)
		}
	}()
	commit := func(sub *SubTransaction) (bool, error) {
		callback := commitFn
		if callback == nil {
			callback = sub.commitFn
		}
		return endOne(callback, sub, &panicked)
	}

	var participantNames [8]string
	for _, name := range t.sortedOpenLocked(participantNames[:0]) {
		if name == t.writeShard {
			continue
		}
		sub := t.subTxns[name]
		if sub.State != "open" {
			continue
		}
		if _, err := commit(sub); err != nil {
			// A reader's failed end may leave what it holds: it is rolled
			// back with the others.
			return t.abortCommitLocked(rollbackFn, &panicked, fmt.Errorf("commit failed on shard '%s': %w", sub.ShardName, err))
		}
		sub.State = "committed"
		sub.CommitOutcome = "committed"
	}
	if writer := t.subTxns[t.writeShard]; writer != nil && writer.State == "open" {
		didPanic, err := commit(writer)
		if err == nil {
			writer.State = "committed"
			writer.CommitOutcome = "committed"
			t.state = "committed"
			return nil
		}
		outcome := writerOutcome(writer, err, didPanic)
		var cleanupErr error
		if outcome != "committed" || didPanic {
			// A commit that failed or panicked may have left what it holds;
			// its rollback, a participant's idempotent finalizer, releases
			// it and keeps a committed transaction committed. Its outcome
			// stays the commit's.
			cleanupErr = t.rollbackOneLocked(writer, rollbackFn, &panicked)
		}
		writer.CommitOutcome = outcome
		if outcome != "rolledback" || writer.CleanupState != StateCleanupFailed {
			writer.State = outcome
		}
		t.state = outcome
		err = fmt.Errorf("commit failed on shard '%s': %w", writer.ShardName, err)
		if cleanupErr != nil {
			err = fmt.Errorf("%w (rollback failed: %w; cleanup not confirmed)", err, cleanupErr)
		}
		switch outcome {
		case "committed":
			// Its storage committed before the failure (a panic after the
			// commit): the write stays committed.
			return nil
		case StateUnknown:
			return fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, err)
		}
		return fmt.Errorf("%w: %w", ErrCommitKeptNothing, err)
	}
	t.state = "committed"
	return nil
}

// writerOutcome is the state the write participant's failed commit leaves
// it in (ParticipantOutcome).
func writerOutcome(writer *SubTransaction, err error, didPanic bool) string {
	if writer.outcomeFn != nil {
		reported := StateUnknown
		func() {
			defer func() {
				if recover() != nil {
					reported = StateUnknown
				}
			}()
			reported = writer.outcomeFn()
		}()
		switch reported {
		case "committed", "rolledback":
			return reported
		}
		return StateUnknown
	}
	if didPanic || errors.Is(err, ErrCommitOutcomeUnknown) {
		return StateUnknown
	}
	return "rolledback"
}

// rollbackOneLocked rolls sub back as a finalizer and records how it ended:
// "rolledback", or StateCleanupFailed with the error.
func (t *FabricTransaction) rollbackOneLocked(sub *SubTransaction, rollbackFn RollbackCallback, panicked **participantPanic) error {
	rollback := rollbackFn
	if rollback == nil {
		rollback = sub.rollbackFn
	}
	if _, err := endOne(rollback, sub, panicked); err != nil {
		sub.State = StateCleanupFailed
		sub.CleanupState = StateCleanupFailed
		return err
	}
	sub.State = "rolledback"
	sub.CleanupState = "rolledback"
	return nil
}

// abortCommitLocked rolls back every sub-transaction still open, the write
// participant among them, after a read-only participant failed to end:
// nothing is committed. A rollback that fails is reported with the cause;
// its participant's cleanup is not confirmed (StateCleanupFailed).
func (t *FabricTransaction) abortCommitLocked(rollbackFn RollbackCallback, panicked **participantPanic, commitErr error) error {
	var rollbackErrs []error
	var participantNames [8]string
	for _, name := range t.sortedOpenLocked(participantNames[:0]) {
		sub := t.subTxns[name]
		sub.CommitOutcome = "rolledback"
		if err := t.rollbackOneLocked(sub, rollbackFn, panicked); err != nil {
			rollbackErrs = append(rollbackErrs, fmt.Errorf("%s: %w", sub.ShardName, err))
		}
	}
	t.state = "rolledback"
	if len(rollbackErrs) > 0 {
		return fmt.Errorf("%w: %w (rollback failed on: %w; cleanup not confirmed)", ErrCommitKeptNothing, commitErr, errors.Join(rollbackErrs...))
	}
	return fmt.Errorf("%w: %w", ErrCommitKeptNothing, commitErr)
}

// sortedOpenLocked is the names of the open sub-transactions, sorted.
func (t *FabricTransaction) sortedOpenLocked(names []string) []string {
	if cap(names) < len(t.subTxns) {
		names = make([]string, 0, len(t.subTxns))
	}
	names = names[:0]
	for name, sub := range t.subTxns {
		if sub.State == "open" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// Rollback rolls back all open sub-transactions using the provided callback.
// Every participant is rolled back however another's rollback ends, a panic
// included: each runs as a finalizer, and once all have ended the first
// panic goes on. A participant whose rollback failed ends StateCleanupFailed
// (nothing of it is committed; its cleanup is not confirmed), and every
// failure is reported.
func (t *FabricTransaction) Rollback(rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer t.cancelLifetime()

	if t.state != "open" {
		return fmt.Errorf("cannot rollback: transaction is %s", t.state)
	}
	var panicked *participantPanic
	defer func() {
		if panicked != nil {
			panic(panicked.value)
		}
	}()
	var failures []error
	var participantNames [8]string
	for _, name := range t.sortedOpenLocked(participantNames[:0]) {
		t.subTxns[name].CommitOutcome = "rolledback"
		if err := t.rollbackOneLocked(t.subTxns[name], rollbackFn, &panicked); err != nil {
			failures = append(failures, fmt.Errorf("shard '%s': %w", name, err))
		}
	}
	t.state = "rolledback"
	if len(failures) > 0 {
		return fmt.Errorf("rollback failed (cleanup not confirmed): %w", errors.Join(failures...))
	}
	return nil
}
