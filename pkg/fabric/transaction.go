package fabric

import (
	"fmt"
	"sort"
	"strings"
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
//   - A shard becomes the write shard when a write is opened on it
//     (ClaimWrite), as the write runs.
//   - Commit ends the read-only sub-transactions first and commits the write
//     shard last; rollback rolls back all.
//
// This mirrors Neo4j's FabricTransaction.java.
type FabricTransaction struct {
	subTxns    map[string]*SubTransaction
	writeShard string // name of the single shard that has writes, or ""
	mu         sync.Mutex
	state      string // "open", "committed", "rolledback"
	txID       string
}

// NewFabricTransaction creates a new distributed transaction.
func NewFabricTransaction(txID string) *FabricTransaction {
	return &FabricTransaction{
		subTxns: make(map[string]*SubTransaction),
		state:   "open",
		txID:    txID,
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

// ClaimWrite makes participant the one write participant of the
// transaction and returns its sub-transaction. Every write opened on a
// constituent claims it first, when the write runs: a local statement that
// writes, a remote write session. A claim on a second participant is
// refused with ErrSecondWriteShard before the write runs, as Neo4j refuses
// writing to more than one database per transaction. It is the only way a
// participant becomes the writer.
func (t *FabricTransaction) ClaimWrite(participant string) (*SubTransaction, error) {
	return t.GetOrOpen(participant, true)
}

// GetOrOpen returns an existing sub-transaction for the shard, or opens a new one.
// If isWrite is true and a different shard already holds the write lock,
// ErrSecondWriteShard is returned. A write is opened through ClaimWrite.
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

// BindParticipantCallbacks binds per-subtransaction commit/rollback callbacks.
// Existing bindings are replaced.
func (t *FabricTransaction) BindParticipantCallbacks(shardName string, commitFn CommitCallback, rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	sub, ok := t.subTxns[shardName]
	if !ok {
		return fmt.Errorf("sub-transaction '%s' not found", shardName)
	}
	sub.commitFn = commitFn
	sub.rollbackFn = rollbackFn
	return nil
}

// CommitCallback is called for each sub-transaction during commit.
// The callback should perform the actual commit on the shard.
type CommitCallback func(sub *SubTransaction) error

// RollbackCallback is called for each sub-transaction during rollback.
type RollbackCallback func(sub *SubTransaction) error

// Commit ends every open sub-transaction. At most one of them holds writes
// (ClaimWrite refuses a second), so the read-only ones end first and the
// write participant commits last: the transaction's outcome is the writer's.
// A failure before the writer's commit rolls the writer back, so nothing is
// committed; nothing runs after it, so a committed write is never reported
// as an error.
func (t *FabricTransaction) Commit(commitFn CommitCallback, rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "open" {
		return fmt.Errorf("cannot commit: transaction is %s", t.state)
	}
	commit := func(sub *SubTransaction) error {
		callback := commitFn
		if callback == nil {
			callback = sub.commitFn
		}
		if callback == nil {
			return nil
		}
		return callback(sub)
	}

	names := make([]string, 0, len(t.subTxns))
	for name := range t.subTxns {
		if name != t.writeShard {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		sub := t.subTxns[name]
		if sub.State != "open" {
			continue
		}
		if err := commit(sub); err != nil {
			return t.abortCommitLocked(rollbackFn, fmt.Errorf("commit failed on shard '%s': %w", sub.ShardName, err))
		}
		sub.State = "committed"
	}
	if writer := t.subTxns[t.writeShard]; writer != nil && writer.State == "open" {
		if err := commit(writer); err != nil {
			writer.State = "rolledback"
			t.state = "rolledback"
			return fmt.Errorf("commit failed on shard '%s': %w", writer.ShardName, err)
		}
		writer.State = "committed"
	}
	t.state = "committed"
	return nil
}

// abortCommitLocked rolls back every sub-transaction still open, the write
// participant among them, after a read-only participant failed to end.
func (t *FabricTransaction) abortCommitLocked(rollbackFn RollbackCallback, commitErr error) error {
	var rollbackErrs []string
	for _, sub := range t.subTxns {
		if sub.State != "open" {
			continue
		}
		rollback := rollbackFn
		if rollback == nil {
			rollback = sub.rollbackFn
		}
		if rollback != nil {
			if err := rollback(sub); err != nil {
				rollbackErrs = append(rollbackErrs, fmt.Sprintf("%s: %v", sub.ShardName, err))
			}
		}
		sub.State = "rolledback"
	}
	t.state = "rolledback"
	if len(rollbackErrs) > 0 {
		return fmt.Errorf("%w (rollback failed on: %s)", commitErr, strings.Join(rollbackErrs, ", "))
	}
	return commitErr
}

// Rollback rolls back all open sub-transactions using the provided callback.
func (t *FabricTransaction) Rollback(rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "open" {
		return fmt.Errorf("cannot rollback: transaction is %s", t.state)
	}

	var lastErr error
	for _, sub := range t.subTxns {
		if sub.State != "open" {
			continue
		}
		rollback := rollbackFn
		if rollback == nil {
			rollback = sub.rollbackFn
		}
		if rollback != nil {
			if err := rollback(sub); err != nil {
				lastErr = fmt.Errorf("rollback failed on shard '%s': %w", sub.ShardName, err)
			}
		}
		sub.State = "rolledback"
	}

	t.state = "rolledback"
	return lastErr
}
