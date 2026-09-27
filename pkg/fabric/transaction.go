package fabric

import (
	"fmt"
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
//   - Commit commits all open sub-transactions; rollback rolls back all.
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
// If any callback returns an error, commit halts and the remaining
// sub-transactions are rolled back via RollbackCallback.
type CommitCallback func(sub *SubTransaction) error

// RollbackCallback is called for each sub-transaction during rollback.
type RollbackCallback func(sub *SubTransaction) error

// Commit commits all open sub-transactions using the provided callback.
// On partial failure, remaining sub-transactions are rolled back via compensation.
//
// Limitation: this is not a full 2PC (two-phase commit) coordinator. Compensation
// rollback is best-effort — if a shard commit succeeds but a subsequent shard fails,
// the already-committed shard's changes cannot be truly undone (only compensated).
// Neo4j Fabric has the same fundamental constraint for cross-shard transactions.
// The many-read/one-write invariant mitigates this by limiting write exposure to a
// single shard per transaction.
func (t *FabricTransaction) Commit(commitFn CommitCallback, rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "open" {
		return fmt.Errorf("cannot commit: transaction is %s", t.state)
	}

	// Commit all sub-transactions. On failure, attempt compensation rollback for
	// every participant (including already-committed shards) to avoid exposing
	// partial commit state.
	var commitErr error

	for _, sub := range t.subTxns {
		if sub.State != "open" {
			continue
		}
		commit := commitFn
		if commit == nil {
			commit = sub.commitFn
		}
		if commit == nil {
			sub.State = "committed"
			continue
		}
		if err := commit(sub); err != nil {
			commitErr = fmt.Errorf("commit failed on shard '%s': %w", sub.ShardName, err)
			break
		}
		sub.State = "committed"
	}

	if commitErr != nil {
		var compensationErrs []string
		for _, sub := range t.subTxns {
			if sub.State == "committed" || sub.State == "open" {
				rollback := rollbackFn
				if rollback == nil {
					rollback = sub.rollbackFn
				}
				if rollback != nil {
					if err := rollback(sub); err != nil {
						compensationErrs = append(compensationErrs, fmt.Sprintf("%s: %v", sub.ShardName, err))
					}
				}
				sub.State = "rolledback"
			}
		}
		t.state = "rolledback"
		if len(compensationErrs) > 0 {
			return fmt.Errorf("%w (compensation failed on: %s)", commitErr, strings.Join(compensationErrs, ", "))
		}
		return commitErr
	}

	t.state = "committed"
	return nil
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
