package replication

import "errors"

// ErrTransactionTooLargeToReplicate refuses, at commit and before any node
// queues, logs or applies it, a transaction whose replication journal entry
// the journal cannot hold (NOR-412). The wrapped cause is the journal's own
// refusal, which names its limit.
var ErrTransactionTooLargeToReplicate = errors.New("transaction too large to replicate")

// replicationJournalAdmission is implemented by a Storage whose replication
// journal decides which entries it can hold (StorageAdapter).
type replicationJournalAdmission interface {
	admitReplicatedCommand(*Command) error
}

// admitReplicatedCommand asks storage's replication journal whether it would
// hold cmd's entry. Every replicated commit enters its replicator through
// SubmitAcceptedCommand (explicit and auto-commit transactions, leaf and bulk
// commands, commands forwarded to the leader, multi-region delegation to
// Raft), which calls this before anything is queued, logged or applied, so a
// cluster commits exactly the transactions its journal can record.
func admitReplicatedCommand(storage Storage, cmd *Command) error {
	journal, ok := storage.(replicationJournalAdmission)
	if !ok || cmd == nil {
		return nil
	}
	return journal.admitReplicatedCommand(cmd)
}
