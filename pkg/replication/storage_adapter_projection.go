package replication

import (
	"encoding/json"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// projectionIdentity is one accepted command's journal identity: the database
// that accepted the command and the caller's logical ID inside that database.
// Logical IDs are only unique per database, so two databases reusing one ID are
// two accepted commands. The typed key makes database and ID separate fields,
// so no caller text can collide with another pair.
type projectionIdentity struct {
	database  string
	logicalID string
}

// projectionCommandIdentity uses the identity validated by the native transaction
// owner. RequestID is transport metadata for transactions; direct commands retain
// their existing request-ID domain, distinguished by an empty database.
func projectionCommandIdentity(cmd *Command, envelope storage.EntityTransactionEnvelope) projectionIdentity {
	if cmd.Type == CmdCommitTransaction {
		return projectionIdentity{database: envelope.Namespace, logicalID: envelope.LogicalTxID}
	}
	return projectionIdentity{logicalID: cmd.RequestID}
}

// appendProjectionLocked returns the original publication position on retry.
// The FIFO writer holds walMu, serializing identity checks with physical append.
func (a *StorageAdapter) appendProjectionLocked(record replicationWALRecord, identity projectionIdentity) (uint64, error) {
	if identity.logicalID != "" {
		if position, exists := a.projectedIDs[identity]; exists {
			return position, nil
		}
	}
	position, err := a.wal.AppendReturningSeq(replicationWALOperation, record)
	if err != nil {
		return 0, localizedError(localization.ReplicationStorageWALAppendFailed(err), err)
	}
	if identity.logicalID != "" {
		if a.projectedIDs == nil {
			a.projectedIDs = make(map[projectionIdentity]uint64)
		}
		a.projectedIDs[identity] = position
	}
	a.appendToMemWAL(position, record, record.Command)
	return position, nil
}

// replicationWALOperation is the operation of every replication journal entry.
const replicationWALOperation = storage.OperationType("replication_command")

// admitReplicatedCommand lets the replication journal decide, before cmd is
// queued, logged or applied anywhere, whether it would hold cmd's entry: the
// journal's own encoder checks the exact record ApplyCommand appends
// (NOR-412). The journal keeps every entry in one frame, so a cluster commits
// exactly the transactions whose journal entry it can record.
func (a *StorageAdapter) admitReplicatedCommand(cmd *Command) error {
	record := replicationWALRecord{Timestamp: cmd.Timestamp.UnixNano(), Command: cmd}
	if err := a.wal.CheckOneFrameRecord(replicationWALOperation, record, ""); err != nil {
		return fmt.Errorf("%w: the replication journal cannot hold its entry: %w", ErrTransactionTooLargeToReplicate, err)
	}
	return nil
}

func decodeProjectionRecord(entry storage.WALEntry) (replicationWALRecord, error) {
	var record replicationWALRecord
	if err := json.Unmarshal(entry.Data, &record); err != nil {
		return record, fmt.Errorf("replication WAL entry %d: %w", entry.Sequence, err)
	}
	if record.Command == nil {
		return record, fmt.Errorf("replication WAL entry %d has no command", entry.Sequence)
	}
	return record, nil
}
