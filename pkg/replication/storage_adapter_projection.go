package replication

import (
	"encoding/json"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// projectionCommandID uses the accepted identity, including older transaction
// envelopes that carry their logical identity only in the encoded payload.
func projectionCommandID(cmd *Command) string {
	if cmd.RequestID != "" {
		return cmd.RequestID
	}
	if cmd.Type == CmdCommitTransaction {
		if envelope, _, err := storage.DecodeEntityTransaction(cmd.Data); err == nil {
			return envelope.LogicalTxID
		}
	}
	return ""
}

// appendProjectionLocked returns the original publication position on retry.
// The FIFO writer holds walMu, serializing identity checks with physical append.
func (a *StorageAdapter) appendProjectionLocked(record replicationWALRecord) (uint64, error) {
	id := projectionCommandID(record.Command)
	if id != "" {
		if position, exists := a.projectedIDs[id]; exists {
			return position, nil
		}
	}
	position, err := a.wal.AppendReturningSeq(storage.OperationType("replication_command"), record)
	if err != nil {
		return 0, localizedError(localization.ReplicationStorageWALAppendFailed(err), err)
	}
	if id != "" {
		if a.projectedIDs == nil {
			a.projectedIDs = make(map[string]uint64)
		}
		a.projectedIDs[id] = position
	}
	a.appendToMemWAL(position, record, record.Command)
	return position, nil
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
