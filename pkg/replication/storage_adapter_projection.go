// Author: NOR-291 implementation lane, Codex, 2026-10-27; TJ rebase 2026-10-29.
package replication

import (
	"encoding/json"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// projectionCommandID uses the accepted identity carried by every production
// submission. Older serialized transaction commands can carry that same identity
// only in the canonical entity envelope. Commands without an accepted identity
// retain their existing one-projection-per-call behavior.
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

// appendProjectionLocked is the sole physical and streaming projection owner.
// Both the async writer and synchronous fallback hold walMu while calling it.
// Identity survives cache pruning and is rebuilt from the retained WAL on reopen.
// This decision never substitutes for native application or its outcome.
func (a *StorageAdapter) appendProjectionLocked(record replicationWALRecord) error {
	id := projectionCommandID(record.Command)
	if id != "" {
		if _, exists := a.projectedIDs[id]; exists {
			return nil
		}
	}
	pos, err := a.wal.AppendReturningSeq(storage.OperationType("replication_command"), record)
	if err != nil {
		return localizedError(localization.ReplicationStorageWALAppendFailed(err), err)
	}
	if id != "" {
		if a.projectedIDs == nil {
			a.projectedIDs = make(map[string]struct{})
		}
		a.projectedIDs[id] = struct{}{}
	}
	a.walPosition.Store(pos)
	a.appendToMemWAL(pos, record, record.Command)
	return nil
}

// Keep disk streaming and startup identity reconstruction on the same decoder.
func decodeReplicationWALRecord(entry storage.WALEntry) (replicationWALRecord, error) {
	var record replicationWALRecord
	// The WAL record is JSON; Command.Data within it uses the transport codec.
	// Preserve TJ's sequence-bearing errors and refusal to skip unreadable
	// replication commands in both streaming and identity reconstruction.
	if err := json.Unmarshal(entry.Data, &record); err != nil {
		return record, fmt.Errorf("replication WAL entry %d: %w", entry.Sequence, err)
	}
	if record.Command == nil {
		return record, fmt.Errorf("replication WAL entry %d has no command", entry.Sequence)
	}
	return record, nil
}
