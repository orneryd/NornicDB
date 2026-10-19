package replication

import (
	"context"
	"log/slog"
)

// Global order: applyMu -> metadata (mu, logMu, peerMu), release metadata ->
// WAL mutation lease -> physical publication -> WAL record lock. No metadata
// lock crosses a storage callback, an Abort, or a future/completion callback.
// Proposals never acquire applyMu or logMu while holding publication.
func (r *RaftReplicator) applyCommittedEntries() {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.applyCommittedEntriesLocked()
}

// Caller owns applyMu, never logMu. Leader, follower and one-node apply all
// publish the same complete state-machine effect before advancing lastApplied.
func (r *RaftReplicator) applyCommittedEntriesLocked() {
	for {
		r.logMu.RLock()
		index := r.lastApplied + 1
		if index > r.commitIndex {
			r.logMu.RUnlock()
			return
		}
		var entry *RaftLogEntry
		if int(index) < len(r.log) { entry = r.log[index] }
		r.logMu.RUnlock()
		var err error
		if entry != nil && entry.Command != nil {
			err = r.applyCommittedCommand(entry.Command)
			if err != nil {
				r.config.logPrintf(context.Background(), slog.LevelError, "[Raft %s] Failed to apply entry %d", r.config.NodeID, index)
			}
		}
		r.logMu.Lock()
		r.lastApplied = index
		r.logMu.Unlock()
		if future := r.takeApplyFuture(index); future != nil { future.resolve(err) }
	}
}
