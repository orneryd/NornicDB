package storage

import (
	"fmt"
)

// bufferedCommit restores the same range and point-write intent for both
// startup recovery and individual WAL entry replay.
func (data *WALBufferedCommitData) bufferedCommit() bufferedCommit {
	c := bufferedCommit{
		recovered: true,
		namespace: data.Namespace, txID: data.TxID, ops: data.Ops,
		writes: data.Writes, version: data.Version,
		counterNodeMax: data.NodeMax, counterEdgeMax: data.EdgeMax,
		hasCounters: data.HasCounters, propKeyDrain: data.propKeyDrain(),
		embeddingRanges: data.EmbeddingRanges,
	}
	if len(data.Deletes) > 0 {
		c.deletes = make(map[string]bool, len(data.Deletes))
		for _, key := range data.Deletes {
			c.deletes[key] = true
		}
	}
	return c
}

// RecoverBufferedCommits replays pending native lifecycle operations and
// acknowledged write-behind commits in journal order. It runs at every durable
// engine open, regardless of the current write-behind configuration, before
// serving traffic. A previous process may have enabled buffering, and native
// lifecycle records are autonomous even when buffering has never been enabled.
//
// Idempotency: every flushed generation writes a per-commit flush marker key
// (prefixBufferedFlush) inside the same Badger transaction as its data, so a
// commit whose marker exists has already been applied and is skipped. A
// replayed commit writes its own marker in the same transaction that applies
// it, so a subsequent restart skips it too.
//
// Derived counts are rebuilt after replay rather than re-applying the stored
// deltas: crash recovery is rare, and the authoritative rebuild is the same
// recovery boundary the sync path uses.
func RecoverBufferedCommits(engine Engine, wal *WAL) (int, error) {
	b, ok := engine.(*BadgerEngine)
	if !ok || b == nil || wal == nil {
		return 0, nil
	}
	defer wal.reconcileNamespaceObligations(b)

	// A kill -9 can leave a torn tail record at EOF. Truncate it back to
	// the last good boundary before reading (the same conservative repair
	// the snapshot+WAL recovery path runs).
	activePath := walActivePath(wal.config.Dir)
	_, _, _ = repairWALTailIfNeeded(activePath, defaultWALLogger{})

	var pending []bufferedCommit
	replayed := 0
	flushPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := b.applyBufferedCommits(pending); err != nil {
			return err
		}
		replayed += len(pending)
		pending = nil
		return nil
	}
	err := VisitWALEntriesAfterFromDir(wal.config.Dir, 0, func(entry WALEntry) error {
		if entry.Operation == OpNamespaceLifecycle {
			if err := flushPending(); err != nil {
				return err
			}
			return ReplayWALEntry(b, entry)
		}
		if entry.Operation != OpBufferedCommit {
			return nil
		}
		var data WALBufferedCommitData
		payload, err := decompressWALBufferedCommitPayload(entry.Data)
		if err != nil {
			return err
		}
		if err := decodeRecoveryPayload(payload, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal buffered commit %d: %w", entry.Sequence, err)
		}
		if data.TxID == "" {
			return nil
		}
		exists, err := b.bufferedFlushMarkerExists(data.TxID)
		if err != nil {
			return fmt.Errorf("wal: failed to check flush marker for %q: %w", data.TxID, err)
		}
		if exists {
			return nil // already applied to Badger
		}
		c := data.bufferedCommit()
		pending = append(pending, c)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 && replayed == 0 {
		return 0, nil
	}

	if err := flushPending(); err != nil {
		return 0, fmt.Errorf("wal: replaying %d buffered commits: %w", len(pending), err)
	}

	release, err := b.beginPublication(true)
	if err != nil {
		return replayed, err
	}
	defer release()

	// The replayed commits carried no count deltas; rebuild the derived
	// counts from committed entities so readers never observe committed
	// entities without their counts.
	if err := b.ensureLabelCounts(); err != nil {
		return replayed, fmt.Errorf("wal: rebuild label counts after buffered replay: %w", err)
	}
	if err := b.ensureEdgeTypeCounts(); err != nil {
		return replayed, fmt.Errorf("wal: rebuild edge-type counts after buffered replay: %w", err)
	}
	return replayed, nil
}
