package storage

import (
	"fmt"
)

// RecoverBufferedCommits replays write-behind commits that were acknowledged
// into the WAL but never reached Badger before a crash lost the in-memory
// buffer. It runs at engine open, after both the engine and the WAL are
// constructed and before the engine serves traffic.
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
	if !ok || b == nil || wal == nil || b.writeBehind == nil {
		return 0, nil
	}
	if err := b.beginOriginalPropertyRecovery(); err != nil {
		return 0, err
	}
	defer b.endOriginalPropertyRecovery()

	// A kill -9 can leave a torn tail record at EOF. Truncate it back to
	// the last good boundary before reading (the same conservative repair
	// the snapshot+WAL recovery path runs).
	activePath := walActivePath(wal.config.Dir)
	_, _, _ = repairWALTailIfNeeded(activePath, defaultWALLogger{})

	var pending []bufferedCommit
	err := VisitWALEntriesAfterFromDir(wal.config.Dir, 0, func(entry WALEntry) error {
		if entry.Operation != OpBufferedCommit {
			return nil
		}
		data, err := decodeBufferedWALRecord(entry)
		if err != nil {
			return err
		}
		if data.TxID == "" {
			return nil
		}
		exists, err := b.bufferedFlushMarkerExists(data.TxID)
		if err != nil {
			return fmt.Errorf("wal: failed to check flush marker for %q: %w", data.TxID, err)
		}
		if exists {
			return b.observeBufferedWALRecord(&data) // source bindings still feed later legacy records
		}
		c, err := b.importBufferedWALRecord(&data)
		if err != nil {
			return err
		}
		pending = append(pending, c)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}

	if err := b.applyBufferedCommits(pending); err != nil {
		return 0, fmt.Errorf("wal: replaying %d buffered commits: %w", len(pending), err)
	}

	// The replayed commits carried no count deltas; rebuild the derived
	// counts from committed entities so readers never observe committed
	// entities without their counts.
	if err := b.ensureLabelCounts(); err != nil {
		return len(pending), fmt.Errorf("wal: rebuild label counts after buffered replay: %w", err)
	}
	if err := b.ensureEdgeTypeCounts(); err != nil {
		return len(pending), fmt.Errorf("wal: rebuild edge-type counts after buffered replay: %w", err)
	}
	return len(pending), nil
}
