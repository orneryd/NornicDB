package storage

import (
	"context"
	"errors"
	"fmt"
)

// Recovery retains one batch plus the record currently being decoded, rather
// than every unflushed commit in the WAL. A single larger commit remains atomic
// and is applied alone; this is a batching target, not an allocator limit.
const bufferedRecoveryBatchBytes int64 = 8 << 20

// RecoverBufferedCommits replays accepted semantic transactions and write-behind
// commits that were acknowledged
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
	return recoverAcceptedCommits(engine, wal, walForeignImport)
}

// RecoverBufferedCommitsAtOpen replays both accepted record kinds and is exclusively the constructor's recovery of a
// store's own configured WAL directory, before traffic. That structural authority
// preserves unstamped historical upgrades; a foreign-populated store cannot use it.
func RecoverBufferedCommitsAtOpen(engine Engine, wal *WAL) (int, error) {
	return recoverAcceptedCommits(engine, wal, walOwnDirectoryAtOpen)
}

func recoverAcceptedCommits(engine Engine, wal *WAL, authority walPropertyAuthority) (int, error) {
	b, ok := OwnedCapability[*BadgerEngine](engine)
	if !ok || b == nil || wal == nil {
		return 0, nil
	}
	// The batch reader retains engine state through import, physical apply
	// and count repair. Close rejects later admissions and waits for this lease.
	if _, err := b.beginHelperTxn(); err != nil {
		return 0, err
	}
	defer b.txnWG.Done()

	// A kill -9 can leave a torn tail record at EOF. Truncate it back to
	// the last good boundary before reading (the same conservative repair
	// the snapshot+WAL recovery path runs).
	activePath := walActivePath(wal.config.Dir)
	_, _, _ = repairWALTailIfNeeded(activePath, defaultWALLogger{})

	var pending []bufferedCommit
	var pendingBytes int64
	replayed := 0
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := b.applyBufferedCommits(pending); err != nil {
			return fmt.Errorf("wal: replaying %d buffered commits: %w", len(pending), err)
		}
		replayed += len(pending)
		clear(pending)
		pending = pending[:0]
		pendingBytes = 0
		return nil
	}
	err := VisitWALEntriesAfterFromDir(wal.config.Dir, 0, func(entry WALEntry) error {
		if entry.Operation == OpEntityCommit {
			// Keep the existing streaming replay owner: an earlier physical
			// batch must finish before this semantic record can inspect its
			// same-commit logical receipt.
			if err := flush(); err != nil {
				return err
			}
			var record walEntityCommitData
			if err := decodeRecoveryPayload(entry.Data, &record); err != nil {
				return err
			}
			envelope, ops, err := DecodeEntityTransaction(record.Payload)
			if err != nil {
				return err
			}
			present, err := b.bufferedFlushMarkerExists(envelope.Namespace, "", envelope.LogicalTxID)
			if err != nil {
				return err
			}
			if present {
				return nil
			}
			// Recovery applies an already accepted record physically, as the
			// existing buffered replay does; it must not emit a fresh ACK or
			// append another buffered record while visiting this WAL.
			envelope.Mode = CommitModeDirect
			receipt, err := submitDecodedEntityTransaction(context.Background(), b, envelope, ops, nil)
			if err != nil {
				return err
			}
			if outcome := receipt.WaitOutcome(context.Background()); outcome.Err != nil {
				return outcome.Err
			}
			replayed++
			return nil
		}
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
		exists, err := b.bufferedFlushMarkerExists(data.Namespace, data.TxID, data.LogicalTxID)
		if err != nil {
			return fmt.Errorf("wal: failed to check flush marker for %q: %w", data.TxID, err)
		}
		if exists {
			return nil // each remaining record supplies its own authority
		}
		c, err := b.importBufferedWALRecord(&data, authority)
		if err != nil {
			return err
		}
		bytes := estimateBufferedCommitBytes(&c)
		if len(pending) > 0 && bytes > bufferedRecoveryBatchBytes-pendingBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		pending = append(pending, c)
		pendingBytes += bytes
		if pendingBytes >= bufferedRecoveryBatchBytes {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if replayed == 0 {
		return 0, err
	}

	// The replayed commits carried no count deltas; rebuild the derived
	// counts from committed entities so readers never observe committed
	// entities without their counts.
	// Completed batches remain durable if a later record fails. Repair their
	// counts as well, while preserving the original recovery failure.
	if countErr := b.ensureLabelCounts(); countErr != nil {
		err = errors.Join(err, fmt.Errorf("wal: rebuild label counts after buffered replay: %w", countErr))
	}
	if countErr := b.ensureEdgeTypeCounts(); countErr != nil {
		err = errors.Join(err, fmt.Errorf("wal: rebuild edge-type counts after buffered replay: %w", countErr))
	}
	return replayed, err
}
