package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const mvccSequenceBlockSize = 1024

func (s *namespaceMVCCState) sequenceBlockExhausted() bool {
	through := s.reservedThrough.Load()
	return through != maxMVCCCommitSequence && s.seq.Load() >= through
}

func (b *BadgerEngine) allocateNamespaceMVCCVersion(ctx context.Context, txn *badger.Txn, namespace string, stamp time.Time, historical bool) (MVCCVersion, error) {
	state, err := b.namespaceMVCC(namespace)
	if err != nil {
		return MVCCVersion{}, err
	}
	for {
		version, issued, err := b.mvccReservations.reserve(txn, namespace, state, stamp, historical)
		if issued {
			return version, err
		}
		if err := b.ensureMVCCSequenceBlock(ctx, state, state.seq.Load()); err != nil {
			return MVCCVersion{}, err
		}
	}
}

// ensureMVCCSequenceBlock synchronizes a ceiling before making its numbers
// available to the allocator. The refill mutex is namespace local and is
// separate from the ledger lock used to take snapshot cuts.
func (b *BadgerEngine) ensureMVCCSequenceBlock(ctx context.Context, state *namespaceMVCCState, floor uint64) error {
	if err := state.refillMu.LockContext(ctx); err != nil {
		return err
	}
	defer state.refillMu.Unlock()
	if state.reservedThrough.Load() == maxMVCCCommitSequence || state.reservedThrough.Load() > floor {
		return nil
	}
	through := floor + min(uint64(mvccSequenceBlockSize), maxMVCCCommitSequence-floor)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], through)
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.work.Leave()
	txn := db.openBatch()
	defer txn.Discard()
	if err := txn.Set(state.persistKey, encoded[:]); err != nil {
		return err
	}
	if err := db.commit(ctx, txn, &commitPublication{metadata: true, syncMetadata: true}); err != nil {
		return fmt.Errorf("reserve durable MVCC sequence block: %w", err)
	}
	state.reservedThrough.Store(through)
	return nil
}

func (b *BadgerEngine) prepareRecoveredMVCCVersion(ctx context.Context, namespace string, version MVCCVersion) error {
	state, err := b.namespaceMVCC(namespace)
	if err != nil {
		return err
	}
	// Older binaries could acknowledge a WAL record without a durable
	// sequence ceiling. Guarantee the replayed version survives WAL retirement.
	if version.CommitSequence > state.reservedThrough.Load() {
		if err := b.ensureMVCCSequenceBlock(ctx, state, version.CommitSequence); err != nil {
			return err
		}
	}
	for current := state.seq.Load(); current < version.CommitSequence; current = state.seq.Load() {
		if state.seq.CompareAndSwap(current, version.CommitSequence) {
			break
		}
	}
	_, err = state.reserveCommitTimestamp(version.CommitTimestamp, false)
	return err
}

func (m *managedBadgerDB) syncMVCCReservation() error {
	if err := m.syncIfPersistent(); err != nil {
		return err
	}
	options := m.DB.Opts()
	if options.InMemory {
		return nil
	}
	// A refill can create a new memtable WAL. Badger synchronizes the file
	// contents, so also make its directory entry durable before issuing IDs.
	return syncDir(options.Dir)
}
