package storage

import (
	"encoding/binary"
	"errors"
	"slices"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func (tx *BadgerTransaction) coldNamespaceSnapshot() (MVCCVersion, error) {
	read := MVCCVersion{CommitTimestamp: tx.StartTime.UTC(), CommitSequence: tx.engine.mvccLegacyGlobalSeed}
	if tx.snapshotTx == nil {
		return read, nil
	}
	// Hydrate the engine for later snapshots, without borrowing its current
	// floor for this transaction. No admission metadata is held over disk IO.
	if _, err := tx.engine.namespaceMVCC(tx.namespace); err != nil {
		return read, err
	}
	item, err := tx.snapshotTx.Get(mvccNamespaceSequenceKey(tx.namespace))
	if err == nil {
		err = item.Value(func(v []byte) error {
			if len(v) != 8 {
				return ErrInvalidData
			}
			read.CommitSequence = max(read.CommitSequence, binary.BigEndian.Uint64(v))
			return nil
		})
	}
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return read, err
	}
	seq, stamp, err := tx.engine.recoverNamespaceMVCCFloorAt(tx.namespace, func(visit func(*badger.Txn) error) error { return visit(tx.snapshotTx) })
	if err != nil {
		return read, err
	}
	read.CommitSequence = max(read.CommitSequence, seq)
	if stamp > read.CommitTimestamp.UnixNano() {
		read.CommitTimestamp = time.Unix(0, stamp).UTC()
	}
	return read, nil
}

// Every writer uses the admission frontier when minting a version. A logical
// sequence allocated before a snapshot is not visible until its physical
// publication. Entity stripes allow unrelated publications to run in parallel.
func (b *BadgerEngine) allocateMVCCVersion(txn *badger.Txn, namespace string, at time.Time) (MVCCVersion, error) {
	w := b.writeBehind
	if w == nil {
		return b.allocateMVCCVersionRaw(txn, namespace, at)
	}
	// Namespace hydration may read disk; never do it under admission metadata.
	if _, err := b.namespaceMVCC(namespace); err != nil {
		return MVCCVersion{}, err
	}
	w.admitMu.Lock()
	defer w.admitMu.Unlock()
	v, err := b.allocateMVCCVersionRaw(txn, namespace, at)
	if err != nil {
		return v, err
	}
	if w.inflight == nil {
		w.inflight = make(map[string][]MVCCVersion)
	}
	if w.synchronousVersions == nil {
		w.synchronousVersions = make(map[*badger.Txn][]bufferedReservation)
	}
	w.inflightCount.Add(1)
	w.inflight[namespace] = append(w.inflight[namespace], v)
	w.synchronousVersions[txn] = append(w.synchronousVersions[txn], bufferedReservation{namespace: namespace, version: v})
	return v, nil
}

func (b *BadgerEngine) releaseCommitVersions(txn *badger.Txn) {
	w := b.writeBehind
	if w == nil {
		return
	}
	w.admitMu.Lock()
	for _, r := range w.synchronousVersions[txn] {
		w.releaseReservationLocked(r)
		w.uncountInflight(r)
	}
	delete(w.synchronousVersions, txn)
	w.admitMu.Unlock()
}

func (cw *commitWriter) releaseVersions() {
	if cw.versionTxn != nil {
		cw.engine.moveCommitVersions(cw.txn, cw.versionTxn)
		cw.engine.releaseCommitVersions(cw.versionTxn)
		cw.versionTxn = nil
	}
}

func (b *BadgerEngine) moveCommitVersions(from, to *badger.Txn) {
	w := b.writeBehind
	if w == nil || from == to {
		return
	}
	w.admitMu.Lock()
	if pending := w.synchronousVersions[from]; len(pending) > 0 {
		w.synchronousVersions[to] = append(w.synchronousVersions[to], pending...)
		delete(w.synchronousVersions, from)
	}
	w.admitMu.Unlock()
}

func (w *WriteBehindBuffer) captureInflightLocked() map[string][]MVCCVersion {
	var out map[string][]MVCCVersion
	for namespace, versions := range w.inflight {
		if len(versions) == 0 {
			continue
		}
		if out == nil {
			out = make(map[string][]MVCCVersion)
		}
		out[namespace] = slices.Clone(versions)
	}
	return out
}

// The epoch closes the interval between opening the physical view and
// copying logical versions. A publication in that interval retries the view;
// no snapshot holds admission metadata while waiting on a storage gate.
func (b *BadgerEngine) captureCommitSnapshot(db *managedBadgerDB) (*badger.Txn, uint64, MVCCVersion, map[string]MVCCVersion, map[string][]MVCCVersion, error) {
	for {
		w := b.writeBehind
		var epoch uint64
		if w != nil {
			epoch = w.publicationEpoch.Load()
		}
		snapshot, physical, err := db.beginTxn(false)
		if err != nil {
			return nil, 0, MVCCVersion{}, nil, nil, err
		}
		if w != nil {
			w.admitMu.Lock()
			if epoch != w.publicationEpoch.Load() {
				w.admitMu.Unlock()
				snapshot.Discard()
				db.endRead(physical)
				continue
			}
		}
		logical := b.currentMVCCReadVersion("")
		namespaces := b.snapshotNamespaceVersions()
		var inflight map[string][]MVCCVersion
		if w != nil {
			inflight = w.captureInflightLocked()
			w.admitMu.Unlock()
		}
		return snapshot, physical, logical, namespaces, inflight, nil
	}
}
