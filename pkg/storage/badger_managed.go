package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// managedBadgerDB is the engine's Badger handle. Badger runs in managed mode
// (badger.OpenManaged): NornicDB assigns every commit timestamp and decides
// which timestamps readers may see, so one transaction can be written as
// several Badger batches and still become visible all at once (#703).
//
// Visibility contract:
//   - Every read (View, transaction snapshots, Backup) reads at the published
//     timestamp: the highest timestamp at or below which every assigned commit
//     has finished. Reads never wait for a commit.
//   - An ordinary commit takes the next timestamp, writes its single Badger
//     batch, and returns once the published timestamp covers it, so the
//     committer always reads its own write. This is the behaviour Badger's own
//     oracle had before managed mode.
//   - A commit larger than one Badger batch (Badger's ErrTxnTooBig limit, 15% of
//     the memtable) holds commitGate exclusively, writes a durable intent
//     record, then writes its batches under consecutive timestamps while the
//     intent's timestamp stays unfinished. The published timestamp therefore
//     stays below all of them until the last batch, which also deletes the
//     intent, is written: readers see none of the commit and then all of it.
//     A failed large commit is undone by rollbackSince; a crash in the middle
//     leaves the intent behind and the next open rolls the commit back.
//
// Managed mode also makes NornicDB responsible for telling Badger which old
// versions compaction may drop and when committed-transaction conflict state
// may be forgotten (SetDiscardTs). The oracle tracks every open read
// timestamp, and advanceDiscardTs never moves past the oldest one.
//
// The methods below shadow the *badger.DB methods that panic or read
// unpublished data in managed mode (View, Update, NewTransaction,
// NewWriteBatch, NewStream, Backup, Load, GetSequence), and DropPrefix, which
// fails concurrent writes. Everything else (Size, Flatten, RunValueLogGC,
// Sync, Close, ...) is Badger's.
type managedBadgerDB struct {
	*badger.DB
	oracle *commitOracle
	// commitGate orders ordinary commits against large commits. An ordinary
	// commit holds it shared only while it assigns its timestamp and writes;
	// a large commit holds it exclusively from its first batch until it has
	// published or rolled back, so no other commit lands between its batches.
	commitGate sync.RWMutex
}

// largeCommitIntentKey marks a large commit in progress. It is written, at
// the timestamp the large commit reserved first, before any of its batches,
// and deleted by its last batch. Finding it at open means a crash interrupted
// a large commit: every version newer than the intent's own is rolled back.
var largeCommitIntentKey = []byte{prefixMVCCMeta, prefixMVCCMetaLargeCommitIntent}

// openManagedBadger opens Badger in managed mode, initialises the commit
// oracle from the highest version on disk (so stores written by earlier
// releases, which let Badger assign timestamps, continue seamlessly), and
// rolls back a large commit that a crash interrupted.
func openManagedBadger(opts badger.Options) (*managedBadgerDB, error) {
	db, err := badger.OpenManaged(opts)
	if err != nil {
		return nil, err
	}
	m := &managedBadgerDB{DB: db, oracle: newCommitOracle(db.MaxVersion())}
	if err := m.recoverInterruptedLargeCommit(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("rolling back interrupted large commit: %w", err)
	}
	return m, nil
}

// recoverInterruptedLargeCommit rolls back a large commit whose intent record
// survived a crash. It runs at open, before any reader or writer exists.
func (m *managedBadgerDB) recoverInterruptedLargeCommit() error {
	txn := m.DB.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	item, err := txn.Get(largeCommitIntentKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return m.rollbackSince(item.Version())
}

// View runs fn in a read-only transaction at the published timestamp.
func (m *managedBadgerDB) View(fn func(txn *badger.Txn) error) error {
	if m.DB.IsClosed() {
		return badger.ErrDBClosed
	}
	readTs, err := m.oracle.beginRead()
	if err != nil {
		return err
	}
	defer m.oracle.endRead(readTs)
	txn := m.DB.NewTransactionAt(readTs, false)
	defer txn.Discard()
	return fn(txn)
}

// viewHeld is View for the loaders that rebuild the engine's in-memory state
// from Badger: at open, before anything else reads, and in Restore, while
// holdReads turns every other read away. It reads past the hold.
func (m *managedBadgerDB) viewHeld(fn func(txn *badger.Txn) error) error {
	if m.DB.IsClosed() {
		return badger.ErrDBClosed
	}
	readTs := m.oracle.beginReadHeld()
	defer m.oracle.endRead(readTs)
	txn := m.DB.NewTransactionAt(readTs, false)
	defer txn.Discard()
	return fn(txn)
}

// heldViewer is viewHeld for the loaders that take a badgerViewer.
type heldViewer struct{ db *managedBadgerDB }

func (v heldViewer) View(fn func(txn *badger.Txn) error) error { return v.db.viewHeld(fn) }

// Update runs fn in a read-write transaction at the published timestamp and
// commits it as one ordinary commit. Badger's per-batch size limit applies.
func (m *managedBadgerDB) Update(fn func(txn *badger.Txn) error) error {
	if m.DB.IsClosed() {
		return badger.ErrDBClosed
	}
	readTs, err := m.oracle.beginRead()
	if err != nil {
		return err
	}
	defer m.oracle.endRead(readTs)
	txn := m.DB.NewTransactionAt(readTs, true)
	defer txn.Discard()
	if err := fn(txn); err != nil {
		return err
	}
	return m.commit(txn)
}

// beginTxn opens a transaction at the published timestamp and registers the
// timestamp as an open read, which keeps compaction from dropping the
// versions it reads. The caller must call endRead(readTs) once it has
// discarded or committed every transaction opened at readTs. While Restore
// holds reads (holdReads), it opens nothing and returns ErrStorageRestoring.
func (m *managedBadgerDB) beginTxn(update bool) (*badger.Txn, uint64, error) {
	readTs, err := m.oracle.beginRead()
	if err != nil {
		return nil, 0, err
	}
	return m.DB.NewTransactionAt(readTs, update), readTs, nil
}

// newTxnAt opens another transaction at a read timestamp the caller already
// holds open with beginTxn.
func (m *managedBadgerDB) newTxnAt(readTs uint64, update bool) *badger.Txn {
	return m.DB.NewTransactionAt(readTs, update)
}

// endRead releases a read timestamp returned by beginTxn.
func (m *managedBadgerDB) endRead(readTs uint64) {
	m.oracle.endRead(readTs)
}

// commit commits txn as one ordinary commit: it takes the next timestamp
// under the shared commit gate, writes, and waits until the published
// timestamp covers the write so the caller reads its own write afterwards.
func (m *managedBadgerDB) commit(txn *badger.Txn) error {
	ts, err := m.commitShared(txn)
	if err != nil {
		return err
	}
	return m.oracle.waitPublished(ts)
}

func (m *managedBadgerDB) commitShared(txn *badger.Txn) (uint64, error) {
	m.commitGate.RLock()
	defer m.commitGate.RUnlock()
	m.advanceDiscardTs()
	ts, err := m.oracle.assign()
	if err != nil {
		return 0, err
	}
	defer m.oracle.finish(ts)
	return ts, txn.CommitAt(ts, nil)
}

// advanceDiscardTs lets compaction drop versions no open read can see any
// more, and lets Badger forget conflict state of commits every open
// transaction already observed. Called on every commit, as Badger's own
// oracle did.
func (m *managedBadgerDB) advanceDiscardTs() {
	if ts, ok := m.oracle.advanceDiscard(); ok {
		m.DB.SetDiscardTs(ts)
	}
}

// DropPrefix drops every key under prefixes. Badger blocks writes while it
// drops and fails them with ErrBlockedWrites, so the drop holds the commit
// gate exclusively and commits wait for it instead. The caller must not hold
// the gate.
func (m *managedBadgerDB) DropPrefix(prefixes ...[]byte) error {
	m.commitGate.Lock()
	defer m.commitGate.Unlock()
	return m.DB.DropPrefix(prefixes...)
}

// NewTransaction is not available on the managed handle: a transaction must
// register its read timestamp. Use beginTxn / endRead.
func (m *managedBadgerDB) NewTransaction(update bool) *badger.Txn {
	panic("storage: use managedBadgerDB.beginTxn; NewTransaction does not register its read timestamp")
}

// NewStream is not available on the managed handle; Backup is the only
// stream user and reads at the published timestamp.
func (m *managedBadgerDB) NewStream() *badger.Stream {
	panic("storage: NewStream is not available on the managed Badger handle")
}

// GetSequence is not supported by Badger in managed mode.
func (m *managedBadgerDB) GetSequence(key []byte, bandwidth uint64) (*badger.Sequence, error) {
	panic("storage: GetSequence is not available on the managed Badger handle")
}

// Backup streams every key visible at the published timestamp, with versions
// newer than since, to w.
func (m *managedBadgerDB) Backup(w io.Writer, since uint64) (uint64, error) {
	readTs, err := m.oracle.beginRead()
	if err != nil {
		return 0, err
	}
	defer m.oracle.endRead(readTs)
	stream := m.DB.NewStreamAt(readTs)
	stream.LogPrefix = "DB.Backup"
	stream.SinceTs = since
	return stream.Backup(w, since)
}

// Load restores a backup written by Backup. Restored entries keep their
// original versions, so the oracle moves past the highest one and publishes
// it; the load holds the commit gate exclusively so no commit overlaps.
func (m *managedBadgerDB) Load(r io.Reader, maxPendingWrites int) error {
	m.commitGate.Lock()
	defer m.commitGate.Unlock()
	if err := m.DB.Load(r, maxPendingWrites); err != nil {
		return err
	}
	m.oracle.advanceTo(m.DB.MaxVersion())
	return nil
}

// batchWriter is the one place that splits writes into Badger batches. Writes
// go into the open batch; when a write does not fit (badger.ErrTxnTooBig) the
// batch is committed, a new one is opened, and the write is retried there.
// Maintenance writes (managedWriteBatch), engine and transaction commits
// (commitWriter), large-commit batches and large-commit rollback all write
// through it and differ only in their batchTarget. Not safe for concurrent
// use.
type batchWriter struct {
	txn *badger.Txn
	// to opens and commits batches. A writer without one writes into txn
	// only: a write that does not fit returns badger.ErrTxnTooBig.
	to batchTarget
}

// batchTarget is where a batchWriter's batches come from and go to.
type batchTarget interface {
	openBatch() *badger.Txn
	commitBatch(txn *badger.Txn) error
}

// write runs op against the open batch, moving to a new batch once if op
// does not fit. op must be repeatable: after it did not fit, it runs again,
// from the start, in the new batch. An op that does not fit an empty batch
// returns badger.ErrTxnTooBig.
func (w *batchWriter) write(op func(txn *badger.Txn) error) error {
	if w.txn == nil {
		w.txn = w.to.openBatch()
	}
	err := op(w.txn)
	if !errors.Is(err, badger.ErrTxnTooBig) || w.to == nil {
		return err
	}
	if err := w.flush(); err != nil {
		return err
	}
	w.txn = w.to.openBatch()
	return op(w.txn)
}

// writeOnce runs op against the open batch only, for an op that cannot be
// repeated: if it does not fit, badger.ErrTxnTooBig is returned. The writer
// must have a batch open (withUpdate starts it on the helper transaction).
func (w *batchWriter) writeOnce(op func(txn *badger.Txn) error) error {
	return op(w.txn)
}

// flush commits the open batch, if any. The writer stays usable.
func (w *batchWriter) flush() error {
	if w.txn == nil {
		return nil
	}
	txn := w.txn
	w.txn = nil
	defer txn.Discard()
	return w.to.commitBatch(txn)
}

// discard drops the open batch without committing it.
func (w *batchWriter) discard() {
	if w.txn != nil {
		w.txn.Discard()
		w.txn = nil
	}
}

// openBatch opens a maintenance batch. Its writes are blind (it reads
// nothing), so its read timestamp needs no registration.
func (m *managedBadgerDB) openBatch() *badger.Txn {
	return m.DB.NewTransactionAt(m.oracle.published.Load(), true)
}

// commitBatch commits a maintenance batch as an ordinary commit.
func (m *managedBadgerDB) commitBatch(txn *badger.Txn) error {
	return m.commit(txn)
}

// NewWriteBatch returns a batched writer for bulk maintenance writes (index
// rebuilds, namespace drops). See managedWriteBatch.
func (m *managedBadgerDB) NewWriteBatch() *managedWriteBatch {
	return &managedWriteBatch{w: batchWriter{to: m}}
}

// managedWriteBatch replaces badger.WriteBatch, which managed mode only
// offers at one fixed timestamp. Each full batch is committed as an ordinary
// commit, so it becomes visible when written, as with Badger's WriteBatch.
// Unlike Badger's WriteBatch, Flush commits what is pending and leaves the
// writer usable, so callers may flush periodically (#819: DROP DATABASE
// flushed every 50,000 keys and kept writing to the finished Badger batch,
// failing with "This transaction has been discarded" and leaving the
// database half-dropped). Not safe for concurrent use.
type managedWriteBatch struct {
	w batchWriter
}

// Set writes key = value.
func (wb *managedWriteBatch) Set(key, value []byte) error {
	return wb.w.write(func(txn *badger.Txn) error { return txn.Set(key, value) })
}

// Delete deletes key.
func (wb *managedWriteBatch) Delete(key []byte) error {
	return wb.w.write(func(txn *badger.Txn) error { return txn.Delete(key) })
}

// Flush commits the pending writes. The writer stays usable.
func (wb *managedWriteBatch) Flush() error { return wb.w.flush() }

// Cancel discards writes not yet flushed.
func (wb *managedWriteBatch) Cancel() { wb.w.discard() }

// largeCommit writes one transaction that is larger than a single Badger
// batch. beginLargeCommit takes the commit gate exclusively; finish or abort
// releases it. Not safe for concurrent use.
type largeCommit struct {
	db *managedBadgerDB
	// intentTs is the timestamp the intent record was written at. It stays
	// unfinished in the oracle until the commit publishes or rolls back,
	// which holds the published timestamp below every batch.
	intentTs uint64
	// lastTs is the timestamp of the latest written batch. The next batch
	// reads at it, so the transaction sees its own earlier batches; no other
	// commit can be newer while the gate is held.
	lastTs uint64
	// batches counts the batches written, the intent record excluded.
	batches int
}

// beginLargeCommit takes the commit gate exclusively and durably records the
// intent of a large commit.
func (m *managedBadgerDB) beginLargeCommit() (*largeCommit, error) {
	m.commitGate.Lock()
	intentTs, err := m.oracle.assign()
	if err != nil {
		m.commitGate.Unlock()
		return nil, err
	}
	lc := &largeCommit{db: m, intentTs: intentTs, lastTs: intentTs}
	txn := m.DB.NewTransactionAt(intentTs-1, true)
	defer txn.Discard()
	var val [8]byte
	binary.BigEndian.PutUint64(val[:], intentTs)
	err = txn.Set(largeCommitIntentKey, val[:])
	if err == nil {
		err = txn.CommitAt(intentTs, nil)
	}
	if err == nil {
		// The intent must be durable before any batch is.
		err = m.syncIfPersistent()
	}
	if err != nil {
		if abortErr := lc.abort(); abortErr != nil {
			err = fmt.Errorf("%w (rolling back: %v)", err, abortErr)
		}
		return nil, err
	}
	return lc, nil
}

// openBatch opens the transaction for the next batch. It reads at the latest
// written batch, so it sees every earlier batch of this commit; no other
// commit can be newer while the gate is held.
func (lc *largeCommit) openBatch() *badger.Txn {
	return lc.db.DB.NewTransactionAt(lc.lastTs, true)
}

// commitBatch writes txn as the next batch of the large commit. A conflict
// on the first batch comes back as badger.ErrConflict.
func (lc *largeCommit) commitBatch(txn *badger.Txn) error {
	ts, err := lc.db.oracle.assign()
	if err != nil {
		return err
	}
	// intentTs is still unfinished, so finishing ts publishes nothing.
	lc.db.oracle.finish(ts)
	if err := txn.CommitAt(ts, nil); err != nil {
		return err
	}
	lc.lastTs = ts
	lc.batches++
	return runLargeCommitBatchHook(lc.batches)
}

// finish writes the last batch together with the intent's deletion,
// publishes the whole commit and releases the gate. On error the caller must
// abort.
func (lc *largeCommit) finish(w *batchWriter) error {
	// Every earlier batch must be durable before the intent's deletion is.
	err := lc.db.syncIfPersistent()
	if err == nil {
		err = w.write(func(txn *badger.Txn) error { return txn.Delete(largeCommitIntentKey) })
	}
	if err == nil {
		err = w.flush()
	}
	if err != nil {
		return err
	}
	lc.release()
	return nil
}

// abort rolls back every batch written so far and releases the gate; it is
// called at most once, instead of finish. If the rollback itself fails, the
// intent stays and the oracle refuses further commits until restart, where
// open completes the rollback; reads continue at the last published
// timestamp, at which none of the batches are visible.
func (lc *largeCommit) abort() error {
	// A closed DB panics inside Badger; the gate must be released either way.
	if err := recoverBadgerClosedPanic(func() error { return lc.db.rollbackSince(lc.intentTs) }); err != nil {
		lc.db.oracle.fail(err)
		lc.db.commitGate.Unlock()
		return err
	}
	lc.release()
	return nil
}

func (lc *largeCommit) release() {
	lc.db.oracle.finish(lc.intentTs)
	lc.db.commitGate.Unlock()
}

// rollbackSince restores every key that has a version newer than sinceTs to
// its value at sinceTs, and deletes the large-commit intent, all at one new
// timestamp. Only a large commit writes after its intent while it holds the
// commit gate (and no one writes at open), so this undoes exactly that
// commit. It is idempotent: a crash part-way leaves the intent, and repeating
// the rollback restores the same values.
//
// Property-key dictionary entries are not rolled back. A token belongs to
// the engine-wide in-memory dictionary from the moment it is allocated, and
// the dictionary keeps it when the commit that allocated it fails, so its
// persisted copy must stay too; an unused token is harmless (see
// propertyKeyDictionary).
func (m *managedBadgerDB) rollbackSince(sinceTs uint64) error {
	restoreTs, err := m.oracle.assign()
	if err != nil {
		return err
	}
	defer m.oracle.finish(restoreTs)

	prior := m.DB.NewTransactionAt(sinceTs, false)
	defer prior.Discard()
	scan := m.DB.NewTransactionAt(math.MaxUint64, false)
	defer scan.Discard()
	it := scan.NewIterator(badger.IteratorOptions{AllVersions: true, SinceTs: sinceTs})
	defer it.Close()

	// Every restore batch is written at restoreTs, which is published only
	// after the intent's deletion, in the last batch, is written.
	w := &batchWriter{to: &restoreBatches{db: m, sinceTs: sinceTs, restoreTs: restoreTs}}
	defer w.discard()

	var last []byte
	for it.Rewind(); it.Valid(); it.Next() {
		key := it.Item().Key()
		if last != nil && bytes.Equal(key, last) {
			continue
		}
		last = append(last[:0], key...)
		if bytes.Equal(last, largeCommitIntentKey) || isPropertyKeyDictionaryKey(last) {
			continue
		}
		restore, err := restoreWrite(prior, last)
		if err == nil {
			err = w.write(restore)
		}
		if err != nil {
			return err
		}
	}
	err = m.syncIfPersistent()
	if err == nil {
		err = w.write(func(txn *badger.Txn) error { return txn.Delete(largeCommitIntentKey) })
	}
	if err == nil {
		err = w.flush()
	}
	return err
}

// restoreWrite returns the write that puts key back to its state in prior:
// its value there, or a deletion when it did not exist there.
func restoreWrite(prior *badger.Txn, key []byte) (func(txn *badger.Txn) error, error) {
	key = append([]byte(nil), key...)
	item, err := prior.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return func(txn *badger.Txn) error { return txn.Delete(key) }, nil
	}
	var value []byte
	if err == nil {
		value, err = item.ValueCopy(nil)
	}
	if err != nil {
		return nil, err
	}
	e := badger.NewEntry(key, value).WithMeta(item.UserMeta())
	e.ExpiresAt = item.ExpiresAt()
	return func(txn *badger.Txn) error { return txn.SetEntry(e) }, nil
}

func (m *managedBadgerDB) syncIfPersistent() error {
	if m.DB.Opts().InMemory {
		return nil
	}
	return m.DB.Sync()
}

// commitOracle assigns commit timestamps and tracks which are published and
// which are still read. See managedBadgerDB for the contract.
type commitOracle struct {
	// published is the highest timestamp at or below which every assigned
	// commit has finished; reads use it.
	published atomic.Uint64

	mu        sync.Mutex
	changed   sync.Cond // broadcast when published advances or the oracle fails
	nextTs    uint64
	inflight  map[uint64]struct{}
	readers   map[uint64]int
	discardTs uint64
	failed    error
	// readsHeld turns new reads away while Restore replaces the store
	// (holdReads); changed is broadcast when the last open read ends.
	readsHeld bool
}

func newCommitOracle(maxVersion uint64) *commitOracle {
	o := &commitOracle{
		nextTs:   maxVersion + 1,
		inflight: make(map[uint64]struct{}),
		readers:  make(map[uint64]int),
	}
	o.changed.L = &o.mu
	o.published.Store(maxVersion)
	return o
}

// beginRead returns the published timestamp and registers it as read. While
// reads are held (holdReads) it registers nothing and returns
// ErrStorageRestoring: a transient error, returned at once rather than after
// a wait, so a read nested in one that is already open can't deadlock the
// restore that waits for the outer one.
func (o *commitOracle) beginRead() (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readsHeld {
		return 0, localizedError(localization.StorageClientRestoring(), ErrStorageRestoring)
	}
	return o.registerReadLocked(), nil
}

// beginReadHeld is beginRead for viewHeld: it reads past a hold.
func (o *commitOracle) beginReadHeld() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.registerReadLocked()
}

func (o *commitOracle) registerReadLocked() uint64 {
	ts := o.published.Load()
	o.readers[ts]++
	return ts
}

func (o *commitOracle) endRead(ts uint64) {
	o.mu.Lock()
	if n := o.readers[ts]; n <= 1 {
		delete(o.readers, ts)
	} else {
		o.readers[ts] = n - 1
	}
	if o.readsHeld && len(o.readers) == 0 {
		o.changed.Broadcast()
	}
	o.mu.Unlock()
}

// holdReads turns new reads away (beginRead returns ErrStorageRestoring) and
// waits up to timeout for the open ones to end: views, engine writes and
// open transactions. It reports whether they did; when they didn't, reads
// are let in again.
func (o *commitOracle) holdReads(timeout time.Duration) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.readsHeld = true
	deadline := time.Now().Add(timeout)
	wake := time.AfterFunc(timeout, func() {
		o.mu.Lock()
		o.changed.Broadcast()
		o.mu.Unlock()
	})
	defer wake.Stop()
	for len(o.readers) > 0 {
		if !time.Now().Before(deadline) {
			o.readsHeld = false
			return false
		}
		o.changed.Wait()
	}
	return true
}

// releaseReads lets reads in again after holdReads.
func (o *commitOracle) releaseReads() {
	o.mu.Lock()
	o.readsHeld = false
	o.mu.Unlock()
}

// assign returns the next commit timestamp, unfinished until finish.
func (o *commitOracle) assign() (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failed != nil {
		return 0, o.failed
	}
	ts := o.nextTs
	o.nextTs++
	o.inflight[ts] = struct{}{}
	return ts, nil
}

// finish marks ts written (or abandoned) and advances published as far as
// every lower timestamp is finished.
func (o *commitOracle) finish(ts uint64) {
	o.mu.Lock()
	delete(o.inflight, ts)
	published := o.nextTs - 1
	for pending := range o.inflight {
		if pending-1 < published {
			published = pending - 1
		}
	}
	if published > o.published.Load() {
		o.published.Store(published)
		o.changed.Broadcast()
	}
	o.mu.Unlock()
}

// advanceTo moves the oracle past ts (restored data) and publishes it.
func (o *commitOracle) advanceTo(ts uint64) {
	o.mu.Lock()
	if ts >= o.nextTs {
		o.nextTs = ts + 1
	}
	if len(o.inflight) == 0 && o.nextTs-1 > o.published.Load() {
		o.published.Store(o.nextTs - 1)
		o.changed.Broadcast()
	}
	o.mu.Unlock()
}

// waitPublished waits until ts is published, so a committer reads its own
// write. It only waits behind commits that were assigned an earlier
// timestamp and are still writing.
func (o *commitOracle) waitPublished(ts uint64) error {
	if o.published.Load() >= ts {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for o.published.Load() < ts && o.failed == nil {
		o.changed.Wait()
	}
	if o.published.Load() < ts {
		return o.failed
	}
	return nil
}

// advanceDiscard returns a new discard timestamp when the oldest open read
// (or, with none open, the published timestamp) has moved forward.
func (o *commitOracle) advanceDiscard() (uint64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	ts := o.published.Load()
	for r := range o.readers {
		if r < ts {
			ts = r
		}
	}
	if ts <= o.discardTs {
		return 0, false
	}
	o.discardTs = ts
	return ts, true
}

// fail stops all further commits after a large commit could be neither
// completed nor rolled back. Reads continue at the last published timestamp.
func (o *commitOracle) fail(cause error) {
	o.mu.Lock()
	if o.failed == nil {
		o.failed = localizedError(localization.StorageTransactionLargeCommitUnrecoverable(cause), cause)
	}
	o.changed.Broadcast()
	o.mu.Unlock()
}

// badgerKV is the transaction surface shared by the engine's managed handle
// and a plain *badger.DB opened by offline tools and tests: helpers that only
// run whole View / Update transactions accept either.
type badgerKV interface {
	View(fn func(txn *badger.Txn) error) error
	Update(fn func(txn *badger.Txn) error) error
}

// badgerViewer is the read side of badgerKV, for the loaders.
type badgerViewer interface {
	View(fn func(txn *badger.Txn) error) error
}

// failure returns the error that stopped commits, if any (see fail).
func (o *commitOracle) failure() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.failed
}

// restoreBatches is the batchTarget of rollbackSince: every batch is written
// at the one restore timestamp.
type restoreBatches struct {
	db                 *managedBadgerDB
	sinceTs, restoreTs uint64
}

func (r *restoreBatches) openBatch() *badger.Txn {
	return r.db.DB.NewTransactionAt(r.sinceTs, true)
}

func (r *restoreBatches) commitBatch(txn *badger.Txn) error {
	return txn.CommitAt(r.restoreTs, nil)
}

// largeCommitBatchHook lets storage tests observe the store between the
// batches of a large commit and fail the commit there. Nil in production.
var (
	largeCommitBatchHook   func(batch int) error
	largeCommitBatchHookMu sync.RWMutex
)

// setLargeCommitBatchHook installs hook and returns a restore func.
func setLargeCommitBatchHook(hook func(batch int) error) func() {
	largeCommitBatchHookMu.Lock()
	previous := largeCommitBatchHook
	largeCommitBatchHook = hook
	largeCommitBatchHookMu.Unlock()
	return func() {
		largeCommitBatchHookMu.Lock()
		largeCommitBatchHook = previous
		largeCommitBatchHookMu.Unlock()
	}
}

func runLargeCommitBatchHook(batch int) error {
	largeCommitBatchHookMu.RLock()
	hook := largeCommitBatchHook
	largeCommitBatchHookMu.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(batch)
}
