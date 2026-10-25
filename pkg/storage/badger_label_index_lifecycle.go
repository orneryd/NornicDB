package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// Label-index generation lifecycle.
//
// labelIndexLifecycle is the single owner of label-index generations: the
// persisted marker, the in-process readiness cache and the retire point. No
// other code writes them. Each caller uses one typed transition:
//
//   - beginLabelIndexRebuild / completeLabelIndexGeneration: the only full
//     generation producer (rebuildLabelIndex). It rewrites every label key, so
//     it alone waits for the readers of the generation it retires and moves
//     the retire point.
//   - beginLabelIndexReset / labelIndexReset.finish: the marker-only
//     transition of a namespace drop. Its deletes are ordinary versioned
//     writes that older snapshots keep, so it has no path to the retention
//     wait or the retire point.
//   - markEmptyLabelIndexReady: an empty store needs no rebuild.
//   - labelIndexStoreReplaced: Restore or reopen replaced the store; no old
//     reader remains and the persisted marker is read again.
//
// Readers resolve IDs only through structures that cannot change under a
// registered snapshot, so no reader checks whether the shared dictionary is
// still valid for it:
//
//   - number -> ID (labelIndexNodeID): the live reverse map plus the retired
//     set. A drop never reassigns a number, and it copies the entries it
//     removes into the retired set first; a reclaimer frees them only after
//     every reader that began before the drop has ended.
//   - ID -> number (labelIndexNodeNumInTxn): the reader's own snapshot. The
//     live forward map changes when an ID is dropped and recreated, so
//     readers never consult it.
//   - materialization: a reader hands each candidate on as a nodeRef, the ID
//     with its snapshot number, and MVCC head and history lookups for it take
//     that number. Only writers and readers outside the label index derive a
//     nodeRef from the live dictionary (liveNodeRef).
//
// Readers are admitted from their own snapshot (labelIndexAdmissionInTxn): a
// reader either sees the ready marker with the complete generation, or scans
// stored node bodies.

var labelIndexReadyKey = []byte{prefixMVCCMeta, prefixMVCCMetaLabelIndexReady}

const (
	labelIndexSchemeVersion  = 3
	labelIndexMarkerReady    = byte(1)
	labelIndexMarkerBuilding = byte(2)
	labelIndexMarkerSize     = 1 + 1 + 8
)

const (
	labelIndexReadinessUnknown uint32 = iota
	labelIndexReadinessNotReady
	labelIndexReadinessReady
)

// labelIndexLifecycle holds the generation state. epoch is an in-process
// invalidation epoch; markerGeneration is the persisted build generation
// captured from the same MVCC snapshot as the index entries. A cached ready
// proof is usable only at or after its marker version (readyAt) and for the
// current epoch. retireBeforeTs is the building-marker version of the last
// full rebuild: readers older than it may still hold the generation the next
// rebuild retires. retired holds the reverse dictionary entries that drops
// removed while older readers may still resolve them.
type labelIndexLifecycle struct {
	readyAt          atomic.Uint64
	epoch            atomic.Uint64
	markerGeneration atomic.Uint64
	retireBeforeTs   atomic.Uint64
	readinessEpoch   atomic.Uint64
	state            atomic.Uint32
	mu               sync.Mutex
	retired          retiredLabelNodeIDs
	// beforeRetireWait lets tests observe the retention bound before a
	// rebuild waits for old readers to drain.
	beforeRetireWait func()
	// Test seams. beforeAdmission and afterAdmission bracket a reader's
	// admission on its snapshot; beforeForwardLookup and beforeReverseLookup
	// fire right before it resolves an ID or a number.
	beforeAdmission     atomic.Pointer[func()]
	afterAdmission      atomic.Pointer[func()]
	beforeForwardLookup atomic.Pointer[func()]
	beforeReverseLookup atomic.Pointer[func()]
}

// retiredLabelNodeIDs keeps reverse entries removed by drops, tagged by drop
// batch, until the readers that began before that drop have ended. Its
// reclaimers run under one context; stopped is set once, by Close, under mu,
// so no reclaimer can start after stop cancels and joins them.
type retiredLabelNodeIDs struct {
	mu      sync.RWMutex
	ids     map[uint64]retiredLabelNodeID
	batch   uint64
	ctx     context.Context
	cancel  context.CancelFunc
	stopped bool
	waiters sync.WaitGroup
}

type retiredLabelNodeID struct {
	id    NodeID
	batch uint64
}

func (r *retiredLabelNodeIDs) lookup(num uint64) (NodeID, bool) {
	r.mu.RLock()
	entry, ok := r.ids[num]
	r.mu.RUnlock()
	return entry.id, ok
}

// add records a drop's entries and returns its batch, plus the context its
// reclaimer must run under. After stop it still records the entries, so a
// drop racing Close never hides a number from an open reader, but returns no
// context: nothing is left to reclaim them except the closed engine itself.
func (r *retiredLabelNodeIDs) add(entries map[uint64]NodeID) (uint64, context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batch++
	if r.ids == nil {
		r.ids = make(map[uint64]retiredLabelNodeID, len(entries))
	}
	for num, id := range entries {
		r.ids[num] = retiredLabelNodeID{id: id, batch: r.batch}
	}
	if r.stopped {
		return r.batch, nil
	}
	if r.ctx == nil {
		r.ctx, r.cancel = context.WithCancel(context.Background())
	}
	r.waiters.Add(1)
	return r.batch, r.ctx
}

// release frees one batch's entries; a later batch that re-retired the same
// number keeps it.
func (r *retiredLabelNodeIDs) release(batch uint64) {
	r.mu.Lock()
	for num, entry := range r.ids {
		if entry.batch == batch {
			delete(r.ids, num)
		}
	}
	r.mu.Unlock()
}

// clear forgets every batch. Store replacement calls it once no old reader
// remains; reclaimers still running only release their own batches.
func (r *retiredLabelNodeIDs) clear() {
	r.mu.Lock()
	r.ids = nil
	r.mu.Unlock()
}

// stop cancels and joins the reclaimers, and refuses new ones. Close calls
// it. Every reclaimer registered before stopped was set is cancelled here;
// none can register after it, so Wait cannot race a later Add.
func (r *retiredLabelNodeIDs) stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.stopped = true
	r.ctx, r.cancel = nil, nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.waiters.Wait()
}

// beginLabelIndexRebuild starts a full generation. It first waits for the
// readers that can still see the generation before the previous full
// rebuild, so at most two generations are retained, then writes the building
// marker and makes it the new retire point.
func (b *BadgerEngine) beginLabelIndexRebuild(ctx context.Context) (uint64, error) {
	db, err := b.beginHelperTxnWithAdmission(true)
	if err != nil {
		return 0, err
	}
	defer b.txnWG.Done()
	if _, err := b.labelIndexReady(); err != nil {
		return 0, err
	}
	if retireBefore := b.labelIndex.retireBeforeTs.Load(); retireBefore != 0 {
		if hook := b.labelIndex.beforeRetireWait; hook != nil {
			hook()
		}
		if err := db.oracle.waitForReadersBefore(ctx, retireBefore); err != nil {
			return 0, err
		}
		db.advanceDiscardTs()
	}
	generation := b.labelIndex.markerGeneration.Load() + 1
	markerAt, err := b.writeLabelIndexBuildingMarker(generation)
	if err != nil {
		return 0, fmt.Errorf("mark label index build: %w", err)
	}
	b.labelIndex.retireBeforeTs.Store(markerAt)
	return generation, nil
}

// labelIndexReset is a namespace drop's marker-only transition. The drop
// deletes its own label keys with versioned writes and older snapshots keep
// them, so the reset neither waits for readers nor retires a generation.
type labelIndexReset struct {
	// Recovery owns an offline lifecycle: Open/Restore schedule the rebuild.
	offline    bool
	b          *BadgerEngine
	started    bool
	wasReady   bool
	generation uint64
}

// beginLabelIndexReset stops a running rebuild, whose markers would race the
// reset's. Call finish exactly once, whatever happens.
func (b *BadgerEngine) beginLabelIndexReset() *labelIndexReset {
	b.stopIndexBackfill(startupLabelIndex)
	maintenancePause(pauseIndexBackfillRecovery)
	return &labelIndexReset{b: b}
}

// markBuilding gives the drop its own generation, so readers admitted while
// it deletes index keys scan stored bodies instead.
func (r *labelIndexReset) markBuilding() error {
	if r.offline {
		return nil
	}
	ready, err := r.b.labelIndexReady()
	if err != nil {
		return err
	}
	generation := r.b.labelIndex.markerGeneration.Load() + 1
	if _, err := r.b.writeLabelIndexBuildingMarker(generation); err != nil {
		return err
	}
	r.started, r.wasReady, r.generation = true, ready, generation
	return nil
}

// retireNodeDictionaryEntries removes the drop's node IDs from the in-memory
// dictionary. Their reverse entries move to the retired set first, and stay
// there until every reader that began before the drop's deletes were
// published has ended, so those readers keep resolving the numbers in their
// snapshot.
func (r *labelIndexReset) retireNodeDictionaryEntries(prefix string) error {
	b := r.b
	victims := make(map[uint64]NodeID)
	for i := range b.idDict.nodes {
		sh := &b.idDict.nodes[i]
		sh.mu.RLock()
		for id, num := range sh.forward {
			if strings.HasPrefix(string(id), prefix) {
				victims[num] = id
			}
		}
		sh.mu.RUnlock()
	}
	if len(victims) == 0 {
		return nil
	}
	// Open/Restore have drained every old reader. Public DROP retains reverse
	// identities until its admitted readers release their snapshots.
	var db *managedBadgerDB
	var batch uint64
	var ctx context.Context
	if !r.offline {
		var err error
		db, err = b.beginHelperTxn()
		if err != nil {
			return err
		}
		batch, ctx = b.labelIndex.retired.add(victims)
	}
	for _, id := range victims {
		b.idDict.nodePairRemoveByID(id)
	}
	if r.offline {
		return nil
	}
	retireTs := db.oracle.published.Load()
	if ctx == nil {
		b.txnWG.Done()
		return nil
	}
	oracle := db.oracle
	go func() {
		defer b.txnWG.Done()
		defer b.labelIndex.retired.waiters.Done()
		if oracle.waitForReadersBefore(ctx, retireTs) == nil {
			b.labelIndex.retired.release(batch)
		}
	}()
	return nil
}

// labelIndexNodeID resolves a number read from a label-index snapshot. The
// live reverse map and the retired set together hold the ID of every number
// any registered snapshot can contain.
func (b *BadgerEngine) labelIndexNodeID(num uint64) (NodeID, bool) {
	if hook := b.labelIndex.beforeReverseLookup.Load(); hook != nil {
		(*hook)()
	}
	if id, ok := b.idDict.lookupNodeIDByNum(num); ok {
		return id, true
	}
	return b.labelIndex.retired.lookup(num)
}

// nodeRef is a node ID with the numeric identity that keys its MVCC head,
// history and index entries.
type nodeRef struct {
	id  NodeID
	num uint64
}

// liveNodeRef numbers id through the live dictionary: right for writers and
// for reads at the current head, wrong for an older snapshot across a drop.
func (b *BadgerEngine) liveNodeRef(id NodeID) (nodeRef, bool) {
	num, ok := b.idDict.lookupNodeNumID(id)
	return nodeRef{id: id, num: num}, ok
}

// snapshotNodeRef numbers id through the reader's own snapshot.
func (b *BadgerEngine) snapshotNodeRef(txn *badger.Txn, id NodeID) (nodeRef, bool, error) {
	num, ok, err := b.labelIndexNodeNumInTxn(txn, id)
	return nodeRef{id: id, num: num}, ok, err
}

// labelIndexNodeNumInTxn resolves an ID through the reader's own snapshot:
// the persisted forward dictionary is written with the index entries, and
// unlike the live map it cannot change when the ID is dropped and recreated.
func (b *BadgerEngine) labelIndexNodeNumInTxn(txn *badger.Txn, id NodeID) (uint64, bool, error) {
	if hook := b.labelIndex.beforeForwardLookup.Load(); hook != nil {
		(*hook)()
	}
	item, err := txn.Get(nodeIDForwardKey(id))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var num uint64
	err = item.Value(func(value []byte) error {
		if len(value) != 8 {
			return ErrInvalidData
		}
		num = binary.BigEndian.Uint64(value)
		return nil
	})
	return num, err == nil, err
}

// finish publishes the reset generation as ready when the index was ready
// and the drop succeeded; otherwise it schedules the rebuild the store needs.
func (r *labelIndexReset) finish(err error) error {
	if r.offline {
		return err
	}
	if r.started && err == nil && r.wasReady {
		return r.b.completeLabelIndexGeneration(r.generation)
	}
	return errors.Join(err, r.b.resumeIndexBackfill(startupLabelIndex))
}

// markEmptyLabelIndexReady records that an empty store needs no rebuild.
func (b *BadgerEngine) markEmptyLabelIndexReady() error {
	return b.completeLabelIndexGeneration(b.labelIndex.markerGeneration.Load() + 1)
}

// labelIndexStoreReplaced resets the lifecycle after Restore or reopen. No
// reader of the old store remains, so nothing is left to retire.
func (b *BadgerEngine) labelIndexStoreReplaced() {
	b.invalidateLabelIndexReadiness()
	maintenancePause(pauseLabelIndexStoreReplaced)
	b.labelIndex.retireBeforeTs.Store(0)
	b.labelIndex.markerGeneration.Store(0)
	b.labelIndex.retired.clear()
}

// completeLabelIndexGeneration commits the ready marker for generation and
// publishes the proof read back from the store.
func (b *BadgerEngine) completeLabelIndexGeneration(generation uint64) error {
	if err := b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(labelIndexReadyKey, encodeLabelIndexMarker(labelIndexMarkerReady, generation))
	}); err != nil {
		return err
	}
	var ready bool
	var markerGeneration, readyAt uint64
	if err := b.withView(func(txn *badger.Txn) error {
		var err error
		ready, markerGeneration, readyAt, err = b.readLabelIndexReadyInTxn(txn)
		return err
	}); err != nil {
		return err
	}
	if !ready || markerGeneration != generation {
		return fmt.Errorf("label index ready marker generation mismatch: wrote %d, read ready=%t generation=%d", generation, ready, markerGeneration)
	}
	b.publishLabelIndexReady(readyAt, generation)
	return nil
}

// writeLabelIndexBuildingMarker commits a building marker for generation. It
// never touches the retire point; only beginLabelIndexRebuild moves it.
func (b *BadgerEngine) writeLabelIndexBuildingMarker(generation uint64) (uint64, error) {
	b.invalidateLabelIndexReadinessForRebuild()
	if err := b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(labelIndexReadyKey, encodeLabelIndexMarker(labelIndexMarkerBuilding, generation))
	}); err != nil {
		b.invalidateLabelIndexReadiness()
		return 0, err
	}
	var ready bool
	var markerGeneration, markerAt uint64
	if err := b.withView(func(txn *badger.Txn) error {
		var err error
		ready, markerGeneration, markerAt, err = b.readLabelIndexReadyInTxn(txn)
		return err
	}); err != nil {
		b.invalidateLabelIndexReadiness()
		return 0, err
	}
	if ready || markerGeneration != generation {
		b.invalidateLabelIndexReadiness()
		return 0, fmt.Errorf("label index build marker mismatch: wrote %d, read ready=%t generation=%d", generation, ready, markerGeneration)
	}
	epoch := b.labelIndex.epoch.Load()
	b.cacheLabelIndexReadiness(epoch, labelIndexReadinessNotReady, markerAt, generation)
	return markerAt, nil
}

// labelIndexReady reports whether the compatibility backfill completed.
func (b *BadgerEngine) labelIndexReady() (bool, error) {
	generation, state, _, _, known := b.cachedLabelIndexReadiness()
	if known {
		return state == labelIndexReadinessReady, nil
	}
	var ready bool
	var markerGeneration uint64
	var readyAt uint64
	maintenancePause(pauseLabelIndexReadinessProbe)
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		ready, markerGeneration, readyAt, err = b.readLabelIndexReadyInTxn(txn)
		return err
	})
	if err == nil {
		state := labelIndexReadinessNotReady
		if ready {
			state = labelIndexReadinessReady
		}
		b.cacheLabelIndexReadiness(generation, state, readyAt, markerGeneration)
	}
	return ready, err
}

// labelIndexReadyInTxn is the single readiness check used by label-index
// readers. Keeping it on the caller's snapshot means a read either sees the
// committed ready marker and the complete index, or scans stored node bodies.
func (b *BadgerEngine) labelIndexReadyInTxn(txn *badger.Txn) (bool, error) {
	ready, _, err := b.labelIndexAdmissionInTxn(txn)
	return ready, err
}

// labelIndexAdmissionInTxn captures the complete marker generation from the
// same Badger snapshot the caller will use for its index stream. The snapshot
// pins that generation's MVCC key versions until the stream closes.
func (b *BadgerEngine) labelIndexAdmissionInTxn(txn *badger.Txn) (bool, uint64, error) {
	if hook := b.labelIndex.beforeAdmission.Load(); hook != nil {
		(*hook)()
	}
	ready, generation, err := b.admitLabelIndexReaderInTxn(txn)
	if hook := b.labelIndex.afterAdmission.Load(); hook != nil {
		(*hook)()
	}
	return ready, generation, err
}

func (b *BadgerEngine) admitLabelIndexReaderInTxn(txn *badger.Txn) (bool, uint64, error) {
	epoch, state, markerAt, markerGeneration, known := b.cachedLabelIndexReadiness()
	if known {
		if markerAt == 0 && state != labelIndexReadinessReady {
			return false, markerGeneration, nil
		}
		if txn.ReadTs() >= markerAt {
			return state == labelIndexReadinessReady, markerGeneration, nil
		}
		// A historical snapshot can predate the cached building/ready marker.
		// Read its marker from that snapshot so a lazy reader is still bound to
		// the generation it admitted, even after the current generation changed.
	}
	ready, markerGeneration, markerAt, err := b.readLabelIndexReadyInTxn(txn)
	if err == nil {
		// The supplied snapshot already determines the result. Publishing a
		// cache hint additionally needs the live oracle; skip that hint if
		// shutdown has closed admission, without changing the snapshot read.
		db, leaseErr := b.beginHelperTxn()
		if leaseErr != nil {
			return ready, markerGeneration, nil
		}
		defer b.txnWG.Done()
		if txn.ReadTs() >= db.oracle.published.Load() {
			cacheState := labelIndexReadinessNotReady
			if ready {
				cacheState = labelIndexReadinessReady
			}
			b.cacheLabelIndexReadiness(epoch, cacheState, markerAt, markerGeneration)
		}
	}
	return ready, markerGeneration, err
}

// readLabelIndexReadyInTxn reads the marker and its Badger version from the
// supplied snapshot.
func (b *BadgerEngine) readLabelIndexReadyInTxn(txn *badger.Txn) (bool, uint64, uint64, error) {
	item, err := txn.Get(labelIndexReadyKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, 0, 0, nil
	}
	if err != nil {
		return false, 0, 0, err
	}
	var ready bool
	var markerGeneration uint64
	if err := item.Value(func(value []byte) error {
		var valid bool
		ready, markerGeneration, valid = decodeLabelIndexMarker(value)
		if !valid {
			ready = false
			markerGeneration = 0
		}
		return nil
	}); err != nil {
		return false, 0, 0, err
	}
	return ready, markerGeneration, item.Version(), nil
}

func encodeLabelIndexMarker(state byte, generation uint64) []byte {
	marker := make([]byte, labelIndexMarkerSize)
	marker[0] = labelIndexSchemeVersion
	marker[1] = state
	binary.BigEndian.PutUint64(marker[2:], generation)
	return marker
}

// decodeLabelIndexMarker accepts the original one-byte v3 marker as
// generation zero. New markers bind readiness to the generation captured by
// the same Badger snapshot as the label-index entries.
func decodeLabelIndexMarker(marker []byte) (ready bool, generation uint64, valid bool) {
	if len(marker) == 1 && marker[0] == labelIndexSchemeVersion {
		return true, 0, true
	}
	if len(marker) != labelIndexMarkerSize || marker[0] != labelIndexSchemeVersion {
		return false, 0, false
	}
	generation = binary.BigEndian.Uint64(marker[2:])
	switch marker[1] {
	case labelIndexMarkerReady:
		return true, generation, true
	case labelIndexMarkerBuilding:
		return false, generation, true
	default:
		return false, 0, false
	}
}

func (b *BadgerEngine) cachedLabelIndexReadiness() (uint64, uint32, uint64, uint64, bool) {
	generation := b.labelIndex.epoch.Load()
	if b.labelIndex.readinessEpoch.Load() != generation {
		return generation, labelIndexReadinessUnknown, 0, b.labelIndex.markerGeneration.Load(), false
	}
	state := b.labelIndex.state.Load()
	if state == labelIndexReadinessReady {
		markerAt := b.labelIndex.readyAt.Load()
		if markerAt == 0 {
			return generation, labelIndexReadinessUnknown, 0, b.labelIndex.markerGeneration.Load(), false
		}
		if b.labelIndex.epoch.Load() != generation ||
			b.labelIndex.readinessEpoch.Load() != generation {
			return generation, labelIndexReadinessUnknown, 0, b.labelIndex.markerGeneration.Load(), false
		}
		return generation, state, markerAt, b.labelIndex.markerGeneration.Load(), true
	}
	if state == labelIndexReadinessNotReady &&
		b.labelIndex.epoch.Load() == generation &&
		b.labelIndex.readinessEpoch.Load() == generation {
		return generation, state, b.labelIndex.readyAt.Load(), b.labelIndex.markerGeneration.Load(), true
	}
	return generation, labelIndexReadinessUnknown, 0, b.labelIndex.markerGeneration.Load(), false
}

func (b *BadgerEngine) cacheLabelIndexReadiness(generation uint64, state uint32, markerAt, markerGeneration uint64) {
	b.labelIndex.mu.Lock()
	defer b.labelIndex.mu.Unlock()
	if b.labelIndex.epoch.Load() != generation {
		return
	}
	b.labelIndex.readyAt.Store(markerAt)
	if markerAt != 0 || state == labelIndexReadinessReady {
		b.labelIndex.markerGeneration.Store(markerGeneration)
	}
	b.labelIndex.state.Store(state)
	b.labelIndex.readinessEpoch.Store(generation)
}

func (b *BadgerEngine) advanceLabelIndexGeneration(state uint32) uint64 {
	b.labelIndex.mu.Lock()
	defer b.labelIndex.mu.Unlock()
	generation := b.labelIndex.epoch.Load() + 1
	b.labelIndex.readyAt.Store(0)
	b.labelIndex.state.Store(state)
	b.labelIndex.readinessEpoch.Store(generation)
	b.labelIndex.epoch.Store(generation)
	return generation
}

func (b *BadgerEngine) invalidateLabelIndexReadiness() {
	b.advanceLabelIndexGeneration(labelIndexReadinessUnknown)
}

func (b *BadgerEngine) invalidateLabelIndexReadinessForRebuild() {
	b.advanceLabelIndexGeneration(labelIndexReadinessNotReady)
}

func (b *BadgerEngine) publishLabelIndexReady(readyAt, markerGeneration uint64) {
	b.labelIndex.mu.Lock()
	generation := b.labelIndex.epoch.Load() + 1
	b.labelIndex.readyAt.Store(readyAt)
	b.labelIndex.markerGeneration.Store(markerGeneration)
	b.labelIndex.state.Store(labelIndexReadinessReady)
	b.labelIndex.readinessEpoch.Store(generation)
	b.labelIndex.epoch.Store(generation)
	b.labelIndex.mu.Unlock()
}

// invalidateLabelIndexReadyReadTs is retained for older in-package test
// helpers. Production lifecycle owners use the explicit invalidation methods.
func (b *BadgerEngine) invalidateLabelIndexReadyReadTs() {
	b.invalidateLabelIndexReadiness()
}
