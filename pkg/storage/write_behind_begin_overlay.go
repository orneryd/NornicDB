package storage

import (
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// beginOverlay is the write-behind overlay a transaction could see when it
// began: the generations then buffered and, of the active generation, only
// the commits appended before the begin. The transaction pins those
// generations, so their images and tombstones stay readable after flush and
// retirement; a retired generation releases its memory leases when its last
// pin goes. Pins live only as long as the transaction.
type beginOverlay struct {
	buffer   *WriteBehindBuffer
	active   *CommitBuffer
	commits  []bufferedCommit // the active generation's commits at begin
	draining []*CommitBuffer  // older generations, newest first
	once     sync.Once
}

// pinBeginOverlayLocked captures the overlay. The caller holds w.mu for
// reading, the lock that appends commits, rotates and retires generations.
func (w *WriteBehindBuffer) pinBeginOverlayLocked() *beginOverlay {
	if w == nil {
		return nil
	}
	n := len(w.active.commits)
	overlay := &beginOverlay{buffer: w, active: w.active, commits: w.active.commits[:n:n]}
	w.active.pins.Add(1)
	for i := len(w.draining) - 1; i >= 0; i-- {
		generation := w.draining[i]
		generation.pins.Add(1)
		overlay.draining = append(overlay.draining, generation)
	}
	w.overlayMu.Lock()
	if !w.overlaysClosed {
		if w.overlays == nil {
			w.overlays = make(map[*beginOverlay]struct{})
		}
		w.overlays[overlay] = struct{}{}
	}
	w.overlayMu.Unlock()
	return overlay
}

// release unpins the overlay's generations once: at the transaction's commit
// or rollback, or when the buffer closes first.
func (o *beginOverlay) release() {
	if o == nil {
		return
	}
	o.once.Do(func() {
		o.buffer.overlayMu.Lock()
		delete(o.buffer.overlays, o)
		o.buffer.overlayMu.Unlock()
		o.active.unpin()
		for _, generation := range o.draining {
			generation.unpin()
		}
	})
}

// releaseOverlays releases every overlay still registered. Close calls it
// after the final drain retired the generations.
func (w *WriteBehindBuffer) releaseOverlays() {
	w.overlayMu.Lock()
	w.overlaysClosed = true
	overlays := make([]*beginOverlay, 0, len(w.overlays))
	for overlay := range w.overlays {
		overlays = append(overlays, overlay)
	}
	w.overlayMu.Unlock()
	for _, overlay := range overlays {
		overlay.release()
	}
}

// entityState resolves id as the overlay stood at begin, newest state first.
func (o *beginOverlay) entityState(kind bufferedEntityKind, id string) bufferedEntityState {
	if o == nil {
		return bufferedEntityState{}
	}
	for i := len(o.commits) - 1; i >= 0; i-- {
		ops := o.commits[i].ops
		for j := len(ops) - 1; j >= 0; j-- {
			if state, ok := operationEntityState(ops[j], kind, id); ok {
				return state
			}
		}
	}
	for _, generation := range o.draining {
		if state := generation.entityState(kind, id); state.found {
			return state
		}
	}
	return bufferedEntityState{}
}

// operationEntityState is what one buffered operation says about id.
func operationEntityState(op Operation, kind bufferedEntityKind, id string) (bufferedEntityState, bool) {
	if kind == bufferedNodeEntity {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil && string(op.Node.ID) == id {
				return bufferedEntityState{node: op.Node, found: true}, true
			}
		case OpDeleteNode:
			if string(op.NodeID) == id {
				return bufferedEntityState{found: true, deleted: true}, true
			}
		}
		return bufferedEntityState{}, false
	}
	switch op.Type {
	case OpCreateEdge, OpUpdateEdge:
		if op.Edge != nil && string(op.Edge.ID) == id {
			return bufferedEntityState{edge: op.Edge, found: true}, true
		}
	case OpDeleteEdge:
		if string(op.EdgeID) == id {
			return bufferedEntityState{found: true, deleted: true}, true
		}
	case OpDeleteNode:
		for _, edgeID := range op.DeletedEdgeIDs {
			if string(edgeID) == id {
				return bufferedEntityState{found: true, deleted: true}, true
			}
		}
	}
	return bufferedEntityState{}, false
}

func (b *CommitBuffer) unpin() {
	if b.pins.Add(-1) == 0 && b.queueRetired.Load() {
		b.releaseLeases()
	}
}

// releaseLeases returns a retired generation's memory once no transaction
// pins it.
func (b *CommitBuffer) releaseLeases() {
	if b.leasesReleased.CompareAndSwap(false, true) {
		for _, commit := range b.commits {
			commit.memoryLease.Release()
		}
	}
}

// beginReadSnapshot opens a transaction's begin snapshot through the shared
// committed/overlay capture: the write-behind generations visible now are
// pinned under the same retirement lock that registers the Badger read
// timestamp, so no buffered write can be in one and missing from the other.
// The transaction owns that read timestamp and ends it when it closes.
func (b *BadgerEngine) beginReadSnapshot(db *managedBadgerDB) (*badger.Txn, uint64, *beginOverlay, error) {
	var overlay *beginOverlay
	view, err := b.captureReadView(func(w *WriteBehindBuffer) { overlay = w.pinBeginOverlayLocked() }, readViewOptions{longLived: true})
	if err != nil {
		overlay.release()
		return nil, 0, nil, err
	}
	if view.db != db {
		overlay.release()
		view.Close()
		return nil, 0, nil, ErrStorageClosed
	}
	return db.newTxnAt(view.readTs, false), view.readTs, overlay, nil
}
