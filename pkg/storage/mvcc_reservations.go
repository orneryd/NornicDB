package storage

import (
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// mvccReservations is the engine's ledger of MVCC versions that a write has
// reserved but not published yet. allocateMVCCVersion enters every version
// it hands out; the write's publication, or the abandonment of a failed
// write, retires it: the Badger commit of a direct write, a transaction
// commit or a replay (commitEngineWrite, applyBufferedCommitsWithCountPublication),
// the write-behind acknowledgement (commitBufferedLocked) and the close of
// a transaction (closeLocked).
//
// A transaction's cut keeps its order: the physical snapshot and buffer
// prefix first, then the logical boundary, so nothing in the snapshot is
// newer than the boundary. The cut announces itself before the snapshot
// (cutStarted) and copies the ledger after the boundary (cutTaken). A
// version published while any cut is in progress stays in the ledger until
// every such cut has taken its copy, so the copy holds every version that
// was unpublished at the snapshot: the ones still in flight, and the ones
// published between the snapshot and the copy. A peer head whose version
// is older than the boundary but in the copy was published after the cut
// unless the transaction's own snapshot or buffer prefix already holds it
// (publishedBeforeCut); that one rule, applied in snapshotHeadConflict,
// answers for buffered and replayed peers alike. The reserved version
// alone cannot: a peer reserves its version before it publishes, and the
// snapshot and prefix hold only what was published when they were taken.
//
// The ledger is small: a write holds its reservation for the length of its
// own commit, and a published entry outlives it only while a cut is in
// progress. A writer reserves one version (byTxn); the rare writer that
// reserves more, a replay of several recovered commits or a bootstrap,
// keeps the rest in more. The copy a cut takes (mvccCut) is nil when the
// ledger is empty, the common case, and pooled otherwise, so neither the
// uncontended path nor a contended BEGIN allocates for it.
type mvccReservations struct {
	mu             sync.Mutex
	byTxn          map[*badger.Txn]mvccReservation
	more           map[*badger.Txn][]mvccReservation
	entries        map[string]map[uint64]struct{}
	count          int
	cutsInProgress int
	retiring       []mvccReservation
}

type mvccReservation struct {
	namespace string
	sequence  uint64
}

// mvccCut is a cut's copy of the ledger: the versions unpublished at the
// cut's snapshot, and the ones published between that snapshot and the
// copy. A transaction releases its copy when it closes or takes its next
// cut.
type mvccCut struct {
	entries []mvccReservation
}

var mvccCutPool = sync.Pool{New: func() any { return &mvccCut{} }}

// holds reports whether version of namespace was in the copy.
func (c *mvccCut) holds(namespace string, version MVCCVersion) bool {
	if c == nil {
		return false
	}
	for i := range c.entries {
		if c.entries[i].namespace == namespace && c.entries[i].sequence == version.CommitSequence {
			return true
		}
	}
	return false
}

// release returns the copy to the pool; nil is a no-op.
func (c *mvccCut) release() {
	if c == nil {
		return
	}
	c.entries = c.entries[:0]
	mvccCutPool.Put(c)
}

// reserve enters a version handed out under txn; txn may be nil for a
// version its owner publishes by value (publish).
func (r *mvccReservations) reserve(txn *badger.Txn, namespace string, sequence uint64) {
	if namespace == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[string]map[uint64]struct{})
	}
	sequences := r.entries[namespace]
	if sequences == nil {
		sequences = make(map[uint64]struct{})
		r.entries[namespace] = sequences
	}
	if _, present := sequences[sequence]; !present {
		sequences[sequence] = struct{}{}
		r.count++
	}
	if txn == nil {
		return
	}
	reservation := mvccReservation{namespace: namespace, sequence: sequence}
	if r.byTxn == nil {
		r.byTxn = make(map[*badger.Txn]mvccReservation)
	}
	if _, reserved := r.byTxn[txn]; !reserved {
		r.byTxn[txn] = reservation
		return
	}
	if r.more == nil {
		r.more = make(map[*badger.Txn][]mvccReservation)
	}
	r.more[txn] = append(r.more[txn], reservation)
}

// publish retires one version: it is published, or its write was abandoned.
func (r *mvccReservations) publish(namespace string, sequence uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retireLocked(mvccReservation{namespace: namespace, sequence: sequence})
}

// publishTxn retires every version reserved under txn.
func (r *mvccReservations) publishTxn(txn *badger.Txn) {
	if txn == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	first, reserved := r.byTxn[txn]
	if !reserved {
		return
	}
	delete(r.byTxn, txn)
	r.retireLocked(first)
	if rest, ok := r.more[txn]; ok {
		delete(r.more, txn)
		for _, reservation := range rest {
			r.retireLocked(reservation)
		}
	}
}

// retireLocked removes a published version, or holds it for the cuts in
// progress, which still have to copy it.
func (r *mvccReservations) retireLocked(reservation mvccReservation) {
	sequences := r.entries[reservation.namespace]
	if sequences == nil {
		return
	}
	if _, present := sequences[reservation.sequence]; !present {
		return
	}
	if r.cutsInProgress > 0 {
		r.retiring = append(r.retiring, reservation)
		return
	}
	delete(sequences, reservation.sequence)
	r.count--
}

// cutStarted announces a cut: from here until cutTaken, published versions
// stay in the ledger.
func (r *mvccReservations) cutStarted() {
	r.mu.Lock()
	r.cutsInProgress++
	r.mu.Unlock()
}

// cutTaken copies the ledger for a cut announced by cutStarted and ends it;
// nil when the ledger is empty, the common case. The last cut in progress
// removes the versions published meanwhile.
func (r *mvccReservations) cutTaken() *mvccCut {
	r.mu.Lock()
	defer r.mu.Unlock()
	var cut *mvccCut
	if r.count > 0 {
		cut = mvccCutPool.Get().(*mvccCut)
		for namespace, sequences := range r.entries {
			for sequence := range sequences {
				cut.entries = append(cut.entries, mvccReservation{namespace: namespace, sequence: sequence})
			}
		}
	}
	r.endCutLocked()
	return cut
}

// cutAbandoned ends a cut announced by cutStarted that takes no copy.
func (r *mvccReservations) cutAbandoned() {
	r.mu.Lock()
	r.endCutLocked()
	r.mu.Unlock()
}

func (r *mvccReservations) endCutLocked() {
	if r.cutsInProgress > 0 {
		r.cutsInProgress--
	}
	if r.cutsInProgress > 0 || len(r.retiring) == 0 {
		return
	}
	for _, reservation := range r.retiring {
		if sequences := r.entries[reservation.namespace]; sequences != nil {
			if _, present := sequences[reservation.sequence]; present {
				delete(sequences, reservation.sequence)
				r.count--
			}
		}
	}
	r.retiring = r.retiring[:0]
}
