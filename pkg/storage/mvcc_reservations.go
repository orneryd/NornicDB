package storage

import (
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// mvccReservations is the engine's ledger of MVCC versions that a write has
// reserved but not published yet. allocateMVCCVersion reserves every version
// it hands out (reserve): under the ledger's lock the namespace sequence
// advances, the commit timestamp is fixed and the ledger learns the version,
// in one step. The write's publication, or the abandonment of a failed
// write, retires it: the Badger commit of a direct write, a transaction
// commit or a replay (commitEngineWrite, the deferred publication in the
// commit paths, applyBufferedCommitsWithCountPublication) and the
// write-behind acknowledgement (commitBufferedLocked).
//
// A transaction's cut keeps its order: the physical snapshot and buffer
// prefix first, then the logical boundary, so nothing in the snapshot is
// newer than the boundary. The cut announces itself before the snapshot
// (cutStarted) and then, under the ledger's lock, takes its wall clock,
// reads the boundary and copies the ledger (cutTaken). A version reserved
// after a cut compares newer than that cut's boundary: its sequence is
// higher, and its timestamp is floored at the cut's wall clock and the
// namespace's high-water mark, the two values the boundary is made of
// (MVCCVersion.Compare orders by timestamp first). A version reserved
// before the cut is in the copy. So a version is never inside the boundary
// and outside the copy. Bootstrap heads keep their entities' historical
// times (allocateHistoricalMVCCVersion); they describe state that existed
// before any cut. A version published
// while any cut is in progress stays in the ledger until every such cut has
// taken its copy, so the copy holds every version that was unpublished at
// the snapshot: the ones still in flight, and the ones published between
// the snapshot and the copy. A peer head whose version is older than the
// boundary but in the copy was published after the cut unless the
// transaction's own snapshot or buffer prefix already holds it
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
	// lastCutNanos is the wall clock of the latest cut; a version reserved
	// afterwards takes at least that time.
	lastCutNanos int64
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

// reserve hands out the namespace's next version under txn: the sequence
// advances, the commit timestamp is fixed and the ledger enters the version
// in one step under the ledger's lock, so a cut (cutTaken) sees the version
// in its boundary and its copy, or in neither. Unless historical, the
// timestamp is floored at the latest cut's wall clock and the namespace's
// high-water mark, so the version compares newer than every cut already
// taken. txn may be nil for a version its owner publishes by value (publish).
func (r *mvccReservations) reserve(txn *badger.Txn, namespace string, state *namespaceMVCCState, commitTime time.Time, historical bool) (MVCCVersion, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sequence, saturated := state.nextSequence()
	commitTime = commitTime.UTC()
	if !historical {
		floor := r.lastCutNanos
		if highWater := state.highWaterNanos.Load(); highWater > floor {
			floor = highWater
		}
		if commitTime.UnixNano() < floor {
			commitTime = time.Unix(0, floor).UTC()
		}
	}
	commitStamp, err := state.reserveCommitTimestamp(commitTime, saturated)
	if err != nil {
		// The advanced sequence is never published: no head carries it.
		return MVCCVersion{}, err
	}
	r.enterLocked(txn, namespace, sequence)
	return MVCCVersion{CommitTimestamp: commitStamp, CommitSequence: sequence}, nil
}

func (r *mvccReservations) enterLocked(txn *badger.Txn, namespace string, sequence uint64) {
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

// cutTaken ends a cut announced by cutStarted: under the ledger's lock it
// takes the cut's wall clock (never earlier than the previous cut's), reads
// the logical boundary through readBoundary at that clock and copies the
// ledger, so no version is in the boundary without being in the copy or
// published. The copy is nil when the ledger is empty, the common case. The
// last cut in progress removes the versions published meanwhile. A boundary
// read error ends the cut without a copy.
func (r *mvccReservations) cutTaken(readBoundary func(now time.Time) (map[string]MVCCVersion, error)) (time.Time, map[string]MVCCVersion, *mvccCut, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	if nanos := now.UnixNano(); nanos > r.lastCutNanos {
		r.lastCutNanos = nanos
	} else {
		now = time.Unix(0, r.lastCutNanos).UTC()
	}
	boundary, err := readBoundary(now)
	if err != nil {
		r.endCutLocked()
		return now, nil, nil, err
	}
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
	return now, boundary, cut, nil
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
