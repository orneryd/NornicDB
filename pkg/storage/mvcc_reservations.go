package storage

import (
	"sort"
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
// own commit, and a published entry outlives it only until the cuts that
// were in progress when it was published have taken their copies. Cuts are
// numbered as they start, and a published version records how many had
// started, so it is dropped as soon as the oldest cut in progress started
// after it: BEGINs that keep overlapping retain only what their own windows
// need, and a cut copies only the in-flight versions and the ones published
// since it started. A writer reserves one version (byTxn); the rare writer
// that reserves more, a replay of several recovered commits or a bootstrap,
// keeps the rest in more. The copy a cut takes (mvccCut) is nil when it holds
// nothing, the common case, and pooled otherwise, so neither the uncontended
// path nor a contended BEGIN allocates for it.
//
// BenchmarkNOR213ContendedNodeUpdate (16 auto-commit writers, 2 CPUs):
// before, the ledger kept every version published while any cut was in
// progress and each BEGIN iterated it under the lock, which was 99% of the
// mutex wait of contended writers.
type mvccReservations struct {
	mu    sync.Mutex
	byTxn map[*badger.Txn]mvccReservation
	more  map[*badger.Txn][]mvccReservation
	// inflight are the reserved, unpublished versions; at is each one's
	// position, so a publication removes it without a search.
	inflight []mvccReservation
	at       map[mvccReservation]int
	// published are the versions published while a cut was in progress,
	// oldest first, each with the number of cuts started by then.
	published []publishedReservation
	// cuts are the numbers of the cuts in progress; cutsStarted numbers them.
	cuts        []uint64
	cutsStarted uint64
	// lastCutNanos is the wall clock of the latest cut; a version reserved
	// afterwards takes at least that time.
	lastCutNanos int64
}

type mvccReservation struct {
	namespace string
	sequence  uint64
}

// publishedReservation is a version published after cut number cut started
// (and after every earlier one): those cuts' snapshots may not hold it.
type publishedReservation struct {
	mvccReservation
	cut uint64
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
	reservation := mvccReservation{namespace: namespace, sequence: sequence}
	if _, present := r.at[reservation]; !present {
		if r.at == nil {
			r.at = make(map[mvccReservation]int)
		}
		r.at[reservation] = len(r.inflight)
		r.inflight = append(r.inflight, reservation)
	}
	if txn == nil {
		return
	}
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

// retireLocked removes a published version from the in-flight versions, and
// keeps it for the cuts in progress, which still have to copy it.
func (r *mvccReservations) retireLocked(reservation mvccReservation) {
	position, present := r.at[reservation]
	if !present {
		return
	}
	last := len(r.inflight) - 1
	if position != last {
		moved := r.inflight[last]
		r.inflight[position] = moved
		r.at[moved] = position
	}
	r.inflight[last] = mvccReservation{}
	r.inflight = r.inflight[:last]
	delete(r.at, reservation)
	if len(r.cuts) > 0 {
		r.published = append(r.published, publishedReservation{mvccReservation: reservation, cut: r.cutsStarted})
	}
}

// cutStarted announces a cut and returns its number: from here until its
// cutTaken (or cutAbandoned), the versions published stay in the ledger.
func (r *mvccReservations) cutStarted() uint64 {
	r.mu.Lock()
	r.cutsStarted++
	cut := r.cutsStarted
	r.cuts = append(r.cuts, cut)
	r.mu.Unlock()
	return cut
}

// cutTaken ends the cut cutStarted numbered: under the ledger's lock it
// takes the cut's wall clock (never earlier than the previous cut's), reads
// the logical boundary through readBoundary at that clock and copies the
// in-flight versions and the ones published since the cut started, so no
// version is in the boundary without being in the copy or published before
// the cut. The copy is nil when it holds nothing, the common case. A
// boundary read error ends the cut without a copy.
func (r *mvccReservations) cutTaken(cut uint64, readBoundary func(now time.Time) (map[string]MVCCVersion, error)) (time.Time, map[string]MVCCVersion, *mvccCut, error) {
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
		r.endCutLocked(cut)
		return now, nil, nil, err
	}
	// published is ordered by cut number: this cut's versions are its tail.
	since := r.publishedLowerBoundLocked(cut)
	var copied *mvccCut
	if len(r.inflight) > 0 || since < len(r.published) {
		copied = mvccCutPool.Get().(*mvccCut)
		copied.entries = append(copied.entries, r.inflight...)
		for i := since; i < len(r.published); i++ {
			copied.entries = append(copied.entries, r.published[i].mvccReservation)
		}
	}
	r.endCutLocked(cut)
	return now, boundary, copied, nil
}

// cutAbandoned ends the cut cutStarted numbered without a copy.
func (r *mvccReservations) cutAbandoned(cut uint64) {
	r.mu.Lock()
	r.endCutLocked(cut)
	r.mu.Unlock()
}

// endCutLocked ends cut and drops the published versions no cut still in
// progress needs: the ones published before the oldest of them started.
func (r *mvccReservations) endCutLocked(cut uint64) {
	oldest := uint64(0)
	for i := 0; i < len(r.cuts); {
		if r.cuts[i] == cut {
			r.cuts[i] = r.cuts[len(r.cuts)-1]
			r.cuts = r.cuts[:len(r.cuts)-1]
			cut = 0
			continue
		}
		if oldest == 0 || r.cuts[i] < oldest {
			oldest = r.cuts[i]
		}
		i++
	}
	if len(r.published) == 0 {
		return
	}
	if oldest == 0 {
		clear(r.published)
		r.published = r.published[:0]
		return
	}
	drop := r.publishedLowerBoundLocked(oldest)
	if drop > 0 {
		kept := copy(r.published, r.published[drop:])
		clear(r.published[kept:])
		r.published = r.published[:kept]
	}
}

// publishedLowerBoundLocked locates the retained publications at or after cut.
// Publication appends in cut order; both snapshot copying and retirement use
// this boundary so they agree about which cuts still need each publication.
func (r *mvccReservations) publishedLowerBoundLocked(cut uint64) int {
	return sort.Search(len(r.published), func(i int) bool { return r.published[i].cut >= cut })
}
