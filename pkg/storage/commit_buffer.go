package storage

// Rotating write-behind commit buffer.
//
// The buffer sits at the single choke point where committed autocommit
// transactions land (BadgerTransaction.Commit). Committed statement state is
// appended to the active generation and acknowledged immediately; when the
// flush interval elapses or a size threshold is crossed, the active
// generation is atomically rotated out to a fresh one and the drained
// generation is replayed into the underlying engine by one background
// flusher. Writers only ever append to the active generation under a short
// lock, so they never wait for the flush.

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// bufferedOpKind identifies one committed mutation kind.
type bufferedOpKind uint8

const (
	bufferedOpNode bufferedOpKind = iota
	bufferedOpEdge
	bufferedOpDelete
)

// bufferedMutation is one ordered entry of a generation's replay log.
type bufferedMutation struct {
	kind bufferedOpKind
	id   string // node or edge ID, or the raw delete key
	node *Node  // payload for bufferedOpNode
	edge *Edge  // payload for bufferedOpEdge
}

// bufferedCommit is one committed statement captured at BadgerTransaction
// commit for background replay: its ordered physical operations, raw KV
// writes/deletes, namespace, and staged ID-counter high-water marks.
type bufferedCommit struct {
	recovered bool // WAL operations require authoritative binary-body hydration before publication.
	namespace string
	txID      string // transaction ID, used for the WAL buffered-commit payload and flush marker
	ops       []Operation
	writes    map[string][]byte
	deletes   map[string]bool

	// version is the MVCC version allocated at ACK time. The DB-local
	// sequence counter orders the commit's history exactly where it
	// happened: a transaction that began after the ACK sees this
	// version, so flushes landing later never look "newer" than reads
	// pinned after the commit.
	version MVCCVersion

	counterNodeMax, counterEdgeMax uint64
	hasCounters                    bool

	// idEntries are the ID dictionary entries this commit wrote while they
	// were pending; replay clears their mark once their keys are durable.
	// Nil for commits recovered from the WAL, whose entries load as stored.
	idEntries *txnCounterState

	// propKeyDrain carries the property-key dictionary tokens staged by this
	// commit; they persist out-of-transaction at replay, as the normal
	// commit path does.
	propKeyDrain propKeyTxnDrain

	// Derived-count deltas the sync commit path would have applied right
	// after the Badger commit. Replay merges them across the generation and
	// applies them once, under the same count locks, instead of rebuilding
	// authoritative counts per generation (O(dataset) per drain).
	labelDeltas         map[namespaceLabel]int64
	edgeTypeDeltas      map[namespaceEdgeType]int64
	edgeTypeLabelDeltas map[edgeTypeLabelDelta]int64
}

// CommitBuffer is one generation of buffered committed writes. It is
// append-only while active and read-only while draining.
type CommitBuffer struct {
	commits []bufferedCommit

	nodes   map[NodeID]*Node
	edges   map[EdgeID]*Edge
	deletes map[string]bufferedEntityKind // legacy generic deletion masks only
	order   []bufferedMutation

	nodesByLabel map[string][]NodeID
	edgesByType  map[string][]EdgeID

	// Adjacency overlay indexes: live buffered edges by endpoint node, and
	// the generation's deleted edge IDs so committed adjacency scans can
	// suppress rows the buffer hides (PR #1043 read-overlay parity).
	outgoingByNode map[NodeID][]EdgeID
	incomingByNode map[NodeID][]EdgeID
	adjSeen        map[edgeAdjSeenKey]struct{}
	deletedEdgeIDs map[EdgeID]struct{}

	// deletedNodeIDs mirrors deletedEdgeIDs so prefix-scoped node counters
	// can fold the buffered delete set into scan fallbacks.
	deletedNodeIDs map[NodeID]struct{}

	// edgeCreateCount / edgeDeleteCount feed the buffered total edge-count
	// delta (creates minus deletes) the engine's EdgeCount readers overlay.
	// Cardinality deltas exclude updates and are independent of entity overlays.
	totalCounts     bufferedCardinalityDeltas
	edgeCreateCount int
	edgeDeleteCount int

	// labelSeen / typeSeen make overlay index inserts O(1): without them,
	// every op would scan the label's/type's accumulated ID slice for
	// duplicates (O(n²) per generation, string compares on hot paths).
	labelSeen map[labelSeenKey]struct{}
	typeSeen  map[typeSeenKey]struct{}

	// ops counts the buffered mutations this generation carries; the
	// adaptive sizer uses it to measure drain throughput.
	ops int

	applied atomic.Bool
	// All counters publish before entity-cache callbacks finish.
	// Retire only their overlay at that boundary; entity overlays remain live.
	countsApplied atomic.Bool
	draining      atomic.Bool // claims the single flusher slot for this generation

	// retired guards the buffer's pending-node index retirement: a
	// generation that reached the applied state retires its nodes from the
	// index exactly once, whichever drain path reaches it first.
	retired atomic.Bool
}

type labelSeenKey struct {
	label string
	id    NodeID
}

type typeSeenKey struct {
	edgeType string
	id       EdgeID
}

func newCommitBuffer() *CommitBuffer {
	return &CommitBuffer{
		nodes:          make(map[NodeID]*Node),
		edges:          make(map[EdgeID]*Edge),
		deletes:        make(map[string]bufferedEntityKind),
		nodesByLabel:   make(map[string][]NodeID),
		edgesByType:    make(map[string][]EdgeID),
		labelSeen:      make(map[labelSeenKey]struct{}),
		typeSeen:       make(map[typeSeenKey]struct{}),
		outgoingByNode: make(map[NodeID][]EdgeID),
		incomingByNode: make(map[NodeID][]EdgeID),
		adjSeen:        make(map[edgeAdjSeenKey]struct{}),
		deletedEdgeIDs: make(map[EdgeID]struct{}),
		deletedNodeIDs: make(map[NodeID]struct{}),
	}
}

// edgeAdjSeenKey deduplicates the adjacency index entries of one generation.
type edgeAdjSeenKey struct {
	node NodeID
	id   EdgeID
}

// Mutations returns the generation's replay log in append order.
func (b *CommitBuffer) Mutations() []bufferedMutation {
	return b.order
}

// Commits returns the generation's buffered commits in append order.
func (b *CommitBuffer) Commits() []bufferedCommit {
	return b.commits
}

// AddCommit appends one committed statement and folds its operations into
// the read overlay. The caller must hand over ownership of the payload. The
// overlay stores the same node/edge pointers as the replay log: nothing
// mutates them after the ACK (replay encodes read-only and copies before
// normalizing; the engine's overlay reads copy before returning), so no
// second copy is paid per operation.
func (b *CommitBuffer) AddCommit(c bufferedCommit) {
	b.commits = append(b.commits, c)
	for _, op := range c.ops {
		visitOperationTotalCounts(op, b.totalCounts.add)
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil && op.Node.ID != "" {
				b.clearEntityDeletion(bufferedNodeEntity, string(op.Node.ID))
				b.nodes[op.Node.ID] = op.Node
				b.indexNodeLabels(op.Node.ID, op.Node.Labels)
				if op.OldNode != nil {
					// Removing a label must shadow its durable membership and
					// any older generation, just as deleting the node does.
					b.indexNodeLabels(op.Node.ID, op.OldNode.Labels)
				}
			}
		case OpDeleteNode:
			b.deletedNodeIDs[op.NodeID] = struct{}{}
			// DETACH DELETE tombstones the node's relationships too. Fold
			// them into the generation's delete set so the overlay hides
			// the committed rows immediately and later buffered batches
			// that still see them through the adjacency index do not
			// delete (and count) them a second time.
			for _, edgeID := range op.DeletedEdgeIDs {
				if edgeID != "" {
					b.deletedEdgeIDs[edgeID] = struct{}{}
					b.edgeDeleteCount++
				}
			}
			if op.OldNode != nil {
				// Mark the deleted node's labels as touched so scan overlays
				// drop the committed row too, not just the buffered one.
				b.indexNodeLabels(op.NodeID, op.OldNode.Labels)
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge != nil && op.Edge.ID != "" {
				b.clearEntityDeletion(bufferedEdgeEntity, string(op.Edge.ID))
				b.edges[op.Edge.ID] = op.Edge
				key := typeSeenKey{edgeType: op.Edge.Type, id: op.Edge.ID}
				if _, seen := b.typeSeen[key]; !seen {
					b.typeSeen[key] = struct{}{}
					b.edgesByType[op.Edge.Type] = append(b.edgesByType[op.Edge.Type], op.Edge.ID)
				}
				b.indexEdgeAdjacency(op.Edge)
				if op.Type == OpCreateEdge {
					b.edgeCreateCount++
				}
			}
		case OpDeleteEdge:
			b.deletedEdgeIDs[op.EdgeID] = struct{}{}
			b.edgeDeleteCount++
			if op.OldEdge != nil && op.OldEdge.Type != "" {
				key := typeSeenKey{edgeType: op.OldEdge.Type, id: op.EdgeID}
				if _, seen := b.typeSeen[key]; !seen {
					b.typeSeen[key] = struct{}{}
					b.edgesByType[op.OldEdge.Type] = append(b.edgesByType[op.OldEdge.Type], op.EdgeID)
				}
			}
		}
	}
}

// IsApplied reports whether this generation has been replayed into the
// underlying engine.
func (b *CommitBuffer) IsApplied() bool {
	return b.applied.Load()
}

// claimDrain atomically claims this generation for a single flusher. It
// returns false when another flusher is already applying it.
func (b *CommitBuffer) claimDrain() bool {
	return b.draining.CompareAndSwap(false, true)
}

// releaseDrain lets a failed apply be retried on a later cycle.
func (b *CommitBuffer) releaseDrain() {
	b.draining.Store(false)
}

func (b *CommitBuffer) markApplied() {
	b.applied.Store(true)
}

func (b *CommitBuffer) addNode(n *Node) {
	if n == nil || n.ID == "" {
		return
	}
	if _, exists := b.nodes[n.ID]; !exists {
		b.totalCounts.add(string(n.ID), false, 1)
	}
	copyNode := *n
	b.clearEntityDeletion(bufferedNodeEntity, string(n.ID))
	b.nodes[n.ID] = &copyNode
	b.order = append(b.order, bufferedMutation{kind: bufferedOpNode, id: string(n.ID), node: &copyNode})
	b.indexNodeLabels(n.ID, n.Labels)
}

// indexNodeLabels records every membership a generation touches. The latest
// body decides whether that membership is still live when readers merge it.
func (b *CommitBuffer) indexNodeLabels(id NodeID, labels []string) {
	for _, label := range labels {
		key := labelSeenKey{label: label, id: id}
		if _, seen := b.labelSeen[key]; !seen {
			b.labelSeen[key] = struct{}{}
			b.nodesByLabel[label] = append(b.nodesByLabel[label], id)
		}
	}
}

func (b *CommitBuffer) addEdge(e *Edge) {
	if e == nil || e.ID == "" {
		return
	}
	if _, exists := b.edges[e.ID]; !exists {
		b.totalCounts.add(string(e.ID), true, 1)
	}
	copyEdge := *e
	b.clearEntityDeletion(bufferedEdgeEntity, string(e.ID))
	b.edges[e.ID] = &copyEdge
	b.order = append(b.order, bufferedMutation{kind: bufferedOpEdge, id: string(e.ID), edge: &copyEdge})
	b.edgesByType[e.Type] = append(b.edgesByType[e.Type], e.ID)
	b.indexEdgeAdjacency(&copyEdge)
	b.edgeCreateCount++
}

// indexEdgeAdjacency records a live buffered edge in the generation's
// adjacency overlay index, once per edge and endpoint.
func (b *CommitBuffer) indexEdgeAdjacency(e *Edge) {
	if e.StartNode != "" {
		key := edgeAdjSeenKey{node: e.StartNode, id: e.ID}
		if _, seen := b.adjSeen[key]; !seen {
			b.adjSeen[key] = struct{}{}
			b.outgoingByNode[e.StartNode] = append(b.outgoingByNode[e.StartNode], e.ID)
		}
	}
	if e.EndNode != "" {
		key := edgeAdjSeenKey{node: e.EndNode, id: e.ID}
		if _, seen := b.adjSeen[key]; !seen {
			b.adjSeen[key] = struct{}{}
			b.incomingByNode[e.EndNode] = append(b.incomingByNode[e.EndNode], e.ID)
		}
	}
}

func (b *CommitBuffer) addDelete(key string) {
	if key == "" {
		return
	}
	b.deletes[key] = bufferedNodeEntity | bufferedEdgeEntity
	b.order = append(b.order, bufferedMutation{kind: bufferedOpDelete, id: key})
}

// getNode resolves the latest buffered state for id in this generation.
// Returns (value, found, deleted). found is false when this generation has
// no entry for id.
func (b *CommitBuffer) getNode(id NodeID) (*Node, bool, bool) {
	state := b.entityState(bufferedNodeEntity, string(id))
	return state.node, state.found, state.deleted
}

// getEdge resolves the latest buffered state for id in this generation.
func (b *CommitBuffer) getEdge(id EdgeID) (*Edge, bool, bool) {
	state := b.entityState(bufferedEdgeEntity, string(id))
	return state.edge, state.found, state.deleted
}

// WriteBehindBuffer holds the active generation and FIFO draining
// generations. Apply is injected so the replay destination (Badger) stays a
// detail of the caller.
type WriteBehindBuffer struct {
	mu       sync.RWMutex
	active   *CommitBuffer
	draining []*CommitBuffer
	opCount  int

	interval time.Duration
	maxOps   int
	apply    func(*CommitBuffer) error

	// Adaptive sizing: draining a generation is deterministic — a drain of
	// size S at throughput R takes S/R — so the size threshold that rotates
	// the active generation tracks measured throughput: targetOps ≈ R ×
	// current delay, clamped to [minAdaptiveOps, effectiveMaxOps]. The
	// current delay itself tracks the measured drain (disk) latency: after
	// every successful drain it moves toward the drain's EWMA duration,
	// clamped to [minDelay, maxDelay], so both the rotation cadence and the
	// generation size follow real write conditions instead of a static
	// guess. drainRate, targetOps, drainDur and curInterval are written
	// only under mu, in the drain path's short bookkeeping section — apply
	// runs without holding any buffer lock.
	//
	// maxOps is the manual cap: >0 uses it, the sentinel 0 means auto and
	// falls back to defaultWriteBehindMaxOps. When a manual cap binds
	// (rate × delay exceeds it), the size rotation fires before the
	// interval tick, so the effective delay shrinks to cap/rate
	// automatically; warn reports the mismatch.
	drainRate   float64       // EWMA ops/sec of successful applies
	targetOps   int           // current size-threshold; effective max until first drain
	drainDur    time.Duration // EWMA duration of successful applies (disk delay)
	curInterval time.Duration // adaptive rotation delay; interval until first drain
	minDelay    time.Duration
	maxDelay    time.Duration

	// warn, when set, receives sizing-mismatch diagnostics. Called at most
	// once per state change: cap-binding, cap-idle, back to balanced.
	warn      func(string)
	warnState int // 0 balanced/unset, 1 cap binds, 2 cap never binds, 3 delay pinned

	// pendingIndex is the buffer's pending-node index, built lazily the
	// first time an attached schema tracks (label, property) pairs: schema
	// property-index lookups merge it in (pendingWriteSource), so buffered
	// but unflushed nodes answer index seeks exactly like committed ones.
	// Guarded by mu.
	pendingIndex *pendingNodeIndex
	// trackedPairs is the set of (label, property) pairs an attached
	// schema indexes by value; the pending index rebuilds from it when it
	// has been released empty. Guarded by mu.
	trackedPairs []pendingPropertyKey

	flushCh chan struct{}
	done    chan struct{}
	closeMu sync.Mutex
	closed  bool
	wg      sync.WaitGroup

	errMu   sync.Mutex
	lastErr error
}

// minAdaptiveOps keeps a slow flusher from rotating into generation sizes so
// small that per-drain fixed costs (Badger commit, count locks) dominate.
const minAdaptiveOps = 1000

// defaultWriteBehindMaxOps caps auto-sized generations when no manual max is
// configured (maxOps sentinel 0). 100,000 buffered operations is the
// researched default: at ~0.5-2KB per buffered write that is ~50-200MB of
// unflushed memory, in the same band as InnoDB's 16MB redo-log buffer,
// RocksDB's 64MB write buffer and MongoDB's 100ms journal group-commit
// window. It bounds memory on oversized hardware while leaving the adaptive
// target (throughput x delay) 100x headroom above minAdaptiveOps (1,000), so
// a manual cap is only needed to tune for unusual hardware.
const defaultWriteBehindMaxOps = 100_000

// defaultWriteBehindMinDelay floors the adaptive flush delay: rotating
// faster than this buys nothing and multiplies per-flush fixed costs.
const defaultWriteBehindMinDelay = 5 * time.Millisecond

// defaultWriteBehindMaxDelay caps the adaptive flush delay: the durability
// loss window and unflushed memory stay bounded even if drains slow down.
// 1s matches the flush cadence of Redis AOF (appendfsync everysec), MySQL
// InnoDB (innodb_flush_log_at_timeout=1s) and the documented 5ms-1s clamp;
// it is far below Cassandra's 10s periodic commit-log sync, keeping the loss
// window conservative.
const defaultWriteBehindMaxDelay = 1 * time.Second

// NewWriteBehindBuffer builds a buffer whose rotation delay and generation
// size adapt to measured drain latency and throughput: after every
// successful drain the delay moves toward the drain duration (clamped to
// [minDelay, maxDelay]; the zero sentinels select the defaults) and the size
// threshold tracks throughput × delay. maxOps is the manual generation cap;
// the sentinel 0 selects auto sizing against an internal default cap. apply
// replays a drained generation into the underlying engine.
func NewWriteBehindBuffer(interval time.Duration, maxOps int, apply func(*CommitBuffer) error) *WriteBehindBuffer {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	w := &WriteBehindBuffer{
		active:      newCommitBuffer(),
		interval:    interval,
		curInterval: interval,
		maxOps:      maxOps,
		targetOps:   maxOps,
		apply:       apply,
		minDelay:    defaultWriteBehindMinDelay,
		maxDelay:    defaultWriteBehindMaxDelay,
		flushCh:     make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	if w.minDelay > interval {
		w.minDelay = interval
	}
	if w.maxDelay < interval {
		w.maxDelay = interval
	}
	w.wg.Add(1)
	go w.run()
	return w
}

// SetDelayBounds overrides the adaptive delay clamp. The zero sentinel keeps
// the default for that bound. The configured interval always stays inside
// the bounds. Must be called before the first drain; safe at any time.
func (w *WriteBehindBuffer) SetDelayBounds(minDelay, maxDelay time.Duration) {
	w.mu.Lock()
	if minDelay > 0 {
		w.minDelay = minDelay
	}
	if maxDelay > 0 {
		w.maxDelay = maxDelay
	}
	if w.minDelay > w.interval {
		w.minDelay = w.interval
	}
	if w.maxDelay < w.interval {
		w.maxDelay = w.interval
	}
	if w.curInterval < w.minDelay {
		w.curInterval = w.minDelay
	}
	if w.curInterval > w.maxDelay {
		w.curInterval = w.maxDelay
	}
	w.mu.Unlock()
}

func (w *WriteBehindBuffer) signal() {
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
}

// AppendNode appends a committed node write to the active generation.
func (w *WriteBehindBuffer) AppendNode(n *Node) {
	w.mu.Lock()
	w.active.addNode(n)
	w.opCount++
	rotate := w.opCount >= w.targetSizeLocked() && w.targetSizeLocked() > 0
	w.mu.Unlock()
	if rotate {
		w.signal()
	}
}

// AppendEdge appends a committed edge write to the active generation.
func (w *WriteBehindBuffer) AppendEdge(e *Edge) {
	w.mu.Lock()
	w.active.addEdge(e)
	w.opCount++
	rotate := w.opCount >= w.targetSizeLocked() && w.targetSizeLocked() > 0
	w.mu.Unlock()
	if rotate {
		w.signal()
	}
}

// AppendDelete appends a committed delete to the active generation.
func (w *WriteBehindBuffer) AppendDelete(key string) {
	w.mu.Lock()
	w.active.addDelete(key)
	w.opCount++
	rotate := w.opCount >= w.targetSizeLocked() && w.targetSizeLocked() > 0
	w.mu.Unlock()
	if rotate {
		w.signal()
	}
}

// AppendCommit appends one committed statement captured at transaction
// commit. It folds the statement's operations into the read overlay and
// counts every operation toward the size threshold.
func (w *WriteBehindBuffer) AppendCommit(c bufferedCommit) {
	w.mu.Lock()
	w.active.AddCommit(c)
	w.opCount += len(c.ops) + len(c.writes) + len(c.deletes)
	if w.opCount < 1 {
		w.opCount = 1
	}
	// Keep the pending-node index in step with the overlay once an
	// attached schema has asked for value indexing: buffered nodes answer
	// property-index seeks until their generation applies, and the schema's
	// committed index takes over from the replay's own maintenance.
	if len(w.trackedPairs) > 0 && w.pendingIndex == nil {
		// The index was released once empty (memory decay after a burst);
		// rebuild it from the tracked pairs before indexing this commit.
		w.pendingIndex = newPendingNodeIndex()
		for _, pair := range w.trackedPairs {
			w.pendingIndex.track(pair.label, pair.property, func(id NodeID) *Node {
				node, _ := w.nodeLocked(id)
				return node
			})
		}
	}
	if w.pendingIndex != nil {
		for _, op := range c.ops {
			switch op.Type {
			case OpCreateNode, OpUpdateNode:
				w.pendingIndex.replace(op.OldNode, op.Node)
			case OpDeleteNode:
				w.pendingIndex.remove(op.OldNode)
			}
		}
	}
	rotate := w.opCount >= w.targetSizeLocked() && w.targetSizeLocked() > 0
	w.mu.Unlock()
	if rotate {
		w.signal()
	}
}

// rotate moves the active generation into draining and installs a fresh one.
// It is O(1); writers immediately continue on the new generation.
func (w *WriteBehindBuffer) rotate() {
	w.mu.Lock()
	if w.opCount == 0 {
		w.mu.Unlock()
		return
	}
	w.active.ops = w.opCount
	w.draining = append(w.draining, w.active)
	w.active = newCommitBuffer()
	w.opCount = 0
	w.mu.Unlock()
}

// targetSizeLocked returns the current size threshold. Caller holds w.mu.
func (w *WriteBehindBuffer) targetSizeLocked() int {
	if w.drainRate <= 0 {
		return w.effectiveMaxLocked() // no measured throughput yet: trust the cap
	}
	target := int(w.drainRate * w.curInterval.Seconds())
	if target < minAdaptiveOps {
		target = minAdaptiveOps
	}
	if max := w.effectiveMaxLocked(); target > max {
		target = max
	}
	return target
}

// effectiveMaxLocked returns the generation size cap: the manual max when
// configured, otherwise the auto default. Caller holds w.mu.
func (w *WriteBehindBuffer) effectiveMaxLocked() int {
	if w.maxOps > 0 {
		return w.maxOps
	}
	return defaultWriteBehindMaxOps
}

func (w *WriteBehindBuffer) run() {
	defer w.wg.Done()
	timer := time.NewTimer(w.interval)
	defer timer.Stop()
	reset := func() {
		w.mu.RLock()
		d := w.curInterval
		w.mu.RUnlock()
		timer.Reset(d)
	}
	for {
		select {
		case <-w.done:
			return
		case <-timer.C:
			w.rotate()
			w.drainOne()
			reset()
		case <-w.flushCh:
			w.rotate()
			w.drainOne()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			reset()
		}
	}
}

// drainOne replays the oldest draining generation, if any.
func (w *WriteBehindBuffer) drainOne() {
	w.mu.RLock()
	buf := (*CommitBuffer)(nil)
	if len(w.draining) > 0 {
		buf = w.draining[0]
	}
	w.mu.RUnlock()
	if buf == nil {
		return
	}
	if buf.IsApplied() {
		w.removeDrained(buf)
		return
	}
	if !buf.claimDrain() {
		return // another flusher (or Flush) is applying it
	}
	if w.apply != nil {
		start := time.Now()
		err := w.apply(buf)
		if err != nil {
			w.setLastErr(err)
			buf.releaseDrain() // retry on the next cycle
			return
		}
		// Throughput feedback, under mu only for the few value writes: a
		// drain of size S taking T seconds implies a sustainable generation
		// size of S/T × delay, and the delay itself moves toward the
		// measured drain latency.
		var warnMsg string
		w.mu.Lock()
		if buf.ops > 0 {
			dur := time.Since(start)
			inst := float64(buf.ops) / dur.Seconds()
			if w.drainRate <= 0 {
				w.drainRate = inst
				w.drainDur = dur
			} else {
				w.drainRate = 0.5*w.drainRate + 0.5*inst
				w.drainDur = time.Duration(0.5*float64(w.drainDur) + 0.5*float64(dur))
			}
			w.curInterval = w.drainDur
			if w.curInterval < w.minDelay {
				w.curInterval = w.minDelay
			}
			if w.curInterval > w.maxDelay {
				w.curInterval = w.maxDelay
			}
			w.targetOps = w.targetSizeLocked()
			warnMsg = w.sizingWarningLocked()
		}
		w.mu.Unlock()
		if warnMsg != "" && w.warn != nil {
			w.warn(warnMsg)
		}
	}
	w.setLastErr(nil) // this generation applied; a prior failure is resolved
	buf.markApplied()
	w.removeDrained(buf)
}

func (w *WriteBehindBuffer) setLastErr(err error) {
	w.errMu.Lock()
	w.lastErr = err
	w.errMu.Unlock()
}

// removeDrained drops the applied generation from the draining queue and
// retires its nodes from the pending-node index exactly once: the committed
// schema index covers them from the replay onward.
func (w *WriteBehindBuffer) removeDrained(buf *CommitBuffer) {
	w.mu.Lock()
	// The generation's writes are committed: the schema's committed index
	// now covers its nodes, so retire them from the pending index exactly
	// once. A later generation may already have replaced a node's entries;
	// the repair re-adds the newest still-pending version.
	if w.pendingIndex != nil && buf.retired.CompareAndSwap(false, true) {
		for _, c := range buf.commits {
			for _, op := range c.ops {
				switch op.Type {
				case OpCreateNode, OpUpdateNode:
					w.pendingIndex.remove(op.Node)
					if node, ok := w.nodeLocked(op.Node.ID); ok {
						w.pendingIndex.add(node)
					}
				case OpDeleteNode:
					w.pendingIndex.remove(op.OldNode)
					if node, ok := w.nodeLocked(op.NodeID); ok {
						w.pendingIndex.add(node)
					}
				}
			}
		}
	}
	if len(w.draining) > 0 && w.draining[0] == buf {
		w.draining = w.draining[1:]
	}
	// A burst fills the pending index with map buckets Go never shrinks;
	// drop it entirely once every generation has retired, so an idle
	// buffer returns its memory and the next lookup rebuilds it lazily.
	if w.pendingIndex != nil && w.pendingIndex.empty() {
		w.pendingIndex = nil
	}
	w.mu.Unlock()
}

// nodeLocked resolves the newest buffered version of id across the active
// and unapplied generations. Caller holds w.mu.
func (w *WriteBehindBuffer) nodeLocked(id NodeID) (*Node, bool) {
	state := w.entityStateLocked(bufferedNodeEntity, string(id))
	return state.node, state.found && !state.deleted
}

// pendingWriteSource: schemas attach the buffer so property-index (and
// ordered / not-null) lookups merge acknowledged-but-unflushed nodes exactly
// as the old AsyncEngine's pending writes did (#719). The Pokec batched
// load exposed the gap: MATCH (n:User {id: $x}) seeks the User(id) index
// while the user writes still sit in the buffer, matches nothing, and the
// CREATE silently produces no relationship.

// lockPendingWrites holds the pending nodes still until unlockPendingWrites,
// so a lookup combines the committed index with a stable buffer view.
func (w *WriteBehindBuffer) lockPendingWrites() pendingWriteView {
	w.mu.RLock()
	return pendingWriteView{index: w.pendingIndex, owner: w}
}

// unlockPendingWrites releases the lock pendingWrites took.
func (w *WriteBehindBuffer) unlockPendingWrites() {
	w.mu.RUnlock()
}

// trackPendingValues indexes these (label, property) pairs of buffered
// nodes by value from now on, including the nodes already buffered.
func (w *WriteBehindBuffer) trackPendingValues(pairs []pendingPropertyKey) {
	if len(pairs) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pendingIndex == nil {
		w.pendingIndex = newPendingNodeIndex()
		// List every buffered node that has not reached the store yet;
		// applied generations are already in the committed index.
		for _, buf := range w.draining {
			if buf.IsApplied() {
				continue
			}
			for _, node := range buf.nodes {
				w.pendingIndex.add(node)
			}
		}
		for _, node := range w.active.nodes {
			w.pendingIndex.add(node)
		}
	}
	for _, pair := range pairs {
		w.pendingIndex.track(pair.label, pair.property, func(id NodeID) *Node {
			node, _ := w.nodeLocked(id)
			return node
		})
		if !slices.Contains(w.trackedPairs, pair) {
			w.trackedPairs = append(w.trackedPairs, pair)
		}
	}
}

// KeyCandidates returns the newest buffered version of every unflushed node
// with label whose property has valueKey (an indexValueKey), looked up in the
// pending value index (the pair is tracked from its first use) instead of by
// copying every buffered node of the label: constraint validation calls it on
// every write of a bulk load.
func (w *WriteBehindBuffer) KeyCandidates(label, property string, valueKey interface{}) []*Node {
	pair := pendingPropertyKey{label: label, property: property}
	w.mu.RLock()
	tracked := slices.Contains(w.trackedPairs, pair)
	w.mu.RUnlock()
	if !tracked {
		w.trackPendingValues([]pendingPropertyKey{pair})
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.pendingIndex == nil {
		return nil // nothing unflushed (the index is released once empty)
	}
	ids := w.pendingIndex.byValue[pendingValueKey{label: label, property: property, value: valueKey}]
	out := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if node, ok := w.newestNodeLocked(id); ok {
			out = append(out, node)
		}
	}
	return out
}

// newestNodeLocked is the newest buffered version of id across the unapplied
// generations, or false when the newest buffered write deleted it or none
// holds it. Caller holds w.mu.
func (w *WriteBehindBuffer) newestNodeLocked(id NodeID) (*Node, bool) {
	return w.nodeLocked(id)
}

// nodeSupersededLocked reports whether a committed index entry for id is
// stale: the buffer created, rewrote or deleted the node. The caller holds
// the pending-write lock (w.mu), so no locking here.
func (w *WriteBehindBuffer) nodeSupersededLocked(id NodeID) bool {
	return w.entityStateLocked(bufferedNodeEntity, string(id)).found
}

// Flush rotates the active generation and drains every generation
// synchronously. It retries failed applies; if no generation makes progress
// for ~2s it returns the last apply error.
func (w *WriteBehindBuffer) Flush() error {
	w.rotate()
	w.signal()
	lastPending := -1
	stalls := 0
	for {
		w.drainOne()
		w.mu.RLock()
		pending := len(w.draining)
		w.mu.RUnlock()
		if pending == 0 {
			return nil
		}
		if pending == lastPending {
			stalls++
		} else {
			stalls = 0
		}
		lastPending = pending
		if stalls > 2000 {
			return w.LastErr()
		}
		time.Sleep(time.Millisecond)
	}
}

// LastErr returns the most recent apply error, if any.
func (w *WriteBehindBuffer) LastErr() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.lastErr
}

// Close stops the background flusher. Callers must Flush first to drain.
func (w *WriteBehindBuffer) Close() {
	w.closeMu.Lock()
	if w.closed {
		w.closeMu.Unlock()
		return
	}
	w.closed = true
	close(w.done)
	w.closeMu.Unlock()
	w.wg.Wait()
}

// GetNode resolves the latest buffered state for id across generations,
// newest first. Returns (value, found, deleted).
func (w *WriteBehindBuffer) GetNode(id NodeID) (*Node, bool, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	state := w.entityStateLocked(bufferedNodeEntity, string(id))
	return state.node, state.found, state.deleted
}

// GetEdge resolves the latest buffered state for id across generations,
// newest first. Returns (value, found, deleted).
func (w *WriteBehindBuffer) GetEdge(id EdgeID) (*Edge, bool, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	state := w.entityStateLocked(bufferedEdgeEntity, string(id))
	return state.edge, state.found, state.deleted
}

// LabelNodes returns buffered nodes carrying label, newest generation first,
// not yet applied. The caller merges these over committed rows.
func (w *WriteBehindBuffer) LabelNodes(label string) []*Node {
	nodes, _ := w.LabelOverlay(label)
	return nodes
}

// LabelOverlay returns the buffered state for label as (nodes, touched):
// nodes holds the newest buffered version of every ID the buffer wrote for
// the label (IDs the buffer deleted are excluded), newest generation first;
// touched holds every ID the buffer created, updated or deleted for the
// label. Callers merge over committed rows: drop committed rows whose ID is
// in touched and append the returned nodes. The returned node pointers are
// buffer-owned; callers must copy before handing them out.
func (w *WriteBehindBuffer) LabelOverlay(label string) ([]*Node, map[NodeID]bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	touched := make(map[NodeID]bool)
	var out []*Node
	w.visitEntityOverlayLocked(bufferedNodeEntity, func(buf *CommitBuffer, emit func(string)) {
		ids := buf.nodesByLabel[label]
		for i := len(ids) - 1; i >= 0; i-- {
			emit(string(ids[i]))
		}
	}, func(id string, state bufferedEntityState) {
		touched[NodeID(id)] = true
		if !state.deleted && hasLabel(state.node.Labels, label) {
			out = append(out, state.node)
		}
	})
	return out, touched
}

// TypeOverlay is LabelOverlay for buffered edges of edgeType.
func (w *WriteBehindBuffer) TypeOverlay(edgeType string) ([]*Edge, map[EdgeID]bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	touched := make(map[EdgeID]bool)
	var out []*Edge
	w.visitEntityOverlayLocked(bufferedEdgeEntity, func(buf *CommitBuffer, emit func(string)) {
		ids := buf.edgesByType[edgeType]
		for i := len(ids) - 1; i >= 0; i-- {
			emit(string(ids[i]))
		}
	}, func(id string, state bufferedEntityState) {
		touched[EdgeID(id)] = true
		if !state.deleted && state.edge.Type == edgeType {
			out = append(out, state.edge)
		}
	})
	return out, touched
}

// LookupNode resolves the newest buffered version of id, if the buffer has
// one. The returned pointer is buffer-owned; callers must copy it.
func (w *WriteBehindBuffer) LookupNode(id NodeID) (*Node, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.nodeLocked(id)
}

// NodeDeleted reports whether the buffer's newest state for id is a delete.
func (w *WriteBehindBuffer) NodeDeleted(id NodeID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.entityStateLocked(bufferedNodeEntity, string(id)).deleted
}

// LookupEdge resolves the newest buffered version of id, if the buffer has
// one. The returned pointer is buffer-owned; callers must copy it.
func (w *WriteBehindBuffer) LookupEdge(id EdgeID) (*Edge, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	state := w.entityStateLocked(bufferedEdgeEntity, string(id))
	return state.edge, state.found && !state.deleted
}

// EdgeDeleted reports whether the buffer's newest state for id is a delete.
func (w *WriteBehindBuffer) EdgeDeleted(id EdgeID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.entityStateLocked(bufferedEdgeEntity, string(id)).deleted
}

// AllNodesOverlay returns the buffered node state as (nodes, touched):
// nodes holds the newest buffered version of every node ID the buffer wrote
// (deleted IDs excluded), newest generation first; touched holds every node
// ID the buffer created, updated or deleted. Callers merge over committed
// rows: drop committed rows whose ID is in touched and append the returned
// nodes. The returned pointers are buffer-owned; callers must copy them.
func (w *WriteBehindBuffer) AllNodesOverlay() ([]*Node, map[NodeID]bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	touched := make(map[NodeID]bool)
	var out []*Node
	w.visitEntityOverlayLocked(bufferedNodeEntity, func(buf *CommitBuffer, emit func(string)) {
		for id := range buf.nodes {
			emit(string(id))
		}
	}, func(id string, state bufferedEntityState) {
		touched[NodeID(id)] = true
		if !state.deleted {
			out = append(out, state.node)
		}
	})
	return out, touched
}

// AllEdgesOverlay is AllNodesOverlay for buffered edges.
func (w *WriteBehindBuffer) AllEdgesOverlay() ([]*Edge, map[EdgeID]bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	touched := make(map[EdgeID]bool)
	var out []*Edge
	w.visitEntityOverlayLocked(bufferedEdgeEntity, func(buf *CommitBuffer, emit func(string)) {
		for id := range buf.edges {
			emit(string(id))
		}
	}, func(id string, state bufferedEntityState) {
		touched[EdgeID(id)] = true
		if !state.deleted {
			out = append(out, state.edge)
		}
	})
	return out, touched
}

// LabelCountDelta sums the derived label-count deltas of every buffered
// commit that has not been replayed yet, across all namespaces. The replay
// applies the same deltas to the persisted counters when it lands the
// generation, so reads see committed counts + this delta.
func (w *WriteBehindBuffer) LabelCountDelta(label string) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var total int64
	sum := func(buf *CommitBuffer) {
		if buf.countsApplied.Load() {
			return
		}
		for _, c := range buf.commits {
			for key, delta := range c.labelDeltas {
				if key.label == label {
					total += delta
				}
			}
		}
	}
	sum(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		sum(w.draining[i])
	}
	return total
}

// LabelCountDeltaInNamespace is LabelCountDelta restricted to namespace.
func (w *WriteBehindBuffer) LabelCountDeltaInNamespace(namespace, label string) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var total int64
	sum := func(buf *CommitBuffer) {
		if buf.countsApplied.Load() {
			return
		}
		for _, c := range buf.commits {
			if delta, ok := c.labelDeltas[namespaceLabel{namespace: namespace, label: label}]; ok {
				total += delta
			}
		}
	}
	sum(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		sum(w.draining[i])
	}
	return total
}

// EdgeTypeCountDelta sums the derived edge-type count deltas of every
// buffered commit that has not been replayed yet, across all namespaces.
func (w *WriteBehindBuffer) EdgeTypeCountDelta(edgeType string) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var total int64
	sum := func(buf *CommitBuffer) {
		if buf.countsApplied.Load() {
			return
		}
		for _, c := range buf.commits {
			for key, delta := range c.edgeTypeDeltas {
				if key.edgeType == edgeType {
					total += delta
				}
			}
		}
	}
	sum(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		sum(w.draining[i])
	}
	return total
}

// EdgeTypeCountDeltaInNamespace is EdgeTypeCountDelta restricted to
// namespace.
func (w *WriteBehindBuffer) EdgeTypeCountDeltaInNamespace(namespace, edgeType string) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var total int64
	sum := func(buf *CommitBuffer) {
		if buf.countsApplied.Load() {
			return
		}
		for _, c := range buf.commits {
			if delta, ok := c.edgeTypeDeltas[namespaceEdgeType{namespace: namespace, edgeType: edgeType}]; ok {
				total += delta
			}
		}
	}
	sum(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		sum(w.draining[i])
	}
	return total
}

// TypeEdges returns buffered edges of edgeType, newest generation first,
// not yet applied.
func (w *WriteBehindBuffer) TypeEdges(edgeType string) []*Edge {
	edges, _ := w.TypeOverlay(edgeType)
	return edges
}

// liveBuffers returns the active generation and every draining generation
// that has not been applied, newest first. The caller must hold w.mu.
func (w *WriteBehindBuffer) liveBuffers() []*CommitBuffer {
	buffers := make([]*CommitBuffer, 0, len(w.draining)+1)
	buffers = append(buffers, w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if !w.draining[i].IsApplied() {
			buffers = append(buffers, w.draining[i])
		}
	}
	return buffers
}

// NodeDeletedOverlay reports whether a buffered delete hides nodeID from
// committed reads (PR #1043 read-overlay parity).
func (w *WriteBehindBuffer) NodeDeletedOverlay(nodeID NodeID) bool {
	return w.NodeDeleted(nodeID)
}

// EdgeAdjacencyOverlay returns the buffered live edges adjacent to nodeID
// (outgoing or incoming) plus every buffered edge ID that committed adjacency
// scans must suppress, including live IDs shadowed by the buffer.
// nodeDeleted reports that a buffered delete
// hides the node itself, which hides all of its committed adjacency too.
// The returned edge pointers are buffer-owned; callers must copy them.
func (w *WriteBehindBuffer) EdgeAdjacencyOverlay(nodeID NodeID, outgoing bool) (live []*Edge, suppressed map[EdgeID]struct{}, nodeDeleted bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	nodeDeleted = w.entityStateLocked(bufferedNodeEntity, string(nodeID)).deleted
	suppressed = make(map[EdgeID]struct{})
	w.visitEntityOverlayLocked(bufferedEdgeEntity, func(buf *CommitBuffer, emit func(string)) {
		ids := buf.incomingByNode[nodeID]
		if outgoing {
			ids = buf.outgoingByNode[nodeID]
		}
		for _, id := range ids {
			emit(string(id))
		}
	}, func(id string, state bufferedEntityState) {
		// Both replacements and tombstones shadow the committed identity.
		suppressed[EdgeID(id)] = struct{}{}
		if state.deleted || nodeDeleted {
			return
		}
		edge := state.edge
		if (outgoing && edge.StartNode == nodeID) || (!outgoing && edge.EndNode == nodeID) {
			live = append(live, edge)
		}
	})
	return live, suppressed, nodeDeleted
}

// EdgesBetweenOverlay returns the buffered live edges from source to target
// and the deleted-edge set committed between-index results must suppress.
// sourceDeleted / targetDeleted report buffered deletes of the endpoints,
// which hide all committed between results for them.
func (w *WriteBehindBuffer) EdgesBetweenOverlay(source, target NodeID) (live []*Edge, suppressed map[EdgeID]struct{}, sourceDeleted, targetDeleted bool) {
	live, suppressed, _ = w.EdgeAdjacencyOverlay(source, true)
	filtered := live[:0]
	for _, e := range live {
		if e.EndNode == target {
			filtered = append(filtered, e)
		}
	}
	sourceDeleted = w.NodeDeletedOverlay(source)
	targetDeleted = w.NodeDeletedOverlay(target)
	return filtered, suppressed, sourceDeleted, targetDeleted
}

// EdgeCountDelta is the buffered total edge-count delta: creates minus
// deletes across every generation that has not been replayed yet.
func (w *WriteBehindBuffer) EdgeCountDelta() int64 {
	return w.totalCountDeltaForPrefix("", true)
}

// EdgeTypeLabelCountDeltaInNamespace sums the buffered positional
// (label, type) count deltas of one tier (start or end label) in one
// namespace, for reads answered from the positional counters.
func (w *WriteBehindBuffer) EdgeTypeLabelCountDeltaInNamespace(sub byte, namespace, label, edgeType string) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var total int64
	sum := func(buf *CommitBuffer) {
		if buf.countsApplied.Load() {
			return
		}
		for _, c := range buf.commits {
			if c.edgeTypeLabelDeltas == nil {
				continue
			}
			for key, delta := range c.edgeTypeLabelDeltas {
				if key.sub == sub && key.namespace == namespace && key.label == label && key.edgeType == edgeType {
					total += delta
				}
			}
		}
	}
	for _, buf := range w.liveBuffers() {
		sum(buf)
	}
	return total
}

// EdgeTypeLabelCountDelta is EdgeTypeLabelCountDeltaInNamespace across every
// namespace.
func (w *WriteBehindBuffer) EdgeTypeLabelCountDelta(sub byte, label, edgeType string) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var total int64
	sum := func(buf *CommitBuffer) {
		if buf.countsApplied.Load() {
			return
		}
		for _, c := range buf.commits {
			if c.edgeTypeLabelDeltas == nil {
				continue
			}
			for key, delta := range c.edgeTypeLabelDeltas {
				if key.sub == sub && key.label == label && key.edgeType == edgeType {
					total += delta
				}
			}
		}
	}
	for _, buf := range w.liveBuffers() {
		sum(buf)
	}
	return total
}

// EdgeCountDeltaForPrefix is the buffered total edge-count delta restricted to
// edges whose ID starts with prefix: live buffered edge IDs minus buffered
// deleted edge IDs. prefix may be empty for the whole graph.
func (w *WriteBehindBuffer) EdgeCountDeltaForPrefix(prefix string) int64 {
	return w.totalCountDeltaForPrefix(prefix, true)
}

// NodeCountDeltaForPrefix is the buffered total node-count delta restricted to
// nodes whose ID starts with prefix: live buffered node IDs minus buffered
// deleted node IDs. prefix may be empty for the whole graph.
func (w *WriteBehindBuffer) NodeCountDeltaForPrefix(prefix string) int64 {
	return w.totalCountDeltaForPrefix(prefix, false)
}

// PendingOps reports how many mutations are buffered but not yet applied.
func (w *WriteBehindBuffer) PendingOps() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	total := w.opCount
	for _, buf := range w.draining {
		if !buf.IsApplied() {
			total += len(buf.Mutations())
		}
	}
	return total
}

// TargetSize returns the adaptive size threshold that rotates the active
// generation: measured drain throughput × interval, clamped to the
// configured maximum. It equals the effective cap until the first
// successful apply.
func (w *WriteBehindBuffer) TargetSize() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.targetSizeLocked()
}

// DrainOpsPerSecond returns the EWMA drain throughput in operations per
// second; zero until the first successful apply.
func (w *WriteBehindBuffer) DrainOpsPerSecond() float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.drainRate
}

// CurrentInterval returns the adaptive rotation delay: the EWMA measured
// drain latency, clamped to [minDelay, maxDelay]. It equals the configured
// interval until the first successful apply.
func (w *WriteBehindBuffer) CurrentInterval() time.Duration {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.curInterval
}

// SetWarn installs the sizing-mismatch reporter. The engine wires it to its
// logger at construction; the callback runs outside the buffer lock.
func (w *WriteBehindBuffer) SetWarn(warn func(string)) {
	w.mu.Lock()
	w.warn = warn
	w.mu.Unlock()
}

// sizingWarningLocked reports a configuration whose relationship with the
// measured throughput changed since the last drain: 1 when the manual max
// binds (natural interval size exceeds it, so the size rotation effectively
// shortens the delay), 2 when it never binds (it has no sizing effect),
// 3 when the adaptive delay is pinned at its bound because the measured
// drain latency keeps exceeding it. Returns "" when there is nothing new to
// say. Caller holds w.mu.
func (w *WriteBehindBuffer) sizingWarningLocked() string {
	if w.drainRate <= 0 {
		return ""
	}
	natural := int(w.drainRate * w.curInterval.Seconds())
	state := 0
	switch {
	case w.maxOps > 0 && natural > w.maxOps:
		state = 1
	case w.maxOps > 0 && natural < w.maxOps/4:
		state = 2
	case w.drainDur > w.maxDelay:
		state = 3
	}
	if state == w.warnState {
		return ""
	}
	w.warnState = state
	switch state {
	case 1:
		effectiveDelay := w.curInterval.Seconds() * float64(w.maxOps) / float64(natural)
		return fmt.Sprintf(
			"write-behind sizing: fixed max %d ops binds at ~%d ops per flush interval (%v); effective flush delay ~%.1fms — a smaller max increases flush frequency and per-flush overhead",
			w.maxOps, natural, w.curInterval, effectiveDelay*1000)
	case 2:
		return fmt.Sprintf(
			"write-behind sizing: fixed max %d ops never binds at ~%d ops per flush interval (%v); the configured max currently has no sizing effect",
			w.maxOps, natural, w.curInterval)
	case 3:
		return fmt.Sprintf(
			"write-behind flush delay pinned at %v: measured drain latency %v exceeds the bound; the flusher cannot keep up with the write rate",
			w.maxDelay, w.drainDur)
	}
	return ""
}
