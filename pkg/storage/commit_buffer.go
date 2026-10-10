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
	namespace string
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
	deletes map[string]bool
	order   []bufferedMutation

	nodesByLabel map[string][]NodeID
	edgesByType  map[string][]EdgeID

	// labelSeen / typeSeen make overlay index inserts O(1): without them,
	// every op would scan the label's/type's accumulated ID slice for
	// duplicates (O(n²) per generation, string compares on hot paths).
	labelSeen map[labelSeenKey]struct{}
	typeSeen  map[typeSeenKey]struct{}

	// ops counts the buffered mutations this generation carries; the
	// adaptive sizer uses it to measure drain throughput.
	ops int

	applied  atomic.Bool
	draining atomic.Bool // claims the single flusher slot for this generation
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
		nodes:        make(map[NodeID]*Node),
		edges:        make(map[EdgeID]*Edge),
		deletes:      make(map[string]bool),
		nodesByLabel: make(map[string][]NodeID),
		edgesByType:  make(map[string][]EdgeID),
		labelSeen:    make(map[labelSeenKey]struct{}),
		typeSeen:     make(map[typeSeenKey]struct{}),
	}
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
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil && op.Node.ID != "" {
				b.nodes[op.Node.ID] = op.Node
				for _, label := range op.Node.Labels {
					key := labelSeenKey{label: label, id: op.Node.ID}
					if _, seen := b.labelSeen[key]; !seen {
						b.labelSeen[key] = struct{}{}
						b.nodesByLabel[label] = append(b.nodesByLabel[label], op.Node.ID)
					}
				}
			}
		case OpDeleteNode:
			b.deletes[string(op.NodeID)] = true
			// DETACH DELETE tombstones the node's relationships too. Fold
			// them into the generation's delete set so the overlay hides
			// the committed rows immediately and later buffered batches
			// that still see them through the adjacency index do not
			// delete (and count) them a second time.
			for _, edgeID := range op.DeletedEdgeIDs {
				if edgeID != "" {
					b.deletes[string(edgeID)] = true
				}
			}
			if op.OldNode != nil {
				// Mark the deleted node's labels as touched so scan overlays
				// drop the committed row too, not just the buffered one.
				for _, label := range op.OldNode.Labels {
					key := labelSeenKey{label: label, id: op.NodeID}
					if _, seen := b.labelSeen[key]; !seen {
						b.labelSeen[key] = struct{}{}
						b.nodesByLabel[label] = append(b.nodesByLabel[label], op.NodeID)
					}
				}
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge != nil && op.Edge.ID != "" {
				b.edges[op.Edge.ID] = op.Edge
				key := typeSeenKey{edgeType: op.Edge.Type, id: op.Edge.ID}
				if _, seen := b.typeSeen[key]; !seen {
					b.typeSeen[key] = struct{}{}
					b.edgesByType[op.Edge.Type] = append(b.edgesByType[op.Edge.Type], op.Edge.ID)
				}
			}
		case OpDeleteEdge:
			b.deletes[string(op.EdgeID)] = true
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
	copyNode := *n
	b.nodes[n.ID] = &copyNode
	b.order = append(b.order, bufferedMutation{kind: bufferedOpNode, id: string(n.ID), node: &copyNode})
	for _, label := range n.Labels {
		b.nodesByLabel[label] = append(b.nodesByLabel[label], n.ID)
	}
}

func (b *CommitBuffer) addEdge(e *Edge) {
	if e == nil || e.ID == "" {
		return
	}
	copyEdge := *e
	b.edges[e.ID] = &copyEdge
	b.order = append(b.order, bufferedMutation{kind: bufferedOpEdge, id: string(e.ID), edge: &copyEdge})
	b.edgesByType[e.Type] = append(b.edgesByType[e.Type], e.ID)
}

func (b *CommitBuffer) addDelete(key string) {
	if key == "" {
		return
	}
	b.deletes[key] = true
	b.order = append(b.order, bufferedMutation{kind: bufferedOpDelete, id: key})
}

// getNode resolves the latest buffered state for id in this generation.
// Returns (value, found, deleted). found is false when this generation has
// no entry for id.
func (b *CommitBuffer) getNode(id NodeID) (*Node, bool, bool) {
	if b.deletes[string(id)] {
		return nil, true, true
	}
	if n, ok := b.nodes[id]; ok {
		return n, true, false
	}
	return nil, false, false
}

// getEdge resolves the latest buffered state for id in this generation.
func (b *CommitBuffer) getEdge(id EdgeID) (*Edge, bool, bool) {
	if b.deletes[string(id)] {
		return nil, true, true
	}
	if e, ok := b.edges[id]; ok {
		return e, true, false
	}
	return nil, false, false
}

// labelNodes lists buffered nodes carrying label, newest append first.
func (b *CommitBuffer) labelNodes(label string) []*Node {
	ids := b.nodesByLabel[label]
	if len(ids) == 0 {
		return nil
	}
	out := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if b.deletes[string(id)] {
			continue
		}
		if n, ok := b.nodes[id]; ok {
			out = append(out, n)
		}
	}
	return out
}

// typeEdges lists buffered edges of the given type, newest append first.
func (b *CommitBuffer) typeEdges(edgeType string) []*Edge {
	ids := b.edgesByType[edgeType]
	if len(ids) == 0 {
		return nil
	}
	out := make([]*Edge, 0, len(ids))
	for _, id := range ids {
		if b.deletes[string(id)] {
			continue
		}
		if e, ok := b.edges[id]; ok {
			out = append(out, e)
		}
	}
	return out
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
// configured (maxOps sentinel 0): ~2M mutations is comfortably above one
// interval of work at any measured drain rate and bounds unflushed memory.
const defaultWriteBehindMaxOps = 2 << 20

// defaultWriteBehindMinDelay floors the adaptive flush delay: rotating
// faster than this buys nothing and multiplies per-flush fixed costs.
const defaultWriteBehindMinDelay = 5 * time.Millisecond

// defaultWriteBehindMaxDelay caps the adaptive flush delay: the durability
// loss window and unflushed memory stay bounded even if drains slow down.
const defaultWriteBehindMaxDelay = 30 * time.Second

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

func (w *WriteBehindBuffer) removeDrained(buf *CommitBuffer) {
	w.mu.Lock()
	if len(w.draining) > 0 && w.draining[0] == buf {
		w.draining = w.draining[1:]
	}
	w.mu.Unlock()
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
	if n, found, deleted := w.active.getNode(id); found {
		return n, true, deleted
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		if n, found, deleted := w.draining[i].getNode(id); found {
			return n, true, deleted
		}
	}
	return nil, false, false
}

// GetEdge resolves the latest buffered state for id across generations,
// newest first. Returns (value, found, deleted).
func (w *WriteBehindBuffer) GetEdge(id EdgeID) (*Edge, bool, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if e, found, deleted := w.active.getEdge(id); found {
		return e, true, deleted
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		if e, found, deleted := w.draining[i].getEdge(id); found {
			return e, true, deleted
		}
	}
	return nil, false, false
}

// LabelNodes returns buffered nodes carrying label, newest generation first,
// not yet applied. The caller merges these over committed rows.
func (w *WriteBehindBuffer) LabelNodes(label string) []*Node {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var out []*Node
	out = append(out, w.active.labelNodes(label)...)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		out = append(out, w.draining[i].labelNodes(label)...)
	}
	return out
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
	seen := make(map[NodeID]bool)
	var out []*Node
	collect := func(buf *CommitBuffer) {
		ids := buf.nodesByLabel[label]
		for i := len(ids) - 1; i >= 0; i-- {
			id := ids[i]
			touched[id] = true
			if seen[id] {
				continue
			}
			seen[id] = true
			if buf.deletes[string(id)] {
				continue
			}
			if n, ok := buf.nodes[id]; ok {
				out = append(out, n)
			}
		}
	}
	collect(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		collect(w.draining[i])
	}
	return out, touched
}

// TypeOverlay is LabelOverlay for buffered edges of edgeType.
func (w *WriteBehindBuffer) TypeOverlay(edgeType string) ([]*Edge, map[EdgeID]bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	touched := make(map[EdgeID]bool)
	seen := make(map[EdgeID]bool)
	var out []*Edge
	collect := func(buf *CommitBuffer) {
		ids := buf.edgesByType[edgeType]
		for i := len(ids) - 1; i >= 0; i-- {
			id := ids[i]
			touched[id] = true
			if seen[id] {
				continue
			}
			seen[id] = true
			if buf.deletes[string(id)] {
				continue
			}
			if e, ok := buf.edges[id]; ok {
				out = append(out, e)
			}
		}
		// Tombstones hide committed rows: a DETACH DELETE folds its
		// relationships' IDs into the delete set without a type, so
		// every buffered delete must shadow the committed row regardless
		// of type (the ID spaces of nodes and edges never collide).
		for key := range buf.deletes {
			touched[EdgeID(key)] = true
		}
	}
	collect(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		collect(w.draining[i])
	}
	return out, touched
}

// LookupNode resolves the newest buffered version of id, if the buffer has
// one. The returned pointer is buffer-owned; callers must copy it.
func (w *WriteBehindBuffer) LookupNode(id NodeID) (*Node, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if n, ok := w.active.nodes[id]; ok {
		return n, true
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		buf := w.draining[i]
		if buf.IsApplied() {
			continue
		}
		if n, ok := buf.nodes[id]; ok {
			return n, true
		}
	}
	return nil, false
}

// NodeDeleted reports whether the buffer's newest state for id is a delete.
func (w *WriteBehindBuffer) NodeDeleted(id NodeID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.active.deletes[string(id)] {
		return true
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		buf := w.draining[i]
		if buf.IsApplied() {
			continue
		}
		if buf.deletes[string(id)] {
			return true
		}
	}
	return false
}

// LookupEdge resolves the newest buffered version of id, if the buffer has
// one. The returned pointer is buffer-owned; callers must copy it.
func (w *WriteBehindBuffer) LookupEdge(id EdgeID) (*Edge, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if e, ok := w.active.edges[id]; ok {
		return e, true
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		buf := w.draining[i]
		if buf.IsApplied() {
			continue
		}
		if e, ok := buf.edges[id]; ok {
			return e, true
		}
	}
	return nil, false
}

// EdgeDeleted reports whether the buffer's newest state for id is a delete.
func (w *WriteBehindBuffer) EdgeDeleted(id EdgeID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.active.deletes[string(id)] {
		return true
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		buf := w.draining[i]
		if buf.IsApplied() {
			continue
		}
		if buf.deletes[string(id)] {
			return true
		}
	}
	return false
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
	seen := make(map[NodeID]bool)
	var out []*Node
	collect := func(buf *CommitBuffer) {
		for id := range buf.nodes {
			touched[id] = true
			if seen[id] {
				continue
			}
			seen[id] = true
			if buf.deletes[string(id)] {
				continue
			}
			out = append(out, buf.nodes[id])
		}
		for key := range buf.deletes {
			touched[NodeID(key)] = true
		}
	}
	collect(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		collect(w.draining[i])
	}
	return out, touched
}

// AllEdgesOverlay is AllNodesOverlay for buffered edges.
func (w *WriteBehindBuffer) AllEdgesOverlay() ([]*Edge, map[EdgeID]bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	touched := make(map[EdgeID]bool)
	seen := make(map[EdgeID]bool)
	var out []*Edge
	collect := func(buf *CommitBuffer) {
		for id := range buf.edges {
			touched[id] = true
			if seen[id] {
				continue
			}
			seen[id] = true
			if buf.deletes[string(id)] {
				continue
			}
			out = append(out, buf.edges[id])
		}
		for key := range buf.deletes {
			touched[EdgeID(key)] = true
		}
	}
	collect(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		collect(w.draining[i])
	}
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
	w.mu.RLock()
	defer w.mu.RUnlock()
	var out []*Edge
	out = append(out, w.active.typeEdges(edgeType)...)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		out = append(out, w.draining[i].typeEdges(edgeType)...)
	}
	return out
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
