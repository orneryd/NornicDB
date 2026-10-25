package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Model-based proof of the write-behind generation lifecycle. Each seed runs
// a random interleaving of buffered (implicit), explicit and direct creates,
// updates, re-keys and deletes under a UNIQUE constraint, with flushes,
// concurrent flushes, Close/reopen, prefix deletion and listeners that write
// back or panic, against a sequential reference model. A seed that does not
// finish within its deadline is a deadlock and fails.
//
// NORNIC_WB_MODEL_SEEDS sets the seed count (default 20); NORNIC_WB_MODEL_SEED
// replays one seed.

const (
	wbModelOps      = 120
	wbModelDeadline = 90 * time.Second
)

var wbModelNamespaces = []string{"m1", "m2"}

type wbModelNode struct {
	tagged bool
	key    string // UNIQUE U.k; "" when absent
	cnt    int64
	rev    int64
}

type wbModelRun struct {
	t      *testing.T
	rng    *rand.Rand
	dir    string
	b      *BadgerEngine
	model  map[NodeID]wbModelNode
	log    []string
	seq    int64
	ivalue time.Duration

	listeners *wbModelListeners
	flushes   sync.WaitGroup
	// acknowledged revisions: buffered ones must be delivered when a flush
	// returns; synchronous ones are delivered by whichever goroutine drains.
	ackedBuffered map[int64]bool
	ackedSync     map[int64]bool
	deletes       map[NodeID]int // acknowledged deletes
}

// wbModelListeners records delivered events and performs the write-back.
type wbModelListeners struct {
	mu        sync.Mutex
	revs      map[int64]bool // delivered create/update revisions
	deletes   map[NodeID]int // delivered delete events
	echoRoute atomic.Int32   // 0 direct, 1 explicit
	echoErr   atomic.Value   // first write-back error
	echoAcked atomic.Int64   // successful write-backs
	srcEvents atomic.Int64   // delivered update events of the source node
	panicOn   atomic.Value   // NodeID whose create event panics
	b         atomic.Pointer[BadgerEngine]
}

func wbModelSeeds(t *testing.T) []int64 {
	if one := os.Getenv("NORNIC_WB_MODEL_SEED"); one != "" {
		seed, err := strconv.ParseInt(one, 10, 64)
		require.NoError(t, err)
		return []int64{seed}
	}
	count := 20
	if v := os.Getenv("NORNIC_WB_MODEL_SEEDS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		count = n
	}
	seeds := make([]int64, count)
	for i := range seeds {
		seeds[i] = int64(i + 1)
	}
	return seeds
}

// wbModelWithin runs body and fails the test when it does not finish: the
// goroutine dump names the blocked lifecycle step.
func wbModelWithin(t *testing.T, limit time.Duration, name string, body func()) {
	t.Helper()
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		body()
	}()
	select {
	case p := <-done:
		if p != nil {
			panic(p)
		}
	case <-time.After(limit):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("%s did not finish within %s (deadlock):\n%s", name, limit, buf[:n])
	}
}

func TestWriteBehindModelSequential(t *testing.T) {
	for _, seed := range wbModelSeeds(t) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			wbModelWithin(t, wbModelDeadline, "model seed", func() { runWriteBehindModel(t, seed) })
		})
	}
}

// This reference model predicts the complete event sequence from successful
// commits, independently of the observer. Every comparison consumes a captured
// reservation frontier, so reordering, loss and duplicate delivery all fail.
// The original lifecycle/state model below remains a separate control.
func TestWriteBehindModelOrderedHistory(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, seed := range wbModelSeeds(t) {
			t.Run(fmt.Sprintf("buffered=%v/seed=%d", buffered, seed), func(t *testing.T) {
				b := wbListenerEngine(t, buffered)
				defer b.Close()
				h := wbObserveHistory(b)
				rng := rand.New(rand.NewSource(seed))
				var commits []string
				nodes := map[NodeID]int64{}
				edges := map[EdgeID]int64{}
				var rev int64
				write := func(f func(*BadgerTransaction) error, direct func() error) {
					switch rng.Intn(3) {
					case 0:
						require.NoError(t, direct())
					case 1:
						require.NoError(t, wbListenerTx(b, false, f))
					case 2:
						require.NoError(t, wbListenerTx(b, true, f))
					}
				}
				create := func(id NodeID) {
					rev++
					n := &Node{ID: id, Properties: map[string]any{"rev": rev}}
					write(func(tx *BadgerTransaction) error { _, err := tx.CreateNode(n); return err }, func() error { _, err := b.CreateNode(n); return err })
					nodes[id] = rev
					commits = append(commits, fmt.Sprintf("create:%s:%d", id, rev))
				}
				barrier := func() {
					frontier, _ := b.listenerFrontier()
					require.NoError(t, b.LandWriteBehind())
					b.awaitCommitNotification(frontier)
					require.NoError(t, wbHistoryCompare(commits, h.snapshot()))
				}
				seedNodes := func() {
					for _, id := range []NodeID{"history:a", "history:b", "history:echo", "history:trigger"} {
						create(id)
					}
				}
				seedNodes()
				for step := 0; step < 80; step++ {
					id := NodeID(fmt.Sprintf("history:n%d", rng.Intn(4)))
					edgeID := EdgeID(fmt.Sprintf("history:e%d", rng.Intn(3)))
					switch rng.Intn(8) {
					case 0, 1:
						if _, exists := nodes[id]; !exists {
							create(id)
							break
						}
						rev++
						n := &Node{ID: id, Properties: map[string]any{"rev": rev}}
						write(func(tx *BadgerTransaction) error { return tx.UpdateNode(n) }, func() error { return b.UpdateNode(n) })
						nodes[id] = rev
						commits = append(commits, fmt.Sprintf("update:%s:%d", id, rev))
					case 2:
						if _, exists := nodes[id]; exists {
							write(func(tx *BadgerTransaction) error { return tx.DeleteNode(id) }, func() error { return b.DeleteNode(id) })
							delete(nodes, id)
							commits = append(commits, "delete:"+string(id))
						}
					case 3, 4:
						rev++
						e := &Edge{ID: edgeID, StartNode: "history:a", EndNode: "history:b", Type: "R", Properties: map[string]any{"rev": rev}}
						if _, exists := edges[edgeID]; exists {
							write(func(tx *BadgerTransaction) error { return tx.UpdateEdge(e) }, func() error { return b.UpdateEdge(e) })
							commits = append(commits, fmt.Sprintf("edge-update:%s:%d", edgeID, rev))
						} else {
							write(func(tx *BadgerTransaction) error { return tx.CreateEdge(e) }, func() error { return b.CreateEdge(e) })
							commits = append(commits, fmt.Sprintf("edge-create:%s:%d", edgeID, rev))
						}
						edges[edgeID] = rev
					case 5:
						if _, exists := edges[edgeID]; exists {
							write(func(tx *BadgerTransaction) error { return tx.DeleteEdge(edgeID) }, func() error { return b.DeleteEdge(edgeID) })
							delete(edges, edgeID)
							commits = append(commits, "edge-delete:"+string(edgeID))
						}
					case 6:
						_, _, err := b.DeleteByPrefix("history:")
						require.NoError(t, err)
						commits = append(commits, "reset:history:")
						clear(nodes)
						clear(edges)
						seedNodes()
					case 7:
						barrier()
					}
				}
				barrier()
				// Capture a frontier while its first callback is popped and gated.
				// Commit unrelated work before letting that callback write back.
				entered, resume, echoDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
				var once sync.Once
				defer once.Do(func() { close(resume) })
				rev++
				triggerRev := rev
				rev++
				queuedRev := rev
				rev++
				echoRev := rev
				b.OnNodeUpdated(func(n *Node) {
					if n.ID != "history:trigger" {
						return
					}
					close(entered)
					<-resume
					echo := &Node{ID: "history:echo", Properties: map[string]any{"rev": echoRev}}
					echoDone <- wbListenerTx(b, buffered, func(tx *BadgerTransaction) error { return tx.UpdateNode(echo) })
				})
				write(func(tx *BadgerTransaction) error {
					return tx.UpdateNode(&Node{ID: "history:trigger", Properties: map[string]any{"rev": triggerRev}})
				}, func() error {
					return b.UpdateNode(&Node{ID: "history:trigger", Properties: map[string]any{"rev": triggerRev}})
				})
				commits = append(commits, fmt.Sprintf("update:history:trigger:%d", triggerRev))
				require.NoError(t, b.LandWriteBehind())
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("reentrant callback never entered")
				}
				queued := &Node{ID: "history:a", Properties: map[string]any{"rev": queuedRev}}
				write(func(tx *BadgerTransaction) error { return tx.UpdateNode(queued) }, func() error { return b.UpdateNode(queued) })
				commits = append(commits, fmt.Sprintf("update:history:a:%d", queuedRev))
				frontier, _ := b.listenerFrontier()
				require.NoError(t, b.LandWriteBehind())
				once.Do(func() { close(resume) })
				require.NoError(t, <-echoDone)
				// First barrier excludes the callback's later reservation. The
				// second captured frontier includes and lands that exact successor.
				b.awaitCommitNotification(frontier)
				commits = append(commits, fmt.Sprintf("update:history:echo:%d", echoRev))
				barrier()
			})
		}
	}
}

func runWriteBehindModel(t *testing.T, seed int64) {
	r := &wbModelRun{t: t, rng: rand.New(rand.NewSource(seed)), dir: t.TempDir(), model: map[NodeID]wbModelNode{},
		ackedBuffered: map[int64]bool{}, ackedSync: map[int64]bool{}, deletes: map[NodeID]int{}}
	r.ivalue = time.Hour
	if r.rng.Intn(2) == 0 {
		r.ivalue = 2 * time.Millisecond
	}
	r.listeners = &wbModelListeners{revs: map[int64]bool{}, deletes: map[NodeID]int{}}
	r.listeners.panicOn.Store(NodeID(""))
	r.open()
	defer func() {
		r.flushes.Wait()
		if r.b != nil {
			_ = r.b.Close()
		}
		if t.Failed() {
			t.Logf("seed %d (interval %s) ops:\n%s", seed, r.ivalue, strings.Join(r.log, "\n"))
		}
	}()
	// The listener write-back pair, never touched by the generated ops.
	r.mustCreate("m1:src", wbModelNode{}, "direct")
	r.mustCreate("m1:echo", wbModelNode{}, "direct")
	r.flushAndCheck()

	for i := 0; i < wbModelOps && !t.Failed(); i++ {
		r.step()
	}
	r.flushes.Wait()
	r.flushAndCheck()
	r.reopen()
	r.check("final reopen")
}

func (r *wbModelRun) open() {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: r.dir, WriteBehind: true, WriteBehindInterval: r.ivalue})
	require.NoError(r.t, err)
	if r.ivalue == time.Hour {
		b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	}
	for _, ns := range wbModelNamespaces {
		r.ensureSchema(b, ns)
	}
	l := r.listeners
	l.b.Store(b)
	b.OnNodeCreated(func(n *Node) {
		if id, _ := l.panicOn.Load().(NodeID); id != "" && n.ID == id {
			panic("model-listener-panic")
		}
		l.record(n)
	})
	b.OnNodeUpdated(func(n *Node) {
		l.record(n)
		if n.ID == "m1:src" {
			l.srcEvents.Add(1)
			l.writeBack()
		}
	})
	b.OnNodeDeleted(func(id NodeID) {
		l.mu.Lock()
		l.deletes[id]++
		l.mu.Unlock()
	})
	r.b = b
}

func (r *wbModelRun) ensureSchema(b *BadgerEngine, ns string) {
	schema := b.GetSchemaForNamespace(ns)
	if !schema.valueOrdered(ConstraintEntityNode, []string{"U"}) {
		require.NoError(r.t, schema.AddUniqueConstraint(ns+"_u_k", "U", "k"))
	}
	require.NoError(r.t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, ns), schema))
}

func (l *wbModelListeners) record(n *Node) {
	rev, ok := n.Properties["rev"].(int64)
	if !ok {
		return
	}
	l.mu.Lock()
	l.revs[rev] = true
	l.mu.Unlock()
}

// writeBack updates the echo node from the listener, through a direct write or
// an explicit transaction: a supported re-entrant write.
func (l *wbModelListeners) writeBack() {
	b := l.b.Load()
	fail := func(err error) {
		if err != nil && !errors.Is(err, ErrStorageClosed) {
			l.echoErr.CompareAndSwap(nil, err.Error())
		}
	}
	if l.echoRoute.Load() == 0 {
		echo, err := b.GetNode("m1:echo")
		if err != nil {
			fail(err)
			return
		}
		echo.Properties["cnt"] = echo.Properties["cnt"].(int64) + 1
		if err := b.UpdateNode(echo); err != nil {
			fail(err)
			return
		}
		l.echoAcked.Add(1)
		return
	}
	tx, err := b.BeginTransaction()
	if err != nil {
		fail(err)
		return
	}
	echo, err := tx.GetNode("m1:echo")
	if err == nil {
		echo.Properties["cnt"] = echo.Properties["cnt"].(int64) + 1
		err = tx.UpdateNode(echo)
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if err != nil {
		fail(err)
		return
	}
	l.echoAcked.Add(1)
}

func (r *wbModelRun) logf(format string, args ...any) {
	r.log = append(r.log, fmt.Sprintf(format, args...))
}

func (r *wbModelRun) pickNode(ns string) NodeID {
	return NodeID(fmt.Sprintf("%s:n%d", ns, r.rng.Intn(6)))
}

func (r *wbModelRun) pickKey() string {
	if r.rng.Intn(4) == 0 {
		return ""
	}
	return string(rune('a' + r.rng.Intn(4)))
}

func (r *wbModelRun) route() string {
	return []string{"implicit", "implicit", "explicit", "direct"}[r.rng.Intn(4)]
}

func (r *wbModelRun) holder(ns, key string) NodeID {
	if key == "" {
		return ""
	}
	for id, n := range r.model {
		if n.key == key && strings.HasPrefix(string(id), ns+":") && strings.HasPrefix(string(id)[len(ns)+1:], "n") {
			return id
		}
	}
	return ""
}

func (r *wbModelRun) body(id NodeID, n wbModelNode) *Node {
	labels := []string{"U"}
	if strings.HasSuffix(string(id), ":src") || strings.HasSuffix(string(id), ":echo") {
		labels = []string{"E"}
	} else if n.tagged {
		labels = append(labels, "T")
	}
	props := map[string]any{"cnt": n.cnt, "rev": n.rev}
	if n.key != "" {
		props["k"] = n.key
	}
	return &Node{ID: id, Labels: labels, Properties: props}
}

// write runs one node write through route; buffered reports a write-behind
// acknowledgement.
func (r *wbModelRun) write(route string, f func(tx *BadgerTransaction) error, direct func() error) (buffered bool, err error) {
	if route == "direct" {
		return false, direct()
	}
	tx, err := r.b.BeginTransaction()
	if err != nil {
		return false, err
	}
	if route == "implicit" {
		require.NoError(r.t, tx.SetImplicit(true))
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	err = tx.Commit()
	return tx.bufferedAck.Load(), err
}

func (r *wbModelRun) acked(rev int64, buffered bool) {
	if buffered {
		r.ackedBuffered[rev] = true
	} else {
		r.ackedSync[rev] = true
	}
}

func (r *wbModelRun) mustCreate(id NodeID, n wbModelNode, route string) {
	r.seq++
	n.rev = r.seq
	body := r.body(id, n)
	buffered, err := r.write(route, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(body); return err },
		func() error { _, err := r.b.CreateNode(body); return err })
	require.NoError(r.t, err)
	r.model[id] = n
	r.acked(n.rev, buffered)
}

func (r *wbModelRun) step() {
	switch op := r.rng.Intn(100); {
	case op < 22:
		r.create()
	case op < 52:
		r.update()
	case op < 62:
		r.delete()
	case op < 70:
		r.updateSource()
	case op < 80:
		r.flushAndCheck()
	case op < 86:
		r.backgroundFlush()
	case op < 90:
		r.reopen()
	case op < 94:
		r.dropPrefix()
	case op < 97:
		r.panicCreate()
	default:
		r.listeners.echoRoute.Store(int32(r.rng.Intn(2)))
	}
}

func (r *wbModelRun) create() {
	ns := wbModelNamespaces[r.rng.Intn(2)]
	id, key, route := r.pickNode(ns), r.pickKey(), r.route()
	_, exists := r.model[id]
	clash := r.holder(ns, key) != ""
	r.seq++
	n := wbModelNode{key: key, tagged: r.rng.Intn(2) == 0, rev: r.seq}
	body := r.body(id, n)
	buffered, err := r.write(route, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(body); return err },
		func() error { _, err := r.b.CreateNode(body); return err })
	r.logf("create %s k=%q via %s -> %v (exists=%v clash=%v)", id, key, route, err, exists, clash)
	if exists || clash {
		require.Error(r.t, err, "create %s must fail", id)
		return
	}
	require.NoError(r.t, err, "create %s", id)
	r.model[id] = n
	r.acked(n.rev, buffered)
}

func (r *wbModelRun) update() {
	ns := wbModelNamespaces[r.rng.Intn(2)]
	id, route := r.pickNode(ns), r.route()
	cur, exists := r.model[id]
	if !exists && route == "direct" {
		route = "explicit" // a direct update of a missing node creates it
	}
	next := cur
	next.cnt++
	if r.rng.Intn(3) == 0 {
		next.key = r.pickKey()
	}
	if r.rng.Intn(3) == 0 {
		next.tagged = !next.tagged
	}
	r.seq++
	next.rev = r.seq
	clash := next.key != cur.key && r.holder(ns, next.key) != "" && r.holder(ns, next.key) != id
	body := r.body(id, next)
	buffered, err := r.write(route, func(tx *BadgerTransaction) error {
		read, err := tx.GetNode(id)
		if err != nil {
			return err
		}
		// The transaction reads exactly the last acknowledged body.
		require.Equal(r.t, r.body(id, cur).Properties, read.Properties, "read of %s", id)
		return tx.UpdateNode(body)
	}, func() error { return r.b.UpdateNode(body) })
	r.logf("update %s k=%q->%q cnt=%d via %s -> %v (exists=%v clash=%v)", id, cur.key, next.key, next.cnt, route, err, exists, clash)
	if !exists || clash {
		require.Error(r.t, err, "update %s must fail", id)
		return
	}
	require.NoError(r.t, err, "update %s", id)
	r.model[id] = next
	r.acked(next.rev, buffered)
}

func (r *wbModelRun) delete() {
	ns := wbModelNamespaces[r.rng.Intn(2)]
	id, route := r.pickNode(ns), r.route()
	_, exists := r.model[id]
	_, err := r.write(route, func(tx *BadgerTransaction) error { return tx.DeleteNode(id) },
		func() error { return r.b.DeleteNode(id) })
	r.logf("delete %s via %s -> %v (exists=%v)", id, route, err, exists)
	if !exists {
		require.Error(r.t, err, "delete %s must fail", id)
		return
	}
	require.NoError(r.t, err, "delete %s", id)
	delete(r.model, id)
	r.deletes[id]++
}

func (r *wbModelRun) updateSource() {
	cur := r.model["m1:src"]
	cur.cnt++
	r.seq++
	cur.rev = r.seq
	route := r.route()
	body := r.body("m1:src", cur)
	buffered, err := r.write(route, func(tx *BadgerTransaction) error { return tx.UpdateNode(body) },
		func() error { return r.b.UpdateNode(body) })
	r.logf("update source cnt=%d via %s -> %v", cur.cnt, route, err)
	require.NoError(r.t, err)
	r.model["m1:src"] = cur
	r.acked(cur.rev, buffered)
}

func (r *wbModelRun) backgroundFlush() {
	b := r.b
	r.flushes.Add(1)
	go func() {
		defer r.flushes.Done()
		if err := b.FlushWriteBehind(); err != nil && !errors.Is(err, ErrStorageClosed) {
			r.t.Errorf("background flush: %v", err)
		}
	}()
	r.logf("background flush")
}

func (r *wbModelRun) panicCreate() {
	// No other goroutine may be delivering when the panicking event is
	// delivered: the panic must surface in this write. A listener's write-back
	// is buffered after the flush that delivered it, or after a queued
	// delivery, so flush and wait for the queue until the buffer stays empty
	// and the background flusher has nothing to deliver.
	r.flushes.Wait()
	for empty := false; !empty; {
		require.NoError(r.t, r.b.FlushWriteBehind())
		require.Eventually(r.t, func() bool {
			r.b.notificationMu.Lock()
			defer r.b.notificationMu.Unlock()
			return r.b.notificationHead == nil && (r.b.notificationLast == nil || notificationConsumed(r.b.notificationLast))
		}, 5*time.Second, time.Millisecond, "listener deliveries did not finish")
		w := r.b.writeBehind
		w.mu.RLock()
		empty = w.opCount == 0 && len(w.draining) == 0
		w.mu.RUnlock()
	}
	id := NodeID(fmt.Sprintf("m1:boom%d", r.seq))
	r.seq++
	r.listeners.panicOn.Store(id)
	defer r.listeners.panicOn.Store(NodeID(""))
	var caught any
	func() {
		defer func() { caught = recover() }()
		_, _ = r.b.CreateNode(&Node{ID: id, Labels: []string{"P"}, Properties: map[string]any{"cnt": int64(0)}})
	}()
	r.logf("panic create %s -> recovered %v", id, caught)
	r.b.WaitForListeners()
	require.Nil(r.t, caught, "listener panic belongs to the dispatcher")
	_, err := r.b.GetStoredNode(id)
	require.NoError(r.t, err, "the write behind a panicking listener is committed")
	require.NoError(r.t, r.b.DeleteNode(id))
}

func (r *wbModelRun) reopen() {
	r.flushes.Wait()
	require.NoError(r.t, r.b.Close())
	r.logf("close/reopen")
	r.open()
	r.check("reopen")
}

func (r *wbModelRun) dropPrefix() {
	_, _, err := r.b.DeleteByPrefix("m2:")
	require.NoError(r.t, err)
	r.logf("drop m2:")
	for id := range r.model {
		if strings.HasPrefix(string(id), "m2:") {
			delete(r.model, id)
		}
	}
	r.ensureSchema(r.b, "m2")
	// Prefix deletion now lands data only. Complete the explicit event
	// frontier before comparing callback-owned write-back accounting.
	r.b.WaitForListeners()
	require.NoError(r.t, r.b.LandWriteBehind())
	r.check("after drop")
}

// flushAndCheck flushes, then checks that the listeners of every buffered
// write acknowledged before the flush have run (strictly), that synchronous
// writes' and deletes' listeners run (eventually), and the whole state.
func (r *wbModelRun) flushAndCheck() {
	require.NoError(r.t, r.b.FlushWriteBehind())
	l := r.listeners
	missing := func(acked map[int64]bool) []int64 {
		l.mu.Lock()
		defer l.mu.Unlock()
		var out []int64
		for rev := range acked {
			if !l.revs[rev] {
				out = append(out, rev)
			}
		}
		return out
	}
	require.Empty(r.t, missing(r.ackedBuffered), "flush returned before these buffered writes' listeners ran")
	require.Eventually(r.t, func() bool {
		if len(missing(r.ackedSync)) > 0 {
			return false
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		for id, n := range r.deletes {
			if l.deletes[id] != n {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Millisecond, "synchronous writes' and deletes' listeners never ran")
	r.check("flush")
}

func (r *wbModelRun) check(stage string) {
	t := r.t
	b := r.b
	if e := r.listeners.echoErr.Load(); e != nil {
		t.Fatalf("%s: listener write-back failed: %v", stage, e)
	}
	for _, ns := range wbModelNamespaces {
		store := NewNamespacedEngine(b, ns)
		stored := map[NodeID]*Node{}
		require.NoError(t, StreamStoredNodes(context.Background(), store, StreamNodesOptions{}, func(n *Node) error {
			if strings.HasPrefix(string(n.ID), "n") {
				stored[NodeID(ns+":"+string(n.ID))] = copyNode(n)
			}
			return nil
		}))
		want := map[NodeID]wbModelNode{}
		for id, n := range r.model {
			if strings.HasPrefix(string(id), ns+":n") {
				want[id] = n
			}
		}
		require.Equal(t, wbModelSortedIDs(want), wbModelSortedStored(stored), "%s: stored nodes in %s", stage, ns)
		holders := map[string]int{}
		tagged := 0
		for id, n := range want {
			got := stored[id]
			require.Equal(t, r.body(id, n).Properties, got.Properties, "%s: stored body of %s", stage, id)
			cached, err := b.GetNode(id)
			require.NoError(t, err, "%s: cached read of %s", stage, id)
			require.Equal(t, got.Properties, cached.Properties, "%s: cache of %s agrees with storage", stage, id)
			require.ElementsMatch(t, got.Labels, cached.Labels, "%s: cached labels of %s", stage, id)
			if n.key != "" {
				holders[n.key]++
			}
			if n.tagged {
				tagged++
			}
		}
		for key, count := range holders {
			require.Equal(t, 1, count, "%s: holders of %s.k=%s", stage, ns, key)
		}
		indexed := 0
		require.NoError(t, StreamStoredNodesByLabelProjected(store, "U", []string{"k"}, func(*Node) error { indexed++; return nil }))
		require.Equal(t, len(want), indexed, "%s: label index of U in %s", stage, ns)
		us, err := b.NodeCountByLabelInNamespace(ns, "U")
		require.NoError(t, err)
		ts, err := b.NodeCountByLabelInNamespace(ns, "T")
		require.NoError(t, err)
		require.Equal(t, [2]int64{int64(len(want)), int64(tagged)}, [2]int64{us, ts}, "%s: label counts in %s", stage, ns)
	}
	echo, err := b.GetStoredNode("m1:echo")
	require.NoError(t, err)
	require.Equal(t, r.listeners.echoAcked.Load(), echo.Properties["cnt"], "%s: every acknowledged write-back is stored", stage)
}

func wbModelSortedIDs(m map[NodeID]wbModelNode) []NodeID {
	out := make([]NodeID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func wbModelSortedStored(m map[NodeID]*Node) []NodeID {
	out := make([]NodeID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Concurrent writers keep the lifecycle invariants: every acknowledged
// increment survives, a UNIQUE value has at most one holder, counts and the
// label index match the stored nodes, and listeners that write back neither
// deadlock nor lose a write, before and after reopen.
func TestWriteBehindModelConcurrent(t *testing.T) {
	seeds := wbModelSeeds(t)
	if len(seeds) > 5 {
		seeds = seeds[:len(seeds)/4]
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			wbModelWithin(t, wbModelDeadline, "concurrent model seed", func() { runWriteBehindModelConcurrent(t, seed) })
		})
	}
}

func runWriteBehindModelConcurrent(t *testing.T, seed int64) {
	dir := t.TempDir()
	open := func() *BadgerEngine {
		b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: 2 * time.Millisecond})
		require.NoError(t, err)
		schema := b.GetSchemaForNamespace("c")
		if !schema.valueOrdered(ConstraintEntityNode, []string{"U"}) {
			require.NoError(t, schema.AddUniqueConstraint("c_u_k", "U", "k"))
		}
		require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, "c"), schema))
		return b
	}
	b := open()
	counters := []NodeID{"c:cnt0", "c:cnt1"}
	for _, id := range counters {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"C"}, Properties: map[string]any{"cnt": int64(0)}})
		require.NoError(t, err)
	}
	_, err := b.CreateNode(&Node{ID: "c:echo", Labels: []string{"E"}, Properties: map[string]any{"cnt": int64(0)}})
	require.NoError(t, err)
	var echoAcked atomic.Int64
	var listenerFail atomic.Value
	b.OnNodeUpdated(func(n *Node) {
		if n.ID != "c:cnt0" {
			return
		}
		echo, err := b.GetNode("c:echo")
		if err == nil {
			echo.Properties["cnt"] = echo.Properties["cnt"].(int64) + 1
			err = b.UpdateNode(echo)
		}
		if err != nil {
			listenerFail.CompareAndSwap(nil, err.Error())
			return
		}
		echoAcked.Add(1)
	})
	var increments [2]atomic.Int64
	var cnt0Events atomic.Int64
	b.OnNodeUpdated(func(n *Node) {
		if n.ID == "c:cnt0" {
			cnt0Events.Add(1)
		}
	})
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*31 + int64(w)))
			for i := 0; i < 60; i++ {
				implicit := rng.Intn(3) != 0
				switch rng.Intn(4) {
				case 0, 1:
					which := rng.Intn(2)
					tx, err := b.BeginTransaction()
					if err != nil {
						continue
					}
					_ = tx.SetImplicit(implicit)
					n, err := tx.GetNode(counters[which])
					if err == nil {
						n.Properties["cnt"] = n.Properties["cnt"].(int64) + 1
						err = tx.UpdateNode(n)
					}
					if err == nil {
						err = tx.Commit()
					} else {
						_ = tx.Rollback()
					}
					if err == nil {
						increments[which].Add(1)
					} else if !errors.Is(err, ErrConflict) {
						t.Errorf("increment: %v", err)
					}
				case 2:
					id := NodeID(fmt.Sprintf("c:u%d", rng.Intn(4)))
					key := string(rune('a' + rng.Intn(3)))
					if rng.Intn(2) == 0 {
						_, _ = b.CreateNode(&Node{ID: id, Labels: []string{"U"}, Properties: map[string]any{"k": key}})
					} else {
						tx, err := b.BeginTransaction()
						if err != nil {
							continue
						}
						_ = tx.SetImplicit(implicit)
						if n, err := tx.GetNode(id); err == nil {
							n.Properties["k"] = key
							err = tx.UpdateNode(n)
							if err == nil {
								_ = tx.Commit()
								continue
							}
						} else if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"U"}, Properties: map[string]any{"k": key}}); err == nil {
							_ = tx.Commit()
							continue
						}
						_ = tx.Rollback()
					}
				case 3:
					if rng.Intn(4) == 0 {
						_ = b.FlushWriteBehind()
					} else {
						_ = b.DeleteNode(NodeID(fmt.Sprintf("c:u%d", rng.Intn(4))))
					}
				}
			}
		}(w)
	}
	wg.Wait()
	require.NoError(t, b.FlushWriteBehind())
	if e := listenerFail.Load(); e != nil {
		t.Fatalf("listener write-back failed: %v", e)
	}
	check := func(stage string, b *BadgerEngine) {
		store := NewNamespacedEngine(b, "c")
		for i, id := range counters {
			n, err := b.GetStoredNode(id)
			require.NoError(t, err)
			require.Equal(t, increments[i].Load(), n.Properties["cnt"], "%s: %s lost acknowledged increments", stage, id)
		}
		echo, err := b.GetStoredNode("c:echo")
		require.NoError(t, err)
		require.Equal(t, echoAcked.Load(), echo.Properties["cnt"], "%s: listener write-backs", stage)
		holders := map[any]int{}
		stored := 0
		require.NoError(t, StreamStoredNodes(context.Background(), store, StreamNodesOptions{}, func(n *Node) error {
			if hasLabel(n.Labels, "U") {
				stored++
				holders[n.Properties["k"]]++
			}
			return nil
		}))
		for key, count := range holders {
			require.Equal(t, 1, count, "%s: holders of U.k=%v", stage, key)
		}
		indexed := 0
		require.NoError(t, StreamStoredNodesByLabelProjected(store, "U", []string{"k"}, func(*Node) error { indexed++; return nil }))
		count, err := b.NodeCountByLabelInNamespace("c", "U")
		require.NoError(t, err)
		require.Equal(t, [2]int64{int64(stored), int64(stored)}, [2]int64{int64(indexed), count}, "%s: U index and count", stage)
	}
	check("flushed", b)
	require.Equal(t, cnt0Events.Load(), echoAcked.Load(), "every delivered event wrote back once")
	require.NoError(t, b.Close())
	reopened := open()
	defer reopened.Close()
	check("reopened", reopened)
}
