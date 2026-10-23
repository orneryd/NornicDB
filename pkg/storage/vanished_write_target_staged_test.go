package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

// A write staged while its target existed, whose target a peer then deletes
// and acknowledges before the writer commits, fails the commit as a
// concurrent modification and writes nothing: the deleted relationship is not
// revived and no relationship to a deleted node is created (independent
// review of d9c2ff3, jobs 5f82/e142).
func TestVanishedWriteTargetDeleteAfterStaging(t *testing.T) {
	for _, producer := range []string{"buffered", "direct", "transaction"} {
		for _, flush := range []bool{false, true} {
			for _, operation := range []string{"node", "relationship", "endpoint"} {
				t.Run(fmt.Sprintf("%s/flush=%t/%s", producer, flush, operation), func(t *testing.T) {
					b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					defer b.Close()
					for _, id := range []NodeID{"test:a", "test:b"} {
						_, err = b.CreateNode(&Node{ID: id})
						require.NoError(t, err)
					}
					require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"note": "old"}}))
					writer, err := b.BeginTransaction()
					require.NoError(t, err)
					defer writer.Rollback()
					require.NoError(t, writer.SetImplicit(true))
					switch operation {
					case "node":
						require.NoError(t, writer.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"note": "set"}}))
					case "relationship":
						require.NoError(t, writer.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"note": "set"}}))
					case "endpoint":
						require.NoError(t, writer.CreateEdge(&Edge{ID: "test:new", StartNode: "test:b", EndNode: "test:a", Type: "R"}))
					}
					drop := func(tx *BadgerTransaction) error {
						if operation == "relationship" {
							return tx.DeleteEdge("test:r")
						}
						return tx.DeleteNode("test:a")
					}
					if producer == "direct" {
						if operation == "relationship" {
							err = b.DeleteEdge("test:r")
						} else {
							err = b.DeleteNode("test:a")
						}
					} else {
						peer, err := b.BeginTransaction()
						require.NoError(t, err)
						require.NoError(t, peer.SetImplicit(producer == "buffered"))
						require.NoError(t, drop(peer))
						err = peer.Commit()
						require.NoError(t, err)
					}
					require.NoError(t, err)
					if flush {
						require.NoError(t, b.FlushWriteBehind())
					}
					require.ErrorIs(t, writer.Commit(), ErrConflict, "a peer deleted a target held by the begin snapshot")
					require.NoError(t, b.FlushWriteBehind())
					_, err = b.GetEdge("test:r")
					require.ErrorIs(t, err, ErrNotFound, "the deleted relationship is not revived")
					_, err = b.GetEdge("test:new")
					require.ErrorIs(t, err, ErrNotFound, "the failed commit wrote nothing")
				})
			}
		}
	}
}

// A direct deletion that took its version before the writer began but
// published after the writer staged its write is a concurrent delete at the
// writer's commit (independent review of d9c2ff3).
func TestVanishedWriteTargetAllocatedDeleteAfterStaging(t *testing.T) {
	for _, operation := range []string{"relationship", "endpoint"} {
		t.Run(operation, func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err = b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
			state, err := b.namespaceMVCC("test")
			require.NoError(t, err)
			before := state.seq.Load()
			b.db.commitGate.Lock()
			unlocked := false
			defer func() {
				if !unlocked {
					b.db.commitGate.Unlock()
				}
			}()
			done := make(chan error, 1)
			go func() {
				if operation == "relationship" {
					done <- b.DeleteEdge("test:r")
				} else {
					done <- b.DeleteNode("test:a")
				}
			}()
			deadline := time.Now().Add(10 * time.Second)
			for state.seq.Load() == before && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			require.Greater(t, state.seq.Load(), before, "the deletion allocated its version")
			writer, err := b.BeginTransaction()
			require.NoError(t, err)
			defer writer.Rollback()
			require.NoError(t, writer.SetImplicit(true))
			if operation == "relationship" {
				err = writer.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"note": "set"}})
			} else {
				err = writer.CreateEdge(&Edge{ID: "test:new", StartNode: "test:b", EndNode: "test:a", Type: "R"})
			}
			require.NoError(t, err)
			b.db.commitGate.Unlock()
			unlocked = true
			require.NoError(t, <-done)
			require.ErrorIs(t, writer.Commit(), ErrConflict)
			require.NoError(t, b.FlushWriteBehind())
			_, err = b.GetEdge("test:r")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func overlayTestPolicy(t *testing.T) *memory.Policy {
	t.Helper()
	p := memory.NewPolicy(1<<20, 0, time.Hour, func() (memory.Sample, error) { return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil }, time.Now)
	require.NoError(t, p.Refresh())
	return p
}

// Closing the engine returns the memory of generations an open transaction
// still pins, so the next user of the same budget is not refused (independent
// review of d9c2ff3, jobs 0ef2/aea2).
func TestBeginOverlayCloseReturnsBufferBudget(t *testing.T) {
	p := overlayTestPolicy(t)
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, MemoryPolicy: p})
	require.NoError(t, err)
	defer b.Close()
	implicitCommit(t, b, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "test:seed"})
		return err
	})
	reader, err := b.BeginTransaction()
	require.NoError(t, err)
	defer reader.Rollback()
	require.Positive(t, p.Reserved())
	require.NoError(t, b.Close())
	require.Zero(t, p.Reserved(), "a closed engine keeps no buffered memory")
	lease, err := p.Reserve((1 << 20) - 2)
	require.NoError(t, err)
	lease.Release()
}

// Every way a transaction ends releases its pins and leases: commit,
// rollback, a cancelled commit, rollback after a panicking commit, and an
// engine Close that finds it open or abandoned.
func TestBeginOverlayPinLifetimes(t *testing.T) {
	for _, exit := range []string{"commit", "rollback", "cancel-then-rollback", "panic-then-rollback", "close", "abandoned-close"} {
		t.Run(exit, func(t *testing.T) {
			p := overlayTestPolicy(t)
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, MemoryPolicy: p})
			require.NoError(t, err)
			defer b.Close()
			implicitCommit(t, b, func(tx *BadgerTransaction) error {
				_, err := tx.CreateNode(&Node{ID: "test:seed"})
				return err
			})
			var generation *CommitBuffer
			var tx *BadgerTransaction
			begin := func() {
				var err error
				tx, err = b.BeginTransaction()
				require.NoError(t, err)
				generation = tx.overlayAtBegin.active
			}
			if exit == "abandoned-close" {
				func() {
					begin()
					tx = nil
				}()
				runtime.GC()
			} else {
				begin()
			}
			require.NoError(t, b.FlushWriteBehind())
			require.Positive(t, p.Reserved())
			require.Equal(t, int32(1), generation.pins.Load())
			switch exit {
			case "commit":
				require.NoError(t, tx.Commit())
			case "rollback":
				require.NoError(t, tx.Rollback())
			case "cancel-then-rollback":
				// A commit whose context is already done does not start; the
				// transaction stays open for its caller to roll back.
				_, err := tx.CreateNode(&Node{ID: "test:cancelled"})
				require.NoError(t, err)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, tx.CommitContext(ctx), context.Canceled)
				require.NoError(t, tx.Rollback())
			case "panic-then-rollback":
				_, err := tx.CreateNode(&Node{ID: "test:panic"})
				require.NoError(t, err)
				restore := setCommitTailHook(func() { panic("pin unwind") })
				func() {
					defer func() { require.Equal(t, "pin unwind", recover()) }()
					_ = tx.Commit()
				}()
				restore()
				require.NoError(t, tx.Rollback())
			case "close", "abandoned-close":
				require.NoError(t, b.Close())
			}
			require.Zero(t, p.Reserved(), "the ended transaction or database returns its buffered leases")
			require.Zero(t, generation.pins.Load())
			if tx != nil {
				_ = tx.Rollback()
				require.Zero(t, generation.pins.Load(), "a rollback after Close releases nothing twice")
				require.Zero(t, p.Reserved())
			}
		})
	}
}

// A generated mix of node and relationship creates and deletes through the
// buffer, synchronous transactions and direct engine writes, with writers
// that begin, stage writes, and commit later while peer deletes, flushes and
// finally an engine Close land between their staging and their commit. A
// reference model of the begin snapshot and of what each writer staged
// decides every outcome: a commit fails as a concurrent modification exactly
// when a target it staged was deleted after the staging, and then writes
// nothing; otherwise it commits its writes.
func TestVanishedWriteTargetStagedCommitModel(t *testing.T) {
	for _, syncWrites := range []bool{false, true} {
		// Two fixed seeds whose physical peer writes met a size-rotated
		// drain (independent review of 2be99f2), then random ones.
		seeds := []int64{9901655118, 9901655231}
		for seedIndex := 0; seedIndex < 8; seedIndex++ {
			seeds = append(seeds, time.Now().UnixNano()+int64(seedIndex))
		}
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("sync=%t/seed=%d", syncWrites, seed), func(t *testing.T) {
				runStagedCommitModel(t, seed, syncWrites)
			})
		}
	}
}

type modelEdge struct {
	id         EdgeID
	start, end NodeID
}

type stagedWriter struct {
	tx       *BadgerTransaction
	implicit bool
	begin    map[string]bool // node and relationship ids live at begin
	staged   []string        // targets the writer's staged writes depend on
	writes   []string        // targets the writer updates
	created  []EdgeID        // relationships the writer creates
	updated  []EdgeID        // relationships the writer updates
	marker   NodeID
}

func runStagedCommitModel(t *testing.T, seed int64, syncWrites bool) {
	rng := rand.New(rand.NewSource(seed))
	p := overlayTestPolicy(t)
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, SyncWrites: syncWrites, MemoryPolicy: p})
	require.NoError(t, err)
	defer b.Close()

	liveNodes := map[NodeID]bool{}
	liveEdges := map[EdgeID]modelEdge{}
	persisted := map[string]bool{}   // in Badger, not only in the buffer
	deletedAfter := map[string]int{} // target -> step it was deleted
	stagedAt := map[*stagedWriter]map[string]int{}
	next, peerRetries := 0, 0
	newID := func(prefix string) string {
		next++
		return fmt.Sprintf("test:%s%d", prefix, next)
	}
	// produce runs a write through a random producer; a target only the
	// buffer holds is written through the buffer.
	produce := func(bufferedOnly bool, viaTx func(*BadgerTransaction) error, direct func() error) {
		producer := rng.Intn(3)
		if bufferedOnly {
			producer = 0
		}
		if producer == 0 {
			implicitCommit(t, b, viaTx)
			return
		}
		// The buffer also rotates a generation by size and drains it in the
		// background. A physical write whose reads overlap keys that drain is
		// landing gets Badger's ordinary key conflict. Every conflict must
		// coincide with a drain; after waiting for it, the write succeeds.
		drainState := func() (int, *CommitBuffer) {
			b.writeBehind.mu.RLock()
			defer b.writeBehind.mu.RUnlock()
			return len(b.writeBehind.draining), b.writeBehind.active
		}
		for attempt := 0; ; attempt++ {
			drainingBefore, activeBefore := drainState()
			var err error
			if producer == 1 {
				tx, beginErr := b.BeginTransaction()
				require.NoError(t, beginErr)
				require.NoError(t, viaTx(tx))
				err = tx.Commit()
			} else {
				err = direct()
			}
			if err == nil {
				return
			}
			drainingAfter, activeAfter := drainState()
			require.ErrorIs(t, err, badger.ErrConflict)
			require.True(t, drainingBefore > 0 || drainingAfter > 0 || activeBefore != activeAfter,
				"seed %d: a physical peer write conflicted with no drain in flight", seed)
			require.Zero(t, attempt, "seed %d: a peer write conflicted again after its drain landed", seed)
			require.NoError(t, b.FlushWriteBehind())
			peerRetries++
		}
	}
	liveNodeList := func() []NodeID {
		var ids []NodeID
		for id := range liveNodes {
			ids = append(ids, id)
		}
		sortNodeIDs(ids)
		return ids
	}
	liveEdgeList := func() []modelEdge {
		var edges []modelEdge
		for _, e := range liveEdges {
			edges = append(edges, e)
		}
		sortModelEdges(edges)
		return edges
	}
	connected := func(id NodeID) bool {
		for _, e := range liveEdges {
			if e.start == id || e.end == id {
				return true
			}
		}
		return false
	}
	var writers []*stagedWriter
	// Each node or relationship is updated by one writer at most, so the
	// only conflicts are the deletes this model is about.
	claimed := map[string]bool{}
	commits, conflicts, keyConflicts, step := 0, 0, 0, 0

	commit := func(w *stagedWriter) {
		expectConflict := false
		for _, target := range w.staged {
			if at, gone := deletedAfter[target]; gone && at > stagedAt[w][target] {
				expectConflict = true
			}
		}
		err := w.tx.Commit()
		// Badger's own key conflict (a flush or a peer write landing on a key
		// an explicit writer read) is an ordinary retry outside this model;
		// it must still write nothing. Any other unexpected error fails.
		badgerConflict := !expectConflict && errors.Is(err, badger.ErrConflict)
		if expectConflict || badgerConflict {
			require.ErrorIs(t, err, ErrConflict, "seed %d: a staged target was deleted after staging", seed)
			if badgerConflict {
				keyConflicts++
			} else {
				conflicts++
			}
			_, err := b.GetNode(w.marker)
			require.ErrorIs(t, err, ErrNotFound, "the failed commit writes nothing")
			for _, id := range w.created {
				_, err := b.GetEdge(id)
				require.ErrorIs(t, err, ErrNotFound, "the failed commit creates no relationship")
			}
			for _, id := range w.updated {
				if _, live := liveEdges[id]; live {
					continue
				}
				_, err := b.GetEdge(id)
				require.ErrorIs(t, err, ErrNotFound, "a deleted relationship is not revived")
			}
			return
		}
		require.NoError(t, err, "seed %d: no staged target was deleted", seed)
		commits++
		// An acknowledged buffered write is the target's latest state until
		// a flush; later deletes of it go through the buffer too.
		liveNodes[w.marker] = true
		persisted[string(w.marker)] = !w.implicit
		for _, target := range w.writes {
			persisted[target] = persisted[target] && !w.implicit
		}
		for _, id := range w.created {
			edge, err := b.GetEdge(id)
			require.NoError(t, err)
			liveEdges[id] = modelEdge{id: id, start: edge.StartNode, end: edge.EndNode}
			persisted[string(id)] = !w.implicit
		}
	}

	for ; step < 220; step++ {
		switch op := rng.Intn(14); {
		case op < 2: // create a node
			id := NodeID(newID("n"))
			produce(false, func(tx *BadgerTransaction) error {
				_, err := tx.CreateNode(&Node{ID: id})
				return err
			}, func() error {
				_, err := b.CreateNode(&Node{ID: id})
				return err
			})
			liveNodes[id] = true
		case op < 4: // create a relationship
			nodes := liveNodeList()
			if len(nodes) < 2 {
				continue
			}
			e := modelEdge{id: EdgeID(newID("e")), start: nodes[rng.Intn(len(nodes))], end: nodes[rng.Intn(len(nodes))]}
			bufferedOnly := !persisted[string(e.start)] || !persisted[string(e.end)]
			edge := &Edge{ID: e.id, StartNode: e.start, EndNode: e.end, Type: "R"}
			produce(bufferedOnly, func(tx *BadgerTransaction) error { return tx.CreateEdge(edge) }, func() error { return b.CreateEdge(edge) })
			liveEdges[e.id] = e
		case op < 6: // delete a relationship
			edges := liveEdgeList()
			if len(edges) == 0 {
				continue
			}
			e := edges[rng.Intn(len(edges))]
			produce(!persisted[string(e.id)], func(tx *BadgerTransaction) error { return tx.DeleteEdge(e.id) }, func() error { return b.DeleteEdge(e.id) })
			delete(liveEdges, e.id)
			deletedAfter[string(e.id)] = step
		case op < 7: // delete an unconnected node
			var candidates []NodeID
			for _, id := range liveNodeList() {
				if !connected(id) {
					candidates = append(candidates, id)
				}
			}
			if len(candidates) == 0 {
				continue
			}
			id := candidates[rng.Intn(len(candidates))]
			produce(!persisted[string(id)], func(tx *BadgerTransaction) error { return tx.DeleteNode(id) }, func() error { return b.DeleteNode(id) })
			delete(liveNodes, id)
			deletedAfter[string(id)] = step
		case op < 9: // begin a writer
			if len(writers) >= 4 {
				continue
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			w := &stagedWriter{tx: tx, implicit: rng.Intn(2) == 0, begin: map[string]bool{}, marker: NodeID(newID("mark"))}
			require.NoError(t, tx.SetImplicit(w.implicit))
			for id := range liveNodes {
				w.begin[string(id)] = true
			}
			for id := range liveEdges {
				w.begin[string(id)] = true
			}
			_, err = tx.CreateNode(&Node{ID: w.marker})
			require.NoError(t, err)
			stagedAt[w] = map[string]int{}
			writers = append(writers, w)
		case op < 12: // stage a write against targets live at begin and now
			if len(writers) == 0 {
				continue
			}
			w := writers[rng.Intn(len(writers))]
			stage := func(targets ...string) {
				for _, target := range targets {
					w.staged = append(w.staged, target)
					if _, already := stagedAt[w][target]; !already {
						stagedAt[w][target] = step
					}
				}
			}
			switch rng.Intn(3) {
			case 0: // update a node
				var candidates []NodeID
				for _, id := range liveNodeList() {
					if w.begin[string(id)] && !claimed[string(id)] {
						candidates = append(candidates, id)
					}
				}
				if len(candidates) == 0 {
					continue
				}
				id := candidates[rng.Intn(len(candidates))]
				require.NoError(t, w.tx.UpdateNode(&Node{ID: id, Properties: map[string]any{"step": step}}))
				claimed[string(id)] = true
				w.writes = append(w.writes, string(id))
				stage(string(id))
			case 1: // update a relationship
				var candidates []modelEdge
				for _, e := range liveEdgeList() {
					if w.begin[string(e.id)] && !claimed[string(e.id)] {
						candidates = append(candidates, e)
					}
				}
				if len(candidates) == 0 {
					continue
				}
				e := candidates[rng.Intn(len(candidates))]
				require.NoError(t, w.tx.UpdateEdge(&Edge{ID: e.id, StartNode: e.start, EndNode: e.end, Type: "R", Properties: map[string]any{"step": step}}))
				claimed[string(e.id)] = true
				w.writes = append(w.writes, string(e.id))
				w.updated = append(w.updated, e.id)
				stage(string(e.id), string(e.start), string(e.end))
			default: // create a relationship between two existing nodes
				var candidates []NodeID
				for _, id := range liveNodeList() {
					if w.begin[string(id)] {
						candidates = append(candidates, id)
					}
				}
				if len(candidates) == 0 {
					continue
				}
				start, end := candidates[rng.Intn(len(candidates))], candidates[rng.Intn(len(candidates))]
				id := EdgeID(newID("w"))
				require.NoError(t, w.tx.CreateEdge(&Edge{ID: id, StartNode: start, EndNode: end, Type: "R"}))
				w.created = append(w.created, id)
				stage(string(start), string(end))
			}
		case op < 13: // commit a writer
			if len(writers) == 0 {
				continue
			}
			i := rng.Intn(len(writers))
			w := writers[i]
			writers = append(writers[:i], writers[i+1:]...)
			commit(w)
		default: // flush and retire
			require.NoError(t, b.FlushWriteBehind())
			for id := range liveNodes {
				persisted[string(id)] = true
			}
			for id := range liveEdges {
				persisted[string(id)] = true
			}
		}
	}
	// The writers still open when the engine closes keep nothing: Close
	// releases their pins and leases, and their commits fail.
	pinned := map[*CommitBuffer]struct{}{}
	for _, w := range writers {
		if w.tx.overlayAtBegin != nil {
			pinned[w.tx.overlayAtBegin.active] = struct{}{}
			for _, generation := range w.tx.overlayAtBegin.draining {
				pinned[generation] = struct{}{}
			}
		}
	}
	require.NoError(t, b.Close())
	require.Zero(t, p.Reserved(), "seed %d: a closed engine keeps no buffered memory", seed)
	for generation := range pinned {
		require.Zero(t, generation.pins.Load())
	}
	for _, w := range writers {
		require.Error(t, w.tx.Commit(), "a commit after Close fails")
	}
	t.Logf("seed %d: %d commits, %d staged-target conflicts, %d Badger key conflicts, %d peer retries, %d writers open at Close", seed, commits, conflicts, keyConflicts, peerRetries, len(writers))
}

func sortNodeIDs(ids []NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}

func sortModelEdges(edges []modelEdge) {
	for i := 1; i < len(edges); i++ {
		for j := i; j > 0 && edges[j].id < edges[j-1].id; j-- {
			edges[j], edges[j-1] = edges[j-1], edges[j]
		}
	}
}
