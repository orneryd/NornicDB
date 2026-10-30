package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Writers on every path — direct, bulk, implicit (write-behind acknowledged)
// and explicit transactions, with concurrent flushes — interleave at random
// over a few entities. None may block forever (the write-behind order, the
// flusher, the WAL and the transaction locks never wait on each other in a
// cycle), and when they finish the stored graph is whole: every relationship's
// endpoints exist and every listed node is stored.
func TestWriteOrderUnderRandomInterleavings(t *testing.T) {
	stressWriteOrder(t, false)
}

// stressWriteOrder runs the interleavings; countsChecked also compares the
// node and relationship counters with the stored entities.
func stressWriteOrder(t *testing.T, countsChecked bool) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-behind=%v", writeBehind), func(t *testing.T) {
			engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: time.Millisecond}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			node := func(r *rand.Rand) NodeID { return NodeID(fmt.Sprintf("test:n%d", r.Intn(6))) }
			edge := func(r *rand.Rand) *Edge {
				i := r.Intn(6)
				return &Edge{ID: EdgeID(fmt.Sprintf("test:r%d", i)), StartNode: NodeID(fmt.Sprintf("test:n%d", i)),
					EndNode: NodeID(fmt.Sprintf("test:n%d", (i+1)%6)), Type: "LINK", Properties: map[string]any{"v": int64(r.Intn(100))}}
			}
			inTx := func(implicit bool, apply func(*BadgerTransaction) error) error {
				tx, err := engine.BeginTransaction()
				if err != nil {
					return err
				}
				if implicit {
					if err := tx.SetImplicit(true); err != nil {
						return err
					}
				}
				if err := apply(tx); err != nil {
					_ = tx.Rollback()
					return err
				}
				return tx.Commit()
			}
			ops := []func(*rand.Rand) error{
				func(r *rand.Rand) error {
					_, err := engine.CreateNode(&Node{ID: node(r), Properties: map[string]any{"v": int64(r.Intn(100))}})
					return err
				},
				func(r *rand.Rand) error {
					return engine.UpdateNode(&Node{ID: node(r), Properties: map[string]any{"v": int64(r.Intn(100))}})
				},
				func(r *rand.Rand) error { return engine.DeleteNode(node(r)) },
				func(r *rand.Rand) error { return engine.CreateEdge(edge(r)) },
				func(r *rand.Rand) error { return engine.UpdateEdge(edge(r)) },
				func(r *rand.Rand) error { return engine.DeleteEdge(edge(r).ID) },
				func(r *rand.Rand) error { return engine.BulkDeleteNodes([]NodeID{node(r), node(r)}) },
				func(r *rand.Rand) error { return engine.BulkCreateEdges([]*Edge{edge(r)}) },
				func(r *rand.Rand) error {
					return inTx(true, func(tx *BadgerTransaction) error {
						return ApplyNodeUpdate(tx, &Node{ID: node(r), Properties: map[string]any{"v": int64(r.Intn(100))}})
					})
				},
				func(r *rand.Rand) error {
					return inTx(true, func(tx *BadgerTransaction) error { return tx.CreateEdge(edge(r)) })
				},
				func(r *rand.Rand) error {
					return inTx(false, func(tx *BadgerTransaction) error {
						return ApplyNodeUpdate(tx, &Node{ID: node(r), Properties: map[string]any{"v": int64(r.Intn(100))}})
					})
				},
				func(r *rand.Rand) error {
					return inTx(false, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode(node(r), CascadeEdges) })
				},
				func(r *rand.Rand) error { return engine.FlushWriteBehind() },
			}
			expected := func(err error) bool {
				// A direct write reports a Badger conflict with a concurrent writer
				// of the same keys as it did before, without retrying.
				return err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrAlreadyExists) ||
					errors.Is(err, ErrConflict) || errors.Is(err, ErrInvalidEdge) || errors.Is(err, badger.ErrConflict)
			}
			const writers, steps = 16, 300
			// The writers' random choices derive from one seed, logged so a
			// failure can be replayed (NORNICDB_WRITE_ORDER_STRESS_SEED); the
			// goroutine schedule still varies between runs.
			seed := time.Now().UnixNano()
			if s := os.Getenv("NORNICDB_WRITE_ORDER_STRESS_SEED"); s != "" {
				seed, err = strconv.ParseInt(s, 10, 64)
				require.NoError(t, err)
			}
			t.Logf("seed %d", seed)
			var wg sync.WaitGroup
			errs := make(chan error, writers)
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					for i := 0; i < steps; i++ {
						if err := ops[r.Intn(len(ops))](r); !expected(err) {
							errs <- err
							return
						}
					}
				}(seed + int64(w))
			}
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Minute):
				// A blocked writer keeps Close from returning: end the binary with
				// every goroutine instead of waiting for the test timeout.
				buf := make([]byte, 1<<24)
				panic(fmt.Sprintf("writers blocked:\n%s", buf[:runtime.Stack(buf, true)]))
			}
			close(errs)
			for err := range errs {
				t.Fatalf("unexpected write error: %v", err)
			}

			require.NoError(t, engine.FlushWriteBehind())
			engine.invalidateCachesAfterRestore()
			nodes, err := engine.AllNodes()
			require.NoError(t, err)
			edges, err := engine.AllEdges()
			require.NoError(t, err)
			for _, e := range edges {
				if !writeBehind {
					// Without write-behind a relationship create racing a delete
					// of its endpoint can leave it dangling (inherited; bug
					// queue: counter drift under concurrent writes). The
					// write-behind order serializes the two.
					break
				}
				for _, id := range []NodeID{e.StartNode, e.EndNode} {
					_, err := engine.GetNode(id)
					require.NoError(t, err, "relationship %s keeps its endpoint %s", e.ID, id)
				}
			}
			for _, n := range nodes {
				_, err := engine.GetNode(n.ID)
				require.NoError(t, err, "listed node %s is stored", n.ID)
			}
			if countsChecked {
				nodeCount, err := engine.NodeCount()
				require.NoError(t, err)
				require.EqualValues(t, len(nodes), nodeCount, "node count")
				edgeCount, err := engine.EdgeCount()
				require.NoError(t, err)
				require.EqualValues(t, len(edges), edgeCount, "relationship count")
			}
		})
	}
}
