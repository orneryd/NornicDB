package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The on-demand suppression mirror (manualSuppressed) is derived from the
// suppression keys: a suppress, an unsuppress and a delete publish it in
// commit order with the entity's other publications
// (badger_publication_stage.go), so it always holds what storage holds.

// storedManualSuppression reports whether an entity's suppression key is
// committed.
func storedManualSuppression(t *testing.T, b *BadgerEngine, kind byte, id string) bool {
	t.Helper()
	stored := false
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(manualSuppressionKey(kind, id))
		stored = err == nil
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}))
	return stored
}

// suppressionTarget is a node or a relationship to suppress on demand.
type suppressionTarget struct {
	kind byte
	id   string
	set  func(b *BadgerEngine, suppressed bool) error
}

var (
	suppressionNode = suppressionTarget{kind: manualSuppressionNode, id: "db:a", set: func(b *BadgerEngine, suppressed bool) error {
		return b.SetNodeManuallySuppressed("db:a", suppressed)
	}}
	suppressionEdge = suppressionTarget{kind: manualSuppressionEdge, id: "db:r", set: func(b *BadgerEngine, suppressed bool) error {
		return b.SetEdgeManuallySuppressed("db:r", suppressed)
	}}
)

// seedSuppressionGraph creates (a)-[r]->(b) in namespace ns.
func seedSuppressionGraph(t *testing.T, b *BadgerEngine, ns string) {
	t.Helper()
	for _, id := range []string{"a", "b"} {
		_, err := b.CreateNode(&Node{ID: NodeID(ns + ":" + id), Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(ns + ":r"), StartNode: NodeID(ns + ":a"), EndNode: NodeID(ns + ":b"), Type: "CITES"}))
}

func requireMirrorMatchesStorage(t *testing.T, b *BadgerEngine, kind byte, id string, msgAndArgs ...any) {
	t.Helper()
	require.Equal(t, storedManualSuppression(t, b, kind, id), b.manuallySuppressed(kind, id), msgAndArgs...)
}

// Concurrent suppress and unsuppress of one node, and of one relationship:
// after each round the mirror holds what storage holds. Before the mirror was
// a publication, each writer marked it after its own commit, so a later
// commit's mark could land first and an earlier one's overwrite it.
func TestManualSuppressionConcurrentToggleMatchesStorage(t *testing.T) {
	const rounds, writers = 300, 4
	for _, target := range []suppressionTarget{suppressionNode, suppressionEdge} {
		t.Run(string(target.kind), func(t *testing.T) {
			b := createTestBadgerEngine(t)
			seedSuppressionGraph(t, b, "db")
			for round := 0; round < rounds; round++ {
				start := make(chan struct{})
				var wg sync.WaitGroup
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(suppressed bool) {
						defer wg.Done()
						<-start
						assert.NoError(t, target.set(b, suppressed))
					}(w%2 == 0)
				}
				close(start)
				wg.Wait()
				requireMirrorMatchesStorage(t, b, target.kind, target.id, "round %d: the mirror differs from storage", round)
			}
		})
	}
}

// A newer suppression write of an entity waits for the older one's
// publication, and the mirror stays at the older state until it runs: the
// mirror is a commit-order prefix. Deleting a suppressed entity clears it in
// the same order, so the entity created again with its ID isn't hidden.
func TestManualSuppressionPublicationOrder(t *testing.T) {
	for _, target := range []suppressionTarget{suppressionNode, suppressionEdge} {
		deleteTarget := func(b *BadgerEngine, explicit bool) error {
			// A transaction reads what a query reads, and a query reaches a
			// suppressed entity only in a reveal scope.
			defer b.BeginQueryRevealScope(true)()
			if target.kind == manualSuppressionNode {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.DeleteNode("db:a") }, func() error { return b.DeleteNode("db:a") })
			}
			return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.DeleteEdge("db:r") }, func() error { return b.DeleteEdge("db:r") })
		}
		recreateTarget := func(b *BadgerEngine, explicit bool) error {
			node := &Node{ID: "db:a", Labels: []string{"Doc"}}
			edge := &Edge{ID: "db:r", StartNode: "db:a", EndNode: "db:b", Type: "CITES"}
			return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error {
				if target.kind == manualSuppressionNode {
					_, err := tx.CreateNode(node)
					return err
				}
				return tx.CreateEdge(edge)
			}, func() error {
				if target.kind == manualSuppressionNode {
					_, err := b.CreateNode(node)
					return err
				}
				return b.CreateEdge(edge)
			})
		}
		for _, c := range []struct {
			name          string
			initial       bool
			older, newer  func(b *BadgerEngine, explicit bool) error
			want          bool
			transactional bool // the writes have a transaction form
		}{
			{name: "suppress overtaken by unsuppress", older: func(b *BadgerEngine, _ bool) error { return target.set(b, true) }, newer: func(b *BadgerEngine, _ bool) error { return target.set(b, false) }},
			{name: "unsuppress overtaken by suppress", initial: true, older: func(b *BadgerEngine, _ bool) error { return target.set(b, false) }, newer: func(b *BadgerEngine, _ bool) error { return target.set(b, true) }, want: true},
			{name: "suppressed, deleted, created again", initial: true, older: deleteTarget, newer: recreateTarget, transactional: true},
		} {
			for _, explicit := range []bool{false, true} {
				if explicit && !c.transactional {
					continue
				}
				t.Run(fmt.Sprintf("%c/%s/tx=%v", target.kind, c.name, explicit), func(t *testing.T) {
					b := createTestBadgerEngine(t)
					seedSuppressionGraph(t, b, "db")
					require.NoError(t, target.set(b, c.initial))
					publishHeld(t, func() error { return c.older(b, explicit) }, func() error { return c.newer(b, explicit) }, func() {
						assert.Equal(t, c.initial, b.manuallySuppressed(target.kind, target.id), "the mirror while the older publication is held")
					})
					assert.Equal(t, c.want, b.manuallySuppressed(target.kind, target.id), "the mirror after both writes")
					requireMirrorMatchesStorage(t, b, target.kind, target.id)
					if c.want {
						return
					}
					if target.kind == manualSuppressionNode {
						_, err := b.GetNode("db:a")
						assert.NoError(t, err, "the node is visible")
						return
					}
					outgoing, err := b.GetOutgoingEdges("db:a")
					require.NoError(t, err)
					assert.Len(t, outgoing, 1, "the relationship is visible")
				})
			}
		}
	}
}

// Every committing path deletes a suppressed entity's key in the delete's own
// commit and clears the mirror in its publication: a node with the
// relationships it takes, and a relationship.
func TestManualSuppressionClearedByEveryDeletePath(t *testing.T) {
	type path struct {
		engine     func(t *testing.T) *BadgerEngine
		ns         string
		deleteNode func(b *BadgerEngine, id NodeID) error
		deleteEdge func(b *BadgerEngine, id EdgeID) error
		large      bool
	}
	inTx := func(b *BadgerEngine, op func(tx *BadgerTransaction) error) error {
		tx, err := b.BeginTransaction()
		if err != nil {
			return err
		}
		if err := op(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	// buffered commits op through write-behind, then replays it.
	buffered := func(b *BadgerEngine, op func(tx *BadgerTransaction) error) error {
		if err := inTx(b, func(tx *BadgerTransaction) error {
			if err := tx.SetImplicit(true); err != nil {
				return err
			}
			if err := tx.SetNamespace("test"); err != nil {
				return err
			}
			return op(tx)
		}); err != nil {
			return err
		}
		return b.FlushWriteBehind()
	}
	padded := func(op func(tx *BadgerTransaction) error) func(tx *BadgerTransaction) error {
		return func(tx *BadgerTransaction) error {
			for i := 0; i < matrixLargeCommitPadding; i++ {
				if _, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("db:pad-%d", i)), Labels: []string{"Pad"}, Properties: map[string]any{"pad": "0123456789abcdef"}}); err != nil {
					return err
				}
			}
			return op(tx)
		}
	}
	paths := map[string]path{
		"direct": {engine: createTestBadgerEngine, ns: "db",
			deleteNode: func(b *BadgerEngine, id NodeID) error { return b.DeleteNode(id) },
			deleteEdge: func(b *BadgerEngine, id EdgeID) error { return b.DeleteEdge(id) }},
		"tx": {engine: createTestBadgerEngine, ns: "db",
			deleteNode: func(b *BadgerEngine, id NodeID) error {
				return inTx(b, func(tx *BadgerTransaction) error { return tx.DeleteNode(id) })
			},
			deleteEdge: func(b *BadgerEngine, id EdgeID) error {
				return inTx(b, func(tx *BadgerTransaction) error { return tx.DeleteEdge(id) })
			}},
		"bulk": {engine: createTestBadgerEngine, ns: "db",
			deleteNode: func(b *BadgerEngine, id NodeID) error { return b.BulkDeleteNodes([]NodeID{id}) },
			deleteEdge: func(b *BadgerEngine, id EdgeID) error { return b.BulkDeleteEdges([]EdgeID{id}) }},
		"large": {large: true, ns: "db",
			engine: func(t *testing.T) *BadgerEngine {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
				require.NoError(t, err)
				t.Cleanup(func() { _ = b.Close() })
				return b
			},
			deleteNode: func(b *BadgerEngine, id NodeID) error {
				return inTx(b, padded(func(tx *BadgerTransaction) error { return tx.DeleteNode(id) }))
			},
			deleteEdge: func(b *BadgerEngine, id EdgeID) error {
				return inTx(b, padded(func(tx *BadgerTransaction) error { return tx.DeleteEdge(id) }))
			}},
		"replay": {ns: "test",
			engine: func(t *testing.T) *BadgerEngine { return newWriteBehindTestEngine(t, time.Hour) },
			deleteNode: func(b *BadgerEngine, id NodeID) error {
				return buffered(b, func(tx *BadgerTransaction) error { return tx.DeleteNode(id) })
			},
			deleteEdge: func(b *BadgerEngine, id EdgeID) error {
				return buffered(b, func(tx *BadgerTransaction) error { return tx.DeleteEdge(id) })
			}},
	}
	for name, p := range paths {
		for _, deleted := range []string{"node", "edge"} {
			t.Run(name+"/"+deleted, func(t *testing.T) {
				b := p.engine(t)
				a, other, r := NodeID(p.ns+":a"), NodeID(p.ns+":b"), EdgeID(p.ns+":r")
				seedSuppressionGraph(t, b, p.ns)
				require.NoError(t, b.FlushWriteBehind())
				require.NoError(t, b.SetNodeManuallySuppressed(a, true))
				require.NoError(t, b.SetEdgeManuallySuppressed(r, true))
				var batches *atomic.Int64
				if p.large {
					batches = countBatches(t)
				}
				// A transaction reads what a query reads, and a query reaches a
				// suppressed entity only in a reveal scope.
				endReveal := b.BeginQueryRevealScope(true)
				if deleted == "node" {
					require.NoError(t, p.deleteNode(b, a))
				} else {
					require.NoError(t, p.deleteEdge(b, r))
				}
				endReveal()
				if p.large {
					require.Greater(t, batches.Load(), int64(1), "the delete must be a large commit")
				}
				// A node delete takes its relationship with it.
				assert.False(t, storedManualSuppression(t, b, manualSuppressionEdge, string(r)), "the deleted relationship's key")
				assert.False(t, b.manuallySuppressed(manualSuppressionEdge, string(r)), "the deleted relationship's mirror")
				requireMirrorMatchesStorage(t, b, manualSuppressionNode, string(a))
				assert.Equal(t, deleted == "edge", b.manuallySuppressed(manualSuppressionNode, string(a)), "the node stays suppressed only when it wasn't deleted")

				// Created again with the same IDs: not hidden.
				if deleted == "node" {
					_, err := b.CreateNode(&Node{ID: a, Labels: []string{"Doc"}})
					require.NoError(t, err)
				} else {
					require.NoError(t, b.SetNodeManuallySuppressed(a, false))
				}
				require.NoError(t, b.CreateEdge(&Edge{ID: r, StartNode: a, EndNode: other, Type: "CITES"}))
				require.NoError(t, b.FlushWriteBehind())
				_, err := b.GetNode(a)
				require.NoError(t, err)
				outgoing, err := b.GetOutgoingEdges(a)
				require.NoError(t, err)
				assert.Len(t, outgoing, 1)
				assert.False(t, b.HasManualSuppressions())
			})
		}
	}
}
