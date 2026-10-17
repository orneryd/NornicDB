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

// F-PUBLISH: a write's post-commit publication runs after its Badger commit,
// so a later write of the same entity can publish first. Every publisher
// derived from committed entities must still end at the entity's latest
// committed state. Each case pauses the older write at runCommitTailHook,
// runs the newer write to completion, then lets the older one publish, and
// checks every node consumer: the node and query-node caches (GetNode,
// GetQueryNode), an arity-1 and a composite property index, and the unique
// value registry (its holder, and a second node with the live value is
// rejected).
func TestPostCommitPublicationOrder(t *testing.T) {
	id := NodeID("db:x")
	node := func(labels []string, value string) *Node {
		return &Node{ID: id, Labels: labels, Properties: map[string]any{"p": value, "q": "w", "u": value}}
	}
	update := func(labels []string, values ...string) func(b *BadgerEngine, explicit bool) error {
		return func(b *BadgerEngine, explicit bool) error {
			return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error {
				for _, value := range values {
					if err := tx.UpdateNode(node(labels, value)); err != nil {
						return err
					}
				}
				return nil
			}, func() error { return b.UpdateNode(node(labels, values[len(values)-1])) })
		}
	}
	for _, scenario := range []struct {
		name         string
		initial      *Node
		older        func(b *BadgerEngine, explicit bool) error
		newer        func(b *BadgerEngine) error
		live         string
		stale        []string
		explicitOnly bool
	}{
		{
			name:    "delete then recreate",
			initial: node([]string{"L"}, "v"),
			older: func(b *BadgerEngine, explicit bool) error {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.DeleteNode(id) }, func() error { return b.DeleteNode(id) })
			},
			newer: func(b *BadgerEngine) error { _, err := b.CreateNode(node([]string{"L"}, "v")); return err },
			live:  "v",
		},
		{
			name:    "update then restore the value",
			initial: node([]string{"L"}, "v1"),
			older:   update([]string{"L"}, "v2"),
			newer:   func(b *BadgerEngine) error { return b.UpdateNode(node([]string{"L"}, "v1")) },
			live:    "v1",
			stale:   []string{"v2"},
		},
		{
			name:    "update overtaken by a later update",
			initial: node([]string{"L"}, "a"),
			older:   update([]string{"L"}, "b"),
			newer:   func(b *BadgerEngine) error { return b.UpdateNode(node([]string{"L"}, "c")) },
			live:    "c",
			stale:   []string{"a", "b"},
		},
		{
			name:    "label removal then re-add",
			initial: node([]string{"L"}, "v"),
			older:   update([]string{"Other"}, "v"),
			newer:   func(b *BadgerEngine) error { return b.UpdateNode(node([]string{"L"}, "v")) },
			live:    "v",
		},
		{
			name: "create overtaken by an update",
			older: func(b *BadgerEngine, explicit bool) error {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(node([]string{"L"}, "v1")); return err },
					func() error { _, err := b.CreateNode(node([]string{"L"}, "v1")); return err })
			},
			newer: func(b *BadgerEngine) error { return b.UpdateNode(node([]string{"L"}, "v2")) },
			live:  "v2",
			stale: []string{"v1"},
		},
		{
			name:         "two updates in one transaction",
			initial:      node([]string{"L"}, "v1"),
			older:        update([]string{"L"}, "v2", "v3"),
			newer:        func(b *BadgerEngine) error { return b.UpdateNode(node([]string{"L"}, "v1")) },
			live:         "v1",
			stale:        []string{"v2", "v3"},
			explicitOnly: true,
		},
	} {
		for _, explicit := range []bool{false, true} {
			if scenario.explicitOnly && !explicit {
				continue
			}
			t.Run(fmt.Sprintf("%s/explicit=%v", scenario.name, explicit), func(t *testing.T) {
				b := newTestEngine(t)
				sm := b.GetSchemaForNamespace("db")
				require.NoError(t, sm.AddPropertyIndex("idx_l_p", "L", []string{"p"}))
				require.NoError(t, sm.AddCompositeIndex("idx_l_pq", "L", []string{"p", "q"}))
				require.NoError(t, sm.AddUniqueConstraint("unique_l_u", "L", "u"))
				sm.MarkPropertyIndexesFilled()
				if scenario.initial != nil {
					_, err := b.CreateNode(scenario.initial)
					require.NoError(t, err)
				}
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(b, sm))
				// Warm the caches with the initial state, so a publication that
				// leaves them alone would be seen too.
				_, _ = b.GetNode(id)
				_, _ = b.GetQueryNode(id)

				publishOutOfOrder(t, func() error { return scenario.older(b, explicit) }, func() error { return scenario.newer(b) })

				live := scenario.live
				assert.Equal(t, live, committedValue(t, b, id, "p"))
				for name, read := range map[string]func(NodeID) (*Node, error){"GetNode": b.GetNode, "GetQueryNode": b.GetQueryNode} {
					got, err := read(id)
					if !assert.NoError(t, err, name) {
						continue
					}
					assert.Equal(t, live, got.Properties["p"], "%s must return the committed node", name)
				}

				assert.Equal(t, []NodeID{id}, sm.PropertyIndexLookup("L", "p", live), "the live node must stay findable")
				composite, ok := sm.GetCompositeIndex("idx_l_pq")
				assert.True(t, ok)
				assert.Equal(t, []NodeID{id}, composite.LookupFull(live, "w"))
				assert.Equal(t, []NodeID{id}, composite.LookupPrefix(live))

				holder, found, complete, constrained := sm.lookupUniqueConstraintValueForValidation("L", "u", live)
				assert.True(t, constrained && complete)
				assert.True(t, found, "the unique registry must hold the live value")
				assert.Equal(t, id, holder)
				_, err := b.CreateNode(&Node{ID: "db:other", Labels: []string{"L"}, Properties: map[string]any{"u": live}})
				assert.Error(t, err, "a second node with the live unique value must be rejected")

				for _, stale := range scenario.stale {
					assert.Empty(t, sm.PropertyIndexLookup("L", "p", stale), "a replaced value must not stay filed")
					assert.Empty(t, composite.LookupFull(stale, "w"))
					holder, found, _, _ := sm.lookupUniqueConstraintValueForValidation("L", "u", stale)
					assert.False(t, found && holder == id, "the registry must not keep a replaced value")
					_, err := b.CreateNode(&Node{ID: NodeID("db:free-" + stale), Labels: []string{"L"}, Properties: map[string]any{"u": stale}})
					assert.NoError(t, err, "a replaced unique value must be free for another node")
				}
			})
		}
	}
}

// The edge publishers: the edge cache and the relationship-uniqueness index
// keep an edge's latest committed state when an older write of it publishes
// last, so a second edge with the live value is rejected and one with a
// replaced value is accepted.
func TestPostCommitEdgePublicationOrder(t *testing.T) {
	id := EdgeID("db:e")
	edge := func(token string) *Edge {
		return &Edge{ID: id, StartNode: "db:a", EndNode: "db:b", Type: "LINK", Properties: map[string]any{"token": token}}
	}
	for _, scenario := range []struct {
		name  string
		older func(b *BadgerEngine, explicit bool) error
		newer func(b *BadgerEngine) error
		live  string
		stale string
	}{
		{
			name: "delete then recreate",
			older: func(b *BadgerEngine, explicit bool) error {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.DeleteEdge(id) }, func() error { return b.DeleteEdge(id) })
			},
			newer: func(b *BadgerEngine) error { return b.CreateEdge(edge("t")) },
			live:  "t",
		},
		{
			name: "update then restore the value",
			older: func(b *BadgerEngine, explicit bool) error {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.UpdateEdge(edge("t2")) }, func() error { return b.UpdateEdge(edge("t2")) })
			},
			newer: func(b *BadgerEngine) error { return b.UpdateEdge(edge("t")) },
			live:  "t",
			stale: "t2",
		},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", scenario.name, explicit), func(t *testing.T) {
				b := newTestEngine(t)
				for _, endpoint := range []NodeID{"db:a", "db:b"} {
					_, err := b.CreateNode(&Node{ID: endpoint, Labels: []string{"N"}})
					require.NoError(t, err)
				}
				require.NoError(t, b.CreateEdge(edge("t")))
				sm := b.GetSchemaForNamespace("db")
				require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"token"}}))
				// A rejected duplicate builds the relationship-uniqueness index.
				require.Error(t, createEdgeInTransaction(b, &Edge{ID: "db:warm", StartNode: "db:b", EndNode: "db:a", Type: "LINK", Properties: map[string]any{"token": "t"}}))

				publishOutOfOrder(t, func() error { return scenario.older(b, explicit) }, func() error { return scenario.newer(b) })

				assert.Equal(t, scenario.live, committedEdgeValue(t, b, id, "token"))
				if cached, ok := b.cacheLoadEdge(id); ok {
					assert.Equal(t, scenario.live, cached.Properties["token"], "the edge cache must not hold a replaced edge")
				}
				// The second read is served from the adjacency and edge caches.
				for read := 0; read < 2; read++ {
					edges, err := b.GetOutgoingEdges("db:a")
					assert.NoError(t, err)
					if !assert.Len(t, edges, 1) {
						continue
					}
					assert.Equal(t, scenario.live, edges[0].Properties["token"], "read %d must return the committed edge", read)
				}
				assert.Error(t, createEdgeInTransaction(b, &Edge{ID: "db:dup", StartNode: "db:b", EndNode: "db:a", Type: "LINK", Properties: map[string]any{"token": scenario.live}}),
					"a second edge with the live unique value must be rejected")
				if scenario.stale != "" {
					assert.NoError(t, createEdgeInTransaction(b, &Edge{ID: "db:free", StartNode: "db:b", EndNode: "db:a", Type: "LINK", Properties: map[string]any{"token": scenario.stale}}),
						"a replaced value must be free again")
				}
			})
		}
	}
}

// publishOutOfOrder runs older until its first commit pauses before
// publication, runs newer to completion, then lets older publish.
func publishOutOfOrder(t *testing.T, older, newer func() error) {
	t.Helper()
	started, resume := make(chan struct{}), make(chan struct{})
	var paused atomic.Bool
	var unblock sync.Once
	defer unblock.Do(func() { close(resume) })
	restore := setCommitTailHook(func() {
		if paused.CompareAndSwap(false, true) {
			close(started)
			<-resume
		}
	})
	defer restore()

	done := make(chan error, 1)
	go func() { done <- older() }()
	select {
	case <-started:
	case <-time.After(4 * time.Second):
		t.Fatal("the older write never reached its publication")
	}
	require.NoError(t, newer())
	unblock.Do(func() { close(resume) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("the older write never finished")
	}
}

func writeMaybeInTransaction(b *BadgerEngine, explicit bool, inTx func(*BadgerTransaction) error, direct func() error) error {
	if !explicit {
		return direct()
	}
	tx, err := b.BeginTransaction()
	if err != nil {
		return err
	}
	if err := inTx(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func createEdgeInTransaction(b *BadgerEngine, edge *Edge) error {
	return writeMaybeInTransaction(b, true, func(tx *BadgerTransaction) error { return tx.CreateEdge(edge) }, nil)
}

// committedValue reads property of the committed node body directly from
// Badger, past the node cache.
func committedValue(t *testing.T, b *BadgerEngine, id NodeID, property string) interface{} {
	t.Helper()
	var value interface{}
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			namespace, _, _ := ParseDatabasePrefix(string(id))
			node, err := b.decodeNode(namespace, val)
			if err == nil {
				value = node.Properties[property]
			}
			return err
		})
	}))
	return value
}

// committedEdgeValue reads property of the committed edge body directly from
// Badger, past the edge cache.
func committedEdgeValue(t *testing.T, b *BadgerEngine, id EdgeID, property string) interface{} {
	t.Helper()
	var value interface{}
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		edge, ok, err := b.readEdgeBodyInTxn(txn, id)
		if ok {
			value = edge.Properties[property]
		}
		return err
	}))
	return value
}
