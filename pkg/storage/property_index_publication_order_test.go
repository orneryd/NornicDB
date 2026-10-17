package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A write's property-index publication runs after its Badger commit, so a
// later write of the same node can publish first. The older write must not
// then remove entries the node's newer state still holds, nor file the
// states the newer write replaced: a delete paused before its publication
// while the ID is recreated, an update paused while a later update restores
// the old value or moves on, a label removal paused while a later write adds
// the label back, and a transaction of two updates of one node.
func TestPropertyIndexPublicationOutOfOrder(t *testing.T) {
	id := NodeID("db:x")
	node := func(labels []string, value string) *Node {
		return &Node{ID: id, Labels: labels, Properties: map[string]any{"p": value}}
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
				sm.MarkPropertyIndexesFilled()
				_, err := b.CreateNode(scenario.initial)
				require.NoError(t, err)

				publishOutOfOrder(t, func() error { return scenario.older(b, explicit) }, func() error { return scenario.newer(b) })

				// The committed state, read past the node cache: the cache has its
				// own publication-order race, outside this index's contract.
				require.Equal(t, scenario.live, committedValue(t, b, id, "p"))
				require.Equal(t, []NodeID{id}, sm.PropertyIndexLookup("L", "p", scenario.live), "the live node must stay findable")
				for _, stale := range scenario.stale {
					require.Empty(t, sm.PropertyIndexLookup("L", "p", stale), "a replaced value must not stay filed")
				}
			})
		}
	}
}

// A composite index keeps the full and prefix entries of an ID recreated
// while its delete was paused before publication.
func TestCompositeIndexPublicationOutOfOrder(t *testing.T) {
	id := NodeID("db:x")
	recreated := &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"p": "v", "q": "w"}}
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			b := newTestEngine(t)
			sm := b.GetSchemaForNamespace("db")
			require.NoError(t, sm.AddCompositeIndex("idx_l_pq", "L", []string{"p", "q"}))
			sm.MarkPropertyIndexesFilled()
			_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"p": "v", "q": "w"}})
			require.NoError(t, err)

			publishOutOfOrder(t, func() error {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.DeleteNode(id) }, func() error { return b.DeleteNode(id) })
			}, func() error { _, err := b.CreateNode(recreated); return err })

			idx, ok := sm.GetCompositeIndex("idx_l_pq")
			require.True(t, ok)
			require.Equal(t, []NodeID{id}, idx.LookupFull("v", "w"))
			require.Equal(t, []NodeID{id}, idx.LookupPrefix("v"))
		})
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
