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
// then remove entries the node's newer state still holds: a delete paused
// before its publication while the ID is recreated, an update paused while a
// later update restores the old value, and a label removal paused while a
// later write adds the label back.
func TestPropertyIndexPublicationOutOfOrder(t *testing.T) {
	id := NodeID("db:x")
	node := func(labels []string, value string) *Node {
		return &Node{ID: id, Labels: labels, Properties: map[string]any{"p": value}}
	}
	for _, scenario := range []struct {
		name    string
		initial *Node
		older   func(b *BadgerEngine, explicit bool) error
		newer   func(b *BadgerEngine) error
		live    string
		stale   string
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
			older: func(b *BadgerEngine, explicit bool) error {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.UpdateNode(node([]string{"L"}, "v2")) }, func() error { return b.UpdateNode(node([]string{"L"}, "v2")) })
			},
			newer: func(b *BadgerEngine) error { return b.UpdateNode(node([]string{"L"}, "v1")) },
			live:  "v1",
			stale: "v2",
		},
		{
			name:    "label removal then re-add",
			initial: node([]string{"L"}, "v"),
			older: func(b *BadgerEngine, explicit bool) error {
				return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.UpdateNode(node([]string{"Other"}, "v")) }, func() error { return b.UpdateNode(node([]string{"Other"}, "v")) })
			},
			newer: func(b *BadgerEngine) error { return b.UpdateNode(node([]string{"L"}, "v")) },
			live:  "v",
		},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", scenario.name, explicit), func(t *testing.T) {
				b := newTestEngine(t)
				sm := b.GetSchemaForNamespace("db")
				require.NoError(t, sm.AddPropertyIndex("idx_l_p", "L", []string{"p"}))
				sm.MarkPropertyIndexesFilled()
				_, err := b.CreateNode(scenario.initial)
				require.NoError(t, err)

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

				older := make(chan error, 1)
				go func() { older <- scenario.older(b, explicit) }()
				select {
				case <-started:
				case <-time.After(4 * time.Second):
					t.Fatal("the older write never reached its publication")
				}
				require.NoError(t, scenario.newer(b))
				unblock.Do(func() { close(resume) })
				select {
				case err := <-older:
					require.NoError(t, err)
				case <-time.After(4 * time.Second):
					t.Fatal("the older write never finished")
				}

				// The committed state, read past the node cache: the cache has its
				// own publication-order race, outside this index's contract.
				require.Equal(t, scenario.live, committedValue(t, b, id, "p"))
				require.Equal(t, []NodeID{id}, sm.PropertyIndexLookup("L", "p", scenario.live), "the live node must stay findable")
				if scenario.stale != "" {
					require.Empty(t, sm.PropertyIndexLookup("L", "p", scenario.stale))
				}
			})
		}
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
