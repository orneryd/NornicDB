package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Keep physical data/index evidence in a failed public regression. In
// particular this distinguishes a reservation defect from a stale read cache.
func (b *BadgerEngine) NodeUniqueDiagnosticForTest(namespace, label, property string, value interface{}) string {
	schema := b.GetSchemaForNamespace(namespace)
	view, source := schema.beginPendingRead()
	defer endPendingRead(source)
	canonical, _ := indexValueKey(value)
	text := fmt.Sprintf("pending=%v", view.valueMatches(nil, label, property, canonical))
	key, _ := nodeUniqueKey(namespace, label, property, value)
	err := b.db.View(func(txn *badger.Txn) error {
		holder, err := readNodeUniqueKey(txn, key)
		text += fmt.Sprintf(" stored holder=%q err=%v", holder, err)
		if err != nil || holder == "" {
			return err
		}
		previous, err := readNodeUniqueReverse(txn, holder)
		text += fmt.Sprintf(" logical keep=%t reverse=%x err=%v", view.keep(holder), previous, err)
		item, err := txn.Get(nodeKey(holder))
		if err != nil {
			text += fmt.Sprintf(" primary=%v", err)
			return err
		}
		return item.Value(func(data []byte) error {
			node, err := b.decodeNode(namespace, data)
			text += fmt.Sprintf(" physical=%+v err=%v", node, err)
			return err
		})
	})
	return fmt.Sprintf("%s; diagnostic err=%v", text, err)
}

func (b *BadgerEngine) CommitConflictStopWriteBehindForTest() { b.writeBehind.Close() }

func (b *BadgerEngine) CommitConflictAfterDetectionForTest(visit func(NodeID)) {
	b.nodeConflictDetected = visit
}

// Fail the actual conflict-head decoder after the public statement has read and
// staged its node. The baseline itself never reads storage during classification.
func (b *BadgerEngine) CommitSnapshotFailureFixture(t *testing.T, id NodeID) string {
	t.Helper()
	before, err := b.GetNode(id)
	require.NoError(t, err)
	key := b.mvccNodeHeadKeyStringLookup(id)
	require.NotNil(t, key)
	corrupt := []byte{0xff}
	_, decodeErr := decodeMVCCHead(corrupt)
	require.Error(t, decodeErr)
	var once sync.Once
	b.nodeBeforeConflictRead = func(nodeID NodeID) {
		if nodeID != id {
			return
		}
		once.Do(func() {
			require.NoError(t, b.db.Update(func(txn *badger.Txn) error { return txn.Set(key, corrupt) }))
		})
	}
	t.Cleanup(func() {
		b.nodeBeforeConflictRead = nil
		require.NoError(t, b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(nodeKey(id))
			if err != nil {
				return err
			}
			return item.Value(func(value []byte) error {
				node, err := b.decodeNode(namespaceForNodeID(id), value)
				if err == nil {
					require.Equal(t, before.Properties, node.Properties, "the failed commit must not publish its pending write")
				}
				return err
			})
		}))
	})
	return decodeErr.Error()
}
