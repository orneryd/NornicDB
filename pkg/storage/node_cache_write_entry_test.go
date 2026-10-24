package storage

import (
	"fmt"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestNodeCacheWriteEntryOwnsRecursiveProperties(t *testing.T) {
	b := createTestBadgerEngine(t)
	input := map[string]any{
		"list":     []any{uint8(128), int8(3)},
		"nested":   map[string]any{"list": []int{-129, 128}},
		"overflow": []any{uint64(math.MaxUint64), uint64(math.MaxUint64)},
		"mixed":    []any{uint64(5), uint64(math.MaxUint64)},
		"float":    []any{float64(3), int(3)},
		"bytes":    []byte{1, 2},
	}
	want := map[string]any{
		"list":     []int64{128, 3},
		"nested":   map[string]any{"list": []int64{-129, 128}},
		"overflow": []uint64{math.MaxUint64, math.MaxUint64},
		"mixed":    []any{int64(5), uint64(math.MaxUint64)},
		"float":    []any{float64(3), int64(3)},
		"bytes":    []byte{1, 2},
	}
	id := NodeID(prefixTestID("write-entry-owned"))
	_, err := b.CreateNode(&Node{ID: id, Properties: input})
	require.NoError(t, err)
	require.IsType(t, uint8(0), input["list"].([]any)[0])
	require.IsType(t, []int{}, input["nested"].(map[string]any)["list"])
	input["list"].([]any)[0] = int(9)
	input["nested"].(map[string]any)["list"].([]int)[0] = 9
	input["bytes"].([]byte)[0] = 9
	warm, err := b.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, want, warm.Properties)
	b.nodeCacheMu.Lock()
	delete(b.nodeCache, id)
	delete(b.queryNodeCache, id)
	b.nodeCacheMu.Unlock()
	cold, err := b.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, want, cold.Properties)
	require.Equal(t, want, canonicalPropertyValue(want), "canonical form must be idempotent")
}

func TestNodeCacheWriteEntryRefusesEveryUndeclaredTerminal(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID(prefixTestID("write-entry-refusal"))
	_, err := b.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	for name, write := range map[string]func(*badger.Txn) error{
		"ordinary": func(txn *badger.Txn) error {
			_, err := b.db.commitShared(txn, b, nil, nodeCacheWriteScope{})
			return err
		},
		"large intent":         func(*badger.Txn) error { _, err := b.db.beginLargeCommit(nodeCacheWriteScope{}); return err },
		"large batch":          func(txn *badger.Txn) error { return (&largeCommit{db: b.db}).commitBatch(txn) },
		"restore batch":        func(txn *badger.Txn) error { return (&restoreBatches{db: b.db, restoreTs: 1}).commitBatch(txn) },
		"writer construction":  func(txn *badger.Txn) error { _, err := b.newCommitWriter(nodeCacheWriteScope{}, b.db, txn); return err },
		"physical prefix drop": func(*badger.Txn) error { return b.db.dropPrefixWithScope(nodeCacheWriteScope{}, nodeKey(id)) },
	} {
		t.Run(name, func(t *testing.T) {
			txn, readTs, err := b.db.beginTxn(true)
			require.NoError(t, err)
			defer b.db.endRead(readTs)
			defer txn.Discard()
			next := &Node{ID: id, Properties: map[string]any{"v": int64(2)}}
			data, _, err := b.encodeNodeInTxn(txn, namespaceForNodeID(id), next)
			require.NoError(t, err)
			require.NoError(t, txn.Set(nodeKey(id), data))
			require.ErrorIs(t, write(txn), ErrInvalidData)
			require.NoError(t, b.db.View(func(read *badger.Txn) error {
				item, err := read.Get(nodeKey(id))
				require.NoError(t, err)
				return item.Value(func(body []byte) error {
					node, err := b.decodeNode(namespaceForNodeID(id), body)
					require.NoError(t, err)
					require.Equal(t, int64(1), node.Properties["v"])
					return nil
				})
			}))
		})
	}
}

func TestNodeCacheWriteEntryCanonicalNullAndEmptyContainers(t *testing.T) {
	b := createTestBadgerEngine(t)
	for index, properties := range []map[string]any{
		nil,
		{"bytes": []byte(nil), "list": []int64(nil), "map": map[string]any(nil), "empty": []int{}},
	} {
		id := NodeID(prefixTestID(fmt.Sprintf("write-entry-container-%d", index)))
		_, err := b.CreateNode(&Node{ID: id, Properties: properties})
		require.NoError(t, err)
		warm, err := b.GetNode(id)
		require.NoError(t, err)
		b.nodeCacheMu.Lock()
		delete(b.nodeCache, id)
		delete(b.queryNodeCache, id)
		b.nodeCacheMu.Unlock()
		cold, err := b.GetNode(id)
		require.NoError(t, err)
		require.Equal(t, cold.Properties, warm.Properties)
		if properties == nil {
			require.Equal(t, map[string]any{}, warm.Properties)
		} else {
			require.Equal(t, map[string]any{"bytes": nil, "list": nil, "map": nil, "empty": []any{}}, warm.Properties)
		}
	}
}
