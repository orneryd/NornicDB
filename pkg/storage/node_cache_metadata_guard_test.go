package storage

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func init() {
	// The benchmark runner explicitly selects no tests. Keep its cost path
	// identical to production; every normal storage test gets the guard.
	for _, arg := range os.Args[1:] {
		if arg == "-test.run=^$" {
			return
		}
	}
	metadataCommitCheck = checkMetadataCacheCommit
	nodeCacheScopeCheck = checkNodeCacheScope
}

func checkMetadataCacheCommit(txn *badger.Txn) error {
	// Badger v4.9.6 has no public write-set visitor. Read its pending key
	// map in tests only, without unsafe or changing the dependency. Fail
	// closed if an upgrade changes that representation. This sees deletes
	// as well as sets and does not scan the existing database.
	pending := reflect.ValueOf(txn).Elem().FieldByName("pendingWrites")
	if !pending.IsValid() || pending.Kind() != reflect.Map {
		return fmt.Errorf("metadata commit guard: Badger write set changed")
	}
	keys := pending.MapRange()
	for keys.Next() {
		key := keys.Key().String()
		if !metadataOnlyCacheKey(key) {
			return fmt.Errorf("metadata-only commit writes data key %x", key)
		}
	}
	return nil
}

func metadataOnlyCacheKey(key string) bool {
	if len(key) == 0 {
		return false
	}
	switch key[0] {
	case prefixIDDictNodeForward, prefixIDDictNodeReverse, prefixIDDictEdgeForward,
		prefixIDDictEdgeReverse, prefixIDDictCounter, prefixIDFreelist,
		prefixPropKeyForward, prefixPropKeyReverse, prefixPropKeyCounter:
		return true
	case prefixMVCCMeta:
		if len(key) < 2 {
			return false
		}
		switch key[1] {
		case prefixMVCCMetaNamespaceSeq, prefixMVCCMetaLabelCountReady,
			prefixMVCCMetaLabelCount, prefixMVCCMetaLabelCountDelta,
			prefixMVCCMetaEdgeTypeCountReady, prefixMVCCMetaEdgeTypeCount,
			prefixMVCCMetaEdgeTypeCountDelta, prefixMVCCMetaEdgeTypeStartLabelCount,
			prefixMVCCMetaEdgeTypeEndLabelCount:
			return true
		}
	}
	return false
}

func TestNodeCacheMetadataGuardRejectsData(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { _ = b.Close() })
	for _, key := range [][]byte{
		nodeKey("test:metadata-guard"), embeddingKey("test:metadata-guard", 0),
		embeddingMetaKey("test:metadata-guard"), {prefixMVCCNode, 1},
		{prefixMVCCNodeHead, 1}, {prefixPendingEmbed, 1},
	} {
		for _, deleting := range []bool{false, true} {
			err := b.withMetadataUpdate(func(txn *badger.Txn) error {
				if deleting {
					return txn.Delete(key)
				}
				return txn.Set(key, []byte{1})
			})
			require.ErrorContains(t, err, "metadata-only commit writes data key")
		}
	}
}

// The same actual-write-set check guards authoritative scoped invalidation.
// A new node/sidecar writer cannot silently declare itself unrelated.
func checkNodeCacheScope(txn *badger.Txn, scope nodeCacheWriteScope) error {
	if !scope.known {
		return nil
	}
	pending := reflect.ValueOf(txn).Elem().FieldByName("pendingWrites")
	if !pending.IsValid() || pending.Kind() != reflect.Map {
		return fmt.Errorf("cache scope guard: Badger write set changed")
	}
	// Borrowed replay inventories can contain tens of thousands of nodes.
	// Index the declaration once, then validate every actual key against it.
	nodes, rawKeys := cacheScopeInventory(scope)
	keys := pending.MapRange()
	for keys.Next() {
		key := keys.Key().String()
		if len(key) == 0 || (key[0] != prefixNode && key[0] != prefixEdge && !nodeCacheEmbeddingPrefix(key[0])) {
			continue
		}
		covered := false
		if key[0] == prefixNode {
			_, covered = nodes[NodeID(key[1:])]
		} else if key[0] == prefixEdge {
			_, covered = rawKeys[key]
		} else if _, raw := rawKeys[key]; raw {
			covered = true
		} else {
			for _, owner := range embeddingKeyOwners([]byte(key)) {
				if _, ok := nodes[owner.id]; ok {
					covered = true
					break
				}
			}
		}
		if !covered {
			return fmt.Errorf("cache scope misses node/embedding key or relationship key %x", key)
		}
	}
	return nil
}

func cacheScopeInventory(scope nodeCacheWriteScope) (map[NodeID]struct{}, map[string]struct{}) {
	nodes := make(map[NodeID]struct{})
	rawKeys := make(map[string]struct{})
	addNode := func(id NodeID) { nodes[id] = struct{}{} }
	addEdge := func(edge *Edge) {
		if edge != nil {
			rawKeys[string(edgeKey(edge.ID))] = struct{}{}
		}
	}
	if scope.relationships != nil {
		scope.relationships(addEdge)
	}
	if scope.nodeIDs != nil {
		scope.nodeIDs(addNode)
	}
	if scope.nodeID != "" {
		addNode(scope.nodeID)
	}
	addRaw := func(key string) {
		rawKeys[key] = struct{}{}
		if id, ok := nodeCacheKeyID(key); ok {
			addNode(id)
		}
	}
	addInventory := func(ops []Operation, writes map[string][]byte, deletes map[string]bool) {
		for _, op := range ops {
			addEdge(op.Edge)
			addEdge(op.OldEdge)
			if op.EdgeID != "" {
				rawKeys[string(edgeKey(op.EdgeID))] = struct{}{}
			}
			for _, id := range op.DeletedEdgeIDs {
				rawKeys[string(edgeKey(id))] = struct{}{}
			}
			if op.Type == OpCreateNode || op.Type == OpUpdateNode || op.Type == OpDeleteNode || op.Type == OpUpdateEmbedding {
				addNode(operationCacheNodeID(op))
			}
		}
		for key := range writes {
			addRaw(key)
		}
		for key, deleting := range deletes {
			if deleting {
				addRaw(key)
			}
		}
	}
	if tx := scope.transaction; tx != nil {
		addInventory(tx.operations, nil, nil)
	}
	addInventory(nil, scope.writes, scope.deletes)
	for _, c := range scope.buffered {
		addInventory(c.ops, c.writes, c.deletes)
	}
	return nodes, rawKeys
}

func TestNodeCacheScopeGuardRejectsUndeclaredNode(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = b.Close() })
	for _, key := range [][]byte{nodeKey("test:unlisted"), edgeKey("test:unlisted"), embeddingKey("test:unlisted", 0), embeddingMetaKey("test:unlisted")} {
		for _, deleting := range []bool{false, true} {
			err := b.withScopedNodeCacheUpdate(singleNodeCacheWrites("test:other"), func(txn *badger.Txn) error {
				if deleting {
					return txn.Delete(key)
				}
				return txn.Set(key, []byte{1})
			}, nil)
			require.ErrorContains(t, err, "cache scope misses node/embedding key", "key=%x deleting=%t", key, deleting)
		}
	}
}

func nodeCacheKeyID(key string) (NodeID, bool) {
	if len(key) > 1 && key[0] == prefixNode {
		return NodeID(key[1:]), true
	}
	owners := embeddingKeyOwners([]byte(key))
	if len(owners) == 1 {
		return owners[0].id, true
	}
	return "", false
}
