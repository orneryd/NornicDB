package storage

import (
	"encoding/binary"
	"strings"
)

// retireNamespaceSideState runs under the exclusive publication barrier after
// the durable key-family cleanup. It retires mirrors and pending entity work as
// one part of the namespace operation; foreign entries are preserved.
func (b *BadgerEngine) retireNamespaceSideState(prefix, namespace string, whole bool, nodes, edges map[uint64]struct{}) {
	b.manualSuppressionMu.Lock()
	for key := range b.manualSuppressed {
		if len(key) > 1 && strings.HasPrefix(key[1:], prefix) {
			delete(b.manualSuppressed, key)
		}
	}
	b.manualSuppressionCount.Store(int64(len(b.manualSuppressed)))
	b.manualSuppressionMu.Unlock()

	b.nodeBodyCacheMu.Lock()
	for id, entry := range b.nodeBodyCache {
		if strings.HasPrefix(string(id), prefix) {
			b.removeNodeBodyCacheEntryLocked(id, entry)
		}
	}
	b.nodeBodyCacheMu.Unlock()
	b.edgeCacheMu.Lock()
	b.edgeCacheGen.advance()
	for id := range b.edgeCache {
		if strings.HasPrefix(string(id), prefix) {
			delete(b.edgeCache, id)
		}
	}
	b.edgeCacheMu.Unlock()
	b.adjCacheMu.Lock()
	b.adjCacheGen.advance()
	for id := range b.outgoingAdjCache {
		if strings.HasPrefix(string(id), prefix) {
			delete(b.outgoingAdjCache, id)
		}
	}
	for id := range b.incomingAdjCache {
		if strings.HasPrefix(string(id), prefix) {
			delete(b.incomingAdjCache, id)
		}
	}
	b.adjCacheMu.Unlock()
	b.labelFirstNodeCacheMu.Lock()
	b.labelFirstCacheGen.advance()
	for label, id := range b.labelFirstNodeCache {
		if strings.HasPrefix(string(id), prefix) {
			delete(b.labelFirstNodeCache, label)
		}
	}
	b.labelFirstNodeCacheMu.Unlock()
	b.mvccFloorKeysMu.Lock()
	for key := range b.mvccFloorKeys {
		id := binary.BigEndian.Uint64(key[1:])
		_, node := nodes[id]
		_, edge := edges[id]
		if (key[0] == prefixMVCCNode && node) || (key[0] == prefixMVCCEdge && edge) {
			delete(b.mvccFloorKeys, key)
		}
	}
	b.mvccFloorKeysMu.Unlock()
	if whole {
		b.mvccByNamespaceMu.Lock()
		delete(b.mvccByNamespace, namespace)
		b.mvccByNamespaceMu.Unlock()
	}
	if pending, ok := b.accumulator.(interface{ ClearNamespace(string) }); ok {
		pending.ClearNamespace(prefix)
	}
}
