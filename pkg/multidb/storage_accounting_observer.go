package multidb

import "github.com/orneryd/nornicdb/pkg/storage"

func (t *sizeTrackingEngine) prepareBaseAccountingProjection() storage.AccountingPreparation {
	initialized, err := t.manager.storageSizeInitialized(t.dbName)
	if err != nil || !initialized {
		return storage.AccountingPreparation{Publish: func(storage.StoredAccountingPairs, error) {
			t.manager.markStorageSizeDirty(t.dbName)
		}}
	}
	return storage.AccountingPreparation{Images: true, Publish: t.prepareBaseAccounting()}
}

// Base maintenance uses the same stored-pair calculation and publication owner
// as admitted commits. It never takes write admission while holding storage
// locks. The epoch fence makes lazy recounts safe without reversing lock order.
func (t *sizeTrackingEngine) prepareBaseAccounting() func(storage.StoredAccountingPairs, error) {
	t.manager.mu.RLock()
	info := t.manager.databases[t.dbName]
	t.manager.mu.RUnlock()
	if info == nil {
		return func(storage.StoredAccountingPairs, error) {}
	}
	info.sizeMu.RLock()
	epoch := info.sizeEpoch
	info.sizeMu.RUnlock()
	return func(pairs storage.StoredAccountingPairs, readErr error) {
		if readErr != nil {
			t.manager.markStorageSizeDirty(t.dbName)
			return
		}
		// These are already decoded full stored images, not staged Go values.
		for _, side := range []*storage.TransactionChanges{&pairs.Prior, &pairs.Final} {
			for _, node := range side.Nodes {
				if node != nil {
					node.ID = storage.NodeID(storage.StripDatabasePrefix(t.dbName, string(node.ID)))
				}
			}
			for _, edge := range side.Edges {
				if edge == nil {
					continue
				}
				edge.ID = storage.EdgeID(storage.StripDatabasePrefix(t.dbName, string(edge.ID)))
				edge.StartNode = storage.NodeID(storage.StripDatabasePrefix(t.dbName, string(edge.StartNode)))
				edge.EndNode = storage.NodeID(storage.StripDatabasePrefix(t.dbName, string(edge.EndNode)))
			}
		}
		nodes, err := accountingForStoredPairs(pairs.Prior.Nodes, pairs.Final.Nodes, calculateNodeSize)
		if err != nil {
			t.manager.markStorageSizeDirty(t.dbName)
			return
		}
		edges, err := accountingForStoredPairs(pairs.Prior.Edges, pairs.Final.Edges, calculateEdgeSize)
		if err != nil {
			t.manager.markStorageSizeDirty(t.dbName)
			return
		}
		t.manager.publishAccountingBatch(t.dbName, accountingBatch{nodes: nodes, edges: edges}, &epoch)
	}
}

func accountingForStoredPairs[K comparable, V any](prior, final map[K]*V, size func(*V) (int64, error)) (entityAccountingDelta, error) {
	pending := make(map[K]*V, len(prior)+len(final))
	for id := range prior {
		pending[id] = nil
	}
	for id, body := range final {
		pending[id] = body
	}
	return storedEntityAccounting(pending, func(id K) (*V, error) { return prior[id], nil }, func(body *V) (*V, error) { return body, nil }, size, true, nil)
}
