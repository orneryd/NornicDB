package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) storageFast() (engine storage.Engine, idPrefix string) {
	if ns, ok := e.storage.(*storage.NamespacedEngine); ok {
		// NamespacedEngine uses ":" as the separator today.
		return ns.GetInnerEngine(), ns.Namespace() + ":"
	}
	return e.storage, ""
}

func (e *StorageExecutor) prefixNodeIDIfNeeded(id storage.NodeID, idPrefix string) storage.NodeID {
	if idPrefix == "" {
		return id
	}
	if strings.HasPrefix(string(id), idPrefix) {
		return id
	}
	return storage.NodeID(idPrefix + string(id))
}

func (e *StorageExecutor) getEdgesByTypeFast(ctx context.Context, edgeType string) ([]*storage.Edge, string, error) {
	engine, idPrefix := e.storageFast()
	edges, err := engine.GetEdgesByType(edgeType)
	if err != nil {
		return nil, "", err
	}
	if idPrefix == "" {
		return edges, "", nil
	}

	filtered := make([]*storage.Edge, 0, len(edges))
	for batch, batchErr := range materializedBatches(ctx, len(edges)) {
		if batchErr != nil {
			return nil, "", batchErr
		}
		for _, edge := range edges[batch.start:batch.end] {
			if edge == nil {
				continue
			}
			if strings.HasPrefix(string(edge.ID), idPrefix) {
				filtered = append(filtered, edge)
			}
		}
	}
	return filtered, idPrefix, nil
}

func (e *StorageExecutor) batchGetNodesFast(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, string, error) {
	engine, idPrefix := e.storageFast()
	nodes, err := engine.BatchGetNodes(ids)
	if err != nil {
		return nil, "", err
	}
	return nodes, idPrefix, nil
}
