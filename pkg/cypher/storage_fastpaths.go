package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type namespacedStorageEngine interface {
	Namespace() string
	GetInnerEngine() storage.Engine
}

func (e *StorageExecutor) storageFast() (engine storage.Engine, idPrefix string) {
	if ns, ok := e.storage.(namespacedStorageEngine); ok {
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

func (e *StorageExecutor) getEdgesByTypeFast(edgeType string) ([]*storage.Edge, string, error) {
	engine, idPrefix := e.storageFast()
	edges, err := engine.GetEdgesByType(edgeType)
	if err != nil {
		return nil, "", err
	}
	if idPrefix == "" {
		return edges, "", nil
	}

	filtered := make([]*storage.Edge, 0, len(edges))
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if strings.HasPrefix(string(edge.ID), idPrefix) {
			filtered = append(filtered, edge)
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

