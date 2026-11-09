package storage

import (
	"context"
	"strings"
)

var (
	_ NodeWithoutEmbeddingsReader          = (*WALEngine)(nil)
	_ BatchNodeWithoutEmbeddingsReader     = (*WALEngine)(nil)
	_ BatchNodeWithoutEmbeddingsCapability = (*WALEngine)(nil)
	_ NodeWithoutEmbeddingsReader          = (*NamespacedEngine)(nil)
	_ BatchNodeWithoutEmbeddingsReader     = (*NamespacedEngine)(nil)
	_ BatchNodeWithoutEmbeddingsCapability = (*NamespacedEngine)(nil)
	_ QueryNodeReader                      = (*BadgerEngine)(nil)
	_ QueryNodeReader                      = (*BadgerTransaction)(nil)
	_ QueryNodeReader                      = (*WALEngine)(nil)
	_ QueryNodeReader                      = (*NamespacedEngine)(nil)
)

// batchNodeWithoutEmbeddingsSupported reports whether engine can perform a
// real batched embedding-free read. Wrappers use this shared check so merely
// implementing the forwarding method does not advertise a broken capability.
func batchNodeWithoutEmbeddingsSupported(engine Engine) bool {
	if _, ok := engine.(BatchNodeWithoutEmbeddingsReader); !ok {
		return false
	}
	if capability, ok := engine.(BatchNodeWithoutEmbeddingsCapability); ok {
		return capability.BatchGetNodesWithoutEmbeddingsSupported()
	}
	return true
}

// namespaceLabelCount is the shared namespace-aware capability dispatch.
// Wrappers preserve their public method while sharing the provider and scan owner.
func namespaceLabelCount(ctx context.Context, engine Engine, namespace, label string) (int64, error) {
	if provider, ok := engine.(NamespaceLabelStatsProvider); ok {
		return provider.NodeCountByLabelInNamespace(ctx, namespace, label)
	}
	nodes, err := engine.GetNodesByLabel(ctx, label)
	if err != nil {
		return 0, err
	}
	prefix := namespace + ":"
	var count int64
	var cancel bulkReadCancellation
	for _, node := range nodes {
		if err := cancel.check(ctx); err != nil {
			return 0, err
		}
		if node != nil && strings.HasPrefix(string(node.ID), prefix) {
			count++
		}
	}
	return count, nil
}
