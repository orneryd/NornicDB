package multidb

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// storageChangesUsage folds the same immutable stored images for admission
// and publication. Stored priors are never re-imaged as staged writer input.
func storageChangesUsage(namespace string, changes []storage.StoredEntityChange, trackBytes bool) (storageUsage, error) {
	return storageChangesUsageWithContext(context.Background(), namespace, changes, trackBytes)
}

// storageChangesUsageWithContext shares the accounting formula during admission.
func storageChangesUsageWithContext(ctx context.Context, namespace string, changes []storage.StoredEntityChange, trackBytes bool) (storageUsage, error) {
	var usage storageUsage
	var err error
	usage.nodes, usage.edges, err = storage.StoredEntityCountDeltaWithContext(ctx, namespace, changes)
	if err != nil {
		return storageUsage{}, err
	}
	if !trackBytes {
		return usage, nil
	}
	prefix := namespace + ":"
	nodeSize := func(node *storage.Node) (int64, error) {
		image := *node
		image.ID = storage.NodeID(strings.TrimPrefix(string(image.ID), prefix))
		return calculateNodeSize(&image)
	}
	edgeSize := func(edge *storage.Edge) (int64, error) {
		image := *edge
		image.ID = storage.EdgeID(strings.TrimPrefix(string(image.ID), prefix))
		image.StartNode = storage.NodeID(strings.TrimPrefix(string(image.StartNode), prefix))
		image.EndNode = storage.NodeID(strings.TrimPrefix(string(image.EndNode), prefix))
		return calculateEdgeSize(&image)
	}
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return storageUsage{}, err
		}
		if change.BeforeNode != nil || change.AfterNode != nil {
			bytes, err := storedPairBytes(change.BeforeNode, change.AfterNode, nodeSize)
			if err != nil {
				return storageUsage{}, err
			}
			usage.nodeBytes += bytes
		} else {
			bytes, err := storedPairBytes(change.BeforeEdge, change.AfterEdge, edgeSize)
			if err != nil {
				return storageUsage{}, err
			}
			usage.edgeBytes += bytes
		}
	}
	return usage, nil
}

// storedPairBytes applies the signed byte sizes of the same immutable pair.
func storedPairBytes[T any](before, after *T, size func(*T) (int64, error)) (bytes int64, err error) {
	for _, image := range [2]struct {
		value *T
		sign  int64
	}{{before, -1}, {after, 1}} {
		if image.value == nil {
			continue
		}
		n, err := size(image.value)
		if err != nil {
			return 0, err
		}
		bytes += image.sign * n
	}
	return bytes, nil
}
