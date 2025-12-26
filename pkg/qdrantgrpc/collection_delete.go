package qdrantgrpc

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func deleteCollectionPoints(ctx context.Context, store storage.Engine, searchService *search.Service, collection string) error {
	if store == nil || collection == "" {
		return nil
	}

	nodes, err := store.GetNodesByLabel(collection)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil
		}
		return err
	}

	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		isQdrantPoint := false
		for _, label := range node.Labels {
			if label == QdrantPointLabel {
				isQdrantPoint = true
				break
			}
		}
		if !isQdrantPoint {
			continue
		}

		if searchService != nil {
			_ = searchService.RemoveNode(node.ID)
		}
		_ = store.DeleteNode(node.ID)
	}

	// Best-effort cleanup of any remaining keys (e.g. edges) for this collection.
	_, _, _ = store.DeleteByPrefix(fmt.Sprintf("qdrant:%s:", collection))
	return nil
}
