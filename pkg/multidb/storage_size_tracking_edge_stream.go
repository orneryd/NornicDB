package multidb

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// StreamEdgesByType preserves relationship-type streaming
// (storage.EdgeTypeStreamer) across this wrapper boundary. Embedding
// storage.Engine does not promote optional interfaces, so without this method
// constraint validation through a database engine would fall back to loading
// the type's edges of every database on the server via GetEdgesByType.
func (t *sizeTrackingEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*storage.Edge) error) error {
	return storage.StreamEdgesByType(ctx, t.Engine, edgeType, visit)
}

var _ storage.EdgeTypeStreamer = (*sizeTrackingEngine)(nil)
