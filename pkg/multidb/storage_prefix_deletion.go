package multidb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// DeleteByPrefixContext preserves namespace translation and cancellation.
func (s *sizeTrackingEngine) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	return storage.DeletePrefixContext(ctx, s.Engine, prefix)
}
