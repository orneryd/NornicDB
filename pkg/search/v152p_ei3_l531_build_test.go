package search

// Batch-3 probe L531 adapters (context-taking storage and refresh API).

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func ei3ROEngine(t *testing.T) storage.Engine {
	e := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func ei3ROCreate(ctx context.Context, e storage.Engine, n *storage.Node) error {
	_, err := e.CreateNode(ctx, n)
	return err
}
func ei3ROUpdate(ctx context.Context, e storage.Engine, n *storage.Node) error {
	return e.UpdateNode(ctx, storage.NodeUpdateRequest{After: n})
}
func ei3ROGet(ctx context.Context, e storage.Engine, id storage.NodeID) (*storage.Node, error) {
	return e.GetNode(ctx, id)
}
func ei3RORefresh(ctx context.Context, svc *Service, id storage.NodeID) error {
	return svc.RefreshNode(ctx, id)
}
func ei3ROApply(ctx context.Context, svc *Service, m NodeMutation) error {
	return svc.RefreshNodeWithMutation(ctx, m)
}
func ei3ROLazyWarm(ctx context.Context, svc *Service) error {
	svc.SetLazyWarming(true, func() { _ = svc.BuildIndexes(svc.LifetimeContext()) })
	return svc.EnsureWarm(ctx)
}
