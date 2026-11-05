// Author: Codex implementation lane, 2026-10-27 (NOR-148).
package search

import (
	"context"
	"errors"
	"iter"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// fulltextAcquisition reads through the view's own release readers, on the
// statement's context. A consumer's stop reaches the reader; the fallback of an
// engine without native streaming belongs to storage.
type fulltextAcquisition struct {
	engine        storage.Engine
	relationships bool
}

func (a *fulltextAcquisition) visitNodes(ctx context.Context, visit func(*storage.Node) error) error {
	return fulltextVisitResult(ctx, storage.StreamNodesWithFallback(ctx, a.engine, storage.StreamChunkSize, visit))
}

func (a *fulltextAcquisition) visitEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return fulltextVisitResult(ctx, storage.StreamEdgesWithFallback(ctx, a.engine, storage.StreamChunkSize, visit))
}

// untilCancelled is the one cancellable traversal of acquired entities and
// documents: cancellation is checked before each item, and the context's or
// visit's error ends the traversal and is returned.
func untilCancelled[K any, V any](ctx context.Context, items iter.Seq2[K, V], visit func(K, V) error) error {
	for key, value := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(key, value); err != nil {
			return err
		}
	}
	return nil
}

func fulltextVisitResult(ctx context.Context, err error) error {
	// Preserve the release's exact sentinel policy; wrapped errors remain errors.
	if err != nil && err != storage.ErrIterationStopped {
		return err
	}
	return ctx.Err()
}

func (a *fulltextAcquisition) each(ctx context.Context, visit func(*storage.Node, *storage.Edge) error) error {
	if a.relationships {
		return a.visitEdges(ctx, func(edge *storage.Edge) error { return visit(nil, edge) })
	}
	return a.visitNodes(ctx, func(node *storage.Node) error { return visit(node, nil) })
}

type fulltextDocumentChange struct {
	id   string
	node *storage.Node
	edge *storage.Edge
}

// Read every changed value before constructing any replacement. The caller
// certifies this bounded read interval before running the query/construction gate.
func (v *fulltextDocumentView) acquireChanges(ctx context.Context, engine storage.Engine, nodes []storage.NodeID, edges []storage.EdgeID) ([]fulltextDocumentChange, error) {
	if v.relationships {
		return acquireByID(ctx, engine, edges,
			func(ctx context.Context, engine storage.Engine, id storage.EdgeID) (*storage.Edge, error) {
				return engine.GetEdge(ctx, id)
			},
			func(id storage.EdgeID, edge *storage.Edge) fulltextDocumentChange {
				return fulltextDocumentChange{id: string(id), edge: edge}
			})
	}
	return acquireByID(ctx, engine, nodes,
		func(ctx context.Context, engine storage.Engine, id storage.NodeID) (*storage.Node, error) {
			return engine.GetNode(ctx, id)
		},
		func(id storage.NodeID, node *storage.Node) fulltextDocumentChange {
			return fulltextDocumentChange{id: string(id), node: node}
		})
}

// acquireByID is the one by-ID acquisition of changed entities, for either
// kind: ids are read in order, cancellation is checked before each read, an
// entity that no longer exists becomes a nil image (entityGone), and any other
// read error ends the acquisition without a partial change list. get reads one
// entity on the statement's context; change constructs the change for it.
func acquireByID[ID any, T any](ctx context.Context, engine storage.Engine, ids []ID, get func(context.Context, storage.Engine, ID) (T, error), change func(ID, T) fulltextDocumentChange) ([]fulltextDocumentChange, error) {
	changes := make([]fulltextDocumentChange, 0, len(ids))
	err := untilCancelled(ctx, slices.All(ids), func(_ int, id ID) error {
		entity, err := get(ctx, engine, id)
		if entityGone(err) {
			var gone T
			entity = gone
		} else if err != nil {
			return err
		}
		changes = append(changes, change(id, entity))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return changes, nil
}

// entityGone is the one rule for a changed entity read by ID: an entity that
// no longer exists is a deletion (nil image). Any other read error is
// returned by the read site itself.
func entityGone(err error) bool { return errors.Is(err, storage.ErrNotFound) }
