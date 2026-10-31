// Author: Codex implementation lane, 2026-10-27 (NOR-148).
package search

import (
	"context"
	"errors"
	"iter"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// fulltextAcquisition preserves the issued reader's work contract. Materialized
// readers finish before semantic construction. A streaming reader instead lets
// its consumer terminate the actual acquisition, without building a corpus first.
type fulltextAcquisition struct {
	engine   storage.Engine
	snapshot storage.QueryReadSnapshot
	reader   storage.SnapshotReader
	// stream lets the consumer's stop reach the edge reader; the fallback of an
	// engine without native streaming belongs to storage.StreamEdgesWithFallback.
	stream bool
	nodes  []*storage.Node
	edges  []*storage.Edge
	loaded bool
}

// fulltextScopeReader names the materialized reader of the scope's entity kind.
func fulltextScopeReader(scope FulltextDocumentScope) storage.SnapshotReader {
	if scope.Relationships {
		return storage.SnapshotAllEdges
	}
	return storage.SnapshotAllNodes
}

// orderedFulltextAcquisition is a traversal. The original issued reader already
// owns its committed read/capture, so it needs neither a retained-view proof nor
// an extra snapshot.
func orderedFulltextAcquisition(engine storage.Engine, scope FulltextDocumentScope) fulltextAcquisition {
	return fulltextAcquisition{engine: engine, reader: fulltextScopeReader(scope), stream: scope.Relationships}
}

func openFulltextAcquisition(ctx context.Context, engine storage.Engine, scope FulltextDocumentScope) (fulltextAcquisition, func(), error) {
	reader := fulltextScopeReader(scope)
	snapshot, err := storage.OpenReadSnapshot(storage.WithSnapshotReader(ctx, reader), engine)
	if err != nil && !errors.Is(err, storage.ErrReadSnapshotUnsupported) {
		return fulltextAcquisition{}, nil, err
	}
	close := func() {}
	if snapshot != nil {
		close = snapshot.Close
	}
	querySnapshot, _ := snapshot.(storage.QueryReadSnapshot)
	if querySnapshot != nil {
		reader = querySnapshot.Reader()
	}
	return fulltextAcquisition{engine: engine, snapshot: querySnapshot, reader: reader}, close, nil
}

func (a *fulltextAcquisition) materialize(ctx context.Context) error {
	if a.loaded || a.stream {
		return nil
	}
	// Visitors append to locals, never through the receiver. A visitor that
	// captured the receiver would make every acquisition, including a streaming
	// one, escape to the heap on each call.
	var err error
	engine, snapshot := a.engine, a.snapshot
	if a.reader == storage.SnapshotAllNodes {
		a.nodes, err = materializeEntities(snapshot, engine.AllNodes, func(add func(*storage.Node)) error {
			return snapshot.IterateVisibleNodes(ctx, func(node *storage.Node) bool {
				add(node)
				return true
			})
		})
	} else {
		a.edges, err = materializeEntities(snapshot, engine.AllEdges, func(add func(*storage.Edge)) error {
			return snapshot.IterateAllEdges(ctx, func(edge *storage.Edge) error {
				add(edge)
				return nil
			})
		})
	}
	if err != nil {
		return err
	}
	a.loaded = true
	return ctx.Err()
}

// materializeEntities is the one choice of a materialized read: the captured
// snapshot's iterator when there is a snapshot, else the issued engine's reader.
func materializeEntities[T any](snapshot storage.QueryReadSnapshot, all func() ([]T, error), iterate func(add func(T)) error) ([]T, error) {
	if snapshot == nil {
		return all()
	}
	var items []T
	err := iterate(func(item T) { items = append(items, item) })
	return items, err
}

func (a *fulltextAcquisition) visitNodes(ctx context.Context, visit func(*storage.Node) error) error {
	err := a.materialize(ctx)
	return visitMaterialized(ctx, err, a.nodes, visit)
}

func (a *fulltextAcquisition) visitEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	if a.stream {
		return fulltextVisitResult(ctx, storage.StreamEdgesWithFallback(ctx, a.engine, 1024, visit))
	}
	err := a.materialize(ctx)
	return visitMaterialized(ctx, err, a.edges, visit)
}

// visitMaterialized is the one traversal of a materialized acquisition, for
// either entity kind: a failed load ends it with the load's error; otherwise
// items are visited in order, cancellation is checked before each visit, a
// visitor's stop or error is translated by fulltextVisitResult, and an
// exhausted traversal returns the context's outcome.
func visitMaterialized[T any](ctx context.Context, loadErr error, items []T, visit func(T) error) error {
	if loadErr != nil {
		return loadErr
	}
	return fulltextVisitResult(ctx, untilCancelled(ctx, slices.All(items), func(_ int, item T) error { return visit(item) }))
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
	if a.reader == storage.SnapshotAllNodes {
		return a.visitNodes(ctx, func(node *storage.Node) error { return visit(node, nil) })
	}
	return a.visitEdges(ctx, func(edge *storage.Edge) error { return visit(nil, edge) })
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
			func(engine storage.Engine, id storage.EdgeID) (*storage.Edge, error) { return engine.GetEdge(id) },
			func(id storage.EdgeID, edge *storage.Edge) fulltextDocumentChange {
				return fulltextDocumentChange{id: string(id), edge: edge}
			})
	}
	return acquireByID(ctx, engine, nodes,
		func(engine storage.Engine, id storage.NodeID) (*storage.Node, error) { return engine.GetNode(id) },
		func(id storage.NodeID, node *storage.Node) fulltextDocumentChange {
			return fulltextDocumentChange{id: string(id), node: node}
		})
}

// acquireByID is the one by-ID acquisition of changed entities, for either
// kind: ids are read in order, cancellation is checked before each read, an
// entity that no longer exists becomes a nil image (entityGone), and any other
// read error ends the acquisition without a partial change list. get and
// change are the kind's getter and change constructor.
func acquireByID[ID any, T any](ctx context.Context, engine storage.Engine, ids []ID, get func(storage.Engine, ID) (T, error), change func(ID, T) fulltextDocumentChange) ([]fulltextDocumentChange, error) {
	changes := make([]fulltextDocumentChange, 0, len(ids))
	err := untilCancelled(ctx, slices.All(ids), func(_ int, id ID) error {
		entity, err := get(engine, id)
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
