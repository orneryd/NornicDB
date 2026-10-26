// Author: Codex implementation lane, 2026-10-27 (NOR-148).
package search

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// fulltextAcquisition preserves the issued reader's work contract. Materialized
// readers finish before semantic construction. A streaming reader instead lets
// its consumer terminate the actual acquisition, without building a corpus first.
type fulltextAcquisition struct {
	engine   storage.Engine
	snapshot storage.QueryReadSnapshot
	reader   storage.SnapshotReader
	nodes    []*storage.Node
	edges    []*storage.Edge
	loaded   bool
}

func openFulltextAcquisition(ctx context.Context, engine storage.Engine, scope FulltextDocumentScope) (fulltextAcquisition, func(), error) {
	reader := storage.SnapshotAllNodes
	if scope.Relationships {
		reader = storage.SnapshotAllEdges
		if scope.Ordered {
			reader = storage.SnapshotStreamEdges
		}
	}
	if scope.Ordered {
		// The original issued reader already owns its committed read/capture.
		// A traversal needs neither a retained-view proof nor an extra snapshot.
		if reader == storage.SnapshotStreamEdges {
			if _, native := engine.(storage.StreamingEngine); !native {
				reader = storage.SnapshotAllEdges
			}
		}
		return fulltextAcquisition{engine: engine, reader: reader}, func() {}, nil
	}
	captureCtx := storage.SnapshotStreamFallbackContext(storage.WithSnapshotReader(ctx, reader), engine)
	snapshot, err := storage.OpenReadSnapshot(captureCtx, engine)
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
	if a.loaded || a.reader == storage.SnapshotStreamEdges {
		return nil
	}
	// Visitors append to locals, never through the receiver. A visitor that
	// captured the receiver would make every acquisition, including a streaming
	// one, escape to the heap on each call.
	var err error
	if a.reader == storage.SnapshotAllNodes {
		if a.snapshot == nil {
			a.nodes, err = a.engine.AllNodes()
		} else {
			nodes := a.nodes
			err = a.snapshot.IterateVisibleNodes(ctx, func(node *storage.Node) bool {
				nodes = append(nodes, node)
				return true
			})
			a.nodes = nodes
		}
	} else {
		if a.snapshot == nil {
			a.edges, err = a.engine.AllEdges()
		} else {
			edges := a.edges
			err = a.snapshot.IterateAllEdges(ctx, func(edge *storage.Edge) error {
				edges = append(edges, edge)
				return nil
			})
			a.edges = edges
		}
	}
	if err != nil {
		return err
	}
	a.loaded = true
	return ctx.Err()
}

func (a *fulltextAcquisition) visitNodes(ctx context.Context, visit func(*storage.Node) error) error {
	if err := a.materialize(ctx); err != nil {
		return err
	}
	for _, node := range a.nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(node); err != nil {
			return fulltextVisitResult(ctx, err)
		}
	}
	return ctx.Err()
}

func (a *fulltextAcquisition) visitEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	if a.reader == storage.SnapshotStreamEdges {
		return fulltextVisitResult(ctx, storage.StreamEdgesWithFallback(ctx, a.engine, 1024, visit))
	}
	if err := a.materialize(ctx); err != nil {
		return err
	}
	for _, edge := range a.edges {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(edge); err != nil {
			return fulltextVisitResult(ctx, err)
		}
	}
	return ctx.Err()
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
	changes := make([]fulltextDocumentChange, 0, len(nodes)+len(edges))
	if v.relationships {
		for _, id := range edges {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			edge, err := engine.GetEdge(id)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			if err != nil {
				edge = nil
			}
			changes = append(changes, fulltextDocumentChange{id: string(id), edge: edge})
		}
	} else {
		for _, id := range nodes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			node, err := engine.GetNode(id)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			if err != nil {
				node = nil
			}
			changes = append(changes, fulltextDocumentChange{id: string(id), node: node})
		}
	}
	return changes, nil
}
