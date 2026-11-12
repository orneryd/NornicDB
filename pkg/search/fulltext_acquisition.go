// Author: Codex implementation lane, 2026-10-27 (NOR-148).
package search

import (
	"context"
	"errors"
	"iter"
	"slices"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// fulltextAcquisition reads through the view's own release readers, on the
// statement's context. A consumer's stop reaches the reader; the fallback of an
// engine without native streaming belongs to storage.
type fulltextAcquisition struct {
	engine        storage.Engine
	relationships bool
	queryNodes    bool
	nodeLabels    []string
}

func (a *fulltextAcquisition) visitNodes(ctx context.Context, visit func(*storage.Node) error) error {
	if a.queryNodes || len(a.nodeLabels) > 0 {
		options := storage.StreamNodesOptions{QueryNodes: a.queryNodes, WithEmbeddings: !a.queryNodes, ApplyDecayFilter: true}
		if len(a.nodeLabels) == 0 {
			return fulltextVisitResult(ctx, a.engine.StreamNodesWithOptions(ctx, options, queryprofile.EntityVisitor(ctx, queryprofile.KindAllNodesScan, "", visit)))
		}
		// Label membership is a union. Remember IDs only when two label
		// streams may overlap; a one-label population needs no extra map.
		var seen map[storage.NodeID]struct{}
		if len(a.nodeLabels) > 1 {
			seen = make(map[storage.NodeID]struct{})
		}
		for _, label := range a.nodeLabels {
			options.Label = label
			err := a.engine.StreamNodesWithOptions(ctx, options, queryprofile.EntityVisitor(ctx, queryprofile.KindNodeByLabelScan, label, func(node *storage.Node) error {
				// Label is a storage hint; the acquisition still owns membership.
				if !slices.Contains(node.Labels, label) {
					return nil
				}
				if seen != nil {
					if _, exists := seen[node.ID]; exists {
						return nil
					}
					seen[node.ID] = struct{}{}
				}
				return visit(node)
			}))
			if err != nil {
				return fulltextVisitResult(ctx, err)
			}
		}
		return ctx.Err()
	}
	return fulltextVisitResult(ctx, storage.StreamNodesWithFallback(ctx, a.engine, storage.StreamChunkSize, queryprofile.EntityVisitor(ctx, queryprofile.KindAllNodesScan, "", visit)))
}

func (a *fulltextAcquisition) visitEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return fulltextVisitResult(ctx, storage.StreamEdgesWithFallback(ctx, a.engine, storage.StreamChunkSize, queryprofile.EntityVisitor(ctx, queryprofile.KindAllRelationshipsScan, "", visit)))
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
		return acquireByID(ctx, engine, edges, queryprofile.KindRelationshipByIDSeek,
			func(ctx context.Context, engine storage.Engine, id storage.EdgeID) (*storage.Edge, error) {
				return engine.GetEdge(ctx, id)
			},
			func(id storage.EdgeID, edge *storage.Edge) fulltextDocumentChange {
				return fulltextDocumentChange{id: string(id), edge: edge}
			})
	}
	return acquireByID(ctx, engine, nodes, queryprofile.KindNodeByIDSeek,
		func(ctx context.Context, engine storage.Engine, id storage.NodeID) (*storage.Node, error) {
			if v.queryNodes {
				if reader, ok := engine.(storage.QueryNodeReader); ok {
					return reader.GetQueryNode(ctx, id)
				}
			}
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
func acquireByID[ID any, T any](ctx context.Context, engine storage.Engine, ids []ID, kind string, get func(context.Context, storage.Engine, ID) (*T, error), change func(ID, *T) fulltextDocumentChange) ([]fulltextDocumentChange, error) {
	changes := make([]fulltextDocumentChange, 0, len(ids))
	var counter *queryprofile.ReadCounter
	if len(ids) > 0 {
		counter = queryprofile.Access(ctx, kind, "")
	}
	err := untilCancelled(ctx, slices.All(ids), func(_ int, id ID) error {
		entity, err := get(ctx, engine, id)
		queryprofile.EntityReadResult(counter, entity, err)
		if entityGone(err) {
			var gone *T
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
