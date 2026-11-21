package auth

import (
	"context"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (g *fineGrainedGraph) GetInDegree(ctx context.Context, nodeID storage.NodeID) (int, error) {
	return storage.CountAdjacentEdges(ctx, g, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: nodeID, Headers: true})
}

func (g *fineGrainedGraph) GetOutDegree(ctx context.Context, nodeID storage.NodeID) (int, error) {
	return storage.CountAdjacentEdges(ctx, g, storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: nodeID, Headers: true})
}

func (g *fineGrainedGraph) NodeCount(ctx context.Context) (int64, error) {
	var count int64
	err := g.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{ApplyDecayFilter: true}, func(*storage.Node) error {
		count++
		return nil
	})
	return count, err
}

func (g *fineGrainedGraph) EdgeCount(ctx context.Context) (int64, error) {
	edges, err := g.AllEdges(ctx)
	return int64(len(edges)), err
}

func (g *fineGrainedGraph) EdgeCountByType(ctx context.Context, relationshipType string) (int64, error) {
	edges, err := g.GetEdgesByType(ctx, relationshipType)
	return int64(len(edges)), err
}

func (g *fineGrainedGraph) EdgeCountByStartLabel(ctx context.Context, label, relationshipType string) (int64, error) {
	return g.countEdgesByEndpointLabel(ctx, label, relationshipType, (*storage.Edge).StartIdentity)
}

func (g *fineGrainedGraph) EdgeCountByEndLabel(ctx context.Context, label, relationshipType string) (int64, error) {
	return g.countEdgesByEndpointLabel(ctx, label, relationshipType, (*storage.Edge).EndIdentity)
}

func (g *fineGrainedGraph) countEdgesByEndpointLabel(ctx context.Context, label, relationshipType string, endpoint func(*storage.Edge) storage.EntityIdentity) (int64, error) {
	edges, err := g.GetEdgesByType(ctx, relationshipType)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		id, local := endpoint(edge).NodeLookupID(g.engine)
		if !local {
			continue
		}
		node, err := g.GetNode(ctx, id)
		if err != nil {
			return 0, err
		}
		if slices.Contains(node.Labels, label) {
			count++
		}
	}
	return count, nil
}

func (g *fineGrainedGraph) GetSchema() *storage.SchemaManager { return g.engine.GetSchema() }
func (g *fineGrainedGraph) Close() error                      { return ErrFineGrainedForbidden }

func (g *fineGrainedGraph) DeleteByPrefix(string) (int64, int64, error) {
	return 0, 0, ErrFineGrainedForbidden
}
