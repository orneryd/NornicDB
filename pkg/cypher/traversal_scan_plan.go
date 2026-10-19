package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// traversalScanPlan owns the access path after the traversal's bound/seek
// operators have had the opportunity to seed it. Both materializing traversal
// and row production use this decision; row production cannot widen it.
type traversalScanPlan struct {
	access traversalScanAccess
}

type traversalScanAccess uint8

const (
	traversalNodeScan traversalScanAccess = iota
	traversalLabelScan
	traversalTypeScan
	traversalSeededAccess
)

func planTraversalScan(match *TraversalMatch) traversalScanPlan {
	// Node constraints belong to the initial-node candidate owner, including
	// any index it can select. A relationship cursor cannot replace that seed.
	if len(match.StartNode.labels) > 0 || len(match.StartNode.properties) > 0 {
		return traversalScanPlan{access: traversalLabelScan}
	}
	if !match.IsChained && !match.Relationship.VariableLength && match.Relationship.MinHops == 1 && match.Relationship.MaxHops == 1 && len(match.Relationship.Types) > 0 {
		return traversalScanPlan{access: traversalTypeScan}
	}
	return traversalScanPlan{access: traversalNodeScan}
}

// planPipelineTraversalScan preserves the earlier bound, predicate and seek
// owners. Only their unseeded continuation can select a scan plan. The plan's
// access path, rather than the emitter's pattern inspection, chooses a cursor.
func planPipelineTraversalScan(match *TraversalMatch, where string, row pipelineRow) traversalScanPlan {
	if where != "" || len(match.StartNode.properties) > 0 || len(match.EndNode.properties) > 0 || len(match.Relationship.Properties) > 0 {
		return traversalScanPlan{access: traversalSeededAccess}
	}
	for _, variable := range []string{match.StartNode.variable, match.EndNode.variable, match.Relationship.Variable, match.PathVariable} {
		if variable != "" {
			if _, bound := row[variable]; bound {
				return traversalScanPlan{access: traversalSeededAccess}
			}
		}
	}
	plan := planTraversalScan(match)
	// End-label seeds belong to the existing reversed/endpoint planner. Do
	// not substitute a whole-graph edge cursor for a constrained endpoint.
	if plan.access == traversalNodeScan && len(match.EndNode.labels) > 0 {
		return traversalScanPlan{access: traversalSeededAccess}
	}
	return plan
}

// visitEdges streams a whole graph only for a plan with no selective seed.
// Label plans expand adjacency; type alternatives each use their own index.
func (plan traversalScanPlan) visitEdges(ctx context.Context, e *StorageExecutor, match *TraversalMatch, visit func(*storage.Edge, storage.NodeID, storage.NodeID) error) error {
	store := e.getStorage(ctx)
	oriented := func(edge *storage.Edge) error {
		if match.Relationship.Direction == "incoming" {
			return visit(edge, edge.EndNode, edge.StartNode)
		}
		if err := visit(edge, edge.StartNode, edge.EndNode); err != nil {
			return err
		}
		if match.Relationship.Direction == "both" && edge.StartNode != edge.EndNode {
			return visit(edge, edge.EndNode, edge.StartNode)
		}
		return nil
	}
	switch plan.access {
	case traversalNodeScan:
		return storage.StreamEdgesByType(ctx, store, "", oriented)
	case traversalTypeScan:
		for index, kind := range match.Relationship.Types {
			if containsString(match.Relationship.Types[:index], kind) {
				continue
			}
			if err := storage.StreamEdgesByType(ctx, store, kind, oriented); err != nil {
				return err
			}
		}
		return nil
	case traversalLabelScan:
		return e.visitPipelineInitialNodeCandidates(ctx, match.StartNode, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1}, func(node *storage.Node) error {
			if node == nil || !e.pipelineNodeMatchesPattern(node, match.StartNode) {
				return nil
			}
			var edges []*storage.Edge
			var err error
			switch match.Relationship.Direction {
			case "incoming":
				edges, err = store.GetIncomingEdges(node.ID)
			case "both":
				edges, err = undirectedIncidentEdges(store, node.ID)
			default:
				edges, err = store.GetOutgoingEdges(node.ID)
			}
			if err != nil {
				return err
			}
			for _, edge := range edges {
				if err := ctx.Err(); err != nil {
					return err
				}
				start, end := edge.StartNode, edge.EndNode
				if match.Relationship.Direction == "incoming" || (match.Relationship.Direction == "both" && start != node.ID) {
					start, end = end, start
				}
				if err := visit(edge, start, end); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return nil
}
