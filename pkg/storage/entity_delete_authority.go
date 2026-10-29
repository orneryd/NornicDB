package storage

import (
	"context"
	"errors"
)

// nativeDeletionFootprint reads stored-inclusive adjacency, including hidden
// and acknowledged entities. Preflight supplies identities only; mutation keeps
// its canonical final read after ownership and write-behind settlement.
func (b *BadgerEngine) nativeDeletionFootprint(ctx context.Context, id string, isEdge bool) (writtenEntities, error) {
	var edges []*Edge
	if isEdge {
		edge, err := b.getEdge(ctx, EdgeID(id), storedRead)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return writtenEntities{}, err
		}
		if edge != nil {
			edges = append(edges, edge)
		}
	} else {
		outgoing, err := b.outgoingEdges(ctx, NodeID(id), storedRead)
		if err != nil {
			return writtenEntities{}, err
		}
		incoming, err := b.incomingEdges(ctx, NodeID(id), storedRead)
		if err != nil {
			return writtenEntities{}, err
		}
		edges = append(outgoing, incoming...)
	}
	nodes := make([]NodeID, 0, len(edges)*2)
	identifiers := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		nodes = append(nodes, edge.StartNode, edge.EndNode)
		identifiers = append(identifiers, edge.ID)
	}
	return writtenEntities{nodes: nodes, edges: identifiers}, nil
}
func suppressionEndFootprints(ends map[string]struct{}) map[string]writtenEntities {
	footprints := make(map[string]writtenEntities)
	for key := range ends {
		id := key[1:]
		namespace := namespaceForNodeID(NodeID(id))
		if key[0] == manualSuppressionEdge {
			namespace = namespaceForEdgeID(EdgeID(id))
		}
		footprint := footprints[namespace]
		if key[0] == manualSuppressionEdge {
			footprint.edges = append(footprint.edges, EdgeID(id))
		} else {
			footprint.nodes = append(footprint.nodes, NodeID(id))
		}
		footprints[namespace] = footprint
	}

	return footprints
}

// requireEntityFootprint checks current topology against actual owned keys. It
// never acquires additional keys or changes graph/metadata state.
func (sm *SchemaManager) requireEntityFootprint(owner string, footprint writtenEntities) error {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	for _, id := range footprint.nodes {
		lock := sm.uniqueConstraintCommitLocks[uniqueConstraintLockKey{entityKind: 1, entityID: string(id)}]
		if lock == nil || lock.owner != owner || lock.holds == 0 {
			return ErrConflict
		}
	}
	for _, id := range footprint.edges {
		lock := sm.uniqueConstraintCommitLocks[uniqueConstraintLockKey{entityKind: 2, entityID: string(id)}]
		if lock == nil || lock.owner != owner || lock.holds == 0 {
			return ErrConflict
		}
	}
	return nil
}

func (b *BadgerEngine) validateNativeDeletionFootprint(write *entityWriteContext, id string, isEdge bool) error {
	footprint, err := b.nativeDeletionFootprint(write.ctx, id, isEdge)
	if err != nil {
		return err
	}
	ns := namespaceForNodeID(NodeID(id))
	if isEdge {
		ns = namespaceForEdgeID(EdgeID(id))
	}
	return b.GetSchemaForNamespace(ns).requireEntityFootprint(write.owner, footprint)
}
