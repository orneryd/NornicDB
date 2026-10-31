package nornicdb

import "github.com/orneryd/nornicdb/pkg/storage"

// Preserve the publication origin through subscription, namespace/service
// resolution, debounce, retry and installation. This callback owns no policy.
func (db *DB) searchEntityCommitted(event storage.EntityCommitEvent) {
	op, origin := event.Operation, event.VectorWorkOrigin
	switch op.Type {
	case storage.OpCreateNode, storage.OpUpdateNode:
		db.indexNodeFromEvent(op.Node, origin)
	case storage.OpDeleteNode:
		db.removeNodeFromEvent(op.NodeID, origin)
		for _, id := range op.DeletedEdgeIDs {
			db.removeRelationshipFromEvent(id, origin)
		}
	case storage.OpCreateEdge, storage.OpUpdateEdge:
		db.indexRelationshipFromEvent(op.Edge, origin)
	case storage.OpDeleteEdge:
		db.removeRelationshipFromEvent(op.EdgeID, origin)
	}
}
