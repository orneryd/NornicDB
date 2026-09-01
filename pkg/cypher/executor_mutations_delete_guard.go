package cypher

import (
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// collectDeleteMutationTargets walks matchResult.Rows and classifies every
// projected value into either a node or relationship delete target,
// deduplicating repeats across rows. It performs no storage mutation --
// this is the "collect" phase of DELETE's collect -> validate -> apply
// pipeline (see validateNoResidualRelationships), which must complete
// before any validation or mutation so a multi-row, multi-variable DELETE
// (e.g. "DELETE a, r") can be judged as a whole statement.
func collectDeleteMutationTargets(matchResult *ExecuteResult) (nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) {
	seenNodes := make(map[string]struct{}, len(matchResult.Rows))
	seenEdges := make(map[string]struct{}, len(matchResult.Rows))
	nodeIDs = make([]storage.NodeID, 0, len(matchResult.Rows))
	edgeIDs = make([]storage.EdgeID, 0, len(matchResult.Rows))

	for _, row := range matchResult.Rows {
		for _, val := range row {
			target := classifyDeleteTargetValue(val)
			switch target.kind {
			case deleteProjectionRelationship:
				key := string(target.edgeID)
				if _, seen := seenEdges[key]; seen {
					continue
				}
				seenEdges[key] = struct{}{}
				edgeIDs = append(edgeIDs, target.edgeID)
			case deleteProjectionNode:
				key := string(target.nodeID)
				if _, seen := seenNodes[key]; seen {
					continue
				}
				seenNodes[key] = struct{}{}
				nodeIDs = append(nodeIDs, target.nodeID)
			}
		}
	}

	return nodeIDs, edgeIDs
}

// validateNoResidualRelationships enforces openCypher/Neo4j DELETE
// semantics: a non-DETACH DELETE of a node that still has relationships is
// an error, not a silent cascade. NornicDB's storage layer
// (BadgerEngine.DeleteNode) cascades every adjacent edge unconditionally,
// so without this check a plain "DELETE n" on a connected node quietly
// deleted its edges too -- the third proven bug behind eshu #5147.
//
// This must run BEFORE any node or edge in the plan is mutated: validating
// mid-apply would let a multi-row DELETE partially commit before a later
// row is found to violate the constraint, breaking statement atomicity.
//
// An edge attached to a candidate node does not count as residual if the
// same statement is also deleting that edge (e.g.
// "MATCH (a)-[r]->(b) DELETE a, r"); edgeIDs holds every edge this
// statement is about to remove, so those are subtracted before the check.
func validateNoResidualRelationships(store storage.Engine, nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) error {
	if len(nodeIDs) == 0 {
		return nil
	}

	beingDeleted := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	for _, id := range edgeIDs {
		beingDeleted[id] = struct{}{}
	}

	for _, nodeID := range nodeIDs {
		outgoing, err := store.GetOutgoingEdges(nodeID)
		if err != nil {
			return err
		}
		for _, edge := range outgoing {
			if _, deleting := beingDeleted[edge.ID]; !deleting {
				return residualRelationshipDeleteError(nodeID)
			}
		}

		incoming, err := store.GetIncomingEdges(nodeID)
		if err != nil {
			return err
		}
		for _, edge := range incoming {
			if _, deleting := beingDeleted[edge.ID]; !deleting {
				return residualRelationshipDeleteError(nodeID)
			}
		}
	}

	return nil
}

// residualRelationshipDeleteError builds the error returned when a
// non-DETACH DELETE targets a node that still has relationships. The
// wording mirrors Neo4j's own "still has relationships" DELETE error so
// clients that pattern-match on that message keep working against
// NornicDB.
func residualRelationshipDeleteError(nodeID storage.NodeID) error {
	return localizedError(localization.CypherTransactionsDeleteResidualRelationships(string(nodeID)), nil)
}
