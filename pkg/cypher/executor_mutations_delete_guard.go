package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

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
func collectDeleteMutationTargets(store storage.Engine, matchResult *ExecuteResult) (nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) {
	seenNodes := make(map[string]struct{}, len(matchResult.Rows))
	seenEdges := make(map[string]struct{}, len(matchResult.Rows))
	nodeIDs = make([]storage.NodeID, 0, len(matchResult.Rows))
	edgeIDs = make([]storage.EdgeID, 0, len(matchResult.Rows))

	for _, row := range matchResult.Rows {
		for _, val := range row {
			collectDeleteTargetValue(store, val, seenNodes, seenEdges, &nodeIDs, &edgeIDs)
		}
	}

	return nodeIDs, edgeIDs
}

func collectDeleteTargetValue(store storage.Engine, value interface{}, seenNodes, seenEdges map[string]struct{}, nodeIDs *[]storage.NodeID, edgeIDs *[]storage.EdgeID) {
	target := classifyDeleteTargetValue(value)
	switch entity := value.(type) {
	case *storage.Node:
		if entity != nil {
			id, ok := entity.Identity().NodeLookupID(store)
			if !ok {
				return
			}
			target.nodeID = id
		}
	case *storage.Edge:
		if entity != nil {
			id, ok := entity.Identity().LookupID(store, "5")
			if !ok {
				return
			}
			target.edgeID = storage.EdgeID(id)
		}
	}
	switch target.kind {
	case deleteProjectionRelationship:
		key := string(target.edgeID)
		if _, seen := seenEdges[key]; !seen {
			seenEdges[key] = struct{}{}
			*edgeIDs = append(*edgeIDs, target.edgeID)
		}
		return
	case deleteProjectionNode:
		key := string(target.nodeID)
		if _, seen := seenNodes[key]; !seen {
			seenNodes[key] = struct{}{}
			*nodeIDs = append(*nodeIDs, target.nodeID)
		}
		return
	}

	switch typed := value.(type) {
	case PathResult:
		for _, node := range typed.Nodes {
			collectDeleteTargetValue(store, node, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
		for _, edge := range typed.Relationships {
			collectDeleteTargetValue(store, edge, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	case *PathResult:
		if typed != nil {
			collectDeleteTargetValue(store, *typed, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	case []interface{}:
		for _, item := range typed {
			collectDeleteTargetValue(store, item, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	case map[string]interface{}:
		if path, ok := typed["_pathResult"]; ok {
			collectDeleteTargetValue(store, path, seenNodes, seenEdges, nodeIDs, edgeIDs)
			return
		}
		for _, item := range typed {
			collectDeleteTargetValue(store, item, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	}
}

func isDeleteTargetValue(value interface{}) bool {
	// Marker keys in an ordinary Cypher map are user data, not authority to
	// delete an entity. Only the evaluator's actual PathResult carrier is a
	// map-shaped DELETE value; legacy projection classification stays private.
	if object, isMap := value.(map[string]interface{}); isMap {
		_, isPath := cypherPathResultOf(value, object, true)
		return isPath
	}
	if value == nil || classifyDeleteTargetValue(value).kind != deleteProjectionUnknown {
		return true
	}
	switch typed := value.(type) {
	case PathResult:
		return true
	case *PathResult:
		return typed != nil
	case []interface{}:
		for _, item := range typed {
			if !isDeleteTargetValue(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func hasTopLevelDeleteLabelQualifier(expression string) bool {
	expression = strings.TrimSpace(expression)
	depth := 0
	for index := 0; index < len(expression); index++ {
		if end, quoted := skipCypherLexicalToken(expression, index); quoted {
			index = end - 1
			continue
		}
		character := expression[index]
		switch character {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case ':':
			if depth == 0 {
				return true
			}
		}
	}
	return false
}

// deleteExpressionRootIdentifier returns the variable a DELETE target starts
// with (n in n, n.list[0], `n`), "" for a function call or a literal: null,
// true and false are never variables there (DELETE null deletes nothing, as
// in Neo4j, #907).
func deleteExpressionRootIdentifier(expression string) string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return ""
	}
	// The name is read by the one identifier rule (cyphertext.ScanIdentifier):
	// n$ and `a``b` are variables too.
	name, ok := cyphertext.ScanIdentifier(expression, 0)
	if !ok || (name.End < len(expression) && expression[name.End] == '(') {
		return ""
	}
	if !strings.HasPrefix(expression, "`") {
		switch lowerASCII(name.Value) {
		case "null", "true", "false":
			return ""
		}
	}
	return name.Value
}

// Author of retained pattern binding input: Codex R17 implementation agent.
func addPipelinePatternBindings(scope map[string]struct{}, clause pipelineClause) {
	addSyntaxPatternBindings(scope, clause.syntax)
}

func extractPathAssignmentVariable(pattern string) string {
	if equals := strings.Index(pattern, "="); equals > 0 {
		candidate := strings.TrimSpace(pattern[:equals])
		right := strings.TrimSpace(pattern[equals+1:])
		if isPatternVariableName(candidate) && (strings.HasPrefix(right, "(") ||
			matchFuncStartAndSuffix(right, "shortestpath") ||
			matchFuncStartAndSuffix(right, "allshortestpaths")) {
			return normalizeProjectionColumnName(candidate)
		}
	}
	return ""
}

func appendUniquePipelineBinding(bindings []string, binding string) []string {
	for _, existing := range bindings {
		if existing == binding {
			return bindings
		}
	}
	return append(bindings, binding)
}

func pipelineUnwindAlias(clause pipelineClause) string {
	if _, alias, ok := parsePipelineIteration(clause); ok {
		return normalizeProjectionColumnName(alias)
	}
	return ""
}

// validateNoResidualRelationships enforces openCypher/Neo4j DELETE
// semantics: a non-DETACH DELETE of a node that still has relationships is
// an error, not a silent cascade. NornicDB's storage layer
// (BadgerEngine.DeleteNode) cascades every adjacent edge unconditionally,
// so without this check a plain "DELETE n" on a connected node quietly
// deleted its edges too, violating Cypher's connected-node constraint.
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
	beingDeleted := deletingEdgeSet(edgeIDs)
	for _, nodeID := range nodeIDs {
		connected, err := nodeHasResidualRelationships(store, nodeID, beingDeleted)
		if err != nil {
			return err
		}
		if connected {
			return residualRelationshipDeleteError(nodeID)
		}
	}
	return nil
}

// connectedDeleteTargets returns the nodes of nodeIDs that relationships
// other than edgeIDs (this DELETE's own) still connect: a non-DETACH DELETE
// deletes them for now, and the transaction's commit fails unless a later
// clause deletes those relationships, as in Neo4j
// (storage.ConnectedNodeDeleter).
func connectedDeleteTargets(store storage.Engine, nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) (map[storage.NodeID]struct{}, error) {
	beingDeleted := deletingEdgeSet(edgeIDs)
	var connected map[storage.NodeID]struct{}
	for _, nodeID := range nodeIDs {
		residual, err := nodeHasResidualRelationships(store, nodeID, beingDeleted)
		if err != nil {
			return nil, err
		}
		if residual {
			if connected == nil {
				connected = make(map[storage.NodeID]struct{})
			}
			connected[nodeID] = struct{}{}
		}
	}
	return connected, nil
}

func deletingEdgeSet(edgeIDs []storage.EdgeID) map[storage.EdgeID]struct{} {
	beingDeleted := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	for _, id := range edgeIDs {
		beingDeleted[id] = struct{}{}
	}
	return beingDeleted
}

// nodeHasResidualRelationships reports whether a relationship other than
// those beingDeleted connects nodeID (connectingRelationships).
func nodeHasResidualRelationships(store storage.Engine, nodeID storage.NodeID, beingDeleted map[storage.EdgeID]struct{}) (bool, error) {
	connecting, err := connectingRelationships(store, nodeID)
	if err != nil {
		return false, err
	}
	for _, edge := range connecting {
		if _, deleting := beingDeleted[deleteEdgeLookupID(store, edge)]; !deleting {
			return true, nil
		}
	}
	return false, nil
}

// connectingRelationships returns the relationships that connect nodeID for
// a DELETE: those DETACH DELETE removes with the node, and those that keep a
// non-DETACH DELETE from removing it. A store that lists the relationships the
// visibility layer hides (storage.IncidentEdgeLister: a transaction) lists
// them too, since the node's delete removes them all the same; without them a
// non-DETACH DELETE removed a node whose only relationships were hidden, as
// in Neo4j it fails with DeleteConnectedNode for any relationship. Stores run
// without a transaction (composite and remote databases) list what their
// reads return.
func connectingRelationships(store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, error) {
	if lister, ok := store.(storage.IncidentEdgeLister); ok {
		return lister.IncidentEdges(nodeID)
	}
	return undirectedIncidentEdges(store, nodeID)
}

// residualRelationshipDeleteError builds the error returned when a
// non-DETACH DELETE targets a node that still has relationships. The
// wording mirrors Neo4j's own "still has relationships" DELETE error so
// clients that pattern-match on that message keep working against
// NornicDB.
func residualRelationshipDeleteError(nodeID storage.NodeID) error {
	message := localization.CypherTransactionsDeleteResidualRelationships(string(nodeID))
	return &classifiedCypherError{
		cause:  localizedError(message, nil),
		code:   "Neo.ClientError.Schema.ConstraintValidationFailed",
		detail: "DeleteConnectedNode",
	}
}

// staticDeleteTargetTypeError keeps known non-entity values as compile-time
// errors, while widened UNWIND map elements remain runtime checks. Strings
// and lists retain the existing NornicDB DELETE extensions.
func staticDeleteTargetTypeError(clause string, scope staticTypeScope) error {
	targets, _, _ := deleteClauseTargets(clause)
	// Undefined targets retain the existing DELETE scope error owner.
	scope.complete = false
	checker := staticOperatorChecker{scope: scope}
	for _, target := range targets {
		operand, err := checker.check(strings.TrimSpace(target))
		if err != nil {
			return err
		}
		// A string target is NornicDB's DELETE-by-id extension, and a list
		// deletes its elements: reject a target none of whose types is one.
		if operand.excludes(isDeleteTargetTypeName) {
			return typeNameMismatchError("Node, Path or Relationship", operand.typeName())
		}
	}
	return nil
}
