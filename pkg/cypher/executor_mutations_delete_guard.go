package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
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
func collectDeleteMutationTargets(ctx context.Context, matchResult *ExecuteResult) (nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID, err error) {
	seenNodes := make(map[string]struct{}, len(matchResult.Rows))
	seenEdges := make(map[string]struct{}, len(matchResult.Rows))
	nodeIDs = make([]storage.NodeID, 0, len(matchResult.Rows))
	edgeIDs = make([]storage.EdgeID, 0, len(matchResult.Rows))

	for batch, batchErr := range materializedBatches(ctx, len(matchResult.Rows)) {
		if batchErr != nil {
			return nil, nil, batchErr
		}
		for _, row := range matchResult.Rows[batch.start:batch.end] {
			pass := bulk.NewCheckpoint(ctx)
			for _, val := range row {
				if err := pass.Observe(); err != nil {
					return nil, nil, err
				}
				collectDeleteTargetValue(&pass, val, seenNodes, seenEdges, &nodeIDs, &edgeIDs)
			}
		}
	}

	return nodeIDs, edgeIDs, nil
}

func collectDeleteTargetValue(walk *materializedRowCheckpoint, value interface{}, seenNodes, seenEdges map[string]struct{}, nodeIDs *[]storage.NodeID, edgeIDs *[]storage.EdgeID) {
	var visit func(value interface{}) (descend, stop bool)
	visit = func(value interface{}) (bool, bool) {
		target := classifyDeleteTargetValue(value)
		switch target.kind {
		case deleteProjectionRelationship:
			key := string(target.edgeID)
			if _, seen := seenEdges[key]; !seen {
				seenEdges[key] = struct{}{}
				*edgeIDs = append(*edgeIDs, target.edgeID)
			}
			return false, false
		case deleteProjectionNode:
			key := string(target.nodeID)
			if _, seen := seenNodes[key]; !seen {
				seenNodes[key] = struct{}{}
				*nodeIDs = append(*nodeIDs, target.nodeID)
			}
			return false, false
		}
		switch typed := value.(type) {
		case PathResult:
			// A path's entities are typed slices: each is a target.
			for _, node := range observedItems(walk, typed.Nodes) {
				visit(node)
			}
			for _, relationship := range observedItems(walk, typed.Relationships) {
				visit(relationship)
			}
		case *PathResult:
			if typed != nil {
				return visit(*typed)
			}
		case []interface{}:
			return true, false
		case map[string]interface{}:
			if path, ok := typed["_pathResult"]; ok {
				cypherfn.VisitValue(walk, path, visit)
				return false, false
			}
			return true, false
		}
		return false, false
	}
	cypherfn.VisitValue(walk, value, visit)
}

func isDeleteTargetValue(walk *materializedRowCheckpoint, value interface{}) bool {
	return !cypherfn.VisitValue(walk, value, func(value interface{}) (descend, stop bool) {
		// Marker keys in an ordinary Cypher map are user data, not authority to
		// delete an entity. Only the evaluator's actual PathResult carrier is a
		// map-shaped DELETE value; legacy projection classification stays private.
		if object, isMap := value.(map[string]interface{}); isMap {
			_, isPath := cypherPathResultOf(value, object, true)
			return false, !isPath
		}
		if value == nil || classifyDeleteTargetValue(value).kind != deleteProjectionUnknown {
			return false, false
		}
		switch typed := value.(type) {
		case PathResult:
			return false, false
		case *PathResult:
			return false, typed == nil
		case []interface{}:
			return true, false
		}
		return false, true
	})
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
	// The name is read by the one identifier rule (scanIdentifierToken):
	// n$ and `a``b` are variables too.
	name, end, ok := scanIdentifierToken(expression, 0)
	if !ok || (end < len(expression) && expression[end] == '(') {
		return ""
	}
	if !strings.HasPrefix(expression, "`") {
		switch lowerASCII(name) {
		case "null", "true", "false":
			return ""
		}
	}
	return name
}

func addPipelinePatternBindings(executor *StorageExecutor, scope map[string]struct{}, clause, keyword string) {
	pattern := strings.TrimSpace(clause)
	if len(pattern) >= len(keyword) && strings.EqualFold(pattern[:len(keyword)], keyword) {
		pattern = strings.TrimSpace(pattern[len(keyword):])
	}
	if where := findKeywordIndexInContext(pattern, "WHERE"); where >= 0 {
		pattern = strings.TrimSpace(pattern[:where])
	}
	for _, variable := range executor.extractVariableNamesFromPattern(pattern) {
		if variable != "" {
			scope[variable] = struct{}{}
		}
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		if variable != "" {
			scope[variable] = struct{}{}
		}
	}
	if variable := extractPathAssignmentVariable(pattern); variable != "" {
		scope[variable] = struct{}{}
	}
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

func pipelineUnwindAlias(clause string) string {
	if _, alias, ok := parsePipelineIteration(clause); ok {
		return normalizeProjectionColumnName(alias)
	}
	if index := findKeywordIndexInContext(clause, "AS"); index >= 0 {
		return strings.TrimSpace(clause[index+len("AS"):])
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
func validateNoResidualRelationships(ctx context.Context, store storage.Engine, nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) error {
	beingDeleted := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	if err := addDeletingEdges(ctx, beingDeleted, edgeIDs); err != nil {
		return err
	}
	for batch, batchErr := range materializedBatches(ctx, len(nodeIDs)) {
		if batchErr != nil {
			return batchErr
		}
		for _, nodeID := range nodeIDs[batch.start:batch.end] {
			connected, err := nodeHasResidualRelationships(ctx, store, nodeID, beingDeleted)
			if err != nil {
				return err
			}
			if connected {
				return residualRelationshipDeleteError(nodeID)
			}
		}
	}
	return nil
}

// connectedDeleteTargets returns the nodes of nodeIDs that relationships
// other than edgeIDs (this DELETE's own) still connect: a non-DETACH DELETE
// deletes them for now, and the transaction's commit fails unless a later
// clause deletes those relationships, as in Neo4j
// (storage.ConnectedNodeDeleter).
func connectedDeleteTargets(ctx context.Context, store storage.Engine, nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) (map[storage.NodeID]struct{}, error) {
	beingDeleted := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	if err := addDeletingEdges(ctx, beingDeleted, edgeIDs); err != nil {
		return nil, err
	}
	var connected map[storage.NodeID]struct{}
	for batch, batchErr := range materializedBatches(ctx, len(nodeIDs)) {
		if batchErr != nil {
			return nil, batchErr
		}
		for _, nodeID := range nodeIDs[batch.start:batch.end] {
			residual, err := nodeHasResidualRelationships(ctx, store, nodeID, beingDeleted)
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
	}
	return connected, nil
}

// addDeletingEdges adds the relationships a DELETE removes to beingDeleted.
// The caller makes the set: it stays in the caller's frame.
func addDeletingEdges(ctx context.Context, beingDeleted map[storage.EdgeID]struct{}, edgeIDs []storage.EdgeID) error {
	ids := bulk.RowsCheckpoint(ctx, len(edgeIDs))
	for _, id := range edgeIDs {
		if err := ids.Observe(); err != nil {
			return err
		}
		beingDeleted[id] = struct{}{}
	}
	return nil
}

// nodeHasResidualRelationships reports whether a relationship other than
// those beingDeleted connects nodeID (connectingRelationships).
func nodeHasResidualRelationships(ctx context.Context, store storage.Engine, nodeID storage.NodeID, beingDeleted map[storage.EdgeID]struct{}) (bool, error) {
	connecting, err := connectingRelationships(store, nodeID)
	if err != nil {
		return false, err
	}
	for batch, batchErr := range materializedBatches(ctx, len(connecting)) {
		if batchErr != nil {
			return false, batchErr
		}
		for _, edge := range connecting[batch.start:batch.end] {
			if _, deleting := beingDeleted[edge.ID]; !deleting {
				return true, nil
			}
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
	return undirectedIncidentEdges(nil, store, nodeID)
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
