package cypher

import (
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
func collectDeleteMutationTargets(matchResult *ExecuteResult) (nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) {
	seenNodes := make(map[string]struct{}, len(matchResult.Rows))
	seenEdges := make(map[string]struct{}, len(matchResult.Rows))
	nodeIDs = make([]storage.NodeID, 0, len(matchResult.Rows))
	edgeIDs = make([]storage.EdgeID, 0, len(matchResult.Rows))

	for _, row := range matchResult.Rows {
		for _, val := range row {
			collectDeleteTargetValue(val, seenNodes, seenEdges, &nodeIDs, &edgeIDs)
		}
	}

	return nodeIDs, edgeIDs
}

func collectDeleteTargetValue(value interface{}, seenNodes, seenEdges map[string]struct{}, nodeIDs *[]storage.NodeID, edgeIDs *[]storage.EdgeID) {
	target := classifyDeleteTargetValue(value)
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
			collectDeleteTargetValue(node, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
		for _, edge := range typed.Relationships {
			collectDeleteTargetValue(edge, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	case *PathResult:
		if typed != nil {
			collectDeleteTargetValue(*typed, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	case []interface{}:
		for _, item := range typed {
			collectDeleteTargetValue(item, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	case map[string]interface{}:
		if path, ok := typed["_pathResult"]; ok {
			collectDeleteTargetValue(path, seenNodes, seenEdges, nodeIDs, edgeIDs)
			return
		}
		for _, item := range typed {
			collectDeleteTargetValue(item, seenNodes, seenEdges, nodeIDs, edgeIDs)
		}
	}
}

func isDeleteTargetValue(value interface{}) bool {
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
	case map[string]interface{}:
		if path, ok := typed["_pathResult"]; ok {
			return isDeleteTargetValue(path)
		}
		for _, item := range typed {
			if !isDeleteTargetValue(item) {
				return false
			}
		}
		return len(typed) > 0
	default:
		return false
	}
}

func hasTopLevelDeleteLabelQualifier(expression string) bool {
	depth := 0
	quote := rune(0)
	escaped := false
	for _, character := range strings.TrimSpace(expression) {
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' && quote != '`' {
				escaped = true
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		switch character {
		case '\'', '"', '`':
			quote = character
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
	if expression[0] == '`' {
		if end := strings.Index(expression[1:], "`"); end >= 0 {
			return expression[1 : end+1]
		}
		return ""
	}
	end := 0
	for end < len(expression) {
		character := expression[end]
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || character == '_' || (end > 0 && character >= '0' && character <= '9')) {
			break
		}
		end++
	}
	if end == 0 || (end < len(expression) && expression[end] == '(') {
		return ""
	}
	switch lowerASCII(expression[:end]) {
	case "null", "true", "false":
		return ""
	}
	return expression[:end]
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
			return candidate
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

func pipelineProjectionScope(previous map[string]struct{}, clause string) map[string]struct{} {
	body := strings.TrimSpace(clause)
	if len(body) >= len("WITH") && strings.EqualFold(body[:len("WITH")], "WITH") {
		body = strings.TrimSpace(body[len("WITH"):])
	}
	if where := findKeywordIndexInContext(body, "WHERE"); where >= 0 {
		body = strings.TrimSpace(body[:where])
	}
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := findKeywordIndexInContext(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	body, _ = cutDistinct(body)
	next := make(map[string]struct{})
	for _, item := range splitTopLevelComma(body) {
		if strings.TrimSpace(item) == "*" {
			for name := range previous {
				next[name] = struct{}{}
			}
			continue
		}
		_, alias := parseProjectionExprAlias(strings.TrimSpace(item))
		if alias != "" {
			next[alias] = struct{}{}
		}
	}
	return next
}

func pipelineUnwindAlias(clause string) string {
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
// those beingDeleted connects nodeID.
func nodeHasResidualRelationships(store storage.Engine, nodeID storage.NodeID, beingDeleted map[storage.EdgeID]struct{}) (bool, error) {
	outgoing, err := store.GetOutgoingEdges(nodeID)
	if err != nil {
		return false, err
	}
	for _, edge := range outgoing {
		if _, deleting := beingDeleted[edge.ID]; !deleting {
			return true, nil
		}
	}
	incoming, err := store.GetIncomingEdges(nodeID)
	if err != nil {
		return false, err
	}
	for _, edge := range incoming {
		if _, deleting := beingDeleted[edge.ID]; !deleting {
			return true, nil
		}
	}
	return false, nil
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
