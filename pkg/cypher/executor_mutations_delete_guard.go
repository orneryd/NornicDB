package cypher

import (
	"context"
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
func collectDeleteMutationTargets(matchResult *ExecuteResult, probe *pipelineCancellation) (nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID) {
	seenNodes := make(map[string]struct{}, len(matchResult.Rows))
	seenEdges := make(map[string]struct{}, len(matchResult.Rows))
	nodeIDs = make([]storage.NodeID, 0, len(matchResult.Rows))
	edgeIDs = make([]storage.EdgeID, 0, len(matchResult.Rows))

	for _, row := range matchResult.Rows {
		for _, val := range row {
			collectDeleteTargetValue(val, seenNodes, seenEdges, &nodeIDs, &edgeIDs, probe)
		}
	}

	return nodeIDs, edgeIDs
}

func collectDeleteTargetValue(value interface{}, seenNodes, seenEdges map[string]struct{}, nodeIDs *[]storage.NodeID, edgeIDs *[]storage.EdgeID, probe *pipelineCancellation) {
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
			collectDeleteTargetValue(node, seenNodes, seenEdges, nodeIDs, edgeIDs, probe)
		}
		for _, edge := range typed.Relationships {
			collectDeleteTargetValue(edge, seenNodes, seenEdges, nodeIDs, edgeIDs, probe)
		}
	case *PathResult:
		if typed != nil {
			collectDeleteTargetValue(*typed, seenNodes, seenEdges, nodeIDs, edgeIDs, probe)
		}
	case []interface{}:
		for _, item := range typed {
			collectDeleteTargetValue(item, seenNodes, seenEdges, nodeIDs, edgeIDs, probe)
		}
	case map[string]interface{}:
		if path, ok := typed["_pathResult"]; ok {
			collectDeleteTargetValue(path, seenNodes, seenEdges, nodeIDs, edgeIDs, probe)
			return
		}
		for _, item := range typed {
			collectDeleteTargetValue(item, seenNodes, seenEdges, nodeIDs, edgeIDs, probe)
		}
	}
}

func isDeleteTargetValue(value interface{}, probe *pipelineCancellation) bool {
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
			if !isDeleteTargetValue(item, probe) {
				return false
			}
		}
		return true
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
	// The name is read by the one identifier rule (scanIdentifierToken):
	// n$ and `a``b` are variables too.
	name, end, ok := scanIdentifierToken(expression, 0)
	if !ok || (end < len(expression) && expression[end] == '(') {
		return ""
	}
	if expression[0] != '`' {
		switch lowerASCII(name) {
		case "null", "true", "false":
			return ""
		}
	}
	return name
}

// addPipelinePatternBindings adds the variables a pattern clause binds to
// scope (eachPipelinePatternBinding), from the statement's clause plan when
// there is one.
func addPipelinePatternBindings(ctx context.Context, executor *StorageExecutor, scope map[string]struct{}, clause, keyword string) {
	if plan := pipelineClausePlanFor(ctx, clause); plan != nil {
		for _, variable := range executor.plannedPatternBindings(plan, clause, keyword) {
			scope[variable] = struct{}{}
		}
		return
	}
	executor.eachPipelinePatternBinding(clause, keyword, func(variable string) {
		scope[variable] = struct{}{}
	})
}

// pipelinePatternBindingNames returns the variables a pattern clause binds
// (eachPipelinePatternBinding).
func (executor *StorageExecutor) pipelinePatternBindingNames(clause, keyword string) []string {
	var names []string
	executor.eachPipelinePatternBinding(clause, keyword, func(variable string) {
		names = append(names, variable)
	})
	return names
}

// eachPipelinePatternBinding visits the node, relationship and path
// variables of a pattern clause (its text after keyword, before WHERE).
func (executor *StorageExecutor) eachPipelinePatternBinding(clause, keyword string, visit func(string)) {
	pattern := strings.TrimSpace(clause)
	if len(pattern) >= len(keyword) && strings.EqualFold(pattern[:len(keyword)], keyword) {
		pattern = strings.TrimSpace(pattern[len(keyword):])
	}
	if where := findKeywordIndexInContext(pattern, "WHERE"); where >= 0 {
		pattern = strings.TrimSpace(pattern[:where])
	}
	for _, variable := range executor.extractVariableNamesFromPattern(pattern) {
		if variable != "" {
			visit(variable)
		}
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		if variable != "" {
			visit(variable)
		}
	}
	if variable := extractPathAssignmentVariable(pattern); variable != "" {
		visit(variable)
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

// pipelineProjectionScope returns the variables in scope after a WITH
// clause: its aliases (every previous variable for *) and the imports.
func pipelineProjectionScope(ctx context.Context, previous map[string]struct{}, clause string, imports pipelineRow) map[string]struct{} {
	next := make(map[string]struct{})
	add := func(alias string, star bool) {
		if !star {
			next[alias] = struct{}{}
			return
		}
		for name := range previous {
			next[name] = struct{}{}
		}
	}
	if plan := pipelineClausePlanFor(ctx, clause); plan != nil {
		names := plannedAnalysis(plan, clauseAnalysisProjectionNames, clause, parsePipelineProjectionNames)
		if names.star {
			add("", true)
		}
		for _, alias := range names.aliases {
			add(alias, false)
		}
	} else {
		eachPipelineProjectionName(clause, add)
	}
	for name := range imports {
		next[name] = struct{}{}
	}
	return next
}

// pipelineProjectionNames is what a WITH clause projects: its aliases, and
// whether it has a * item.
type pipelineProjectionNames struct {
	star    bool
	aliases []string
}

func parsePipelineProjectionNames(clause string) pipelineProjectionNames {
	var names pipelineProjectionNames
	eachPipelineProjectionName(clause, func(alias string, star bool) {
		if star {
			names.star = true
		} else {
			names.aliases = append(names.aliases, alias)
		}
	})
	return names
}

// eachPipelineProjectionName visits a WITH clause's aliases, and its *
// items (star).
func eachPipelineProjectionName(clause string, visit func(alias string, star bool)) {
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
	for _, item := range splitTopLevelComma(body) {
		if strings.TrimSpace(item) == "*" {
			visit("", true)
			continue
		}
		_, alias := parseProjectionExprAlias(strings.TrimSpace(item))
		if alias != "" {
			visit(alias, false)
		}
	}
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
func validateNoResidualRelationships(store storage.Engine, nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID, probe *pipelineCancellation) error {
	beingDeleted := deletingEdgeSet(edgeIDs, probe)
	for _, nodeID := range nodeIDs {
		connected, err := nodeHasResidualRelationships(store, nodeID, beingDeleted, probe)
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
func connectedDeleteTargets(store storage.Engine, nodeIDs []storage.NodeID, edgeIDs []storage.EdgeID, probe *pipelineCancellation) (map[storage.NodeID]struct{}, error) {
	beingDeleted := deletingEdgeSet(edgeIDs, probe)
	var connected map[storage.NodeID]struct{}
	for _, nodeID := range nodeIDs {
		residual, err := nodeHasResidualRelationships(store, nodeID, beingDeleted, probe)
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

func deletingEdgeSet(edgeIDs []storage.EdgeID, probe *pipelineCancellation) map[storage.EdgeID]struct{} {
	beingDeleted := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	for _, id := range edgeIDs {
		beingDeleted[id] = struct{}{}
	}
	return beingDeleted
}

// nodeHasResidualRelationships reports whether a relationship other than
// those beingDeleted connects nodeID.
func nodeHasResidualRelationships(store storage.Engine, nodeID storage.NodeID, beingDeleted map[storage.EdgeID]struct{}, probe *pipelineCancellation) (bool, error) {
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

// staticDeleteTargetTypeError keeps known non-entity values as compile-time
// errors, while widened UNWIND map elements remain runtime checks. Strings
// and lists retain the existing NornicDB DELETE extensions.
func staticDeleteTargetTypeError(clause string, scope staticTypeScope, probe *pipelineCancellation) error {
	targets, _, _ := deleteClauseTargets(clause)
	// Undefined targets retain the existing DELETE scope error owner.
	scope.complete = false
	checker := staticOperatorChecker{scope: scope}
	for _, target := range targets {
		operand, err := checker.check(strings.TrimSpace(target), probe)
		if err != nil {
			return err
		}
		if operand.known() && operand.bindingKind() == matchBindingValue && operand.kind != "String" && !operand.list() {
			return typeNameMismatchError("Node, Path or Relationship", operand.kind)
		}
	}
	return nil
}
