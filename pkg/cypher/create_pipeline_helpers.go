package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (plan *createPlan) bindings() (map[string]*storage.Node, map[string]*storage.Edge) {
	if plan.nodeBindings == nil {
		plan.nodeBindings = make(map[string]*storage.Node)
	}
	if plan.edgeBindings == nil {
		plan.edgeBindings = make(map[string]*storage.Edge)
	}
	return plan.nodeBindings, plan.edgeBindings
}

// containsString checks if a slice contains a string.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// validateSetAssignments pre-validates SET clause assignments before executing CREATE
// This ensures we fail fast on invalid function calls, preventing orphaned nodes
func (e *StorageExecutor) validateSetAssignments(assignments []string) error {
	// Known Cypher functions
	knownFunctions := map[string]bool{
		"COALESCE": true, "TOSTRING": true, "TOINT": true, "TOFLOAT": true,
		"TOBOOLEAN": true, "TOLOWER": true, "TOUPPER": true, "TRIM": true,
		"SIZE": true, "LENGTH": true, "ABS": true, "CEIL": true, "FLOOR": true,
		"ROUND": true, "RAND": true, "SQRT": true, "SIGN": true, "LOG": true,
		"LOG10": true, "EXP": true, "SIN": true, "COS": true, "TAN": true,
		"DATE": true, "DATETIME": true, "TIME": true, "TIMESTAMP": true,
		"DURATION": true, "LOCALDATETIME": true, "LOCALTIME": true,
		"HEAD": true, "LAST": true, "TAIL": true, "KEYS": true, "LABELS": true,
		"TYPE": true, "ID": true, "ELEMENTID": true, "PROPERTIES": true,
		"POINT": true, "DISTANCE": true, "REPLACE": true, "SUBSTRING": true,
		"LEFT": true, "RIGHT": true, "SPLIT": true, "REVERSE": true,
		"LTRIM": true, "RTRIM": true, "COLLECT": true, "RANGE": true,
	}

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Parse assignment: var.property = value or var:Label
		eqIdx := strings.Index(assignment, "=")
		if eqIdx == -1 {
			// Could be a label assignment like "n:Label" - these are valid
			continue
		}

		rightSide := strings.TrimSpace(assignment[eqIdx+1:])

		// Check if right side looks like a function call
		if strings.Contains(rightSide, "(") && strings.HasSuffix(strings.TrimSpace(rightSide), ")") {
			// Extract function name (before first parenthesis)
			parenIdx := strings.Index(rightSide, "(")
			funcName := upperASCII(strings.TrimSpace(rightSide[:parenIdx]))
			if !knownFunctions[funcName] {
				return localizedError(localization.CypherMergeUnknownFunction(funcName), nil)
			}
		}
	}
	return nil
}

// extractCreateVariableRefs returns variable names used as simple refs in CREATE relationship patterns
// (e.g. (o)-[:R]->(pharmacy) yields ["o", "pharmacy"]). Used to know which bindings the pipeline must return.
func extractCreateVariableRefs(createPart braceView) []string {
	seen := make(map[string]bool)
	exec := &StorageExecutor{}
	createClauses := createPart.keywordParts("CREATE", keywordSplitCreate)
	for _, clause := range createClauses {
		clause = clause.trim()
		if clause.empty() {
			continue
		}
		for _, pattern := range exec.splitCreatePatterns(clause) {
			pattern = pattern.trim()
			if pattern.empty() {
				continue
			}
			_, pattern = parseCreatePathAssignment(pattern)
			src, _, tgt, _, remainder, err := exec.parseCreateRelPatternWithVars(pattern)
			if err != nil || !remainder.trim().empty() {
				continue
			}
			if isSimpleVariable(src.text()) {
				seen[src.text()] = true
			}
			if isSimpleVariable(tgt.text()) {
				seen[tgt.text()] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

func sortNodesByProperty(nodes []*storage.Node, prop string) {
	// Simple sort by property value (string comparison)
	// OrderStatus by orderId, Pharmacy by id
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			vi := fmt.Sprint(getNodeProp(nodes[i], prop))
			vj := fmt.Sprint(getNodeProp(nodes[j], prop))
			if vi > vj {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}
}

type createPatternSplit struct {
	view          braceView
	all           []braceView
	nodes         []braceView
	relationships []braceView
}

var createPatternSplits = newBoundedCache[string, createPatternSplit](4096, boundedCacheStringShardIndex)

func (e *StorageExecutor) createPatternSplitFor(pattern braceView) createPatternSplit {
	if split, cached := createPatternSplits.get(pattern.text()); cached {
		rebase := func(views []braceView) []braceView {
			result := make([]braceView, len(views))
			for i, part := range views {
				result[i] = braceView{syntax: pattern.syntax, source: part.source.Rebase(split.view.source, pattern.source)}
			}
			return result
		}
		split.all, split.nodes, split.relationships = rebase(split.all), rebase(split.nodes), rebase(split.relationships)
		split.view = pattern
		return split
	}
	split := createPatternSplit{view: pattern, all: e.scanCreatePatterns(pattern)}
	for _, fragment := range split.all {
		fragment = fragment.trim()
		if fragment.empty() {
			continue
		}
		if patternHasRelationship(fragment) {
			split.relationships = append(split.relationships, fragment)
		} else {
			split.nodes = append(split.nodes, fragment)
		}
	}
	createPatternSplits.put(pattern.text(), split)
	return split
}

type createRelationshipSyntax struct {
	source, relationship, target, remainder cyphertext.Span
	reverse                                 bool
	err                                     error
}

var createRelationshipSyntaxPlans = newBoundedCache[string, createRelationshipSyntax](4096, boundedCacheStringShardIndex)

func getNodeProp(n *storage.Node, prop string) interface{} {
	if n == nil || n.Properties == nil {
		return nil
	}
	return n.Properties[prop]
}

// parseCreateRelPatternWithVars parses patterns like (varA)-[r:TYPE {props}]->(varB)
// where varA and varB are variable references (not full node definitions)
// Returns: sourceVar, relContent, targetVar, isReverse, remainder, error
// remainder is any content after the target node (for chained patterns)
func (e *StorageExecutor) parseCreateRelPatternWithVars(view braceView) (braceView, braceView, braceView, bool, braceView, error) {
	view = view.trim()
	plan, cached := createRelationshipSyntaxPlans.get(view.text())
	if !cached {
		plan = e.scanCreateRelationshipSyntax(view.text())
		createRelationshipSyntaxPlans.put(view.text(), plan)
	}
	return view.spanWindow(plan.source), view.spanWindow(plan.relationship), view.spanWindow(plan.target), plan.reverse, view.spanWindow(plan.remainder), plan.err
}

// The existing relationship grammar retains its extents under the exact text
// cache key. Each invocation projects those extents into its carried input;
// the cache never retains another statement's brace facts. Author: #1663 lane.
func (e *StorageExecutor) scanCreateRelationshipSyntax(pattern string) createRelationshipSyntax {
	findNodeEnd := findMatchingParen

	// Find the first node: (varA)
	if !strings.HasPrefix(pattern, "(") {
		return createRelationshipSyntax{err: localizedError(localization.CypherMutationsRelationshipPatternMustStartNode(), nil)}
	}

	// Find end of first node
	firstNodeEnd := findNodeEnd(pattern, 0)
	if firstNodeEnd < 0 {
		return createRelationshipSyntax{err: localizedError(localization.CypherMutationsRelationshipPatternUnmatchedParen(), nil)}
	}

	sourceVar := cyphertext.TrimWhitespaceSpan(pattern, cyphertext.Span{Start: 1, End: firstNodeEnd})
	restSpan := cyphertext.TrimWhitespaceSpan(pattern, cyphertext.Span{Start: firstNodeEnd + 1, End: len(pattern)})
	rest := pattern[restSpan.Start:restSpan.End]

	// Detect direction and find relationship bracket
	isReverse := false
	var relStart int

	if strings.HasPrefix(rest, "-[") {
		relStart = 2 // Skip "-["
	} else if strings.HasPrefix(rest, "<-[") {
		isReverse = true
		relStart = 3 // Skip "<-["
	} else {
		return createRelationshipSyntax{err: localizedError(localization.CypherResidualRelationshipConnectorExpected(rest[:min(20, len(rest))]), nil)}
	}

	// Find matching ] considering nested brackets in properties
	relEnd := findMatchingBracket(rest, relStart-1)
	if relEnd < 0 {
		return createRelationshipSyntax{err: localizedError(localization.CypherMutationsRelationshipPatternUnmatchedBracket(), nil)}
	}

	relContent := cyphertext.Span{Start: restSpan.Start + relStart, End: restSpan.Start + relEnd}
	afterSpan := cyphertext.TrimWhitespaceSpan(pattern, cyphertext.Span{Start: restSpan.Start + relEnd + 1, End: restSpan.End})
	afterRel := pattern[afterSpan.Start:afterSpan.End]

	// Now find the second node
	var secondNodeStart int
	if isReverse {
		if !strings.HasPrefix(afterRel, "-(") {
			return createRelationshipSyntax{err: localizedError(localization.CypherMutationsRelationshipPatternForwardExpected(), nil)}
		}
		secondNodeStart = 2
	} else {
		if !strings.HasPrefix(afterRel, "->(") {
			return createRelationshipSyntax{err: localizedError(localization.CypherMutationsRelationshipPatternArrowExpected(), nil)}
		}
		secondNodeStart = 3
	}

	// Find end of second node
	secondNodeEnd := findNodeEnd(afterRel, secondNodeStart-1)
	if secondNodeEnd < 0 {
		return createRelationshipSyntax{err: localizedError(localization.CypherMutationsRelationshipPatternSecondUnmatched(), nil)}
	}

	targetVar := cyphertext.TrimWhitespaceSpan(pattern, cyphertext.Span{Start: afterSpan.Start + secondNodeStart, End: afterSpan.Start + secondNodeEnd})
	remainder := cyphertext.TrimWhitespaceSpan(pattern, cyphertext.Span{Start: afterSpan.Start + secondNodeEnd + 1, End: afterSpan.End})

	return createRelationshipSyntax{source: sourceVar, relationship: relContent, target: targetVar, remainder: remainder, reverse: isReverse}
}
