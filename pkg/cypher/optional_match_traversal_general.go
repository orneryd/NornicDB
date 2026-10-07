package cypher

// General (non-seeded) OPTIONAL MATCH clause evaluation for the
// traversal-seeded pipeline, mirroring Neo4j's Apply + Optional operator
// semantics (community/cypher/interpreted-runtime/.../pipes/ApplyPipe.scala
// and OptionalPipe.scala): for every left row the clause pattern is matched;
// rows that match extend the left row with the pattern's new bindings, and a
// left row with no match is preserved once with every newly-introduced
// variable bound to null. A clause is never rejected for not sharing a
// variable with earlier clauses — a disconnected pattern is a valid
// cartesian-style optional join in Neo4j, and a pattern whose only variables
// are already bound simply preserves rows (there is nothing new to null).
//
// The single-hop seeded path in optional_match_traversal.go (mirroring
// OptionalExpandAllPipe.scala) remains the fast path; this file handles
// everything else: disconnected patterns, single-node patterns, multi-hop
// chains, and patterns whose relationship variable is already bound.

import (
	"context"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// scanOptionalPatternShape counts top-level node groups '(' and relationships
// outside quoted strings: bracket sections '[' and bracketless hops (-->, <--,
// --, <-->, which parseOptionalClauseEndpoints normalizes to -[]->). Used to
// route a clause to the seeded single-hop fast path (2 groups, 1
// relationship) or the general path.
func scanOptionalPatternShape(pattern string) (nodeGroups, brackets int) {
	var inQuote bool
	var quoteChar byte
	depth := 0
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if inQuote {
			if c == quoteChar && !isBackslashEscaped(pattern, i) {
				inQuote = false
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inQuote = true
			quoteChar = c
		case '(':
			nodeGroups++
		case '[':
			brackets++
			depth++
		case ']':
			depth--
		case '-':
			if depth == 0 && i+1 < len(pattern) && pattern[i+1] == '-' {
				brackets++
				i++
			}
		}
	}
	return nodeGroups, brackets
}

// firstParenGroup returns the first parenthesized node group of pattern,
// including the parentheses, or "" when none exists.
func firstParenGroup(pattern string) string {
	open := strings.Index(pattern, "(")
	if open < 0 {
		return ""
	}
	closeIdx := strings.Index(pattern[open:], ")")
	if closeIdx <= 0 {
		return ""
	}
	return pattern[open : open+closeIdx+1]
}

// traversalAnonVar names the synthetic variable injected into an anonymous
// leading node group so a pattern with no named variables can still be
// enumerated (the synthetic binding is invisible to RETURN).
const traversalAnonVar = "__nornic_opt_anon"

// ensureLeadingNodeNamed injects a synthetic variable into the pattern's first
// node group when the pattern has no named variables at all, so the pattern
// can be executed and its match multiplicity preserved.
func ensureLeadingNodeNamed(pattern string) string {
	if len(extractNodeVariables(pattern)) > 0 || len(extractRelationshipVariables(pattern)) > 0 {
		return pattern
	}
	open := strings.Index(pattern, "(")
	if open < 0 {
		return pattern
	}
	return pattern[:open+1] + traversalAnonVar + pattern[open+1:]
}

// extendTraversalRowMulti returns a copy of row with additional node,
// relationship, and scalar or path bindings.
func extendTraversalRowMulti(row traversalOptRow, nodeBinds map[string]*storage.Node, relBinds map[string]*storage.Edge, valueBinds map[string]interface{}) traversalOptRow {
	out := traversalOptRow{
		nodes: make(map[string]*storage.Node, util.SafePreallocSum(len(row.nodes), len(nodeBinds))),
		rels:  make(map[string]*storage.Edge, util.SafePreallocSum(len(row.rels), len(relBinds))),
	}
	for k, v := range row.nodes {
		out.nodes[k] = v
	}
	for k, v := range row.rels {
		out.rels[k] = v
	}
	for k, v := range nodeBinds {
		out.nodes[k] = v
	}
	for k, v := range relBinds {
		out.rels[k] = v
	}
	if len(row.values)+len(valueBinds) > 0 {
		out.values = make(map[string]interface{}, util.SafePreallocSum(len(row.values), len(valueBinds)))
		for k, v := range row.values {
			out.values[k] = v
		}
		for k, v := range valueBinds {
			out.values[k] = v
		}
	}
	return out
}

// applySingleNodeOptionalClause handles OPTIONAL MATCH (x[:Label {props}]).
// A bound variable introduces no new bindings, so every row is preserved
// unchanged whether or not the pattern holds (Neo4j nulls only NEWLY
// introduced variables — with none, the optional match is a row-preserving
// no-op). An unbound variable is an independent node scan: each row joins
// with every matching node, or carries a null binding when nothing matches.
func (e *StorageExecutor) applySingleNodeOptionalClause(ctx context.Context, rows []traversalOptRow, clause optionalMatchClause) ([]traversalOptRow, error) {
	group := firstParenGroup(clause.pattern)
	if group == "" {
		return rows, nil
	}
	np := e.parseNodePattern(ctx, group)
	varName := np.variable
	if varName == "" {
		varName = traversalAnonVar
	} else if _, bound := rows[0].nodes[varName]; bound {
		return rows, nil
	}
	propertyExpressions := nodePatternPropertyExpressions(group)

	candidates, err := e.loadNodesWithTemporalViewport(ctx, np.labels)
	if err != nil {
		return nil, err
	}

	out := make([]traversalOptRow, 0, len(rows))
	for _, row := range rows {
		scope := pipelineRowFromTraversalOptionalRow(row)
		matched := false
		for _, node := range candidates {
			propertyMatch := true
			for name, expression := range propertyExpressions {
				expected, resolved, err := e.evaluateRowValue(expression, scope)
				if err != nil {
					return nil, err
				}
				if !resolved {
					expected = np.properties[name]
				}
				actual, exists := node.Properties[name]
				if !exists || !e.compareEqual(actual, expected) {
					propertyMatch = false
					break
				}
			}
			if !propertyMatch {
				continue
			}
			cand := extendTraversalRow(row, varName, node, "", nil)
			if !e.traversalOptionalWhereMatches(ctx, clause.where, cand) {
				continue
			}
			cand.optionalMatched = true
			out = append(out, cand)
			matched = true
		}
		if !matched {
			out = append(out, extendTraversalRow(row, varName, nil, "", nil))
		}
	}
	return out, nil
}

func nodePatternPropertyExpressions(group string) map[string]string {
	open := strings.IndexByte(group, '{')
	close := strings.LastIndexByte(group, '}')
	if open < 0 || close <= open {
		return nil
	}
	expressions := make(map[string]string)
	for _, pair := range splitTopLevelComma(group[open+1 : close]) {
		separator := findTopLevelMapKeyValueSeparator(pair)
		if separator <= 0 {
			continue
		}
		name := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
		expressions[name] = strings.TrimSpace(pair[separator+1:])
	}
	return expressions
}

func pipelineRowFromTraversalOptionalRow(row traversalOptRow) pipelineRow {
	scope := make(pipelineRow, len(row.nodes)+len(row.rels)+len(row.values))
	for name, node := range row.nodes {
		if node == nil {
			scope[name] = nil
		} else {
			scope[name] = node
		}
	}
	for name, relationship := range row.rels {
		if relationship == nil {
			scope[name] = nil
		} else {
			scope[name] = relationship
		}
	}
	for name, value := range row.values {
		scope[name] = value
	}
	return scope
}

// applyGeneralOptionalClause handles every clause shape the seeded single-hop
// fast path does not: disconnected patterns, multi-hop chains, and patterns
// whose relationship variable is already bound. The pattern is enumerated once
// as an independent MATCH; each left row joins with the candidates whose
// shared-variable bindings agree by identity, and left rows with no agreeing
// candidate are preserved with null bindings for the new variables — the
// Apply + Optional contract.
func (e *StorageExecutor) applyGeneralOptionalClause(ctx context.Context, rows []traversalOptRow, clause optionalMatchClause) ([]traversalOptRow, error) {
	pattern := ensureLeadingNodeNamed(clause.pattern)
	nodeVars := extractNodeVariables(pattern)
	relVars := extractRelationshipVariables(pattern)
	pathVar := extractPathAssignmentVariable(pattern)
	allVars := append(append([]string{}, nodeVars...), relVars...)
	if pathVar != "" {
		allVars = appendUniquePipelineBinding(allVars, pathVar)
	}
	if len(rows) == 0 || len(allVars) == 0 {
		return rows, nil
	}
	matchClause := "MATCH " + pattern
	if strings.TrimSpace(clause.where) != "" {
		matchClause += " WHERE " + clause.where
	}
	out := make([]traversalOptRow, 0, len(rows))
	for _, row := range rows {
		scope := pipelineRowFromTraversalOptionalRow(row)
		matched, handled, err := e.pipelineApplyMatch(withValueBindings(ctx, scope), []pipelineRow{scope}, matchClause)
		if err != nil {
			return nil, err
		}
		if !handled {
			return nil, unsupportedOptionalMatchShapeError(matchClause)
		}
		for _, binding := range matched {
			candidate := extendTraversalRowMulti(row, nil, nil, nil)
			if candidate.values == nil {
				candidate.values = make(map[string]interface{})
			}
			for _, name := range allVars {
				switch value := binding[name].(type) {
				case *storage.Node:
					candidate.nodes[name] = value
				case *storage.Edge:
					candidate.rels[name] = value
				default:
					candidate.values[name] = value
				}
			}
			candidate.optionalMatched = true
			out = append(out, candidate)
		}
		if len(matched) == 0 {
			candidate := extendTraversalRowMulti(row, nil, nil, nil)
			if candidate.values == nil {
				candidate.values = make(map[string]interface{})
			}
			variableLength := variableLengthRelationshipVariableSet(pattern)
			for _, name := range allVars {
				if _, bound := scope[name]; bound {
					continue
				}
				if name == pathVar {
					candidate.values[name] = nil
				} else if containsString(nodeVars, name) {
					candidate.nodes[name] = nil
				} else if _, list := variableLength[name]; list {
					candidate.values[name] = nil
				} else {
					candidate.rels[name] = nil
				}
			}
			out = append(out, candidate)
		}
	}
	return out, nil
}

func candidateValuesAgreeWithRow(row, candidate traversalOptRow, variables []string) bool {
	for _, variable := range variables {
		left, leftBound := row.values[variable]
		right, rightBound := candidate.values[variable]
		if !leftBound || !rightBound || left == nil || right == nil || !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}

// candidateAgreesWithRow reports whether a candidate's bindings for the shared
// variables match the left row's bindings by identity. A left binding that is
// null (an earlier optional miss) can never agree — mirroring
// OptionalExpandAllPipe's null-source behavior, the row then null-fills.
func candidateAgreesWithRow(row, cand traversalOptRow, sharedNodeVars, sharedRelVars []string) bool {
	for _, v := range sharedNodeVars {
		left := row.nodes[v]
		right := cand.nodes[v]
		if left == nil || right == nil || left.ID != right.ID {
			return false
		}
	}
	for _, v := range sharedRelVars {
		left := row.rels[v]
		right := cand.rels[v]
		if left == nil || right == nil || left.ID != right.ID {
			return false
		}
	}
	return true
}
