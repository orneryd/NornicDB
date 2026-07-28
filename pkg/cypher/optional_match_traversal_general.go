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
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// scanOptionalPatternShape counts top-level node groups '(' and relationship
// bracket sections '[' outside quoted strings. Used to route a clause to the
// seeded single-hop fast path (2 groups, 1 bracket) or the general path.
func scanOptionalPatternShape(pattern string) (nodeGroups, brackets int) {
	var inQuote bool
	var quoteChar byte
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if inQuote {
			if c == quoteChar && (i == 0 || pattern[i-1] != '\\') {
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

// extendTraversalRowMulti returns a copy of row with additional node and
// relationship bindings.
func extendTraversalRowMulti(row traversalOptRow, nodeBinds map[string]*storage.Node, relBinds map[string]*storage.Edge) traversalOptRow {
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

	candidates, err := e.loadNodesWithTemporalViewport(ctx, np.labels)
	if err != nil {
		return nil, err
	}
	filtered := make([]*storage.Node, 0, len(candidates))
	for _, node := range candidates {
		match := true
		for k, expected := range np.properties {
			actual, ok := node.Properties[k]
			if !ok || !e.compareEqual(actual, expected) {
				match = false
				break
			}
		}
		if match {
			filtered = append(filtered, node)
		}
	}

	out := make([]traversalOptRow, 0, len(rows))
	for _, row := range rows {
		matched := false
		for _, node := range filtered {
			cand := extendTraversalRow(row, varName, node, "", nil)
			if clause.where != "" {
				passes, ok := e.evaluateExpressionWithContext(ctx, clause.where, cand.nodes, cand.rels).(bool)
				if !ok || !passes {
					continue
				}
			}
			out = append(out, cand)
			matched = true
		}
		if !matched {
			out = append(out, extendTraversalRow(row, varName, nil, "", nil))
		}
	}
	return out, nil
}

// applyGeneralOptionalClause handles every clause shape the seeded single-hop
// fast path does not: disconnected patterns, multi-hop chains, and patterns
// whose relationship variable is already bound. The pattern is enumerated once
// as an independent MATCH; each left row joins with the candidates whose
// shared-variable bindings agree by identity, and left rows with no agreeing
// candidate are preserved with null bindings for the new variables — the
// Apply + Optional contract.
func (e *StorageExecutor) applyGeneralOptionalClause(ctx context.Context, rows []traversalOptRow, clause optionalMatchClause) ([]traversalOptRow, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	pattern := ensureLeadingNodeNamed(clause.pattern)
	nodeVars := extractNodeVariables(pattern)
	relVars := extractRelationshipVariables(pattern)
	allVars := append(append([]string{}, nodeVars...), relVars...)
	if len(allVars) == 0 {
		return rows, nil
	}

	matchResult, err := e.executeMatch(ctx, "MATCH "+pattern+" RETURN "+strings.Join(allVars, ", "))
	if err != nil {
		return nil, err
	}
	candidates := make([]traversalOptRow, 0, len(matchResult.Rows))
	for _, r := range matchResult.Rows {
		cand := traversalOptRow{
			nodes: make(map[string]*storage.Node, len(matchResult.Columns)),
			rels:  make(map[string]*storage.Edge),
		}
		for ci, col := range matchResult.Columns {
			if ci >= len(r) {
				break
			}
			switch v := r[ci].(type) {
			case *storage.Node:
				cand.nodes[col] = v
			case *storage.Edge:
				cand.rels[col] = v
			}
		}
		candidates = append(candidates, cand)
	}

	// Split the pattern variables into shared (already bound on the left) and
	// new, using the uniform binding keys of the first row.
	var sharedNodeVars, newNodeVars, sharedRelVars, newRelVars []string
	for _, v := range nodeVars {
		if _, bound := rows[0].nodes[v]; bound {
			sharedNodeVars = append(sharedNodeVars, v)
		} else {
			newNodeVars = append(newNodeVars, v)
		}
	}
	for _, v := range relVars {
		if _, bound := rows[0].rels[v]; bound {
			sharedRelVars = append(sharedRelVars, v)
		} else {
			newRelVars = append(newRelVars, v)
		}
	}

	nullBindsNodes := make(map[string]*storage.Node, len(newNodeVars))
	for _, v := range newNodeVars {
		nullBindsNodes[v] = nil
	}
	nullBindsRels := make(map[string]*storage.Edge, len(newRelVars))
	for _, v := range newRelVars {
		nullBindsRels[v] = nil
	}

	out := make([]traversalOptRow, 0, len(rows))
	for _, row := range rows {
		matched := false
		for _, cand := range candidates {
			if !candidateAgreesWithRow(row, cand, sharedNodeVars, sharedRelVars) {
				continue
			}
			nodeBinds := make(map[string]*storage.Node, len(newNodeVars))
			for _, v := range newNodeVars {
				nodeBinds[v] = cand.nodes[v]
			}
			relBinds := make(map[string]*storage.Edge, len(newRelVars))
			for _, v := range newRelVars {
				relBinds[v] = cand.rels[v]
			}
			merged := extendTraversalRowMulti(row, nodeBinds, relBinds)
			if clause.where != "" {
				passes, ok := e.evaluateExpressionWithContext(ctx, clause.where, merged.nodes, merged.rels).(bool)
				if !ok || !passes {
					continue
				}
			}
			out = append(out, merged)
			matched = true
		}
		if !matched {
			out = append(out, extendTraversalRowMulti(row, nullBindsNodes, nullBindsRels))
		}
	}
	return out, nil
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
