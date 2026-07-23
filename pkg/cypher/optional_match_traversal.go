package cypher

// Traversal-seeded OPTIONAL MATCH execution.
//
// executeCompoundMatchOptionalMatch routes here when the primary MATCH clause
// contains a relationship pattern (a traversal) and one or more OPTIONAL MATCH
// clauses follow it without an intervening WITH. Historically this branch
// resolved the RETURN projection with resolveReturnExprFromVarMap, which only
// understood "var.prop" and bare variables — every other expression (type(),
// coalesce(), labels(), aggregates, properties of second-level OPTIONAL
// bindings, and the primary MATCH's relationship variable) leaked back to the
// client as its literal source text. This file replaces that path with:
//
//  1. a seed MATCH that binds relationship variables as well as node variables,
//  2. an iterative left-outer join across EVERY chained OPTIONAL MATCH clause
//     (binding whichever endpoint is not yet bound, in either direction), and
//  3. projection through the real expression evaluator
//     (evaluateExpressionWithContext), with implicit-grouping aggregation and
//     ORDER BY / SKIP / LIMIT handling that mirrors buildJoinedResult.

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// traversalOptRow is one joined row in the traversal-seeded OPTIONAL MATCH
// pipeline. Every bound variable maps to its node or relationship; a variable
// bound to nil records an OPTIONAL MATCH that found no counterpart (Cypher
// null), which downstream projection must distinguish from "never bound".
type traversalOptRow struct {
	nodes map[string]*storage.Node
	rels  map[string]*storage.Edge
}

// optionalMatchClause is a single OPTIONAL MATCH clause: its relationship
// pattern and the optional trailing WHERE predicate.
type optionalMatchClause struct {
	pattern string
	where   string
}

// optionalClauseEndpoints is the fully parsed form of one OPTIONAL MATCH
// relationship pattern: both endpoint node patterns (variable, labels,
// properties), the relationship variable/type, and the direction relative to
// the source (left) endpoint.
type optionalClauseEndpoints struct {
	source    nodePatternInfo
	target    nodePatternInfo
	relVar    string
	relType   string
	direction string // "out", "in", or "both", relative to source
}

// splitOptionalMatchClauses splits the text that follows the first
// "OPTIONAL MATCH" keyword (already sliced to end before WITH/RETURN) into
// individual clauses. Each clause's own WHERE predicate is separated from its
// pattern. The first clause has no leading "OPTIONAL MATCH" keyword because
// the caller consumed it.
func splitOptionalMatchClauses(section string) []optionalMatchClause {
	var clauses []optionalMatchClause
	rest := strings.TrimSpace(section)
	for rest != "" {
		clauseText := rest
		next := findMultiWordKeywordIndex(rest, "OPTIONAL", "MATCH")
		if next > 0 {
			clauseText = strings.TrimSpace(rest[:next])
			after := rest[next:]
			mIdx := findKeywordIndex(after, "MATCH")
			rest = strings.TrimSpace(after[mIdx+len("MATCH"):])
		} else {
			rest = ""
		}
		clause := optionalMatchClause{pattern: clauseText}
		if wIdx := findKeywordIndex(clauseText, "WHERE"); wIdx > 0 {
			clause.where = strings.TrimSpace(clauseText[wIdx+len("WHERE"):])
			clause.pattern = strings.TrimSpace(clauseText[:wIdx])
		}
		if clause.pattern != "" {
			clauses = append(clauses, clause)
		}
	}
	return clauses
}

// extractRelationshipVariables extracts relationship variable names from a
// MATCH pattern, e.g. "rel" from "(a)-[rel:INHERITS]->(b)". Anonymous
// relationships ("[:TYPE]", "[*1..2]") contribute nothing.
func extractRelationshipVariables(matchClause string) []string {
	var vars []string
	for i := 0; i < len(matchClause); i++ {
		if matchClause[i] != '[' {
			continue
		}
		j := i + 1
		for j < len(matchClause) && isWhitespace(matchClause[j]) {
			j++
		}
		name, next, ok := scanIdentifierToken(matchClause, j)
		if !ok {
			continue
		}
		for next < len(matchClause) && isWhitespace(matchClause[next]) {
			next++
		}
		if next < len(matchClause) &&
			(matchClause[next] == ':' || matchClause[next] == ']' || matchClause[next] == '*') {
			vars = append(vars, name)
		}
	}
	return vars
}

// parseOptionalClauseEndpoints parses one OPTIONAL MATCH relationship pattern
// into both endpoint node patterns plus relationship variable, type, and
// direction. It returns an error for patterns without two node endpoints.
func (e *StorageExecutor) parseOptionalClauseEndpoints(ctx context.Context, pattern string) (optionalClauseEndpoints, error) {
	eps := optionalClauseEndpoints{direction: "both"}
	if strings.Contains(pattern, "<-") {
		eps.direction = "in"
	} else if strings.Contains(pattern, "->") {
		eps.direction = "out"
	}

	openIdx := strings.Index(pattern, "(")
	if openIdx < 0 {
		return eps, fmt.Errorf("optional match pattern %q has no node endpoint", truncateQuery(pattern, 60))
	}
	closeIdx := strings.Index(pattern[openIdx:], ")")
	if closeIdx <= 0 {
		return eps, fmt.Errorf("optional match pattern %q has an unterminated node endpoint", truncateQuery(pattern, 60))
	}
	eps.source = e.parseNodePattern(ctx, pattern[openIdx:openIdx+closeIdx+1])

	if brIdx := strings.Index(pattern, "["); brIdx >= 0 {
		brEnd := strings.Index(pattern[brIdx:], "]")
		if brEnd > 0 {
			relStr := pattern[brIdx+1 : brIdx+brEnd]
			if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
				eps.relVar = strings.TrimSpace(relStr[:colonIdx])
				relType := strings.TrimSpace(relStr[colonIdx+1:])
				if propIdx := strings.Index(relType, "{"); propIdx >= 0 {
					relType = strings.TrimSpace(relType[:propIdx])
				}
				if starIdx := strings.Index(relType, "*"); starIdx >= 0 {
					relType = strings.TrimSpace(relType[:starIdx])
				}
				eps.relType = relType
			} else if starIdx := strings.Index(relStr, "*"); starIdx >= 0 {
				eps.relVar = strings.TrimSpace(relStr[:starIdx])
			} else {
				eps.relVar = strings.TrimSpace(relStr)
			}
		}
	}

	rest := pattern[openIdx+closeIdx+1:]
	if relEnd := strings.Index(rest, "]"); relEnd >= 0 {
		rest = rest[relEnd+1:]
	}
	tOpen := strings.Index(rest, "(")
	if tOpen < 0 {
		return eps, fmt.Errorf("optional match pattern %q has no target endpoint", truncateQuery(pattern, 60))
	}
	tClose := strings.Index(rest[tOpen:], ")")
	if tClose <= 0 {
		return eps, fmt.Errorf("optional match pattern %q has an unterminated target endpoint", truncateQuery(pattern, 60))
	}
	eps.target = e.parseNodePattern(ctx, rest[tOpen:tOpen+tClose+1])
	return eps, nil
}

// invertOptionalDirection flips a traversal direction for seeding a join from
// the pattern's target endpoint instead of its source endpoint.
func invertOptionalDirection(direction string) string {
	switch direction {
	case "out":
		return "in"
	case "in":
		return "out"
	default:
		return direction
	}
}

// extendTraversalRow returns a copy of row with an additional node binding
// (when nodeVar is non-empty) and relationship binding (when relVar is
// non-empty). Rows are copied because a single input row can fan out into
// multiple joined rows.
func extendTraversalRow(row traversalOptRow, nodeVar string, node *storage.Node, relVar string, edge *storage.Edge) traversalOptRow {
	out := traversalOptRow{
		nodes: make(map[string]*storage.Node, len(row.nodes)+1),
		rels:  make(map[string]*storage.Edge, len(row.rels)+1),
	}
	for k, v := range row.nodes {
		out.nodes[k] = v
	}
	for k, v := range row.rels {
		out.rels[k] = v
	}
	if nodeVar != "" {
		out.nodes[nodeVar] = node
	}
	if relVar != "" {
		out.rels[relVar] = edge
	}
	return out
}

// executeTraversalSeededOptionalMatch executes a
// "MATCH <traversal> OPTIONAL MATCH ... [OPTIONAL MATCH ...] RETURN ..." query.
// initialSection is everything between MATCH and the first OPTIONAL MATCH
// (pattern plus optional WHERE), nodePatternStr is the bare pattern,
// optSection is the raw text of all OPTIONAL MATCH clauses, and restOfQuery
// begins at the first WITH/RETURN after them.
func (e *StorageExecutor) executeTraversalSeededOptionalMatch(ctx context.Context, initialSection, nodePatternStr, optSection, restOfQuery string) (*ExecuteResult, error) {
	returnVars := strings.Join(append(extractNodeVariables(nodePatternStr), extractRelationshipVariables(nodePatternStr)...), ", ")
	if returnVars == "" {
		returnVars = "*"
	}
	matchQuery := "MATCH " + initialSection + " RETURN " + returnVars
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute initial traversal MATCH: %w", err)
	}

	rows := make([]traversalOptRow, 0, len(matchResult.Rows))
	for _, r := range matchResult.Rows {
		row := traversalOptRow{
			nodes: make(map[string]*storage.Node, len(matchResult.Columns)),
			rels:  make(map[string]*storage.Edge),
		}
		for ci, col := range matchResult.Columns {
			if ci >= len(r) {
				break
			}
			switch v := r[ci].(type) {
			case *storage.Node:
				row.nodes[col] = v
			case *storage.Edge:
				row.rels[col] = v
			case nil:
				row.nodes[col] = nil
			}
		}
		rows = append(rows, row)
	}

	for _, clause := range splitOptionalMatchClauses(optSection) {
		rows, err = e.applyTraversalOptionalClause(ctx, rows, clause)
		if err != nil {
			return nil, err
		}
	}

	return e.projectTraversalOptionalRows(ctx, rows, restOfQuery)
}

// applyTraversalOptionalClause left-outer-joins one OPTIONAL MATCH clause
// against every row. The bound endpoint seeds the traversal; the unbound
// endpoint (and the relationship variable, when named) is bound on each row.
// When both endpoints are already bound the clause acts as a filter that binds
// only the relationship variable. A row whose seed is null (an earlier
// OPTIONAL MATCH miss) propagates null bindings, matching Cypher semantics.
func (e *StorageExecutor) applyTraversalOptionalClause(ctx context.Context, rows []traversalOptRow, clause optionalMatchClause) ([]traversalOptRow, error) {
	eps, err := e.parseOptionalClauseEndpoints(ctx, clause.pattern)
	if err != nil {
		return nil, err
	}

	out := make([]traversalOptRow, 0, len(rows))
	for _, row := range rows {
		srcNode, srcBound := row.nodes[eps.source.variable]
		tgtNode, tgtBound := row.nodes[eps.target.variable]

		var seed *storage.Node
		var pattern optionalRelPattern
		newNodeVar := ""
		switch {
		case srcBound:
			seed = srcNode
			pattern = optionalRelPattern{
				sourceVar:    eps.source.variable,
				relVar:       eps.relVar,
				relType:      eps.relType,
				targetVar:    eps.target.variable,
				targetLabels: eps.target.labels,
				targetProps:  eps.target.properties,
				direction:    eps.direction,
			}
			if !tgtBound {
				newNodeVar = eps.target.variable
			}
		case tgtBound:
			seed = tgtNode
			pattern = optionalRelPattern{
				sourceVar:    eps.target.variable,
				relVar:       eps.relVar,
				relType:      eps.relType,
				targetVar:    eps.source.variable,
				targetLabels: eps.source.labels,
				targetProps:  eps.source.properties,
				direction:    invertOptionalDirection(eps.direction),
			}
			newNodeVar = eps.source.variable
		default:
			return nil, fmt.Errorf(
				"unsupported OPTIONAL MATCH: pattern %q references no variable bound by an earlier clause",
				truncateQuery(clause.pattern, 60))
		}

		if seed == nil {
			out = append(out, extendTraversalRow(row, newNodeVar, nil, eps.relVar, nil))
			continue
		}

		matched := false
		for _, rel := range e.findOptionalRelatedNodes(ctx, seed, clause.pattern, pattern) {
			if srcBound && tgtBound {
				if rel.node == nil || tgtNode == nil || rel.node.ID != tgtNode.ID {
					continue
				}
			}
			cand := extendTraversalRow(row, newNodeVar, rel.node, eps.relVar, rel.edge)
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
			out = append(out, extendTraversalRow(row, newNodeVar, nil, eps.relVar, nil))
		}
	}
	return out, nil
}

// projectTraversalOptionalRows evaluates the RETURN clause of restOfQuery
// against the joined rows using the real expression evaluator, routing
// aggregate projections through implicit-grouping aggregation, then applies
// ORDER BY / SKIP / LIMIT.
func (e *StorageExecutor) projectTraversalOptionalRows(ctx context.Context, rows []traversalOptRow, restOfQuery string) (*ExecuteResult, error) {
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx < 0 {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	returnClause := strings.TrimSpace(restOfQuery[returnIdx+len("RETURN"):])
	items := e.parseReturnItems(stripTrailingReturnClauses(returnClause))

	result := &ExecuteResult{Columns: make([]string, len(items))}
	for i, item := range items {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	if rowHasAggregate(items) {
		aggRows, err := e.aggregateTraversalOptionalRows(ctx, rows, items)
		if err != nil {
			return nil, err
		}
		result.Rows = aggRows
	} else {
		result.Rows = make([][]interface{}, 0, len(rows))
		for _, row := range rows {
			outRow := make([]interface{}, len(items))
			for i, item := range items {
				outRow[i] = e.evalTraversalProjection(ctx, item.expr, row)
			}
			result.Rows = append(result.Rows, outRow)
		}
	}

	e.applyTraversalReturnModifiers(result, returnClause)
	return result, nil
}

// evalTraversalProjection evaluates one projection expression against a joined
// row: the "var.prop" / bare-variable fast path first, then the full
// expression evaluator for everything else (functions, coalesce, CASE, ...).
func (e *StorageExecutor) evalTraversalProjection(ctx context.Context, expr string, row traversalOptRow) interface{} {
	if v, ok := fastTraversalExprValue(expr, row); ok {
		return v
	}
	return e.evaluateExpressionWithContext(ctx, expr, row.nodes, row.rels)
}

// isSimpleTraversalIdentifier reports whether s is a bare Cypher identifier
// (letters, digits, underscores; not starting with a digit). Used to gate the
// projection fast path so any richer expression falls back to the full
// evaluator.
func isSimpleTraversalIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// fastTraversalExprValue resolves the two hot projection shapes — "var.prop"
// and a bare bound variable — without walking the full expression evaluator's
// dispatch chain. It replicates evaluateExpressionWithContext's semantics for
// exactly those shapes (nil bindings project as null, has_embedding reads
// EmbedMeta, single-"value" pseudo-nodes unwrap to their scalar) and reports
// ok=false for everything else so the caller falls back to the evaluator.
func fastTraversalExprValue(expr string, row traversalOptRow) (interface{}, bool) {
	expr = strings.TrimSpace(expr)

	if dotIdx := strings.IndexByte(expr, '.'); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]
		if !isSimpleTraversalIdentifier(varName) || !isSimpleTraversalIdentifier(propName) {
			return nil, false
		}
		if node, ok := row.nodes[varName]; ok {
			if node == nil {
				return nil, true
			}
			if propName == "has_embedding" {
				if node.EmbedMeta != nil {
					if val, ok := node.EmbedMeta["has_embedding"]; ok {
						return val, true
					}
				}
				return len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0, true
			}
			return node.Properties[propName], true
		}
		if rel, ok := row.rels[varName]; ok {
			if rel == nil {
				return nil, true
			}
			return rel.Properties[propName], true
		}
		return nil, false
	}

	if !isSimpleTraversalIdentifier(expr) {
		return nil, false
	}
	if node, ok := row.nodes[expr]; ok {
		if node == nil {
			return nil, true
		}
		// Scalar wrapper pseudo-node (YIELD variables): unwrap single "value".
		if len(node.Properties) == 1 {
			if val, hasValue := node.Properties["value"]; hasValue {
				return val, true
			}
		}
		return node, true
	}
	if rel, ok := row.rels[expr]; ok {
		if rel == nil {
			return nil, true
		}
		return rel, true
	}
	return nil, false
}

// applyTraversalReturnModifiers applies ORDER BY, SKIP, and LIMIT from the
// RETURN clause tail, mirroring buildJoinedResult's handling.
func (e *StorageExecutor) applyTraversalReturnModifiers(result *ExecuteResult, returnClause string) {
	if orderByIdx := findMultiWordKeywordIndex(returnClause, "ORDER", "BY"); orderByIdx >= 0 {
		orderPart := returnClause[orderByIdx:]
		if mIdx := findKeywordIndex(orderPart, "BY"); mIdx >= 0 {
			orderPart = orderPart[mIdx+len("BY"):]
		}
		endIdx := len(orderPart)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		result.Rows = e.orderResultRows(result.Rows, result.Columns, strings.TrimSpace(orderPart[:endIdx]))
	}

	skip := 0
	if skipIdx := findKeywordIndex(returnClause, "SKIP"); skipIdx >= 0 {
		if fields := strings.Fields(returnClause[skipIdx+len("SKIP"):]); len(fields) > 0 {
			if s, err := strconv.Atoi(fields[0]); err == nil {
				skip = s
			}
		}
	}
	limit := -1
	if limitIdx := findKeywordIndex(returnClause, "LIMIT"); limitIdx >= 0 {
		if fields := strings.Fields(returnClause[limitIdx+len("LIMIT"):]); len(fields) > 0 {
			if l, err := strconv.Atoi(fields[0]); err == nil {
				limit = l
			}
		}
	}
	if skip > 0 || limit >= 0 {
		start := skip
		if start > len(result.Rows) {
			start = len(result.Rows)
		}
		end := len(result.Rows)
		if limit >= 0 && start+limit < end {
			end = start + limit
		}
		result.Rows = result.Rows[start:end]
	}
}
