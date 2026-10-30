package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// Traversal-seeded OPTIONAL MATCH execution.
//
// The pipeline's optional-match plan (tryExecutePipelineOptionalMatchPlan)
// routes here for a read-only MATCH followed by one or more OPTIONAL MATCH
// clauses and a RETURN. It runs:
//
//  1. the MATCH through the pipeline's MATCH operator, binding relationship
//     and path variables as well as node variables,
//  2. an iterative left-outer join across EVERY chained OPTIONAL MATCH clause
//     (binding whichever endpoint is not yet bound, in either direction), and
//  3. projection through the real expression evaluator
//     (evaluateExpressionWithContext), with implicit-grouping aggregation and
//     ORDER BY / SKIP / LIMIT.

// traversalOptRow is one joined row in the traversal-seeded OPTIONAL MATCH
// pipeline. Every bound variable maps to its node or relationship; a variable
// bound to nil records an OPTIONAL MATCH that found no counterpart (Cypher
// null), which downstream projection must distinguish from "never bound".
type traversalOptRow struct {
	nodes           map[string]*storage.Node
	rels            map[string]*storage.Edge
	values          map[string]interface{}
	optionalMatched bool
}

// optionalMatchClause is a single OPTIONAL MATCH clause: its relationship
// pattern and the optional trailing WHERE predicate.
type optionalMatchClause struct {
	pattern braceView
	where   braceView
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

func (e *StorageExecutor) traversalOptionalWhereMatches(ctx context.Context, predicate braceView, row traversalOptRow) bool {
	if predicate.trim().empty() {
		return true
	}
	graphValues := util.SafePreallocSum(len(row.nodes), len(row.rels))
	values := make(map[string]interface{}, util.SafePreallocSum(graphValues, len(row.values)))
	for variable, node := range row.nodes {
		values[variable] = node
	}
	for variable, relationship := range row.rels {
		values[variable] = relationship
	}
	for variable, value := range row.values {
		values[variable] = value
	}
	predicate = substituteWithWhereLabelTests(predicate, values)
	if value, evaluated := e.evaluateRowExpressionWithContext(ctx, predicate, pipelineRow(values)); evaluated {
		return predicateValueIsTrue(ctx, value, predicate.text())
	}
	return predicateValueIsTrue(ctx, e.evaluateExpressionWithContext(ctx, predicate, row.nodes, row.rels), predicate.text())
}

// splitOptionalMatchClauses splits the text that follows the first
// "OPTIONAL MATCH" keyword (already sliced to end before WITH/RETURN) into
// individual clauses. Each clause's own WHERE predicate is separated from its
// pattern. The first clause has no leading "OPTIONAL MATCH" keyword because
// the caller consumed it.
func splitOptionalMatchClauses(section braceView) []optionalMatchClause {
	var clauses []optionalMatchClause
	rest := section.trim()
	for !rest.empty() {
		clauseText := rest
		next := findMultiWordKeywordIndex(rest, "OPTIONAL", "MATCH")
		if next > 0 {
			clauseText = rest.window(0, next).trim()
			after := rest.window(next, len(rest.text()))
			mIdx := findKeywordIndex(after, "MATCH")
			rest = after.window(mIdx+len("MATCH"), len(after.text())).trim()
		} else {
			rest = rest.window(0, 0)
		}
		clause := optionalMatchClause{pattern: clauseText, where: clauseText.window(0, 0)}
		if wIdx := findKeywordIndex(clauseText, "WHERE"); wIdx > 0 {
			clause.where = clauseText.window(wIdx+len("WHERE"), len(clauseText.text())).trim()
			clause.pattern = clauseText.window(0, wIdx).trim()
		}
		if !clause.pattern.empty() {
			clauses = append(clauses, clause)
		}
	}
	return clauses
}

// extractRelationshipVariables extracts relationship variable names from a
// MATCH pattern, e.g. "rel" from "(a)-[rel:INHERITS]->(b)". Anonymous
// relationships ("[:TYPE]", "[*1..2]") contribute nothing. A backtick-quoted
// variable (`r r`) is returned as written.
func extractRelationshipVariables(view braceView) []string {
	matchClause := view.text()
	var vars []string
	for i, end := readRelationshipBracket(view, 0, requireClosedRelationship); i >= 0; i, end = readRelationshipBracket(view, end+1, requireClosedRelationship) {
		j := i + 1
		for j < len(matchClause) && cyphertext.IsWhitespaceAt(matchClause, j) {
			j++
		}
		name, next, ok := scanSymbolicName(matchClause, j)
		if !ok {
			continue
		}
		for next < len(matchClause) && cyphertext.IsWhitespaceAt(matchClause, next) {
			next++
		}
		if next < len(matchClause) &&
			(matchClause[next] == ':' || matchClause[next] == ']' || matchClause[next] == '*' || cyphertext.IsOpenBrace(matchClause[next]) || matchClause[next] == '$') {
			vars = append(vars, name)
		}
	}
	return vars
}

// parseOptionalClauseEndpoints parses one OPTIONAL MATCH relationship pattern
// into both endpoint node patterns plus relationship variable, type, and
// direction. It returns an error for patterns without two node endpoints.
func (e *StorageExecutor) parseOptionalClauseEndpoints(ctx context.Context, view braceView) (optionalClauseEndpoints, error) {
	eps := optionalClauseEndpoints{direction: "both"}
	// Anonymous relationships (-->, <--, --) become bracketed ones, so the
	// relationship bracket below always carries the direction.
	view = normalizeAnonymousTraversalView(view)
	pattern := view.text()

	openIdx := strings.Index(pattern, "(")
	if openIdx < 0 {
		return eps, localizedError(localization.CypherMatchingOptionalMatchNodeEndpointMissing(truncateQuery(pattern, 60)), nil)
	}
	closeIdx := findMatchingParen(pattern, openIdx)
	if closeIdx < 0 {
		return eps, localizedError(localization.CypherMatchingOptionalMatchNodeEndpointUnterminated(truncateQuery(pattern, 60)), nil)
	}
	var parseErr error
	eps.source, parseErr = e.parseNodePattern(ctx, view.window(openIdx, closeIdx+1))
	if parseErr != nil {
		return eps, parseErr
	}

	restView := view.window(closeIdx+1, len(pattern))
	rest := restView.text()
	if relOpen, relClose := readRelationshipBracket(restView, 0, requireClosedRelationship); relOpen >= 0 {
		// The arrows around the relationship give the direction.
		if strings.HasSuffix(strings.TrimSpace(rest[:relOpen]), "<-") {
			eps.direction = "in"
		} else if strings.HasPrefix(strings.TrimSpace(rest[relClose+1:]), "->") {
			eps.direction = "out"
		} else {
			eps.direction = "both"
		}
		relStr := rest[relOpen+1 : relClose]
		if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
			eps.relVar = strings.TrimSpace(relStr[:colonIdx])
			relTypeView := restView.window(relOpen+1+colonIdx+1, relClose).trim()
			relType := relTypeView.text()
			if propIdx, _, _, found := relTypeView.firstBrace(); found {
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
		restView = restView.window(relClose+1, len(rest))
		rest = restView.text()
	}
	tOpen := strings.Index(rest, "(")
	if tOpen < 0 {
		return eps, localizedError(localization.CypherMatchingOptionalMatchTargetEndpointMissing(truncateQuery(pattern, 60)), nil)
	}
	tClose := findMatchingParen(rest, tOpen)
	if tClose < 0 {
		return eps, localizedError(localization.CypherMatchingOptionalMatchTargetEndpointUnterminated(truncateQuery(pattern, 60)), nil)
	}
	eps.target, parseErr = e.parseNodePattern(ctx, restView.window(tOpen, tClose+1))
	if parseErr != nil {
		return eps, parseErr
	}
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
		nodes: make(map[string]*storage.Node, util.SafePreallocSum(len(row.nodes), 1)),
		rels:  make(map[string]*storage.Edge, util.SafePreallocSum(len(row.rels), 1)),
	}
	for k, v := range row.nodes {
		out.nodes[k] = v
	}
	for k, v := range row.rels {
		out.rels[k] = v
	}
	if len(row.values) > 0 {
		out.values = make(map[string]interface{}, len(row.values))
		for k, v := range row.values {
			out.values[k] = v
		}
	}
	if nodeVar != "" {
		out.nodes[nodeVar] = node
	}
	if relVar != "" {
		out.rels[relVar] = edge
	}
	return out
}

// applyTraversalOptionalClause left-outer-joins one OPTIONAL MATCH clause
// against every row, routing by pattern shape (mirroring how Neo4j's planner
// picks OptionalExpandAll for a connected single hop and Apply + Optional for
// everything else):
//
//   - single node group, no relationship: applySingleNodeOptionalClause;
//   - one hop with a bound endpoint and an unbound (or absent) relationship
//     variable: the seeded expansion below (OptionalExpandAllPipe semantics —
//     the bound endpoint seeds the traversal, the unbound endpoint and
//     relationship variable bind per match, both-endpoints-bound acts as a
//     relationship filter, and a null seed propagates null bindings);
//   - everything else (disconnected patterns, multi-hop chains, bound
//     relationship variables): applyGeneralOptionalClause, the Apply +
//     Optional contract. No valid shape is rejected.
func (e *StorageExecutor) applyTraversalOptionalClause(ctx context.Context, rows []traversalOptRow, clause optionalMatchClause) ([]traversalOptRow, error) {
	return e.applyGeneralOptionalClause(ctx, rows, clause)
}

// isSimpleTraversalIdentifier reports whether s is a bare Cypher identifier
// (letters, digits, underscores; not starting with a digit). Used to gate the
// projection fast path so any richer expression falls back to the full
// evaluator.
func isSimpleTraversalIdentifier(s string) bool { return cyphertext.IsIdentifier(s) }

// fastTraversalExprValue resolves the two hot projection shapes — "var.prop"
// and a bare bound variable — without walking the full expression evaluator's
// dispatch chain. It replicates evaluateExpressionWithContext's semantics for
// exactly those shapes (nil bindings project as null, has_embedding reads
// EmbedMeta, non-entity values come from row.values) and reports ok=false for
// everything else so the caller falls back to the evaluator.
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
				return nodeHasEmbeddingValue(node), true
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
		return node, true
	}
	if rel, ok := row.rels[expr]; ok {
		if rel == nil {
			return nil, true
		}
		return rel, true
	}
	if value, ok := row.values[expr]; ok {
		return value, true
	}
	return nil, false
}
