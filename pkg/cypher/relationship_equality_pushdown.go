package cypher

import (
	"context"
	"strconv"
	"strings"
)

// pushRelationshipEqualities copies each conjunct of whereClause that is
// r.key = <literal or parameter>, for the single-hop relationship variable r
// of a non-chained pattern, into the relationship's property filter, so the
// expansion skips a relationship that fails it before reading its far node
// ((a)-[r:T]-(b) WHERE r.kind = 'x' on a hub node; a quantified group's
// iteration runs as this shape). The property filter is the same Cypher
// equality, and the WHERE still runs on every path, so the paths are the
// same.
func (e *StorageExecutor) pushRelationshipEqualities(admission admittedRelationshipStatement, ctx context.Context, matches *TraversalMatch, whereClause string) {
	rel := &matches.Relationship
	if whereClause == "" || matches.IsChained || rel.Variable == "" || rel.VariableLength || rel.MinHops != 1 || rel.MaxHops != 1 ||
		rel.Variable == matches.StartNode.variable || rel.Variable == matches.EndNode.variable || rel.Variable == matches.PathVariable {
		return
	}
	var pushed map[string]interface{}
	for _, term := range clauseAnalysisFor(ctx, clauseAnalysisRelationshipEqualities, whereClause, parseRelationshipEqualityTerms) {
		if term.variable != rel.Variable {
			continue
		}
		if _, exists := rel.Properties[term.property]; exists {
			continue
		}
		if _, exists := pushed[term.property]; exists {
			continue
		}
		value, ok := e.indexSeekConstant(admission, ctx, term.constant)
		if !ok {
			continue
		}
		if pushed == nil {
			pushed = make(map[string]interface{}, len(rel.Properties)+1)
			for key, existing := range rel.Properties {
				pushed[key] = existing
			}
		}
		pushed[term.property] = value
	}
	if pushed != nil {
		rel.Properties = pushed
	}
}

// relationshipEqualityTerm is one variable.property = <literal or
// parameter> conjunct of a WHERE: the variable, the property and the
// constant's text.
type relationshipEqualityTerm struct {
	variable, property, constant string
}

// parseRelationshipEqualityTerms is the text part of
// pushRelationshipEqualities, read once per WHERE text through the
// statement plan (clauseAnalysisRelationshipEqualities): a quantified
// group's iteration runs the same WHERE for every row it expands. Each
// conjunct whose one side is exactly variable.property and whose other is a
// literal or parameter is a term.
func parseRelationshipEqualityTerms(whereClause string) []relationshipEqualityTerm {
	var terms []relationshipEqualityTerm
	for _, conjunct := range splitTopLevelAndConjuncts(whereClause) {
		clause := unwrapOuterParens(strings.TrimSpace(conjunct))
		eq := topLevelEqualsIndex(clause)
		if eq <= 0 {
			continue
		}
		for _, side := range []string{strings.TrimSpace(clause[:eq]), strings.TrimSpace(clause[eq+1:])} {
			dot := strings.IndexByte(side, '.')
			if dot <= 0 || !isPlainSymbolicName(side[:dot]) || !isPlainSymbolicName(side[dot+1:]) {
				continue
			}
			// parseVariableProperty folds case; a variable is case-sensitive,
			// so the side must be exactly variable.property.
			variable := side[:dot]
			shape := parseSimpleIndexedEqualityShape(variable, conjunct)
			if !shape.ok || shape.property != side[dot+1:] || !isLiteralOrParameter(shape.constant) {
				continue
			}
			terms = append(terms, relationshipEqualityTerm{variable: variable, property: shape.property, constant: shape.constant})
			break
		}
	}
	return terms
}

// isPlainSymbolicName reports whether name is an unquoted identifier.
func isPlainSymbolicName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c != '_' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// isLiteralOrParameter reports whether expr is one string, number or boolean
// literal or one parameter: a value that is the same for every relationship.
func isLiteralOrParameter(expr string) bool {
	expr = strings.TrimSpace(expr)
	switch {
	case expr == "":
		return false
	case expr[0] == '$':
		return isPlainSymbolicName(expr[1:])
	case expr[0] == '\'' || expr[0] == '"':
		return skipCypherQuotedText(expr, 0, expr[0]) == len(expr)
	case strings.EqualFold(expr, "true") || strings.EqualFold(expr, "false"):
		return true
	}
	if expr[0] == '-' {
		expr = expr[1:]
	}
	if _, err := strconv.ParseFloat(expr, 64); err != nil || expr == "" || !(expr[0] >= '0' && expr[0] <= '9') {
		return false
	}
	return true
}
