package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// A relationship variable named twice in one MATCH (#907).
//
// Neo4j reads MATCH (a)-[r]->(b), (c)-[r]->(d) as one relationship bound
// to both places. Under the default match mode (DIFFERENT RELATIONSHIPS) no
// relationship appears twice in a MATCH, so the clause matches nothing (an
// OPTIONAL MATCH binds null); it is not an error. The statement rewrite
// (desugarLabelExpressions) gives every later place a variable of its own
// and adds r = <that variable> to the clause's WHERE, so every route reads
// the clause as Neo4j does: relationship uniqueness leaves it no rows.
// Places of different kinds (a relationship and a variable-length list) are
// Neo4j's type mismatch.

// relationshipPlace is a relationship element of a MATCH pattern: its
// variable's text query[start:end], and whether it binds a list
// (variable-length or quantified).
type relationshipPlace struct {
	start, end int
	list       bool
}

// repeatedRelationshipVariables renames the later places of each
// relationship variable the MATCH pattern query[start:end] names more than
// once and returns the predicates that tie them to the first. A variable
// named both as a relationship and as a list is Neo4j's type mismatch.
func (r *labelExpressionRewriter) repeatedRelationshipVariables(start, end int) ([]string, error) {
	q := r.query
	places := make(map[string][]relationshipPlace)
	var order []string
	for i := start; i < end; i++ {
		switch c := q[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(q, i, c) - 1
		case '{':
			if close := findMatchingDelimiter(q[:end], i, '{', '}'); close > i {
				i = close
			}
		case '[':
			close := findMatchingDelimiter(q[:end], i, '[', ']')
			if close < 0 {
				return nil, nil
			}
			name, nameEnd, ok := relationshipVariableAt(q[:close], i)
			if ok && relationshipBracketAt(q, start, i) {
				_, _, quantified := quantifierAfterArrow(q, close, end)
				inner := close
				if where := elementWhereIndex(q, i, close); where >= 0 {
					inner = where
				}
				if brace := indexOutsideQuotes(q[i+1:inner], '{'); brace >= 0 {
					inner = i + 1 + brace
				}
				list := quantified || indexOutsideQuotes(q[i+1:inner], '*') >= 0
				if _, seen := places[name]; !seen {
					order = append(order, name)
				}
				places[name] = append(places[name], relationshipPlace{start: nameEnd - len(name), end: nameEnd, list: list})
			}
			i = close
		}
	}
	var predicates []string
	for _, name := range order {
		named := places[name]
		if len(named) < 2 {
			continue
		}
		for _, place := range named[1:] {
			if place.list != named[0].list {
				types := map[bool]string{false: "Relationship", true: "List<Relationship>"}
				return nil, labelExpressionSyntaxError(localization.CypherMatchingVariableTypeConflict(name, types[named[0].list], types[place.list]))
			}
		}
		for _, place := range named[1:] {
			variable := r.variable()
			r.edit(place.start, place.end, variable)
			predicates = append(predicates, name+" = "+variable)
		}
	}
	return predicates, nil
}

// mayRepeatRelationshipVariable reports whether query may name a
// relationship variable twice: two relationship brackets that start with the
// same name. It never answers false for one.
func mayRepeatRelationshipVariable(query string) bool {
	var seen [16]string
	count := 0
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case '[':
			name, _, ok := relationshipVariableAt(query, i)
			if !ok || !relationshipBracketAt(query, 0, i) {
				continue
			}
			if count == len(seen) {
				return true
			}
			for _, earlier := range seen[:count] {
				if earlier == name {
					return true
				}
			}
			seen[count] = name
			count++
		}
	}
	return false
}

// relationshipBracketAt reports whether the [ at query[i] opens a
// relationship: a - comes right before it, spaces skipped.
func relationshipBracketAt(query string, start, i int) bool {
	j := trimRightIndex(query, start, i)
	return j > start && query[j-1] == '-'
}

// relationshipVariableAt reads the variable of the relationship bracket at
// query[open]; IS followed by a space starts a type expression ([IS T]).
func relationshipVariableAt(query string, open int) (string, int, bool) {
	name, end, ok := scanSymbolicName(query, skipASCIISpaces(query, open+1, len(query)))
	if ok && strings.EqualFold(name, "IS") && end < len(query) && isASCIISpace(query[end]) {
		return "", end, false
	}
	return name, end, ok
}
