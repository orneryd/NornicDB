package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Relationship quantifiers (#864).
//
// A quantifier after a relationship pattern (GQL, Neo4j 5.9+) repeats it:
// (a)-[:R]->{1,3}(b), (a)-->+(b), (a)<-[r]-*(b). Neo4j matches the same paths
// as the variable-length relationship -[:R*1..3]->: no relationship repeats,
// and a relationship variable is bound to the list of relationships. The
// statement rewrite (desugarLabelExpressions) writes the quantifier as that
// length, so every MATCH route sees a variable-length relationship. A type
// expression on a quantified relationship applies to each of its
// relationships (relationshipElement). As in Neo4j, a quantifier on a
// variable-length relationship and quantifiers in CREATE or MERGE are
// SyntaxErrors.

// relationshipQuantifier is a quantifier's bounds (max < 0: unbounded) and
// the end of its text.
type relationshipQuantifier struct {
	min, max int
	end      int
}

// length is the variable-length form of the quantifier: *min..max.
func (q relationshipQuantifier) length() string {
	text := "*" + strconv.Itoa(q.min) + ".."
	if q.max >= 0 {
		text += strconv.Itoa(q.max)
	}
	return text
}

// relationshipQuantifierAt reads a quantifier at query[at] (before end): +,
// *, {n}, {m,n}, {m,} or {,n}.
func relationshipQuantifierAt(query string, at, end int) (relationshipQuantifier, bool) {
	if at >= end {
		return relationshipQuantifier{}, false
	}
	switch query[at] {
	case '+':
		return relationshipQuantifier{min: 1, max: -1, end: at + 1}, true
	case '*':
		return relationshipQuantifier{min: 0, max: -1, end: at + 1}, true
	case '{':
	default:
		return relationshipQuantifier{}, false
	}
	close := strings.IndexByte(query[at:end], '}')
	if close < 0 {
		return relationshipQuantifier{}, false
	}
	close += at
	bound := func(text string, missing int) (int, bool) {
		if text = strings.TrimSpace(text); text == "" {
			return missing, true
		}
		value, err := strconv.Atoi(text)
		return value, err == nil && value >= 0
	}
	inner := query[at+1 : close]
	lower, upper, ranged := strings.Cut(inner, ",")
	if !ranged {
		n, ok := bound(lower, -1)
		if !ok || n < 0 {
			return relationshipQuantifier{}, false
		}
		return relationshipQuantifier{min: n, max: n, end: close + 1}, true
	}
	min, minOK := bound(lower, 0)
	max, maxOK := bound(upper, -1)
	if !minOK || !maxOK {
		return relationshipQuantifier{}, false
	}
	return relationshipQuantifier{min: min, max: max, end: close + 1}, true
}

// arrowRunEnd returns the end of the run of arrow characters (-, <, >) that
// starts at query[i].
func arrowRunEnd(query string, i, end int) int {
	for i < end && (query[i] == '-' || query[i] == '<' || query[i] == '>') {
		i++
	}
	return i
}

// quantifierAfterArrow returns the quantifier after the arrow that follows a
// relationship's bracket (query[close] is ']').
func quantifierAfterArrow(query string, close, end int) (relationshipQuantifier, int, bool) {
	at := skipASCIISpaces(query, arrowRunEnd(query, close+1, end), end)
	quantifier, ok := relationshipQuantifierAt(query, at, end)
	return quantifier, at, ok
}

// quantifiedRelationship rewrites the bracketed relationship
// query[open:close+1], followed by a quantifier at query[at:quantifier.end],
// as a variable-length relationship.
func (r *labelExpressionRewriter) quantifiedRelationship(open, close, at int, quantifier relationshipQuantifier, mode labelPatternMode) error {
	if mode != labelPatternMatch {
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathInWritePattern(mode.clause()))
	}
	q := r.query
	inner := q[open+1 : close]
	insert := close
	if where := topLevelKeywordIndex(inner, "WHERE"); where >= 0 {
		insert = open + 1 + where
	}
	if brace := indexOutsideQuotes(q[open+1:insert], '{'); brace >= 0 {
		insert = open + 1 + brace
	}
	if indexOutsideQuotes(q[open+1:insert], '*') >= 0 {
		return labelExpressionSyntaxError(localization.CypherMatchingVariableLengthInQuantifiedPath())
	}
	insert = trimRightIndex(q, open+1, insert)
	r.edit(insert, insert, quantifier.length())
	r.edit(at, quantifier.end, "")
	return nil
}

// quantifiedArrow rewrites an abbreviated relationship (--, -->, <--) at
// query[start:arrowEnd], followed by a quantifier at query[at:], as a
// bracketed variable-length relationship.
func (r *labelExpressionRewriter) quantifiedArrow(start, arrowEnd, at int, quantifier relationshipQuantifier, mode labelPatternMode) error {
	if mode != labelPatternMatch {
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathInWritePattern(mode.clause()))
	}
	arrow := r.query[start:arrowEnd]
	left, right := "-", "-"
	if strings.HasPrefix(arrow, "<") {
		left = "<-"
	}
	if strings.HasSuffix(arrow, ">") {
		right = "->"
	}
	r.edit(start, arrowEnd, left+"["+quantifier.length()+"]"+right)
	r.edit(at, quantifier.end, "")
	return nil
}

// arrowEndsAt reports whether a relationship arrow (->, ]- or --) ends right
// before query[i], spaces skipped.
func arrowEndsAt(query string, start, i int) bool {
	j := trimRightIndex(query, start, i)
	if j-2 < start {
		return false
	}
	return query[j-1] == '>' && query[j-2] == '-' || query[j-1] == '-' && (query[j-2] == '-' || query[j-2] == ']')
}

// blankQuotedText returns s with every quoted text (', " or `, quotes
// included) replaced by spaces, so a scan for syntax characters can't match
// inside a string or a backticked name (#879). Indexes are unchanged.
func blankQuotedText(s string) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'', '"', '`':
			if out == nil {
				out = []byte(s)
			}
			end := skipCypherQuotedText(s, i, c)
			for j := i; j < end && j < len(out); j++ {
				out[j] = ' '
			}
			i = end - 1
		}
	}
	if out == nil {
		return s
	}
	return string(out)
}

// indexOutsideQuotes is the index of c in s outside quoted text, or -1.
func indexOutsideQuotes(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"', '`':
			i = skipCypherQuotedText(s, i, s[i]) - 1
		case c:
			return i
		}
	}
	return -1
}

// mayUseRelationshipQuantifier reports whether query may hold a relationship
// quantifier: a {, + or * after an arrow character, outside quotes. It never
// answers false for one.
func mayUseRelationshipQuantifier(query string) bool {
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case '{', '+', '*':
			j := trimRightIndex(query, 0, i)
			if j > 0 && (query[j-1] == '-' || query[j-1] == '>') {
				return true
			}
		}
	}
	return false
}
