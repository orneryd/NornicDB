package cyphertext

import "strings"

// ExtractCorrelationWhere selects the first property/import equality for an
// indexed correlated lookup and retains the other conjuncts in source order.
// The execution route supplies its existing conjunct, equality and property
// readers, preserving its supported syntax and fallback decisions.
// For example, n.id = seed AND n.active returns n, id, n.active and true.
func ExtractCorrelationWhere(whereClause, importCol string, splitConjuncts func(string) []string, splitEquality func(string) (string, string, bool), parseProperty func(string) (string, string, bool), importIdentifier func(string) bool) (matchVar, matchProp, otherWhere string, ok bool) {
	terms := splitConjuncts(whereClause)
	if len(terms) == 0 {
		return "", "", "", false
	}
	correlationIdx := -1
	for i, term := range terms {
		lhs, rhs, isEq := splitEquality(term)
		if !isEq {
			continue
		}
		leftVar, leftProp, leftOK := parseProperty(lhs)
		rightVar, rightProp, rightOK := parseProperty(rhs)
		switch {
		case leftOK && importIdentifier(strings.TrimSpace(rhs)) && strings.EqualFold(strings.TrimSpace(rhs), importCol):
			matchVar, matchProp = leftVar, leftProp
			correlationIdx = i
		case rightOK && importIdentifier(strings.TrimSpace(lhs)) && strings.EqualFold(strings.TrimSpace(lhs), importCol):
			matchVar, matchProp = rightVar, rightProp
			correlationIdx = i
		}
		if correlationIdx >= 0 {
			break
		}
	}
	if correlationIdx < 0 || matchVar == "" || matchProp == "" {
		return "", "", "", false
	}
	remaining := make([]string, 0, len(terms)-1)
	for i, term := range terms {
		if i == correlationIdx {
			continue
		}
		t := strings.TrimSpace(term)
		if t != "" {
			remaining = append(remaining, t)
		}
	}
	return matchVar, matchProp, strings.Join(remaining, " AND "), true
}
