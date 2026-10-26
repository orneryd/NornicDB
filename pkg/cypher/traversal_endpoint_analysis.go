package cypher

import "strings"

// traversalSafeArgumentFunctions are the functions whose node argument needs
// no property values: identity, labels and row existence. count(v), id(v),
// elementId(v) and labels(v) are answered from ID + labels alone, so a
// labels-only projected read can satisfy them (#1110).
var traversalSafeArgumentFunctions = map[string]bool{
	"count":     true,
	"id":        true,
	"elementid": true,
	"labels":    true,
}

// traversalUnusedPropertyVariables computes, for every named node variable of
// match, whether no later clause (the RETURN items or the WHERE predicate)
// reads its properties. The map omits variables whose properties are read, so
// a missing entry means "read"; a nil map means "not analysed", which every
// consumer treats as read.
func (e *StorageExecutor) traversalUnusedPropertyVariables(match *TraversalMatch, returnItems []returnItem, whereClause string) map[string]bool {
	expressions := make([]string, 0, len(returnItems)+1)
	for _, item := range returnItems {
		expressions = append(expressions, item.expr)
	}
	if whereClause != "" {
		expressions = append(expressions, whereClause)
	}
	return e.traversalUnusedPropertyVariablesForExpressions(match, expressions)
}

// traversalUnusedPropertyVariablesForTail is the pipeline form of the same
// analysis: the MATCH clause materialises every binding as a bare variable,
// so the real property reads live in the read-only clauses after it
// (readTail) and the MATCH's own pushed-down WHERE. A nil readTail means the
// tail is not read-only, so every variable is conservatively treated as read.
func (e *StorageExecutor) traversalUnusedPropertyVariablesForTail(match *TraversalMatch, whereClause string, readTail []string) map[string]bool {
	if readTail == nil {
		return nil
	}
	expressions := make([]string, 0, len(readTail)+1)
	expressions = append(expressions, readTail...)
	if whereClause != "" {
		expressions = append(expressions, whereClause)
	}
	return e.traversalUnusedPropertyVariablesForExpressions(match, expressions)
}

// traversalUnusedPropertyVariablesForExpressions returns, for every named node
// variable of match, whether none of expressions reads its properties.
func (e *StorageExecutor) traversalUnusedPropertyVariablesForExpressions(match *TraversalMatch, expressions []string) map[string]bool {
	names := make(map[string]struct{}, 4)
	add := func(v string) {
		if v != "" {
			names[v] = struct{}{}
		}
	}
	add(match.StartNode.variable)
	add(match.EndNode.variable)
	add(match.Relationship.Variable)
	for _, seg := range match.Segments {
		add(seg.FromNode.variable)
		add(seg.ToNode.variable)
	}
	if len(names) == 0 {
		return nil
	}

	unused := make(map[string]bool, len(names))
	for name := range names {
		read := false
		for _, expression := range expressions {
			if traversalVariablePropertiesRead(expression, name) {
				read = true
				break
			}
		}
		if !read {
			unused[name] = true
		}
	}
	if len(unused) == 0 {
		return nil
	}
	return unused
}

// traversalVariablePropertiesRead reports whether expression reads the
// properties of the named variable: a property access v.x anywhere, or a bare
// v outside a property-agnostic function (count / id / elementId / labels).
// Conservative: any reference it cannot prove safe reports true.
func traversalVariablePropertiesRead(expression, variable string) bool {
	if variable == "" || expression == "" {
		return false
	}
	want := normalizeProjectionColumnName(variable)
	facts := readExpressionLexicalFacts(expression)
	for _, token := range facts.tokens {
		if token.kind != lexicalName || token.role == expressionParameter || token.role == expressionProperty || token.role == expressionMapKey || token.role == expressionKeyword || token.call >= 0 {
			continue
		}
		if normalizeProjectionColumnName(token.name) != want {
			continue
		}
		if facts.byteAt(token.next) == '.' {
			return true
		}
		if !traversalBareVariableInSafeCall(expression, token.start) {
			return true
		}
	}
	return false
}

// Direct safe arguments use retained predecessor and call-opening context.
func traversalBareVariableInSafeCall(expression string, nameStart int) bool {
	facts := readExpressionLexicalFacts(expression)
	i, ok := facts.at[nameStart]
	if !ok {
		return false
	}
	previous := facts.tokens[i].previous
	if previous >= 0 && strings.EqualFold(facts.tokens[previous].name, "DISTINCT") {
		previous = facts.tokens[previous].previous
	}
	if facts.byteAt(previous) != '(' {
		return false
	}
	token := facts.tokens[previous]
	return token.call >= 0 && traversalSafeArgumentFunctions[facts.calls[token.call].normalized]
}
