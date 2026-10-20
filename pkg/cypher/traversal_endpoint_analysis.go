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
func (e *StorageExecutor) traversalUnusedPropertyVariablesForTail(match *TraversalMatch, whereClause string, readTail []pipelineClause) map[string]bool {
	if readTail == nil {
		return nil
	}
	expressions := make([]string, 0, len(readTail)+1)
	for _, clause := range readTail {
		expressions = append(expressions, clause.text)
	}
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
	for index := 0; index < len(expression); {
		c := expression[index]
		if c == '\'' || c == '"' {
			index = skipQuotedSemanticText(expression, index)
			continue
		}
		if c == '$' {
			_, next, ok := scanIdentifierToken(expression, index+1)
			if ok {
				index = next
				continue
			}
			index++
			continue
		}
		nameStart := index
		name, next, ok := scanSymbolicName(expression, index)
		if !ok {
			index++
			continue
		}
		// Mirror semanticExpressionReferences: a name right after '.' is a
		// property key, and a name followed by '(' is a function name — neither
		// is a variable reference.
		before := nameStart
		for before > 0 && isASCIIWhitespace(expression[before-1]) {
			before--
		}
		cursor := next
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		index = next
		isPropertyKey := before > 0 && expression[before-1] == '.' && (before < 2 || expression[before-2] != '.')
		isCallName := cursor < len(expression) && expression[cursor] == '('
		if isPropertyKey || isCallName {
			continue
		}
		if normalizeProjectionColumnName(name) != want {
			continue
		}
		if cursor < len(expression) && expression[cursor] == '.' {
			return true // v.x property access
		}
		if !traversalBareVariableInSafeCall(expression, nameStart) {
			return true // bare v in value position, or inside a property-reading call
		}
	}
	return false
}

// traversalBareVariableInSafeCall reports whether the bare variable occurrence
// starting at nameStart is the direct argument of a property-agnostic function:
// fn(v) or fn(DISTINCT v).
func traversalBareVariableInSafeCall(expression string, nameStart int) bool {
	pos := nameStart
	for pos > 0 && isASCIIWhitespace(expression[pos-1]) {
		pos--
	}
	if pos > 0 && expression[pos-1] == '(' {
		return traversalCallNameIsSafe(expression, pos-1)
	}
	if pos >= len("DISTINCT") {
		distinct := pos - len("DISTINCT")
		if strings.EqualFold(expression[distinct:pos], "DISTINCT") {
			p := distinct
			for p > 0 && isASCIIWhitespace(expression[p-1]) {
				p--
			}
			if p > 0 && expression[p-1] == '(' {
				return traversalCallNameIsSafe(expression, p-1)
			}
		}
	}
	return false
}

// traversalCallNameIsSafe reports whether the function whose opening paren is
// at openParen is property-agnostic for a node argument.
func traversalCallNameIsSafe(expression string, openParen int) bool {
	end := openParen
	for end > 0 && isASCIIWhitespace(expression[end-1]) {
		end--
	}
	start := end
	for start > 0 {
		b := expression[start-1]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' {
			start--
			continue
		}
		break
	}
	if start == end {
		return false
	}
	return traversalSafeArgumentFunctions[strings.ToLower(expression[start:end])]
}
