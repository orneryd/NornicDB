package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// traversalCallReadsHeaderOnly reports whether a call of name reads only
// identity, labels and existence from each argument value: a registered
// function whose input-image demand is HeaderInputImage (id, elementId, labels,
// type), or an aggregate whose argument the aggregate path reads as a row. The
// declaration, not the name, is the promise: a custom implementation that keeps
// a header scalar's name but declares another demand reads its arguments'
// values or any visible binding, so it is not header-only here either.
func traversalCallReadsHeaderOnly(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	return pipelineCallInputOf(name) == pipelineCallHeader
}

// traversalUnusedPropertyVariables computes, for every named node variable of
// match, whether no later clause (the RETURN items or the WHERE predicate)
// reads its properties. The map omits variables whose properties are read, so
// a missing entry means "read"; a nil map means "not analysed", which every
// consumer treats as read.
func (e *StorageExecutor) traversalUnusedPropertyVariables(ctx context.Context, match *TraversalMatch, returnItems []returnItem, whereClause string) map[string]bool {
	expressions := make([]string, 0, len(returnItems)+1)
	for _, item := range returnItems {
		expressions = append(expressions, item.expr)
	}
	if whereClause != "" {
		expressions = append(expressions, whereClause)
	}
	return e.traversalUnusedPropertyVariablesForExpressions(ctx, match, expressions)
}

// traversalUnusedPropertyVariablesForTail is the pipeline form of the same
// decision: the MATCH clause materialises every binding as a bare variable, so
// the real property reads live in the read-only clauses after it (readTail) and
// the MATCH's own pushed-down WHERE. A variable is unused when the demand the
// shared owner computes for it is complete and names no property, which is the
// same decision the endpoint reads that follow it are given. A nil readTail
// means the tail is not read-only, so every variable is conservatively treated
// as read, and so is every variable whose demand ctx's cancellation left
// incomplete.
func (e *StorageExecutor) traversalUnusedPropertyVariablesForTail(ctx context.Context, match *TraversalMatch, whereClause string, readTail []pipelineClause) map[string]bool {
	if readTail == nil {
		return nil
	}
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
		properties, complete := pipelineNodePropertyDemand(ctx, name, whereClause, nil, readTail)
		if complete && len(properties) == 0 {
			unused[name] = true
		}
	}
	if len(unused) == 0 {
		return nil
	}
	return unused
}

// traversalUnusedPropertyVariablesForExpressions returns, for every named node
// variable of match, whether none of expressions reads its properties. A
// statement canceled while the shared scan read expressions has no complete
// verdict, so no variable is reported unused.
func (e *StorageExecutor) traversalUnusedPropertyVariablesForExpressions(ctx context.Context, match *TraversalMatch, expressions []string) map[string]bool {
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

	if pipelineConsumersReadWholeVisibleScope(ctx, expressions) {
		// A full-scope callback reads the row, not only its arguments, so no
		// binding this MATCH materialises is header-only-safe while one of the
		// expressions holds such a call.
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
		if isCypherStringQuoteByte(c) {
			index = skipQuotedSemanticText(expression, index)
			continue
		}
		if c == '$' {
			next, ok := cyphertext.ScanIdentifier(expression, index+1)
			if ok {
				index = next.End
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
		// Mirror forEachSemanticExpressionReference: a name right after '.' is a
		// property key, and a name followed by '(' is a function name — neither
		// is a variable reference.
		before := nameStart
		for before > 0 && cyphertext.IsWhitespaceAt(expression, before-1) {
			before--
		}
		cursor := next
		for cursor < len(expression) && cyphertext.IsWhitespaceAt(expression, cursor) {
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
// starting at nameStart is the direct argument of a call that reads only its
// argument's header: fn(v) or fn(DISTINCT v).
func traversalBareVariableInSafeCall(expression string, nameStart int) bool {
	pos := nameStart
	for pos > 0 && cyphertext.IsWhitespaceAt(expression, pos-1) {
		pos--
	}
	if pos > 0 && expression[pos-1] == '(' {
		return traversalCallNameIsSafe(expression, pos-1)
	}
	if pos >= len("DISTINCT") {
		distinct := pos - len("DISTINCT")
		if strings.EqualFold(expression[distinct:pos], "DISTINCT") {
			p := distinct
			for p > 0 && cyphertext.IsWhitespaceAt(expression, p-1) {
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
// at openParen declares a header-only read of its argument.
func traversalCallNameIsSafe(expression string, openParen int) bool {
	end := openParen
	for end > 0 && cyphertext.IsWhitespaceAt(expression, end-1) {
		end--
	}
	start := end
	for start > 0 {
		if cyphertext.NamePartAt(expression, start-1) {
			start--
			continue
		}
		break
	}
	if start == end {
		return false
	}
	return traversalCallReadsHeaderOnly(expression[start:end])
}
