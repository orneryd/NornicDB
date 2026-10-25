package cypher

import (
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

func (clause *boundClause) combinedBindings() (map[string]struct{}, map[string]statementBindingID) {
	names, ids := copyBindingNames(clause.before), cloneBindingIDs(clause.beforeIDs)
	for name := range clause.after {
		names[name] = struct{}{}
		ids[name] = clause.afterIDs[name]
	}
	return names, ids
}

// Projection predicates and ordering can read incoming expressions as well as
// output aliases. DISTINCT/aggregation restrict those reads to expressions the
// projection actually carries. This is a view of retained grammar nodes, not a
// second expression parser or a scope walk over rewritten text.
func (clause *boundClause) projectionTailBindings(read cypherantlr.SyntaxNode) (map[string]struct{}, map[string]statementBindingID, bool) {
	restricted := clause.plan != nil && (clause.plan.distinct || clause.plan.hasAggregate)
	// Every retained operand handle, including a compound expression's root,
	// sees the same current constant identities as its admitted symbol reads.
	// The transition owner decides this membership; the entry-name list may
	// still contain a weak input shadowed and dropped by an earlier clause.
	ordering := (projectionSemantics{clause: clause.source.syntax}).readPhase(read) == projectionOrdered
	constants := clause.branch.bindings.projectionConstants(ordering)
	projected, projectedIDs := clause.after, clause.afterIDs
	if restricted && len(constants) > 0 {
		projected, projectedIDs = copyBindingNames(projected), cloneBindingIDs(projectedIDs)
		for name := range constants {
			if _, current := clause.before[name]; !current {
				continue
			}
			if _, output := projected[name]; !output {
				projected[name], projectedIDs[name] = struct{}{}, clause.beforeIDs[name]
			}
		}
	}
	if !restricted {
		names, ids := clause.combinedBindings()
		return names, ids, false
	}
	if name, symbol := read.Name(); symbol {
		if _, output := clause.after[name]; output {
			return projected, projectedIDs, true
		}
	}
	for candidate := read; candidate.Valid() && candidate.ID() != clause.source.syntax.ID(); {
		for _, expression := range clause.projectionSyntax {
			if sameProjectionSyntax(candidate, expression) {
				names, ids := clause.combinedBindings()
				return names, ids, true
			}
		}
		parent, exists := candidate.Parent()
		if !exists {
			break
		}
		candidate = parent
	}
	return projected, projectedIDs, true
}

func projectionSyntaxCore(node cypherantlr.SyntaxNode) cypherantlr.SyntaxNode {
	return node.SemanticExpression()
}

// Compare grammatical expression structure. Symbol identities are already
// decoded by the lexical product; comments and whitespace are not CST edges.
// Built-in function spelling follows the existing function catalog, while
// variable identities and literal strings remain case-sensitive.
func sameProjectionSyntax(left, right cypherantlr.SyntaxNode) bool {
	left, right = projectionSyntaxCore(left), projectionSyntaxCore(right)
	if !left.Valid() || !right.Valid() || left.Rule() != right.Rule() {
		return false
	}
	if name, symbol := left.Name(); symbol {
		other, isSymbol := right.Name()
		return isSymbol && name == other
	}
	if left.Rule() == cypherantlr.CypherParserRULE_invocationName {
		name, other := left.QualifiedName(), right.QualifiedName()
		_, builtin := builtInCypherFunctions[lowerASCII(name)]
		return name == other || builtin && strings.EqualFold(name, other)
	}
	if left.Rule() < 0 {
		written, other := left.Text(), right.Text()
		if strings.HasPrefix(written, "'") || strings.HasPrefix(written, "\"") {
			return written == other
		}
		return strings.EqualFold(written, other)
	}
	if left.ChildCount() != right.ChildCount() {
		return false
	}
	for index := 0; index < left.ChildCount(); index++ {
		if !sameProjectionSyntax(left.Child(index), right.Child(index)) {
			return false
		}
	}
	return true
}
