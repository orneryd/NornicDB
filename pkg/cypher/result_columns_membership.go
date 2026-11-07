package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// promoteInput records an actual read of an imported CALL binding in this
// query part. Retaining a name makes it readable; reading it makes it a member
// of the current part until the next projection drops it.
func (compilation *boundCompilation) promoteInput(clause *boundClause, name string, id statementBindingID) {
	if (clause.branch.body.role != boundScopedCallBody && clause.branch.body.role != boundImplicitCallBody) || id == 0 || clause.branch.retainedIDs[name] != id {
		return
	}
	if _, current := clause.before[name]; current {
		return
	}
	clause.before[name] = struct{}{}
	if _, visible := clause.after[name]; !visible {
		clause.after[name] = struct{}{}
	}

	clause.branch.ids[name] = id
	if clause.beforeIDs != nil {
		clause.beforeIDs[name] = id
	}
	if clause.afterIDs != nil && clause.afterIDs[name] == 0 {
		clause.afterIDs[name] = id
	}
	clause.step.Before = projectionScope(projectedMapNames(clause.before))
}

// promoteClauseInputs reads the clause's existing CST before expanding its
// wildcard. Child query bodies and projection tails have independent scope
// transfers and cannot introduce members into this query part.
func (compilation *boundCompilation) promoteClauseInputs(ctx context.Context, clause *boundClause) {
	if (clause.branch.body.role != boundScopedCallBody && clause.branch.body.role != boundImplicitCallBody) || len(clause.branch.retainedIDs) == 0 || (clause.source.kind != pipelineClauseWith && clause.source.kind != pipelineClauseReturn) {
		return
	}
	hidden := 0
	clause.source.syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if ctx.Err() != nil {
			return nil
		}
		skip := false
		if node.ID() != clause.source.syntax.ID() {
			switch node.Rule() {
			case cypherantlr.CypherParserRULE_subqueryBody, cypherantlr.CypherParserRULE_collectSubquery, cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist,
				cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
				skip = true
			case cypherantlr.CypherParserRULE_where:
				parent, ok := node.Parent()
				skip = ok && parent.ID() == clause.source.syntax.ID() && clause.source.kind == pipelineClauseWith
			}
		}
		if skip {
			hidden++
			return func() { hidden-- }
		}
		if hidden == 0 && variableSyntaxRead(node) {
			name, _ := node.Name()
			_, ids, _ := compilation.expressionBindings(clause, node)
			compilation.promoteInput(clause, name, ids[name])
		}
		return nil
	})
}

// usedInputs is the proven dependency set of a body/branch. Stable binding IDs
// distinguish an outer input from a same-spelled local in any descendant.
func (compilation *boundCompilation) usedInputs(body *boundBody, firstUse int) map[string]struct{} {
	names := map[string]struct{}{}
	for _, use := range compilation.product.uses[firstUse:] {
		if use.binding == 0 {
			continue
		}
		if body.incomingIDs[use.name] == use.binding {
			names[use.name] = struct{}{}
		}
	}
	return names
}

// importableBindings enumerates a CALL wildcard's caller inputs. Scoped CALL
// globals are importable, while scalar-only captures are not. Implicit inputs
// remain ordinary candidates until a local declaration replaces their identity.
// Legacy WITH * uses only current members outside an implicit caller.
func (clause *boundClause) importableBindings() map[string]struct{} {
	if clause.branch.body.role == boundScopedCallBody {
		names, _ := clause.readableBindings(clause.before, clause.branch.ids)
		return names
	}
	if clause.branch.body.role != boundImplicitCallBody {
		return clause.before
	}
	names := copyBindingNames(clause.before)
	for name, id := range clause.branch.body.incomingIDs {
		if _, weak := clause.branch.body.weakInputs[id]; !weak {
			continue
		}
		if current, exists := clause.branch.ids[name]; !exists || current == id {
			names[name] = struct{}{}
		}
	}
	return names
}
