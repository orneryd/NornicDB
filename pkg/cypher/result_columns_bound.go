package cypher

import (
	"slices"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type statementBindingID uint32

type statementBinding struct {
	id         statementBindingID
	name       string
	start, end int
}
type statementBindingUse struct {
	binding                statementBindingID
	name                   string
	start, end             int
	clauseStart, clauseEnd int
}

// boundStatementProgram is immutable once published by the statement cache.
// Consumers use stable identities and original byte spans, never row positions.
type boundStatementProgram struct {
	syntax      cypherantlr.StatementSyntax
	bindings    []statementBinding
	uses        []statementBindingUse
	projections map[int]*returnProjectionPlan
	effects     map[int]pipelineClauseEffects
	scalars     map[int]scalarExpressionSyntax
}

func cloneBindingIDs(input map[string]statementBindingID) map[string]statementBindingID {
	output := make(map[string]statementBindingID, len(input))
	for name, id := range input {
		output[name] = id
	}
	return output
}
func (compilation *boundCompilation) define(name string, start, end int) statementBindingID {
	id := statementBindingID(len(compilation.product.bindings) + 1)
	compilation.product.bindings = append(compilation.product.bindings, statementBinding{id: id, name: name, start: start, end: end})
	return id
}
func (compilation *boundCompilation) defineClause(clause *boundClause) {
	clause.beforeIDs = cloneBindingIDs(clause.branch.ids)
	clause.afterIDs = cloneBindingIDs(clause.beforeIDs)
	var names []string
	for name := range clause.after {
		names = append(names, name)
	}
	slices.Sort(names)
	projected := map[string]returnProjection{}
	if clause.plan != nil {
		for _, item := range clause.plan.projections {
			projected[item.alias] = item
		}
	}
	for _, name := range names {
		if item, found := projected[name]; found && (!item.direct || item.variable != item.alias) {
			clause.afterIDs[name] = compilation.define(name, item.definitionStart, item.definitionEnd)
			continue
		}
		if clause.source.kind == pipelineClauseUnwind && name == syntaxSymbolName(clause.source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)) {
			symbol := clause.source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)
			start, end := symbol.Span()
			clause.afterIDs[name] = compilation.define(name, start, end)
			continue
		}
		if _, exists := clause.afterIDs[name]; !exists {
			start, end := syntaxDeclarationSpan(clause.source.syntax, name)
			clause.afterIDs[name] = compilation.define(name, start, end)
		}
	}
	for name := range clause.afterIDs {
		if _, visible := clause.after[name]; !visible {
			delete(clause.afterIDs, name)
		}
	}
}

func syntaxDeclarationSpan(clause cypherantlr.SyntaxNode, name string) (start, end int) {
	start, end = clause.Span()
	found := false
	clause.Walk(func(node cypherantlr.SyntaxNode) func() {
		if found {
			return nil
		}
		identity, symbol := node.Name()
		if !symbol || identity != name {
			return nil
		}
		parent, ok := node.Parent()
		if !ok {
			return nil
		}
		switch parent.Rule() {
		case cypherantlr.CypherParserRULE_nodePattern, cypherantlr.CypherParserRULE_relationDetail, cypherantlr.CypherParserRULE_patternPart, cypherantlr.CypherParserRULE_projectionItem, cypherantlr.CypherParserRULE_unwindSt, cypherantlr.CypherParserRULE_forSt, cypherantlr.CypherParserRULE_letItem, cypherantlr.CypherParserRULE_yieldItem:
			start, end = node.Span()
			found = true
		}
		return nil
	})
	return
}

// variableSyntaxRead classifies a decoded symbol by its grammatical role.
// Identifiers used as labels, map/property keys, function names and aliases
// never become expression dependencies.
func variableSyntaxRead(node cypherantlr.SyntaxNode) bool {
	if node.Rule() != cypherantlr.CypherParserRULE_symbol {
		return false
	}
	// normalize's second argument is a grammar keyword, not a variable read.
	if name, named := node.Name(); named {
		if name != "" {
			for ancestor, ok := node.Parent(); ok; ancestor, ok = ancestor.Parent() {
				if ancestor.Rule() == cypherantlr.CypherParserRULE_functionInvocation {
					if invocation := ancestor.ChildRule(cypherantlr.CypherParserRULE_invocationName); invocation.Valid() && strings.EqualFold(invocation.QualifiedName(), "normalize") {
						chain := ancestor.ChildRule(cypherantlr.CypherParserRULE_expressionChain)
						argument := 0
						for index := 0; index < chain.ChildCount(); index++ {
							child := chain.Child(index)
							if child.Rule() != cypherantlr.CypherParserRULE_expression {
								continue
							}
							start, end := child.Span()
							at, until := node.Span()
							if argument == 1 && start == at && end == until {
								return false
							}
							argument++
						}
					}
					break
				}
			}
		}
	}
	parent, ok := node.Parent()
	if !ok {
		return false
	}
	switch parent.Rule() {
	case cypherantlr.CypherParserRULE_atom, cypherantlr.CypherParserRULE_mapProjection, cypherantlr.CypherParserRULE_mapProjectionItem,
		cypherantlr.CypherParserRULE_setItem, cypherantlr.CypherParserRULE_removeItem,
		cypherantlr.CypherParserRULE_nodePattern, cypherantlr.CypherParserRULE_relationDetail:
		return true
	default:
		return false
	}
}

func (compilation *boundCompilation) bindSymbol(clause *boundClause, node cypherantlr.SyntaxNode) {
	if clause.transparent || !variableSyntaxRead(node) {
		return
	}
	name, _ := node.Name()
	start, end := node.Span()
	scope, identities, restricted := compilation.expressionBindings(clause, node)
	if _, found := scope[name]; !found {
		if _, previous := clause.before[name]; restricted && previous {
			compilation.fail(resultmetadata.ReasonUndefinedVariable, start, localizedStatusError(
				"Neo.ClientError.Statement.SyntaxError", "UndefinedVariable", localization.CypherMatchingAggregationScopeVariable(name)))
			return
		}
		if !strings.HasPrefix(node.Text(), "`") {
			semantic := make(matchSemanticScope, len(scope))
			for variable := range scope {
				semantic[variable] = matchBindingUnknown
			}
			if undefinedExpressionVariable(semantic, compilation.view.code(node)) == nil {
				return
			}
		}
		compilation.failUndefined(start, name)
		return
	}
	// The canonical expression checker remains the semantic error owner. The
	// CST role has already distinguished a quoted literal-word variable.
	semantic := make(matchSemanticScope, len(scope))
	for variable := range scope {
		semantic[variable] = matchBindingUnknown
	}
	if err := undefinedExpressionVariable(semantic, compilation.view.code(node)); err != nil {
		compilation.failUndefined(start, name)
		return
	}
	id := identities[name]
	if id != 0 {
		definition := compilation.product.bindings[int(id)-1]
		if definition.start == start && definition.end == end {
			return
		}
	}
	compilation.product.uses = append(compilation.product.uses, statementBindingUse{binding: id, name: name, start: start, end: end, clauseStart: clause.source.start, clauseEnd: clause.source.end})
}

func (compilation *boundCompilation) localBindingIdentity(name string, declaration int) statementBindingID {
	key := localIdentityKey{name: name, start: declaration}
	if id := compilation.locals[key]; id != 0 {
		return id
	}
	end := declaration
	for index := 0; index < compilation.product.syntax.TokenCount(); index++ {
		from, to := compilation.product.syntax.Token(index).Span()
		if from == declaration {
			end = to
			break
		}
	}
	id := compilation.define(name, declaration, end)
	compilation.locals[key] = id
	return id
}

type localIdentityKey struct {
	name  string
	start int
}
