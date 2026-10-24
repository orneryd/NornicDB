package cypher

import (
	"slices"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

type syntaxVariableUse uint8

const (
	syntaxVariableUnused syntaxVariableUse = iota
	syntaxVariableRead
	syntaxVariableDefined
)

// syntaxBindingDefinition is the declaration role shared by binding identities
// and implicit CALL inputs. Procedure output spellings are declarations only
// at the selected output name, never at an aliased source-column spelling.
func syntaxBindingDefinition(node cypherantlr.SyntaxNode) bool {
	if projectionOutputSymbol(node) {
		return true
	}
	parent, ok := node.Parent()
	if !ok {
		return false
	}
	switch parent.Rule() {
	case cypherantlr.CypherParserRULE_nodePattern, cypherantlr.CypherParserRULE_relationDetail,
		cypherantlr.CypherParserRULE_patternPart,
		cypherantlr.CypherParserRULE_unwindSt, cypherantlr.CypherParserRULE_forSt,
		cypherantlr.CypherParserRULE_letItem:
		return true
	case cypherantlr.CypherParserRULE_yieldItem:
		last := cypherantlr.SyntaxNode{}
		for index := 0; index < parent.ChildCount(); index++ {
			if child := parent.Child(index); child.Rule() == cypherantlr.CypherParserRULE_symbol {
				last = child
			}
		}
		return last.Valid() && last.ID() == node.ID()
	}
	return false
}

// branchSyntaxVariableUses is the one first-use policy for NornicDB's implicit
// imports. Each UNION branch has its own input; another branch's declarations
// cannot suppress a read. It consumes decoded grammar roles and expression-local
// bindings from the same product as the bound program, without a text scanner.
func branchSyntaxVariableUses(branch queryBodyBranch, outer map[string]struct{}) map[string]syntaxVariableUse {
	uses := map[string]syntaxVariableUse{}
	for _, clause := range branch.clauses {
		observe := func(name string, node cypherantlr.SyntaxNode, definition bool) {
			if uses[name] != syntaxVariableUnused {
				return
			}
			if _, candidate := outer[name]; !candidate || syntaxLocalAt(node, clause.syntax, name, outer) {
				return
			}
			for parent, exists := node.Parent(); exists && parent.ID() != clause.syntax.ID(); parent, exists = parent.Parent() {
				if parent.Rule() == cypherantlr.CypherParserRULE_useClause {
					return
				}
			}
			use := syntaxVariableRead
			if definition {
				use = syntaxVariableDefined
				for parent, exists := node.Parent(); exists && parent.ID() != clause.syntax.ID(); parent, exists = parent.Parent() {
					switch parent.Rule() {
					case cypherantlr.CypherParserRULE_subqueryBody, cypherantlr.CypherParserRULE_patternComprehension,
						cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist,
						cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery:
						use = syntaxVariableRead
					}
				}
			}
			uses[name] = use
		}
		bindProjection := func(name string, definition cypherantlr.SyntaxNode) { observe(name, definition, true) }
		clause.syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
			// The canonical projection rule publishes all outputs only after
			// every item has read the incoming scope, including nested bodies.
			commit := projectionOutputsOnLeave(node, bindProjection)
			name, symbolic := node.Name()
			if !symbolic || projectionOutputSymbol(node) {
				return commit
			}
			if syntaxBindingDefinition(node) {
				observe(name, node, true)
			} else if variableSyntaxRead(node) {
				observe(name, node, false)
			} else if parent, exists := node.Parent(); exists && parent.Rule() == cypherantlr.CypherParserRULE_callSubquery {
				observe(name, node, false)
			}
			return commit
		})
	}
	return uses
}
func implicitBranchSyntaxImports(branch queryBodyBranch, outer map[string]struct{}) []string {
	var imports []string
	for name, use := range branchSyntaxVariableUses(branch, outer) {
		if use == syntaxVariableRead {
			imports = append(imports, name)
		}
	}
	slices.Sort(imports)
	return imports
}
