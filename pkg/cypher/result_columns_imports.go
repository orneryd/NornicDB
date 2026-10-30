package cypher

import (
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
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
