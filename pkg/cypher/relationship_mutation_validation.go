package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type relationshipMutationPurpose uint8

const (
	createRelationshipMutation relationshipMutationPurpose = iota
	mergeRelationshipMutation
)

// validateRelationshipMutation owns the declaration boundary for CREATE and
// MERGE. CREATE additionally requires one explicit direction; MERGE permits
// its existing undirected shape. Dynamic token cardinality is checked per row.
func validateRelationshipMutation(view braceView, purpose relationshipMutationPurpose) error {
	pattern := view.text()
	clause, verb := "CREATE", "created"
	if purpose == mergeRelationshipMutation {
		clause, verb = "MERGE", "merged"
	}
	open, close := readRelationshipBracket(view, 0, requireClosedRelationship)
	if open < 0 || close < 0 {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "NoSingleRelationshipType", clause+" relationships require exactly one relationship type")
	}
	if purpose == createRelationshipMutation {
		left := strings.TrimSpace(pattern[:open])
		right := strings.TrimSpace(pattern[close+1:])
		if strings.HasSuffix(left, "<-") == strings.HasPrefix(right, "->") {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "RequiresDirectedRelationship", "CREATE relationships require exactly one direction")
		}
	}
	content := view.window(open+1, close)
	declaration := relationshipDeclarationOf(content)
	if declaration.hasLength {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"CreatingVarLength",
			"variable-length relationships cannot be "+verb,
		)
	}
	if declaration.hasColon && hasDynamicToken(declaration.typeText) {
		return nil
	}
	if declaration.hasColon && indexByteOutsideBackticks(declaration.typeText, '|') >= 0 {
		return singleRelationshipTypeError(clause)
	}
	if declaration.invalidType != "" {
		return &classifiedCypherError{
			cause:  localizedError(localization.CypherMutationsInvalidRelationshipType(declaration.invalidType), nil),
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "InvalidRelationshipType",
		}
	}
	if len(declaration.types) != 1 {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"NoSingleRelationshipType",
			clause+" relationships require exactly one relationship type",
		)
	}
	return nil
}
