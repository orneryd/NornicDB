package cypher

import (
	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

func parseCreateIndexForPattern(forClause string) (label string, relationshipType string, isRelationship bool, err error) {
	label, declaration, isRelationship, err := parseSchemaIndexForPattern(forClause)
	if err != nil || !isRelationship {
		return label, "", isRelationship, err
	}
	return "", declaration.staticTypeName(), true, nil
}
func parseSchemaIndexForPattern(forClause string) (label string, declaration relationshipDeclaration, isRelationship bool, err error) {
	forClause = strings.TrimSpace(forClause)
	if forClause == "" {
		return "", declaration, false, localizedError(localization.CypherSchemaEmptyClause("FOR"), nil)
	}
	if hop, ok := readRelationshipHop(forClause); ok {
		declaration = hop.declaration
		if !declaration.hasColon || declaration.staticTypeName() == "" {
			return "", declaration, false, localizedError(localization.CypherSchemaInvalidClause("relationship FOR"), nil)
		}
		return "", declaration, true, nil
	}
	inside, rest, ok := extractParenSection(forClause)
	if !ok || strings.TrimSpace(rest) != "" {
		return "", declaration, false, localizedError(localization.CypherSchemaInvalidClause("node FOR"), nil)
	}

	colon := strings.Index(inside, ":")
	if colon < 0 {
		return "", declaration, false, localizedError(localization.CypherSchemaInvalidClause("node FOR"), nil)
	}
	labelRaw := strings.TrimSpace(inside[colon+1:])
	label = normalizeIdentifierToken(labelRaw)
	if label == "" {
		return "", declaration, false, localizedError(localization.CypherSchemaInvalidClause("node FOR"), nil)
	}
	return label, declaration, false, nil
}

func parseCreateFulltextForPattern(forClause string) (label string, relationshipTypes []string, isRelationship bool, err error) {
	label, declaration, isRelationship, err := parseSchemaIndexForPattern(forClause)
	if err != nil {
		// Neo4j compatibility: allow non-parenthesized node pattern `n:Label`.
		if !strings.HasPrefix(strings.TrimSpace(forClause), "(") {
			parts := strings.SplitN(strings.TrimSpace(forClause), ":", 2)
			if len(parts) == 2 && (strings.TrimSpace(parts[0]) == "" || isPatternVariableName(strings.TrimSpace(parts[0]))) {
				compatLabel := normalizeIdentifierToken(strings.TrimSpace(parts[1]))
				if compatLabel != "" {
					return compatLabel, nil, false, nil
				}
			}
		}
		return "", nil, false, err
	}
	if !isRelationship {
		return label, nil, false, nil
	}
	relTypes, valid := declaration.fulltextTypes()
	if !valid {
		return "", nil, false, nerrors.ErrInvalidFulltextRelationshipTypes
	}
	return "", relTypes, true, nil
}

func parseRelationshipForDirection(forClause string) (relType string, direction string, ok bool) {
	hop, valid := readRelationshipHop(forClause)
	if !valid || !hop.declaration.hasColon {
		return "", "", false
	}
	relType = hop.declaration.staticTypeName()
	if relType == "" {
		return "", "", false
	}
	if hop.outgoing {
		return relType, "OUTGOING", true
	}
	if hop.incoming {
		return relType, "INCOMING", true
	}
	return "", "", false
}

func parsePolicyForClause(forClause string) (sourceLabel string, relType string, targetLabel string, ok bool) {
	hop, valid := readRelationshipHop(forClause)
	if !valid || !hop.outgoing || !hop.declaration.hasColon {
		return "", "", "", false
	}
	sourceLabel, ok = parseNodeLabelOnlyPattern(hop.from)
	if !ok {
		return "", "", "", false
	}
	relType = hop.declaration.staticTypeName()
	if relType == "" {
		return "", "", "", false
	}
	targetLabel, ok = parseNodeLabelOnlyPattern(hop.to)
	if !ok {
		return "", "", "", false
	}
	return sourceLabel, relType, targetLabel, true
}
