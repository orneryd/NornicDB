package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// showConstraintKind is the kind a SHOW … CONSTRAINTS command lists: an
// entity (a node or relationship constraint, "" for both) and a category
// ("" for every constraint, NornicDB's own kinds included).
type showConstraintKind struct {
	entity   storage.ConstraintEntityType
	category showConstraintCategory
}

type showConstraintCategory string

const (
	showConstraintsAll        showConstraintCategory = ""
	showConstraintsUniqueness showConstraintCategory = "uniqueness"
	showConstraintsExistence  showConstraintCategory = "existence"
	showConstraintsKey        showConstraintCategory = "key"
	showConstraintsType       showConstraintCategory = "type"
)

// showConstraintKindOf reads a SHOW command's words before CONSTRAINT or
// CONSTRAINTS as the kind it lists, as Neo4j does: ALL, or an optional
// entity (NODE, RELATIONSHIP, REL) followed by UNIQUE / UNIQUENESS,
// [PROPERTY] EXIST / EXISTENCE, KEY or PROPERTY TYPE. A Cypher 25 statement
// also takes PROPERTY UNIQUE / UNIQUENESS (Neo4j 2026.09; 5.26 rejects it).
// isConstraints is false when cypher isn't a SHOW … CONSTRAINT[S] command;
// other words before CONSTRAINT[S] are Neo4j's SyntaxError.
func showConstraintKindOf(cypher string, cypher25 bool) (kind showConstraintKind, isConstraints bool, err error) {
	fields := strings.Fields(cypher)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SHOW") {
		return kind, false, nil
	}
	// The kind is at most three words (REL PROPERTY EXISTENCE).
	end := -1
	for index := 1; index < len(fields) && index <= 4; index++ {
		if strings.EqualFold(fields[index], "CONSTRAINT") || strings.EqualFold(fields[index], "CONSTRAINTS") {
			end = index
			break
		}
	}
	if end < 0 {
		return kind, false, nil
	}
	words := make([]string, 0, end-1)
	for _, word := range fields[1:end] {
		word = strings.ToUpper(word)
		if !showConstraintKindWords[word] {
			return kind, false, nil
		}
		words = append(words, word)
	}
	invalid := func() (showConstraintKind, bool, error) {
		return showConstraintKind{}, true, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInput(fields[1]))
	}
	if len(words) == 1 && words[0] == "ALL" {
		return kind, true, nil
	}
	if len(words) > 0 {
		switch words[0] {
		case "NODE":
			kind.entity, words = storage.ConstraintEntityNode, words[1:]
		case "RELATIONSHIP", "REL":
			kind.entity, words = storage.ConstraintEntityRelationship, words[1:]
		}
	}
	property := len(words) > 0 && words[0] == "PROPERTY"
	if property {
		words = words[1:]
	}
	if len(words) == 0 {
		if property || kind.entity != "" {
			return invalid()
		}
		return kind, true, nil
	}
	if len(words) != 1 {
		return invalid()
	}
	switch words[0] {
	case "UNIQUE", "UNIQUENESS":
		if property && !cypher25 {
			return invalid()
		}
		kind.category = showConstraintsUniqueness
	case "EXIST", "EXISTENCE":
		kind.category = showConstraintsExistence
	case "KEY":
		if property {
			return invalid()
		}
		kind.category = showConstraintsKey
	case "TYPE":
		if !property {
			return invalid()
		}
		kind.category = showConstraintsType
	default:
		return invalid()
	}
	return kind, true, nil
}

// showConstraintKindWords are the words a constraint kind is made of; a
// SHOW command with any other word before CONSTRAINT[S] isn't a SHOW …
// CONSTRAINTS command (SHOW PROCEDURES YIELD constraint).
var showConstraintKindWords = map[string]bool{
	"ALL": true, "NODE": true, "RELATIONSHIP": true, "REL": true, "PROPERTY": true, "UNIQUE": true,
	"UNIQUENESS": true, "EXIST": true, "EXISTENCE": true, "KEY": true, "TYPE": true,
}

// lists reports whether a SHOW … CONSTRAINTS of the kind lists a constraint
// of constraintType on entityType.
func (kind showConstraintKind) lists(constraintType storage.ConstraintType, entityType storage.ConstraintEntityType) bool {
	if kind.entity != "" && kind.entity != entityType {
		return false
	}
	switch kind.category {
	case showConstraintsAll:
		return true
	case showConstraintsUniqueness:
		return constraintType == storage.ConstraintUnique
	case showConstraintsExistence:
		return constraintType == storage.ConstraintExists
	case showConstraintsKey:
		return constraintType == storage.ConstraintNodeKey || constraintType == storage.ConstraintRelationshipKey
	default: // showConstraintsType
		return constraintType == storage.ConstraintPropertyType
	}
}
