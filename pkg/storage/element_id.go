package storage

import (
	"fmt"
	"strings"
)

// NodeElementID returns the opaque Cypher element identifier for a node in a
// database. Already-formatted node element identifiers are preserved.
func NodeElementID(database string, id NodeID) string {
	return formatElementID("4", database, string(id))
}

// RelationshipElementID returns the opaque Cypher element identifier for a
// relationship in a database. Already-formatted relationship element
// identifiers are preserved.
func RelationshipElementID(database string, id EdgeID) string {
	return formatElementID("5", database, string(id))
}

// SourceNodeElementID qualifies a stored local ID whose source is known. Unlike
// NodeElementID's legacy convenience form, an ID resembling an element ID is
// still local data here and cannot replace the authoritative source database.
func SourceNodeElementID(database string, id NodeID) string {
	return "4:" + database + ":" + string(id)
}

// SourceRelationshipElementID qualifies a relationship's stored local ID.
func SourceRelationshipElementID(database string, id EdgeID) string {
	return "5:" + database + ":" + string(id)
}

// ParseElementID reads the database and local identity from a native element ID.
// Local IDs may contain colons; only the two structural separators are split.
func ParseElementID(value string) (kind, database, id string, ok bool) {
	kind, rest, found := strings.Cut(value, ":")
	if !found || kind != "4" && kind != "5" {
		return "", "", "", false
	}
	database, id, found = strings.Cut(rest, ":")
	return kind, database, id, found
}

func formatElementID(kind, database, id string) string {
	id = strings.TrimSpace(id)
	if parsedKind, _, _, ok := ParseElementID(id); ok && parsedKind == kind {
		return id
	}
	database = strings.TrimSpace(database)
	if database == "" {
		database = "nornic"
	}
	return fmt.Sprintf("%s:%s:%s", kind, database, id)
}
