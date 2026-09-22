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

func formatElementID(kind, database, id string) string {
	id = strings.TrimSpace(id)
	if parts := strings.SplitN(id, ":", 3); len(parts) == 3 && parts[0] == kind {
		return id
	}
	database = strings.TrimSpace(database)
	if database == "" {
		database = "nornic"
	}
	return fmt.Sprintf("%s:%s:%s", kind, database, id)
}
