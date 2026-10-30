package cypher

import (
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// missingRelationshipEndpoint reports whether variable, a node of a
// relationship pattern that CREATE or MERGE writes, is bound to null in
// bindings, as after an OPTIONAL MATCH that found no node. Neo4j 5.26 fails
// the statement then (relationshipEndpointMissingError) instead of creating
// a new node in the variable's place (#907). An unbound variable is a node
// the pattern creates.
func missingRelationshipEndpoint(bindings map[string]interface{}, variable string) bool {
	if variable == "" {
		return false
	}
	value, bound := bindings[variable]
	if !bound {
		return false
	}
	switch typed := value.(type) {
	case nil:
		return true
	case *storage.Node:
		return typed == nil
	}
	return false
}

// relationshipEndpointMissingError is Neo4j's ArgumentError for a CREATE or
// MERGE relationship whose endpoint variable is null.
func relationshipEndpointMissingError(variable string) error {
	return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "RelationshipEndpointMissing",
		localization.CypherMergeRelationshipEndpointMissing(variable))
}
