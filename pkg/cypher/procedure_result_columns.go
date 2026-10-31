package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30.

import "strings"

// legacyProcedureColumns declares the existing standalone result headers of
// compatibility procedures whose Neo4j signature is void or has broader yields.
// Callers receive owned slices; observations never extend these declarations.
func legacyProcedureColumns(name string) []string {
	switch name {
	case "db.index.fulltext.createNodeIndex":
		return []string{"name", "labels", "properties"}
	case "db.index.fulltext.createRelationshipIndex":
		return []string{"name", "relationshipTypes", "properties"}
	case "db.index.vector.createRelationshipIndex":
		return []string{"name", "relationshipType", "property", "dimension", "similarityFunction"}
	case "db.index.fulltext.drop", "db.index.vector.drop":
		return []string{"name", "dropped"}
	case "db.stats.retrieveAllAnTheStats":
		// The same sections as db.stats.retrieve.
		return columnNames(queryStatisticsColumns("retrieve"))
	case "apoc.export.csv.all":
		return []string{"file", "nodes", "relationships", "rows"}
	case "apoc.import.json":
		return []string{"source", "nodes", "relationships"}
	case "apoc.export.csv.query", "apoc.export.json.query":
		return []string{"file", "rows", "data"}
	case "apoc.export.json.all":
		return []string{"file", "nodes", "relationships", "properties", "data"}
	default:
		return nil
	}
}

// columnNames is the ordered names of declared columns: the header a
// handler emits, and the names static header and YIELD readers see; nil
// when there are none. Callers receive an owned slice.
func columnNames(columns []ProcedureColumn) []string {
	if len(columns) == 0 {
		return nil
	}
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	return names
}

// describeColumns is a registration's description of declared columns: the
// same header, with one description per column.
func describeColumns(columns []ProcedureColumn, descriptions ...string) []ProcedureColumn {
	described := append([]ProcedureColumn(nil), columns...)
	for i := range described {
		described[i].Description = descriptions[i]
	}
	return described
}

// queryStatisticsColumns is the declared header of each db.stats action.
func queryStatisticsColumns(action string) []ProcedureColumn {
	switch action {
	case "status":
		return queryStatisticsStatusColumns
	case "retrieve":
		return queryStatisticsRetrieveColumns
	default:
		return queryStatisticsOutcomeColumns
	}
}

// schemaTypePropertiesColumns is the declared header of
// db.schema.nodeTypeProperties or db.schema.relTypeProperties.
func schemaTypePropertiesColumns(nodes bool) []ProcedureColumn {
	if nodes {
		return dbSchemaNodeTypePropertiesColumns
	}
	return dbSchemaRelTypePropertiesColumns
}

// procedureReturnSignature appends the YIELD list of returns to a procedure
// signature head, so a registration that describes its returns declares
// them once.
func procedureReturnSignature(head string, returns []ProcedureColumn) string {
	var signature strings.Builder
	signature.WriteString(head)
	signature.WriteString(" :: (")
	for i, column := range returns {
		if i > 0 {
			signature.WriteString(", ")
		}
		signature.WriteString(column.Name)
		signature.WriteString(" :: ")
		signature.WriteString(column.Type)
	}
	signature.WriteString(")")
	return signature.String()
}
