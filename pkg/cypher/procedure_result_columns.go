package cypher

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
		return []string{"section", "data"}
	case "apoc.export.csv.all":
		return []string{"file", "nodes", "relationships", "rows"}
	case "apoc.export.csv.query", "apoc.export.json.query":
		return []string{"file", "rows", "data"}
	case "apoc.export.json.all":
		return []string{"file", "nodes", "relationships", "properties", "data"}
	case "apoc.import.json":
		return []string{"file", "nodes", "relationships", "rows"}
	default:
		return nil
	}
}
