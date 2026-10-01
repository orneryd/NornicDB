package cypher

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeCreateTypedPropertyIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	kind := storage.IndexKindText
	if startsWithKeywords(cypher, "CREATE", "POINT INDEX") {
		kind = storage.IndexKindPoint
	}
	parsed, err := e.parseCreateIndexDDL(cypher, "CREATE "+string(kind)+" INDEX")
	if err != nil {
		return nil, err
	}
	if len(parsed.properties) != 1 {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax("CREATE "+string(kind)+" INDEX"), nil)
	}
	entityType := storage.ConstraintEntityNode
	label := parsed.label
	if parsed.isRelationship {
		entityType, label = storage.ConstraintEntityRelationship, parsed.relationshipType
	}
	name := parsed.indexName
	if name == "" {
		name = fmt.Sprintf("index_%s_%s_%s", lowerASCII(label), lowerASCII(parsed.properties[0]), lowerASCII(string(kind)))
	}
	if err := e.storage.GetSchema().AddTypedIndexForEntity(kind, name, label, parsed.properties, entityType); err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
