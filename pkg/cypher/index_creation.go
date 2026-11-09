package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// executeCreateIndex owns RANGE and VECTOR index creation: parsing the target,
// binding the OPTIONS payload the statement declared, admitting the schema
// change and applying the kind-specific representation. Explicit RANGE syntax
// uses its original input instead of rewriting the query.
func (e *StorageExecutor) executeCreateIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	prefix := "CREATE INDEX"
	query := strings.TrimSpace(cypher)
	vector := false
	if _, ok := keywordSpanAt(query, 0, "CREATE VECTOR INDEX"); ok {
		prefix, vector = "CREATE VECTOR INDEX", true
	} else if _, ok := keywordSpanAt(query, 0, "CREATE RANGE INDEX"); ok {
		prefix = "CREATE RANGE INDEX"
	}
	parsed, err := e.parseCreateIndexDDL(ctx, cypher, prefix)
	if err != nil {
		if !vector {
			// This is the supported Neo4j ON :Label(properties) grammar,
			// retaining its property-index path and existing error contract.
			if legacy, legacyErr := e.parseCreateIndexLegacyDDL(ctx, cypher, prefix); legacyErr == nil {
				// The legacy head admits the same OPTIONS payload, so the
				// options it declares bind before the index exists.
				if _, err := e.schemaOptionMap(ctx, legacy.options, "range index"); err != nil {
					return nil, err
				}
				name := legacy.indexName
				if name == "" {
					name = fmt.Sprintf("index_%s_%s", legacy.label, strings.Join(legacy.properties, "_"))
				}
				if err := e.addPropertyIndex(ctx, name, legacy.label, legacy.properties); err != nil {
					return nil, err
				}
				return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
			}
			prefix = "CREATE INDEX"
		}
		return nil, localizedError(localization.CypherSchemaInvalidSyntax(prefix), nil)
	}
	if vector && (len(parsed.properties) != 1 || parsed.indexName == "" ||
		(!parsed.isRelationship && parsed.label == "") ||
		(parsed.isRelationship && parsed.relationshipType == "")) {
		return nil, localizedError(localization.CypherSchemaInvalidSyntax(prefix), nil)
	}

	entityType := storage.ConstraintEntityNode
	label := parsed.label
	if parsed.isRelationship {
		entityType, label = storage.ConstraintEntityRelationship, parsed.relationshipType
	}
	name := parsed.indexName
	if name == "" {
		name = fmt.Sprintf("index_%s_%s", label, strings.Join(parsed.properties, "_"))
	}
	kind := "RANGE"
	subject := "range index"
	if vector {
		kind, subject = "VECTOR", "vector index"
	}
	options, err := e.schemaOptionMap(ctx, parsed.options, subject)
	if err != nil {
		return nil, err
	}
	dimensions, similarity := 0, ""
	if vector {
		dimensions, similarity, err = schemaVectorOptions(options, e.GetDefaultEmbeddingDimensions(), "cosine")
		if err != nil {
			return nil, err
		}
	}
	create, err := e.admitIndexCreation(ctx, cypher, name, kind, label, parsed.properties, entityType)
	if err != nil {
		return nil, err
	}
	if create {
		if vector {
			property := parsed.properties[0]
			// Persist operator intent even with vector search disabled. Only
			// node indexes register an in-memory vector space, after commit.
			publication := e.declareVectorIndexPublication(ctx, name, label, property, dimensions, parsed.isRelationship)
			if err := e.storage.GetSchema().AddVectorIndex(name, label, property, dimensions, similarity, entityType); err != nil {
				publication.abort()
				return nil, err
			}
			svc := e.search()
			register := !parsed.isRelationship && (svc == nil || svc.VectorEnabled())
			if err := e.afterCommit(ctx, func(ctx context.Context) error {
				if register {
					e.registerVectorSpace(name, label, property, dimensions, similarity)
				}
				return e.populateVectorIndexPublication(ctx, publication, e.search())
			}); err != nil {
				return nil, err
			}
		} else if parsed.isRelationship {
			if err := e.storage.GetSchema().AddRangeIndexForEntity(name, label, parsed.properties, entityType); err != nil {
				return nil, err
			}
		} else if err := e.addPropertyIndex(ctx, name, label, parsed.properties); err != nil {
			return nil, err
		}
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
