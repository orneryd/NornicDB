package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// createIndexHead reads the DDL head a CREATE INDEX statement declares: the
// prefix keyword that head uses and whether it creates a vector index. owned is
// false for a statement whose head belongs to another index kind's execute owner
// (FULLTEXT, TEXT, POINT, LOOKUP), which keeps its own syntax and error
// contract; the prefix answered here is then the property-index default the
// statement's own parse reports through.
func createIndexHead(cypher string) (prefix string, vector bool, owned bool) {
	query := strings.TrimSpace(cypher)
	if _, ok := keywordSpanAt(query, 0, "CREATE VECTOR INDEX"); ok {
		return "CREATE VECTOR INDEX", true, true
	}
	if _, ok := keywordSpanAt(query, 0, "CREATE RANGE INDEX"); ok {
		return "CREATE RANGE INDEX", false, true
	}
	if _, ok := keywordSpanAt(query, 0, "CREATE INDEX"); ok {
		return "CREATE INDEX", false, true
	}
	return "CREATE INDEX", false, false
}

// preparedCreateIndex is a CREATE INDEX statement's validated target, before
// the OPTIONS payload is bound or any schema change is admitted. legacy records that
// the statement used the supported ON :Label(properties) grammar.
type preparedCreateIndex struct {
	vector bool
	legacy bool
	parsed parsedCreateIndexDDL
}

// prepareCreateIndex validates a CREATE INDEX statement's native DDL syntax
// through the parsers its execute owner uses, and returns the target it names.
// It reads no parameter and publishes no schema change, so statement metadata
// (EXPLAIN, PROFILE, the public header view) validates the statement the execute
// owner would run - and reports the error it would report - without running it.
func (e *StorageExecutor) prepareCreateIndex(ctx context.Context, cypher string) (preparedCreateIndex, error) {
	prefix, vector, _ := createIndexHead(cypher)
	parsed, err := e.parseCreateIndexDDL(ctx, cypher, prefix)
	if err != nil {
		if !vector {
			// This is the supported Neo4j ON :Label(properties) grammar,
			// retaining its property-index path and existing error contract.
			if legacy, legacyErr := e.parseCreateIndexLegacyDDL(ctx, cypher, prefix); legacyErr == nil {
				return preparedCreateIndex{legacy: true, parsed: legacy}, nil
			}
			prefix = "CREATE INDEX"
		}
		return preparedCreateIndex{}, localizedError(localization.CypherSchemaInvalidSyntax(prefix), nil)
	}
	if vector && (len(parsed.properties) != 1 || parsed.indexName == "" ||
		(!parsed.isRelationship && parsed.label == "") ||
		(parsed.isRelationship && parsed.relationshipType == "")) {
		return preparedCreateIndex{}, localizedError(localization.CypherSchemaInvalidSyntax(prefix), nil)
	}
	return preparedCreateIndex{vector: vector, parsed: parsed}, nil
}

// executeCreateIndex owns RANGE and VECTOR index creation: parsing the target,
// binding the OPTIONS payload the statement declared, admitting the schema
// change and applying the kind-specific representation. Explicit RANGE syntax
// uses its original input instead of rewriting the query.
func (e *StorageExecutor) executeCreateIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	prepared, err := e.prepareCreateIndex(ctx, cypher)
	if err != nil {
		return nil, err
	}
	parsed, vector := prepared.parsed, prepared.vector
	if prepared.legacy {
		// The legacy head admits the same OPTIONS payload, so the options it
		// declares bind before the index exists.
		if _, err := e.schemaOptionMap(ctx, parsed.options, "range index"); err != nil {
			return nil, err
		}
		name := parsed.indexName
		if name == "" {
			name = fmt.Sprintf("index_%s_%s", parsed.label, strings.Join(parsed.properties, "_"))
		}
		if err := e.addPropertyIndex(ctx, name, parsed.label, parsed.properties); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
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
			// Build before catalog publication: a failed build cannot advertise a
			// usable declaration or remove one admitted concurrently. The existing
			// derived owner joins committed writes independently of catalog rows.
			if err := storage.PrepareDeclaredRelationshipPropertyEqualityIndexForEngine(ctx, e.storage, label, parsed.properties); err != nil {
				return nil, err
			}
			if err := e.storage.GetSchema().AddRangeIndexForEntity(name, label, parsed.properties, entityType); err != nil {
				return nil, err
			}
		} else if err := e.addPropertyIndex(ctx, name, label, parsed.properties); err != nil {
			return nil, err
		}
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
