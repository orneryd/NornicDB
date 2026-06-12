// Schema command parsing and execution for Cypher.
//
// This file implements Neo4j schema management commands:
//   - CREATE CONSTRAINT
//   - CREATE INDEX
//   - CREATE RANGE INDEX
//   - CREATE FULLTEXT INDEX
//   - CREATE VECTOR INDEX
package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// isCompositeRoot returns true if the storage engine is a composite database root.
// Schema DDL and introspection commands must be rejected on composite roots —
// callers must target a specific constituent via USE <composite>.<alias>.
func isCompositeRoot(engine storage.Engine) bool {
	type compositeChecker interface {
		IsComposite() bool
	}
	if cc, ok := engine.(compositeChecker); ok {
		return cc.IsComposite()
	}
	return false
}

// isCompositeAllowedCommand returns true for system/admin commands that are
// valid at composite root level without requiring a constituent target.
func isCompositeAllowedCommand(cypher string) bool {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	prefixes := []string{
		"SHOW DATABASE", "SHOW COMPOSITE", "SHOW CONSTITUENTS",
		"SHOW ALIASES", "SHOW LIMITS", "SHOW PROCEDURES", "SHOW FUNCTIONS",
		// Schema introspection/DDL commands pass through to their own handlers
		// which return more specific composite-root error messages.
		"SHOW INDEX", "SHOW FULLTEXT INDEX", "SHOW RANGE INDEX", "SHOW VECTOR INDEX",
		"SHOW CONSTRAINT",
		"CREATE INDEX", "CREATE RANGE INDEX", "CREATE FULLTEXT INDEX", "CREATE VECTOR INDEX",
		"CREATE CONSTRAINT",
		"DROP INDEX", "DROP CONSTRAINT",
		"CREATE DATABASE", "DROP DATABASE",
		"CREATE COMPOSITE", "DROP COMPOSITE", "ALTER COMPOSITE",
		"CREATE ALIAS", "DROP ALIAS",
		"BEGIN", "COMMIT", "ROLLBACK",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return false
}

// executeSchemaCommand handles CREATE CONSTRAINT and CREATE INDEX commands.
func (e *StorageExecutor) executeSchemaCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if isCompositeRoot(e.storage) {
		return nil, fmt.Errorf("Neo.ClientError.Statement.NotAllowed: " +
			"Schema DDL on composite databases requires a constituent target. " +
			"Use USE <composite>.<alias> to target a specific constituent")
	}
	if err := flushPendingAsyncWritesBeforeSchemaDDL(e.storage); err != nil {
		return nil, err
	}

	upper := strings.ToUpper(cypher)

	var result *ExecuteResult
	var err error

	// Order matters: check more specific patterns first
	if strings.Contains(upper, "CREATE CONSTRAINT") {
		result, err = e.executeCreateConstraint(ctx, cypher)
	} else if strings.Contains(upper, "DROP CONSTRAINT") {
		result, err = e.executeDropConstraint(ctx, cypher)
	} else if strings.Contains(upper, "CREATE FULLTEXT INDEX") {
		result, err = e.executeCreateFulltextIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE VECTOR INDEX") {
		result, err = e.executeCreateVectorIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE RANGE INDEX") {
		result, err = e.executeCreateRangeIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE INDEX") {
		result, err = e.executeCreateIndex(ctx, cypher)
	} else {
		return nil, fmt.Errorf("unknown schema command: %s", cypher)
	}

	// Invalidate query cache — cached SHOW INDEXES/CONSTRAINTS results are now stale.
	if err == nil && e.cache != nil {
		e.cache.Invalidate()
	}

	return result, err
}

func flushPendingAsyncWritesBeforeSchemaDDL(engine storage.Engine) error {
	visited := make(map[storage.Engine]bool)
	for engine != nil && !visited[engine] {
		visited[engine] = true

		if async, ok := engine.(interface {
			HasPendingWrites() bool
			Flush() error
		}); ok {
			if async.HasPendingWrites() {
				if err := async.Flush(); err != nil {
					return fmt.Errorf("flush pending async writes before schema DDL: %w", err)
				}
			}
		}

		switch wrapper := engine.(type) {
		case interface{ GetEngine() storage.Engine }:
			engine = wrapper.GetEngine()
		case interface{ GetInnerEngine() storage.Engine }:
			engine = wrapper.GetInnerEngine()
		default:
			engine = nil
		}
	}
	return nil
}

// executeCreateConstraint handles CREATE CONSTRAINT commands.
//
// Supported syntax (Neo4j 5.x):
//
//	CREATE CONSTRAINT constraint_name IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
//
// Supported syntax (Neo4j 4.x):
//
//	CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT n.property IS UNIQUE
func (e *StorageExecutor) executeCreateConstraint(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Detect IF NOT EXISTS to pass through to AddConstraint for duplicate-schema handling.
	ifNotExists := strings.Contains(strings.ToUpper(cypher), "IF NOT EXISTS")
	if result, handled, err := e.executeCreateConstraintContract(ctx, cypher, ifNotExists); handled {
		return result, err
	}

	if parsed, err := e.parseCreateConstraintNodeKeyDDL(cypher); err == nil {
		if len(parsed.properties) == 0 {
			return nil, fmt.Errorf("NODE KEY constraint requires properties")
		}
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_node_key", strings.ToLower(parsed.label), strings.ToLower(strings.Join(parsed.properties, "_")))
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      parsed.label,
			Properties: parsed.properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if parsed, err := e.parseCreateConstraintTemporalDDL(cypher); err == nil {
		if parsed.isRelationship {
			if len(parsed.properties) < 3 {
				return nil, fmt.Errorf("TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)")
			}
			constraintName := parsed.name
			if constraintName == "" {
				constraintName = fmt.Sprintf("constraint_%s_%s_temporal", strings.ToLower(parsed.label), strings.ToLower(strings.Join(parsed.properties, "_")))
			}
			constraint := storage.Constraint{
				Name:       constraintName,
				Type:       storage.ConstraintTemporal,
				EntityType: storage.ConstraintEntityRelationship,
				Label:      parsed.label,
				Properties: parsed.properties,
			}
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}

		if len(parsed.properties) != 3 {
			return nil, fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
		}
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_temporal", strings.ToLower(parsed.label), strings.ToLower(strings.Join(parsed.properties, "_")))
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			Label:      parsed.label,
			Properties: parsed.properties,
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if parsed, err := e.parseCreateConstraintDomainDDL(cypher); err == nil {
		allowedValues, err := parseDomainValueList(parsed.allowedRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid domain value list: %w", err)
		}
		if len(allowedValues) == 0 {
			return nil, fmt.Errorf("DOMAIN constraint requires at least one allowed value")
		}
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_domain", strings.ToLower(parsed.label), strings.ToLower(parsed.property))
		}
		constraint := storage.Constraint{
			Name:          constraintName,
			Type:          storage.ConstraintDomain,
			Label:         parsed.label,
			Properties:    []string{parsed.property},
			AllowedValues: allowedValues,
		}
		if parsed.isRelationship {
			constraint.EntityType = storage.ConstraintEntityRelationship
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if parsed, err := e.parseCreateConstraintSimplePropertyDDL(cypher); err == nil {
		if parsed.kind == "unique" {
			if parsed.isRelationship {
				constraintName := parsed.name
				if constraintName == "" {
					constraintName = fmt.Sprintf("constraint_%s_%s_unique", strings.ToLower(parsed.label), strings.ToLower(parsed.property))
				}
				constraint := storage.Constraint{
					Name:       constraintName,
					Type:       storage.ConstraintUnique,
					EntityType: storage.ConstraintEntityRelationship,
					Label:      parsed.label,
					Properties: []string{parsed.property},
				}
				if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
					return nil, err
				}
				if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
					return nil, err
				}
				return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
			}

			constraintName := parsed.name
			if constraintName == "" {
				constraintName = fmt.Sprintf("constraint_%s_%s", strings.ToLower(parsed.label), strings.ToLower(parsed.property))
			}
			constraint := storage.Constraint{
				Name:       constraintName,
				Type:       storage.ConstraintUnique,
				Label:      parsed.label,
				Properties: []string{parsed.property},
			}
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			schema := e.storage.GetSchema()
			if err := schema.AddUniqueConstraint(constraintName, parsed.label, parsed.property, ifNotExists); err != nil {
				return nil, err
			}
			if err := storage.RefreshUniqueConstraintValuesForEngine(e.storage, schema); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}

		if parsed.kind == "exists" {
			constraintName := parsed.name
			if constraintName == "" {
				suffix := "exists"
				if parsed.isRelationship {
					constraintName = fmt.Sprintf("constraint_%s_%s_%s", strings.ToLower(parsed.label), strings.ToLower(parsed.property), suffix)
				} else {
					constraintName = fmt.Sprintf("constraint_%s_%s_%s", strings.ToLower(parsed.label), strings.ToLower(parsed.property), suffix)
				}
			}
			constraint := storage.Constraint{
				Name:       constraintName,
				Type:       storage.ConstraintExists,
				Label:      parsed.label,
				Properties: []string{parsed.property},
			}
			if parsed.isRelationship {
				constraint.EntityType = storage.ConstraintEntityRelationship
			}
			if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
				return nil, err
			}
			if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
	}

	if parsed, err := e.parseCreateConstraintTypeDDL(cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_type", strings.ToLower(parsed.label), strings.ToLower(parsed.property))
		}
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        parsed.label,
			Property:     parsed.property,
			ExpectedType: parsed.expectedType,
		}
		opts := storage.PropertyTypeConstraintOptions{IfNotExists: ifNotExists}
		if parsed.isRelationship {
			ptc.EntityType = storage.ConstraintEntityRelationship
			opts.EntityType = storage.ConstraintEntityRelationship
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraintWithOptions(constraintName, parsed.label, parsed.property, parsed.expectedType, opts); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	} else if startsWithKeywordFold(strings.TrimSpace(cypher), "CREATE CONSTRAINT") &&
		(strings.Contains(strings.ToUpper(cypher), " IS ::") || strings.Contains(strings.ToUpper(cypher), " IS TYPED")) &&
		strings.Contains(strings.ToUpper(err.Error()), "UNSUPPORTED PROPERTY TYPE") {
		return nil, err
	}

	// Single-property UNIQUE/EXISTS branches are handled by parseCreateConstraintSimplePropertyDDL.

	// =========================================================================
	// Relationship constraint patterns
	// =========================================================================

	if parsed, err := e.parseCreateConstraintCardinalityDDL(cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			directionSuffix := "outgoing"
			if parsed.direction == "INCOMING" {
				directionSuffix = "incoming"
			}
			constraintName = fmt.Sprintf("constraint_%s_max_%s_%d", strings.ToLower(parsed.relType), directionSuffix, parsed.maxCount)
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintCardinality,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      parsed.relType,
			MaxCount:   parsed.maxCount,
			Direction:  parsed.direction,
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	} else if startsWithKeywordFold(strings.TrimSpace(cypher), "CREATE CONSTRAINT") &&
		strings.Contains(strings.ToUpper(cypher), "REQUIRE MAX COUNT") &&
		strings.Contains(err.Error(), "MAX COUNT must be a positive integer") {
		return nil, err
	}

	if parsed, err := e.parseCreateConstraintPolicyDDL(cypher); err == nil {
		constraintName := parsed.name
		if constraintName == "" {
			constraintName = fmt.Sprintf("constraint_%s_%s_%s_%s", strings.ToLower(parsed.sourceLabel), strings.ToLower(parsed.relType), strings.ToLower(parsed.targetLabel), strings.ToLower(parsed.policyMode))
		}
		constraint := storage.Constraint{
			Name:        constraintName,
			Type:        storage.ConstraintPolicy,
			EntityType:  storage.ConstraintEntityRelationship,
			Label:       parsed.relType,
			SourceLabel: parsed.sourceLabel,
			TargetLabel: parsed.targetLabel,
			PolicyMode:  parsed.policyMode,
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	} else if startsWithKeywordFold(strings.TrimSpace(cypher), "CREATE CONSTRAINT") &&
		(strings.Contains(strings.ToUpper(cypher), "REQUIRE ALLOWED") || strings.Contains(strings.ToUpper(cypher), "REQUIRE DISALLOWED")) {
		return nil, err
	}

	// Relationship temporal/domain branches are handled by parseCreateConstraintTemporalDDL and parseCreateConstraintDomainDDL.

	if parsed, err := e.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL(cypher); err == nil {
		if len(parsed.properties) == 0 {
			if parsed.kind == "rel_key" {
				return nil, fmt.Errorf("RELATIONSHIP KEY constraint requires properties")
			}
			return nil, fmt.Errorf("UNIQUE constraint requires properties")
		}

		constraint := storage.Constraint{
			EntityType: storage.ConstraintEntityRelationship,
			Label:      parsed.label,
			Properties: parsed.properties,
		}
		if parsed.kind == "rel_key" {
			constraint.Type = storage.ConstraintRelationshipKey
			constraint.Name = parsed.name
			if constraint.Name == "" {
				constraint.Name = fmt.Sprintf("constraint_%s_%s_rel_key", strings.ToLower(parsed.label), strings.ToLower(strings.Join(parsed.properties, "_")))
			}
		} else {
			constraint.Type = storage.ConstraintUnique
			constraint.Name = parsed.name
			if constraint.Name == "" {
				constraint.Name = fmt.Sprintf("constraint_%s_%s_unique", strings.ToLower(parsed.label), strings.ToLower(strings.Join(parsed.properties, "_")))
			}
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship single-property UNIQUE/EXISTS branches are handled by parseCreateConstraintSimplePropertyDDL.

	// Relationship property type branches are handled by parseCreateConstraintTypeDDL.

	return nil, fmt.Errorf("invalid CREATE CONSTRAINT syntax")
}

// executeDropIndex handles DROP INDEX commands.
//
// Supported syntax:
//
//	DROP INDEX index_name
//	DROP INDEX index_name IF EXISTS
func (e *StorageExecutor) executeDropIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if isCompositeRoot(e.storage) {
		return nil, fmt.Errorf("Neo.ClientError.Statement.NotAllowed: " +
			"Schema DDL on composite databases requires a constituent target. " +
			"Use USE <composite>.<alias> to target a specific constituent")
	}

	trimmed := strings.TrimSpace(cypher)
	upper := strings.ToUpper(trimmed)

	// Strip "DROP INDEX" prefix.
	rest := strings.TrimSpace(trimmed[len("DROP INDEX"):])
	if rest == "" {
		return nil, fmt.Errorf("invalid DROP INDEX syntax: index name required")
	}

	ifExists := false
	upperRest := strings.ToUpper(rest)
	// Check for trailing IF EXISTS.
	if idx := strings.Index(upperRest, "IF EXISTS"); idx >= 0 {
		ifExists = true
		rest = strings.TrimSpace(rest[:idx])
	}
	_ = upper // suppress unused

	// Extract index name (may be backtick-quoted).
	name := strings.TrimSpace(rest)
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		name = strings.ReplaceAll(name[1:len(name)-1], "``", "`")
	}
	if name == "" {
		return nil, fmt.Errorf("invalid DROP INDEX syntax: index name required")
	}

	// Look up the schema entry BEFORE dropping it so we can also tear down
	// any in-memory index data the schema entry was the only handle for.
	// Vector indexes carry their declared (label, property) — when the user
	// drops one we must also delete every "<nodeID>-prop-<property>" vector
	// from the search service, otherwise a recreate-from-scratch of the same
	// index inherits orphaned vectors that still match queries.
	var droppedVectorIndex *storage.VectorIndex
	if schema := e.storage.GetSchema(); schema != nil {
		if vi, ok := schema.GetVectorIndex(name); ok && vi != nil {
			copyVI := *vi
			droppedVectorIndex = &copyVI
		}
	}

	if err := e.storage.GetSchema().DropIndex(name); err != nil {
		if ifExists && strings.Contains(err.Error(), "does not exist") {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
		return nil, err
	}

	// Tear down the matching in-memory vector data after the schema entry
	// is gone so concurrent reads can't see a half-dropped index.
	if droppedVectorIndex != nil {
		if e.searchService != nil && droppedVectorIndex.Property != "" {
			e.searchService.RemovePropertyVectorIndex(droppedVectorIndex.Property)
		}
		e.unregisterVectorSpace(name)
	}

	// Invalidate query cache — cached SHOW INDEXES results are now stale.
	if e.cache != nil {
		e.cache.Invalidate()
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// executeDropConstraint handles DROP CONSTRAINT commands.
func (e *StorageExecutor) executeDropConstraint(ctx context.Context, cypher string) (*ExecuteResult, error) {
	trimmed := strings.TrimSpace(cypher)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "DROP CONSTRAINT") {
		return nil, fmt.Errorf("invalid DROP CONSTRAINT syntax")
	}

	rest := strings.TrimSpace(trimmed[len("DROP CONSTRAINT"):])
	if rest == "" {
		return nil, fmt.Errorf("invalid DROP CONSTRAINT syntax")
	}

	ifExists := false
	upperRest := strings.ToUpper(rest)
	if strings.HasPrefix(upperRest, "IF EXISTS") {
		ifExists = true
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
		upperRest = strings.ToUpper(rest)
	}
	if strings.HasSuffix(upperRest, " IF EXISTS") {
		ifExists = true
		rest = strings.TrimSpace(rest[:len(rest)-len(" IF EXISTS")])
	}

	if rest == "" {
		return nil, fmt.Errorf("invalid DROP CONSTRAINT syntax")
	}
	if !strings.HasPrefix(strings.TrimSpace(rest), "`") && len(strings.Fields(rest)) > 1 {
		return nil, fmt.Errorf("invalid DROP CONSTRAINT syntax")
	}
	name := normalizeIdentifierToken(rest)
	if name == "" {
		return nil, fmt.Errorf("invalid DROP CONSTRAINT syntax")
	}

	if err := e.storage.GetSchema().DropConstraint(name); err != nil {
		// If IF EXISTS was used, swallow missing constraint errors.
		if ifExists && strings.Contains(err.Error(), "does not exist") {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
		return nil, err
	}

	if e.cache != nil {
		e.cache.Invalidate()
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// executeCreateIndex handles CREATE INDEX commands.
//
// Supported syntax:
//
//	CREATE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.property)
//	CREATE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.prop1, n.prop2)
//	CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.prop1, n.prop2, n.prop3)
//
// Supports both single-property and composite (multi-property) indexes.
func (e *StorageExecutor) executeCreateIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if parsed, err := e.parseCreateIndexDDL(cypher, "CREATE INDEX"); err == nil {
		indexName := parsed.indexName
		if indexName == "" {
			propsJoined := strings.Join(parsed.properties, "_")
			entity := parsed.label
			if parsed.isRelationship {
				entity = parsed.relationshipType
			}
			indexName = fmt.Sprintf("index_%s_%s", strings.ToLower(entity), strings.ToLower(propsJoined))
		}

		if parsed.isRelationship {
			// Relationship property indexes are persisted as relationship-scoped
			// range metadata (Neo4j-compatible SHOW INDEXES shape).
			if err := e.storage.GetSchema().AddRangeIndexForEntity(
				indexName,
				parsed.relationshipType,
				parsed.properties,
				storage.ConstraintEntityRelationship,
			); err != nil {
				return nil, err
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}

		if err := e.storage.GetSchema().AddPropertyIndex(indexName, parsed.label, parsed.properties); err != nil {
			return nil, err
		}
		if err := e.backfillPropertyIndex(parsed.label, parsed.properties); err != nil {
			return nil, err
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Neo4j legacy syntax: CREATE INDEX [name] ON :Label(prop[, prop2])
	if parsed, err := e.parseCreateIndexLegacyDDL(cypher); err == nil {
		indexName := parsed.indexName
		if indexName == "" {
			propsJoined := strings.Join(parsed.properties, "_")
			indexName = fmt.Sprintf("index_%s_%s", strings.ToLower(parsed.label), strings.ToLower(propsJoined))
		}
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, parsed.label, parsed.properties); err != nil {
			return nil, err
		}
		if err := e.backfillPropertyIndex(parsed.label, parsed.properties); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE INDEX syntax")
}

// executeCreateRangeIndex handles CREATE RANGE INDEX commands.
//
// Supported syntax:
//
//	CREATE RANGE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.property)
//
// Range indexes optimize queries with range predicates (>, <, >=, <=, BETWEEN).
func (e *StorageExecutor) executeCreateRangeIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	parsed, err := e.parseCreateIndexDDL(cypher, "CREATE RANGE INDEX")
	if err != nil {
		return nil, fmt.Errorf("invalid CREATE RANGE INDEX syntax")
	}
	if parsed.isRelationship {
		return nil, fmt.Errorf("invalid CREATE RANGE INDEX syntax")
	}

	if len(parsed.properties) != 1 {
		return nil, fmt.Errorf("RANGE INDEX only supports single property, got %d", len(parsed.properties))
	}

	indexName := parsed.indexName
	if indexName == "" {
		indexName = fmt.Sprintf("range_idx_%s_%s", strings.ToLower(parsed.label), parsed.properties[0])
	}

	if err := e.storage.GetSchema().AddRangeIndex(indexName, parsed.label, parsed.properties[0]); err != nil {
		return nil, fmt.Errorf("failed to create range index: %w", err)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

type parsedCreateIndexDDL struct {
	indexName        string
	label            string
	relationshipType string
	properties       []string
	isRelationship   bool
	optionsClause    string
}

func keywordSpanAt(s string, pos int, keyword string) (int, bool) {
	ks, ke := trimKeywordWSBounds(keyword)
	return keywordMatchAt(s, pos, keyword, ks, ke)
}

func parseOptionalDDLName(segment string) (string, error) {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return "", nil
	}

	ifPos := keywordIndexFrom(segment, "IF NOT EXISTS", 0, defaultKeywordScanOpts())
	if ifPos < 0 {
		if !isValidDDLIdentifierSegment(segment) {
			return "", fmt.Errorf("invalid identifier segment")
		}
		name := normalizeIdentifierToken(segment)
		if name == "" {
			return "", fmt.Errorf("invalid identifier segment")
		}
		return name, nil
	}

	ifEnd, ok := keywordSpanAt(segment, ifPos, "IF NOT EXISTS")
	if !ok || strings.TrimSpace(segment[ifEnd:]) != "" {
		return "", fmt.Errorf("invalid IF NOT EXISTS clause")
	}

	namePart := strings.TrimSpace(segment[:ifPos])
	if namePart == "" {
		return "", nil
	}
	if !isValidDDLIdentifierSegment(namePart) {
		return "", fmt.Errorf("invalid identifier segment")
	}
	name := normalizeIdentifierToken(namePart)
	if name == "" {
		return "", fmt.Errorf("invalid identifier segment")
	}
	return name, nil
}

func isValidDDLIdentifierSegment(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if strings.HasPrefix(s, "`") {
		if !strings.HasSuffix(s, "`") {
			return false
		}
		return normalizeIdentifierToken(s) != ""
	}
	return len(strings.Fields(s)) == 1
}

func extractIndexPropertiesSegment(onClause string) (string, string, error) {
	onClause = strings.TrimSpace(onClause)
	if onClause == "" {
		return "", "", fmt.Errorf("empty ON clause")
	}

	if strings.HasPrefix(onClause, "(") {
		inside, rest, ok := extractParenSection(onClause)
		if !ok {
			return "", "", fmt.Errorf("invalid ON clause")
		}
		return strings.TrimSpace(inside), strings.TrimSpace(rest), nil
	}

	optionsPos := keywordIndexFrom(onClause, "OPTIONS", 0, defaultKeywordScanOpts())
	if optionsPos < 0 {
		return strings.TrimSpace(onClause), "", nil
	}
	return strings.TrimSpace(onClause[:optionsPos]), strings.TrimSpace(onClause[optionsPos:]), nil
}

func parseCreateIndexForPattern(forClause string) (label string, relationshipType string, isRelationship bool, err error) {
	forClause = strings.TrimSpace(forClause)
	if forClause == "" {
		return "", "", false, fmt.Errorf("empty FOR clause")
	}

	if strings.Contains(forClause, "[") {
		bracketStart := strings.Index(forClause, "[")
		if bracketStart < 0 {
			return "", "", false, fmt.Errorf("invalid relationship FOR clause")
		}
		inside, rest, ok := extractBracketSection(forClause[bracketStart:])
		if !ok {
			return "", "", false, fmt.Errorf("invalid relationship FOR clause")
		}

		prefix := strings.TrimSpace(forClause[:bracketStart])
		suffix := strings.TrimSpace(rest)
		if !strings.Contains(prefix, "(") || !strings.Contains(suffix, "(") {
			return "", "", false, fmt.Errorf("invalid relationship FOR clause")
		}

		colon := strings.Index(inside, ":")
		if colon < 0 {
			return "", "", false, fmt.Errorf("invalid relationship FOR clause")
		}
		relRaw := strings.TrimSpace(inside[colon+1:])
		if relRaw == "" {
			return "", "", false, fmt.Errorf("invalid relationship FOR clause")
		}
		relType := normalizeIdentifierToken(relRaw)
		if relType == "" {
			return "", "", false, fmt.Errorf("invalid relationship FOR clause")
		}
		return "", relType, true, nil
	}

	inside, rest, ok := extractParenSection(forClause)
	if !ok || strings.TrimSpace(rest) != "" {
		return "", "", false, fmt.Errorf("invalid node FOR clause")
	}

	colon := strings.Index(inside, ":")
	if colon < 0 {
		return "", "", false, fmt.Errorf("invalid node FOR clause")
	}
	labelRaw := strings.TrimSpace(inside[colon+1:])
	label = normalizeIdentifierToken(labelRaw)
	if label == "" {
		return "", "", false, fmt.Errorf("invalid node FOR clause")
	}
	return label, "", false, nil
}

func (e *StorageExecutor) parseCreateIndexDDL(cypher string, prefixKeyword string) (parsedCreateIndexDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCreateIndexDDL{}, fmt.Errorf("empty statement")
	}

	prefixPos := keywordIndexFrom(q, prefixKeyword, 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, prefixKeyword)
	if !ok {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid prefix")
	}

	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("missing FOR")
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid FOR")
	}

	onPos := keywordIndexFrom(q, "ON", forEnd, defaultKeywordScanOpts())
	if onPos < 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("missing ON")
	}
	onEnd, ok := keywordSpanAt(q, onPos, "ON")
	if !ok {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid ON")
	}

	name, err := parseOptionalDDLName(q[prefixEnd:forPos])
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}

	label, relType, isRelationship, err := parseCreateIndexForPattern(q[forEnd:onPos])
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}

	propsSegment, tail, err := extractIndexPropertiesSegment(q[onEnd:])
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}
	if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid trailing syntax")
	}

	properties := e.parseQualifiedIndexProperties(propsSegment)
	if len(properties) == 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("no properties specified for index")
	}

	return parsedCreateIndexDDL{
		indexName:        name,
		label:            label,
		relationshipType: relType,
		properties:       properties,
		isRelationship:   isRelationship,
		optionsClause:    tail,
	}, nil
}

func (e *StorageExecutor) parseCreateIndexLegacyDDL(cypher string) (parsedCreateIndexDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCreateIndexDDL{}, fmt.Errorf("empty statement")
	}

	prefixPos := keywordIndexFrom(q, "CREATE INDEX", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE INDEX")
	if !ok {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid prefix")
	}

	onPos := keywordIndexFrom(q, "ON", prefixEnd, defaultKeywordScanOpts())
	if onPos < 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("missing ON")
	}
	onEnd, ok := keywordSpanAt(q, onPos, "ON")
	if !ok {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid ON")
	}

	name, err := parseOptionalDDLName(q[prefixEnd:onPos])
	if err != nil {
		return parsedCreateIndexDDL{}, err
	}

	onClause := strings.TrimSpace(q[onEnd:])
	if !strings.HasPrefix(onClause, ":") {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid legacy ON clause")
	}
	onClause = strings.TrimSpace(onClause[1:])

	openParen := strings.Index(onClause, "(")
	if openParen < 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid legacy ON clause")
	}
	label := normalizeIdentifierToken(strings.TrimSpace(onClause[:openParen]))
	if label == "" {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid legacy ON clause")
	}

	inside, tail, ok := extractParenSection(onClause[openParen:])
	if !ok {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid legacy ON clause")
	}
	tail = strings.TrimSpace(tail)
	if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
		return parsedCreateIndexDDL{}, fmt.Errorf("invalid trailing syntax")
	}

	properties := e.parseIndexProperties(inside)
	if len(properties) == 0 {
		return parsedCreateIndexDDL{}, fmt.Errorf("no properties specified for index")
	}

	return parsedCreateIndexDDL{
		indexName:      name,
		label:          label,
		properties:     properties,
		isRelationship: false,
		optionsClause:  tail,
	}, nil
}

func isVectorKeyBoundaryChar(b byte) bool {
	if b >= 'a' && b <= 'z' {
		return false
	}
	if b >= 'A' && b <= 'Z' {
		return false
	}
	if b >= '0' && b <= '9' {
		return false
	}
	return b != '_' && b != '.'
}

func extractVectorOptionValue(optionsClause, key string) (string, bool) {
	if optionsClause == "" || key == "" {
		return "", false
	}

	sanitized := strings.ReplaceAll(optionsClause, "`", "")
	sanitized = strings.ReplaceAll(sanitized, "\"", "")
	sanitized = strings.ReplaceAll(sanitized, "'", "")
	lower := strings.ToLower(sanitized)
	keyLower := strings.ToLower(key)

	start := 0
	for start < len(lower) {
		rel := strings.Index(lower[start:], keyLower)
		if rel < 0 {
			return "", false
		}
		idx := start + rel
		leftOK := idx == 0 || isVectorKeyBoundaryChar(sanitized[idx-1])
		rightIdx := idx + len(keyLower)
		rightOK := rightIdx >= len(sanitized) || isVectorKeyBoundaryChar(sanitized[rightIdx])
		if !leftOK || !rightOK {
			start = idx + len(keyLower)
			continue
		}

		i := rightIdx
		for i < len(sanitized) && (sanitized[i] == ' ' || sanitized[i] == '\t' || sanitized[i] == ':') {
			i++
		}
		if i >= len(sanitized) {
			return "", false
		}

		if sanitized[i] == '\'' || sanitized[i] == '"' {
			quote := sanitized[i]
			i++
			j := i
			for j < len(sanitized) && sanitized[j] != quote {
				j++
			}
			if j >= len(sanitized) {
				return "", false
			}
			return strings.TrimSpace(sanitized[i:j]), true
		}

		j := i
		for j < len(sanitized) {
			c := sanitized[j]
			if c == ',' || c == '}' || c == ')' || c == '\n' || c == '\r' {
				break
			}
			j++
		}
		return strings.TrimSpace(sanitized[i:j]), true
	}

	return "", false
}

func parseVectorOptions(optionsClause string, defaultDimensions int, defaultSimilarity string) (int, string) {
	dimensions := defaultDimensions
	similarity := defaultSimilarity

	if raw, ok := extractVectorOptionValue(optionsClause, "vector.dimensions"); ok {
		if dim, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
			dimensions = dim
		}
	}

	if raw, ok := extractVectorOptionValue(optionsClause, "vector.similarity_function"); ok {
		raw = strings.TrimSpace(raw)
		if raw != "" {
			similarity = raw
		}
	}

	return dimensions, similarity
}

type parsedCreateFulltextIndexDDL struct {
	indexName         string
	label             string
	relationshipTypes []string
	properties        []string
	isRelationship    bool
}

func parseCreateFulltextForPattern(forClause string) (label string, relationshipTypes []string, isRelationship bool, err error) {
	label, relTypeRaw, isRelationship, err := parseCreateIndexForPattern(forClause)
	if err != nil {
		// Neo4j compatibility: allow non-parenthesized node pattern `n:Label`.
		if !strings.Contains(forClause, "[") {
			parts := strings.SplitN(strings.TrimSpace(forClause), ":", 2)
			if len(parts) == 2 {
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
	relTypes, err := parseFulltextRelationshipTypes(relTypeRaw)
	if err != nil {
		return "", nil, false, err
	}
	return "", relTypes, true, nil
}

func extractFulltextPropertiesSegment(onClause string) (string, string, error) {
	onClause = strings.TrimSpace(onClause)
	if onClause == "" {
		return "", "", fmt.Errorf("empty ON clause")
	}

	if startsWithKeywordFold(onClause, "EACH") {
		eachEnd, ok := keywordSpanAt(onClause, 0, "EACH")
		if !ok {
			return "", "", fmt.Errorf("invalid EACH clause")
		}
		onClause = strings.TrimSpace(onClause[eachEnd:])
	}

	if onClause == "" {
		return "", "", fmt.Errorf("empty ON clause")
	}

	if strings.HasPrefix(onClause, "[") {
		inside, rest, ok := extractBracketSection(onClause)
		if !ok {
			return "", "", fmt.Errorf("invalid ON EACH clause")
		}
		return strings.TrimSpace(inside), strings.TrimSpace(rest), nil
	}

	if strings.HasPrefix(onClause, "(") {
		inside, rest, ok := extractParenSection(onClause)
		if !ok {
			return "", "", fmt.Errorf("invalid ON clause")
		}
		return strings.TrimSpace(inside), strings.TrimSpace(rest), nil
	}

	optionsPos := keywordIndexFrom(onClause, "OPTIONS", 0, defaultKeywordScanOpts())
	if optionsPos < 0 {
		return strings.TrimSpace(onClause), "", nil
	}
	return strings.TrimSpace(onClause[:optionsPos]), strings.TrimSpace(onClause[optionsPos:]), nil
}

func (e *StorageExecutor) parseCreateFulltextIndexDDL(cypher string) (parsedCreateFulltextIndexDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("empty statement")
	}

	prefixPos := keywordIndexFrom(q, "CREATE FULLTEXT INDEX", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE FULLTEXT INDEX")
	if !ok {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("invalid prefix")
	}

	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("missing FOR")
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("invalid FOR")
	}

	onPos := keywordIndexFrom(q, "ON", forEnd, defaultKeywordScanOpts())
	if onPos < 0 {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("missing ON")
	}
	onEnd, ok := keywordSpanAt(q, onPos, "ON")
	if !ok {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("invalid ON")
	}

	name, err := parseOptionalDDLName(q[prefixEnd:forPos])
	if err != nil {
		return parsedCreateFulltextIndexDDL{}, err
	}
	if name == "" {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("missing index name")
	}

	label, relationshipTypes, isRelationship, err := parseCreateFulltextForPattern(q[forEnd:onPos])
	if err != nil {
		return parsedCreateFulltextIndexDDL{}, err
	}

	propsSegment, tail, err := extractFulltextPropertiesSegment(q[onEnd:])
	if err != nil {
		return parsedCreateFulltextIndexDDL{}, err
	}
	if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("invalid trailing syntax")
	}

	properties := e.parseQualifiedIndexProperties(propsSegment)
	if len(properties) == 0 {
		return parsedCreateFulltextIndexDDL{}, fmt.Errorf("no properties found in fulltext index definition")
	}

	return parsedCreateFulltextIndexDDL{
		indexName:         name,
		label:             label,
		relationshipTypes: relationshipTypes,
		properties:        properties,
		isRelationship:    isRelationship,
	}, nil
}

type parsedSimplePropertyConstraintDDL struct {
	name           string
	label          string
	property       string
	isRelationship bool
	kind           string // unique | exists
}

type parsedTypeConstraintDDL struct {
	name           string
	label          string
	property       string
	expectedType   storage.PropertyType
	isRelationship bool
}

type parsedNodeKeyConstraintDDL struct {
	name       string
	label      string
	properties []string
}

type parsedRelationshipKeyOrCompositeUniqueDDL struct {
	name       string
	label      string
	properties []string
	kind       string // rel_key | unique
}

type parsedConstraintForRequireDDL struct {
	name           string
	label          string
	isRelationship bool
	requireExpr    string
}

type parsedTemporalConstraintDDL struct {
	name           string
	label          string
	isRelationship bool
	properties     []string
}

type parsedDomainConstraintDDL struct {
	name           string
	label          string
	isRelationship bool
	property       string
	allowedRaw     string
}

type parsedCardinalityConstraintDDL struct {
	name      string
	relType   string
	direction string // OUTGOING | INCOMING
	maxCount  int
}

type parsedPolicyConstraintDDL struct {
	name        string
	sourceLabel string
	relType     string
	targetLabel string
	policyMode  string // ALLOWED | DISALLOWED
}

func splitDDLOptionsTail(s string) (expr string, optionsTail string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	if idx := keywordIndexFrom(s, "OPTIONS", 0, defaultKeywordScanOpts()); idx >= 0 {
		return strings.TrimSpace(s[:idx]), strings.TrimSpace(s[idx:])
	}
	return s, ""
}

func parseConstraintQualifiedProperty(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	if strings.ContainsAny(expr, ",()") {
		return "", "", false
	}
	dot := strings.LastIndex(expr, ".")
	if dot <= 0 || dot >= len(expr)-1 {
		return "", "", false
	}
	left := strings.TrimSpace(expr[:dot])
	right := normalizeIdentifierToken(expr[dot+1:])
	if left == "" || right == "" {
		return "", "", false
	}
	return left, right, true
}

func parseConstraintPredicate(predicateRaw string) (kind string, property string, ok bool) {
	predicateRaw = strings.TrimSpace(predicateRaw)
	if predicateRaw == "" {
		return "", "", false
	}

	upper := strings.ToUpper(predicateRaw)
	if strings.HasPrefix(upper, "EXISTS") {
		rest := strings.TrimSpace(predicateRaw[len("EXISTS"):])
		inside, trailing, ok := extractParenSection(rest)
		if !ok || strings.TrimSpace(trailing) != "" {
			return "", "", false
		}
		if _, prop, ok := parseConstraintQualifiedProperty(inside); ok {
			return "exists", prop, true
		}
		return "", "", false
	}

	if idx := keywordIndexFrom(predicateRaw, "IS UNIQUE", 0, defaultKeywordScanOpts()); idx >= 0 {
		end, ok := keywordSpanAt(predicateRaw, idx, "IS UNIQUE")
		if ok && strings.TrimSpace(predicateRaw[end:]) == "" {
			if _, prop, ok := parseConstraintQualifiedProperty(predicateRaw[:idx]); ok {
				return "unique", prop, true
			}
		}
	}

	if idx := keywordIndexFrom(predicateRaw, "IS NOT NULL", 0, defaultKeywordScanOpts()); idx >= 0 {
		end, ok := keywordSpanAt(predicateRaw, idx, "IS NOT NULL")
		if ok && strings.TrimSpace(predicateRaw[end:]) == "" {
			if _, prop, ok := parseConstraintQualifiedProperty(predicateRaw[:idx]); ok {
				return "exists", prop, true
			}
		}
	}

	return "", "", false
}

func parseConstraintTypePredicate(predicateRaw string) (property string, typeName string, ok bool) {
	predicateRaw = strings.TrimSpace(predicateRaw)
	if predicateRaw == "" {
		return "", "", false
	}

	idx := keywordIndexFrom(predicateRaw, "IS", 0, defaultKeywordScanOpts())
	if idx < 0 {
		return "", "", false
	}
	isEnd, ok := keywordSpanAt(predicateRaw, idx, "IS")
	if !ok {
		return "", "", false
	}

	_, prop, ok := parseConstraintQualifiedProperty(predicateRaw[:idx])
	if !ok {
		return "", "", false
	}

	rhs := strings.TrimSpace(predicateRaw[isEnd:])
	rhsUpper := strings.ToUpper(rhs)
	if strings.HasPrefix(rhsUpper, "::") {
		typeName = strings.TrimSpace(rhs[2:])
	} else if strings.HasPrefix(rhsUpper, "TYPED") {
		typeName = strings.TrimSpace(rhs[len("TYPED"):])
	} else {
		return "", "", false
	}
	if typeName == "" {
		return "", "", false
	}
	return prop, typeName, true
}

func (e *StorageExecutor) parseNodeKeyPropertyList(expr string) ([]string, bool) {
	expr = strings.TrimSpace(expr)
	inside, rest, ok := extractParenSection(expr)
	if !ok {
		return nil, false
	}
	rest = strings.TrimSpace(rest)
	idx := keywordIndexFrom(rest, "IS NODE KEY", 0, defaultKeywordScanOpts())
	if idx != 0 {
		return nil, false
	}
	end, ok := keywordSpanAt(rest, 0, "IS NODE KEY")
	if !ok || strings.TrimSpace(rest[end:]) != "" {
		return nil, false
	}
	props := e.parseConstraintProperties(inside)
	return props, true
}

func (e *StorageExecutor) parseCreateConstraintNodeKeyDDL(cypher string) (parsedNodeKeyConstraintDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedNodeKeyConstraintDDL{}, fmt.Errorf("empty statement")
	}

	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid prefix")
	}

	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	onPos := keywordIndexFrom(q, "ON", prefixEnd, defaultKeywordScanOpts())

	if forPos >= 0 && (onPos < 0 || forPos < onPos) {
		forEnd, ok := keywordSpanAt(q, forPos, "FOR")
		if !ok {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid FOR")
		}
		reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
		if reqPos < 0 {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("missing REQUIRE")
		}
		reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
		if !ok {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid REQUIRE")
		}

		name, err := parseOptionalDDLName(q[prefixEnd:forPos])
		if err != nil {
			return parsedNodeKeyConstraintDDL{}, err
		}

		label, _, isRelationship, err := parseCreateIndexForPattern(q[forEnd:reqPos])
		if err != nil || isRelationship {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid NODE KEY pattern")
		}

		predicateExpr, tail := splitDDLOptionsTail(q[reqEnd:])
		if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
		}
		props, ok := e.parseNodeKeyPropertyList(predicateExpr)
		if !ok {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("unsupported NODE KEY predicate")
		}
		return parsedNodeKeyConstraintDDL{name: name, label: label, properties: props}, nil
	}

	if onPos >= 0 {
		onEnd, ok := keywordSpanAt(q, onPos, "ON")
		if !ok {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid ON")
		}
		assertPos := keywordIndexFrom(q, "ASSERT", onEnd, defaultKeywordScanOpts())
		if assertPos < 0 {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("missing ASSERT")
		}
		assertEnd, ok := keywordSpanAt(q, assertPos, "ASSERT")
		if !ok {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid ASSERT")
		}

		_, err := parseOptionalDDLName(q[prefixEnd:onPos])
		if err != nil {
			return parsedNodeKeyConstraintDDL{}, err
		}
		label, _, isRelationship, err := parseCreateIndexForPattern(q[onEnd:assertPos])
		if err != nil || isRelationship {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid ASSERT pattern")
		}

		predicateExpr, tail := splitDDLOptionsTail(q[assertEnd:])
		if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
		}
		props, ok := e.parseNodeKeyPropertyList(predicateExpr)
		if !ok {
			return parsedNodeKeyConstraintDDL{}, fmt.Errorf("unsupported NODE KEY predicate")
		}
		return parsedNodeKeyConstraintDDL{name: "", label: label, properties: props}, nil
	}

	return parsedNodeKeyConstraintDDL{}, fmt.Errorf("unsupported CREATE CONSTRAINT shape")
}

func (e *StorageExecutor) parseRelationshipKeyOrCompositeUniquePredicate(predicateExpr string) (string, []string, bool) {
	predicateExpr = strings.TrimSpace(predicateExpr)
	if predicateExpr == "" {
		return "", nil, false
	}

	if inside, rest, ok := extractParenSection(predicateExpr); ok {
		rest = strings.TrimSpace(rest)
		if idx := keywordIndexFrom(rest, "IS RELATIONSHIP KEY", 0, defaultKeywordScanOpts()); idx == 0 {
			end, ok := keywordSpanAt(rest, 0, "IS RELATIONSHIP KEY")
			if ok && strings.TrimSpace(rest[end:]) == "" {
				return "rel_key", e.parseConstraintProperties(inside), true
			}
		}
		if idx := keywordIndexFrom(rest, "IS UNIQUE", 0, defaultKeywordScanOpts()); idx == 0 {
			end, ok := keywordSpanAt(rest, 0, "IS UNIQUE")
			if ok && strings.TrimSpace(rest[end:]) == "" {
				return "unique", e.parseConstraintProperties(inside), true
			}
		}
	}

	if idx := keywordIndexFrom(predicateExpr, "IS RELATIONSHIP KEY", 0, defaultKeywordScanOpts()); idx >= 0 {
		end, ok := keywordSpanAt(predicateExpr, idx, "IS RELATIONSHIP KEY")
		if ok && strings.TrimSpace(predicateExpr[end:]) == "" {
			_, prop, ok := parseConstraintQualifiedProperty(predicateExpr[:idx])
			if ok {
				return "rel_key", []string{prop}, true
			}
		}
	}

	return "", nil, false
}

func (e *StorageExecutor) parseCreateConstraintForRequireDDL(cypher string) (parsedConstraintForRequireDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("empty statement")
	}
	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("invalid prefix")
	}

	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("missing FOR")
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("invalid FOR")
	}
	reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
	if reqPos < 0 {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("missing REQUIRE")
	}
	reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
	if !ok {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("invalid REQUIRE")
	}

	name, err := parseOptionalDDLName(q[prefixEnd:forPos])
	if err != nil {
		return parsedConstraintForRequireDDL{}, err
	}
	label, relType, isRelationship, err := parseCreateIndexForPattern(q[forEnd:reqPos])
	if err != nil {
		return parsedConstraintForRequireDDL{}, err
	}
	requireExpr, tail := splitDDLOptionsTail(q[reqEnd:])
	if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
		return parsedConstraintForRequireDDL{}, fmt.Errorf("invalid trailing syntax")
	}
	out := parsedConstraintForRequireDDL{name: name, isRelationship: isRelationship, requireExpr: requireExpr}
	if isRelationship {
		out.label = relType
	} else {
		out.label = label
	}
	return out, nil
}

func (e *StorageExecutor) parseTemporalConstraintPredicate(requireExpr string) ([]string, bool) {
	requireExpr = strings.TrimSpace(requireExpr)
	inside, rest, ok := extractParenSection(requireExpr)
	if !ok {
		return nil, false
	}
	rest = strings.TrimSpace(rest)
	if idx := keywordIndexFrom(rest, "IS TEMPORAL", 0, defaultKeywordScanOpts()); idx != 0 {
		return nil, false
	}
	end, ok := keywordSpanAt(rest, 0, "IS TEMPORAL")
	if !ok {
		return nil, false
	}
	tail := strings.TrimSpace(rest[end:])
	if tail != "" {
		if idx := keywordIndexFrom(tail, "NO OVERLAP", 0, defaultKeywordScanOpts()); idx != 0 {
			return nil, false
		}
		end2, ok := keywordSpanAt(tail, 0, "NO OVERLAP")
		if !ok || strings.TrimSpace(tail[end2:]) != "" {
			return nil, false
		}
	}
	return e.parseConstraintProperties(inside), true
}

func (e *StorageExecutor) parseCreateConstraintTemporalDDL(cypher string) (parsedTemporalConstraintDDL, error) {
	parsed, err := e.parseCreateConstraintForRequireDDL(cypher)
	if err != nil {
		return parsedTemporalConstraintDDL{}, err
	}
	props, ok := e.parseTemporalConstraintPredicate(parsed.requireExpr)
	if !ok {
		return parsedTemporalConstraintDDL{}, fmt.Errorf("unsupported temporal predicate")
	}
	return parsedTemporalConstraintDDL{name: parsed.name, label: parsed.label, isRelationship: parsed.isRelationship, properties: props}, nil
}

func parseDomainConstraintPredicate(requireExpr string) (property string, allowedRaw string, ok bool) {
	requireExpr = strings.TrimSpace(requireExpr)
	if requireExpr == "" {
		return "", "", false
	}
	inPos := keywordIndexFrom(requireExpr, "IN", 0, defaultKeywordScanOpts())
	if inPos < 0 {
		return "", "", false
	}
	inEnd, ok := keywordSpanAt(requireExpr, inPos, "IN")
	if !ok {
		return "", "", false
	}
	_, prop, ok := parseConstraintQualifiedProperty(requireExpr[:inPos])
	if !ok {
		return "", "", false
	}
	rhs := strings.TrimSpace(requireExpr[inEnd:])
	inside, rest, ok := extractBracketSection(rhs)
	if !ok || strings.TrimSpace(rest) != "" {
		return "", "", false
	}
	return prop, inside, true
}

func (e *StorageExecutor) parseCreateConstraintDomainDDL(cypher string) (parsedDomainConstraintDDL, error) {
	parsed, err := e.parseCreateConstraintForRequireDDL(cypher)
	if err != nil {
		return parsedDomainConstraintDDL{}, err
	}
	property, allowedRaw, ok := parseDomainConstraintPredicate(parsed.requireExpr)
	if !ok {
		return parsedDomainConstraintDDL{}, fmt.Errorf("unsupported domain predicate")
	}
	return parsedDomainConstraintDDL{name: parsed.name, label: parsed.label, isRelationship: parsed.isRelationship, property: property, allowedRaw: allowedRaw}, nil
}

func parseRelationshipForDirection(forClause string) (relType string, direction string, ok bool) {
	forClause = strings.TrimSpace(forClause)
	if forClause == "" {
		return "", "", false
	}
	bracketStart := strings.Index(forClause, "[")
	if bracketStart < 0 {
		return "", "", false
	}
	inside, rest, ok := extractBracketSection(forClause[bracketStart:])
	if !ok {
		return "", "", false
	}
	prefix := strings.TrimSpace(forClause[:bracketStart])
	suffix := strings.TrimSpace(rest)

	colon := strings.Index(inside, ":")
	if colon < 0 {
		return "", "", false
	}
	relType = normalizeIdentifierToken(strings.TrimSpace(inside[colon+1:]))
	if relType == "" {
		return "", "", false
	}

	if strings.Contains(suffix, "->") {
		direction = "OUTGOING"
	} else if strings.Contains(prefix, "<-") {
		direction = "INCOMING"
	} else {
		return "", "", false
	}
	return relType, direction, true
}

func parseCardinalityRequireExpr(requireExpr string) (int, bool) {
	requireExpr = strings.TrimSpace(requireExpr)
	idx := keywordIndexFrom(requireExpr, "MAX COUNT", 0, defaultKeywordScanOpts())
	if idx != 0 {
		return 0, false
	}
	end, ok := keywordSpanAt(requireExpr, 0, "MAX COUNT")
	if !ok {
		return 0, false
	}
	raw := strings.TrimSpace(requireExpr[end:])
	if raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return 0, false
	}
	return v, true
}

func (e *StorageExecutor) parseCreateConstraintCardinalityDDL(cypher string) (parsedCardinalityConstraintDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("empty statement")
	}
	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("invalid prefix")
	}
	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("missing FOR")
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("invalid FOR")
	}
	reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
	if reqPos < 0 {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("missing REQUIRE")
	}
	reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
	if !ok {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("invalid REQUIRE")
	}

	name, err := parseOptionalDDLName(q[prefixEnd:forPos])
	if err != nil {
		return parsedCardinalityConstraintDDL{}, err
	}
	relType, direction, ok := parseRelationshipForDirection(q[forEnd:reqPos])
	if !ok {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("invalid cardinality FOR pattern")
	}
	requireExpr, tail := splitDDLOptionsTail(q[reqEnd:])
	if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
	}
	maxCount, ok := parseCardinalityRequireExpr(requireExpr)
	if !ok {
		if startsWithKeywordFold(strings.TrimSpace(requireExpr), "MAX COUNT") {
			end, spanOK := keywordSpanAt(strings.TrimSpace(requireExpr), 0, "MAX COUNT")
			if spanOK {
				raw := strings.TrimSpace(strings.TrimSpace(requireExpr)[end:])
				if raw != "" {
					digitsOnly := true
					for _, r := range raw {
						if r < '0' || r > '9' {
							digitsOnly = false
							break
						}
					}
					if digitsOnly {
						v, convErr := strconv.Atoi(raw)
						if convErr != nil || v < 1 {
							return parsedCardinalityConstraintDDL{}, fmt.Errorf("MAX COUNT must be a positive integer, got %q", raw)
						}
					}
				}
			}
		}
		return parsedCardinalityConstraintDDL{}, fmt.Errorf("invalid cardinality require clause")
	}

	return parsedCardinalityConstraintDDL{name: name, relType: relType, direction: direction, maxCount: maxCount}, nil
}

func parseNodeLabelOnlyPattern(inner string) (string, bool) {
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return "", false
	}
	if !strings.HasPrefix(inner, ":") {
		return "", false
	}
	label := normalizeIdentifierToken(strings.TrimSpace(inner[1:]))
	if label == "" {
		return "", false
	}
	return label, true
}

func parsePolicyForClause(forClause string) (sourceLabel string, relType string, targetLabel string, ok bool) {
	forClause = strings.TrimSpace(forClause)
	if !strings.HasPrefix(forClause, "(") {
		return "", "", "", false
	}
	leftInside, leftRest, ok := extractParenSection(forClause)
	if !ok {
		return "", "", "", false
	}
	sourceLabel, ok = parseNodeLabelOnlyPattern(leftInside)
	if !ok {
		return "", "", "", false
	}

	leftRest = strings.TrimSpace(leftRest)
	if !strings.HasPrefix(leftRest, "-") {
		return "", "", "", false
	}
	bracketStart := strings.Index(leftRest, "[")
	if bracketStart < 0 {
		return "", "", "", false
	}
	relInside, relRest, ok := extractBracketSection(leftRest[bracketStart:])
	if !ok {
		return "", "", "", false
	}
	colon := strings.Index(relInside, ":")
	if colon < 0 {
		return "", "", "", false
	}
	relType = normalizeIdentifierToken(strings.TrimSpace(relInside[colon+1:]))
	if relType == "" {
		return "", "", "", false
	}

	relRest = strings.TrimSpace(relRest)
	if !strings.HasPrefix(relRest, "->") {
		return "", "", "", false
	}
	relRest = strings.TrimSpace(relRest[2:])
	if !strings.HasPrefix(relRest, "(") {
		return "", "", "", false
	}
	rightInside, trailing, ok := extractParenSection(relRest)
	if !ok || strings.TrimSpace(trailing) != "" {
		return "", "", "", false
	}
	targetLabel, ok = parseNodeLabelOnlyPattern(rightInside)
	if !ok {
		return "", "", "", false
	}
	return sourceLabel, relType, targetLabel, true
}

func parsePolicyModeRequireExpr(requireExpr string) (string, bool) {
	requireExpr = strings.TrimSpace(requireExpr)
	if startsWithKeywordFold(requireExpr, "ALLOWED") {
		end, ok := keywordSpanAt(requireExpr, 0, "ALLOWED")
		if ok && strings.TrimSpace(requireExpr[end:]) == "" {
			return "ALLOWED", true
		}
	}
	if startsWithKeywordFold(requireExpr, "DISALLOWED") {
		end, ok := keywordSpanAt(requireExpr, 0, "DISALLOWED")
		if ok && strings.TrimSpace(requireExpr[end:]) == "" {
			return "DISALLOWED", true
		}
	}
	return "", false
}

func (e *StorageExecutor) parseCreateConstraintPolicyDDL(cypher string) (parsedPolicyConstraintDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("empty statement")
	}
	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("invalid prefix")
	}
	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("missing FOR")
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("invalid FOR")
	}
	reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
	if reqPos < 0 {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("missing REQUIRE")
	}
	reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
	if !ok {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("invalid REQUIRE")
	}

	name, err := parseOptionalDDLName(q[prefixEnd:forPos])
	if err != nil {
		return parsedPolicyConstraintDDL{}, err
	}
	source, relType, target, ok := parsePolicyForClause(q[forEnd:reqPos])
	if !ok {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("invalid policy FOR pattern")
	}
	requireExpr, tail := splitDDLOptionsTail(q[reqEnd:])
	if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
	}
	mode, ok := parsePolicyModeRequireExpr(requireExpr)
	if !ok {
		return parsedPolicyConstraintDDL{}, fmt.Errorf("invalid policy require clause")
	}

	return parsedPolicyConstraintDDL{name: name, sourceLabel: source, relType: relType, targetLabel: target, policyMode: mode}, nil
}

func (e *StorageExecutor) parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL(cypher string) (parsedRelationshipKeyOrCompositeUniqueDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("empty statement")
	}
	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("invalid prefix")
	}

	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	if forPos < 0 {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("missing FOR")
	}
	forEnd, ok := keywordSpanAt(q, forPos, "FOR")
	if !ok {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("invalid FOR")
	}
	reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
	if reqPos < 0 {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("missing REQUIRE")
	}
	reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
	if !ok {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("invalid REQUIRE")
	}

	name, err := parseOptionalDDLName(q[prefixEnd:forPos])
	if err != nil {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, err
	}

	_, relType, isRelationship, err := parseCreateIndexForPattern(q[forEnd:reqPos])
	if err != nil || !isRelationship {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("invalid relationship FOR pattern")
	}

	predicateExpr, tail := splitDDLOptionsTail(q[reqEnd:])
	if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("invalid trailing syntax")
	}
	kind, props, ok := e.parseRelationshipKeyOrCompositeUniquePredicate(predicateExpr)
	if !ok {
		return parsedRelationshipKeyOrCompositeUniqueDDL{}, fmt.Errorf("unsupported relationship key/unique predicate")
	}

	return parsedRelationshipKeyOrCompositeUniqueDDL{name: name, label: relType, properties: props, kind: kind}, nil
}

func (e *StorageExecutor) parseCreateConstraintTypeDDL(cypher string) (parsedTypeConstraintDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedTypeConstraintDDL{}, fmt.Errorf("empty statement")
	}

	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedTypeConstraintDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedTypeConstraintDDL{}, fmt.Errorf("invalid prefix")
	}

	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	onPos := keywordIndexFrom(q, "ON", prefixEnd, defaultKeywordScanOpts())

	if forPos >= 0 && (onPos < 0 || forPos < onPos) {
		forEnd, ok := keywordSpanAt(q, forPos, "FOR")
		if !ok {
			return parsedTypeConstraintDDL{}, fmt.Errorf("invalid FOR")
		}
		reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
		if reqPos < 0 {
			return parsedTypeConstraintDDL{}, fmt.Errorf("missing REQUIRE")
		}
		reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
		if !ok {
			return parsedTypeConstraintDDL{}, fmt.Errorf("invalid REQUIRE")
		}

		name, err := parseOptionalDDLName(q[prefixEnd:forPos])
		if err != nil {
			return parsedTypeConstraintDDL{}, err
		}

		label, relType, isRelationship, err := parseCreateIndexForPattern(q[forEnd:reqPos])
		if err != nil {
			return parsedTypeConstraintDDL{}, err
		}

		predicateExpr, tail := splitDDLOptionsTail(q[reqEnd:])
		if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
			return parsedTypeConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
		}
		property, typeName, ok := parseConstraintTypePredicate(predicateExpr)
		if !ok {
			return parsedTypeConstraintDDL{}, fmt.Errorf("unsupported type predicate")
		}
		expectedType, err := parsePropertyType(typeName)
		if err != nil {
			return parsedTypeConstraintDDL{}, err
		}

		out := parsedTypeConstraintDDL{name: name, property: property, expectedType: expectedType, isRelationship: isRelationship}
		if isRelationship {
			out.label = relType
		} else {
			out.label = label
		}
		return out, nil
	}

	if onPos >= 0 {
		onEnd, ok := keywordSpanAt(q, onPos, "ON")
		if !ok {
			return parsedTypeConstraintDDL{}, fmt.Errorf("invalid ON")
		}
		assertPos := keywordIndexFrom(q, "ASSERT", onEnd, defaultKeywordScanOpts())
		if assertPos < 0 {
			return parsedTypeConstraintDDL{}, fmt.Errorf("missing ASSERT")
		}
		assertEnd, ok := keywordSpanAt(q, assertPos, "ASSERT")
		if !ok {
			return parsedTypeConstraintDDL{}, fmt.Errorf("invalid ASSERT")
		}

		name, err := parseOptionalDDLName(q[prefixEnd:onPos])
		if err != nil {
			return parsedTypeConstraintDDL{}, err
		}
		label, _, isRelationship, err := parseCreateIndexForPattern(q[onEnd:assertPos])
		if err != nil || isRelationship {
			return parsedTypeConstraintDDL{}, fmt.Errorf("invalid ASSERT pattern")
		}

		predicateExpr, tail := splitDDLOptionsTail(q[assertEnd:])
		if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
			return parsedTypeConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
		}
		property, typeName, ok := parseConstraintTypePredicate(predicateExpr)
		if !ok {
			return parsedTypeConstraintDDL{}, fmt.Errorf("unsupported type predicate")
		}
		expectedType, err := parsePropertyType(typeName)
		if err != nil {
			return parsedTypeConstraintDDL{}, err
		}
		return parsedTypeConstraintDDL{name: name, label: label, property: property, expectedType: expectedType}, nil
	}

	return parsedTypeConstraintDDL{}, fmt.Errorf("unsupported CREATE CONSTRAINT shape")
}

func (e *StorageExecutor) parseCreateConstraintSimplePropertyDDL(cypher string) (parsedSimplePropertyConstraintDDL, error) {
	q := strings.TrimSpace(cypher)
	if q == "" {
		return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("empty statement")
	}

	prefixPos := keywordIndexFrom(q, "CREATE CONSTRAINT", 0, defaultKeywordScanOpts())
	if prefixPos != 0 {
		return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid prefix")
	}
	prefixEnd, ok := keywordSpanAt(q, 0, "CREATE CONSTRAINT")
	if !ok {
		return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid prefix")
	}

	forPos := keywordIndexFrom(q, "FOR", prefixEnd, defaultKeywordScanOpts())
	onPos := keywordIndexFrom(q, "ON", prefixEnd, defaultKeywordScanOpts())

	if forPos >= 0 && (onPos < 0 || forPos < onPos) {
		forEnd, ok := keywordSpanAt(q, forPos, "FOR")
		if !ok {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid FOR")
		}
		reqPos := keywordIndexFrom(q, "REQUIRE", forEnd, defaultKeywordScanOpts())
		if reqPos < 0 {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("missing REQUIRE")
		}
		reqEnd, ok := keywordSpanAt(q, reqPos, "REQUIRE")
		if !ok {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid REQUIRE")
		}

		name, err := parseOptionalDDLName(q[prefixEnd:forPos])
		if err != nil {
			return parsedSimplePropertyConstraintDDL{}, err
		}

		label, relType, isRelationship, err := parseCreateIndexForPattern(q[forEnd:reqPos])
		if err != nil {
			return parsedSimplePropertyConstraintDDL{}, err
		}

		predicateExpr, tail := splitDDLOptionsTail(q[reqEnd:])
		if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
		}
		kind, property, ok := parseConstraintPredicate(predicateExpr)
		if !ok {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("unsupported constraint predicate")
		}

		out := parsedSimplePropertyConstraintDDL{name: name, property: property, kind: kind, isRelationship: isRelationship}
		if isRelationship {
			out.label = relType
		} else {
			out.label = label
		}
		return out, nil
	}

	if onPos >= 0 {
		onEnd, ok := keywordSpanAt(q, onPos, "ON")
		if !ok {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid ON")
		}
		assertPos := keywordIndexFrom(q, "ASSERT", onEnd, defaultKeywordScanOpts())
		if assertPos < 0 {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("missing ASSERT")
		}
		assertEnd, ok := keywordSpanAt(q, assertPos, "ASSERT")
		if !ok {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid ASSERT")
		}

		name, err := parseOptionalDDLName(q[prefixEnd:onPos])
		if err != nil {
			return parsedSimplePropertyConstraintDDL{}, err
		}

		label, _, isRelationship, err := parseCreateIndexForPattern(q[onEnd:assertPos])
		if err != nil || isRelationship {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid ASSERT pattern")
		}

		predicateExpr, tail := splitDDLOptionsTail(q[assertEnd:])
		if tail != "" && !startsWithKeywordFold(tail, "OPTIONS") {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("invalid trailing syntax")
		}
		kind, property, ok := parseConstraintPredicate(predicateExpr)
		if !ok {
			return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("unsupported constraint predicate")
		}
		return parsedSimplePropertyConstraintDDL{name: name, label: label, property: property, kind: kind, isRelationship: false}, nil
	}

	return parsedSimplePropertyConstraintDDL{}, fmt.Errorf("unsupported CREATE CONSTRAINT shape")
}

// parseIndexProperties parses property list from index ON clause.
//
// Handles both single and composite property syntax:
//   - "n.property"           -> ["property"]
//   - "n.prop1, n.prop2"     -> ["prop1", "prop2"]
//   - "n.a, n.b, n.c"        -> ["a", "b", "c"]
func (e *StorageExecutor) parseIndexProperties(propertiesStr string) []string {
	return e.parseIndexPropertiesWithMode(propertiesStr, true)
}

func (e *StorageExecutor) backfillPropertyIndex(label string, properties []string) error {
	// Current runtime lookup path uses single-property indexes.
	if len(properties) != 1 {
		return nil
	}
	property := properties[0]
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil
	}

	nodes, err := e.storage.GetNodesByLabel(label)
	if err != nil {
		return fmt.Errorf("failed to backfill index for label %s: %w", label, err)
	}
	for _, node := range nodes {
		if node == nil || node.Properties == nil {
			continue
		}
		value, ok := node.Properties[property]
		if !ok {
			continue
		}
		nodeID := storage.EnsureNodeIDDatabasePrefixForEngine(e.storage, node.ID)
		if err := schema.PropertyIndexInsert(label, property, nodeID, value); err != nil {
			return fmt.Errorf("failed to backfill property index %s(%s): %w", label, property, err)
		}
	}
	return nil
}

func (e *StorageExecutor) parseQualifiedIndexProperties(propertiesStr string) []string {
	return e.parseIndexPropertiesWithMode(propertiesStr, false)
}

func (e *StorageExecutor) parseIndexPropertiesWithMode(propertiesStr string, allowBare bool) []string {
	// Split by comma
	parts := strings.Split(propertiesStr, ",")
	var properties []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Extract property name after dot (e.g., "n.prop" -> "prop")
		if dotIdx := strings.LastIndex(part, "."); dotIdx >= 0 && dotIdx < len(part)-1 {
			propName := normalizeIdentifierToken(part[dotIdx+1:])
			if propName != "" {
				properties = append(properties, propName)
			}
		} else if allowBare {
			// Also support bare property names used by legacy syntax ON :Label(prop)
			propName := normalizeIdentifierToken(part)
			if propName != "" {
				properties = append(properties, propName)
			}
		}
	}

	return properties
}

func (e *StorageExecutor) parseConstraintProperties(propertiesStr string) []string {
	parts := strings.Split(propertiesStr, ",")
	properties := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if dotIdx := strings.LastIndex(part, "."); dotIdx >= 0 && dotIdx < len(part)-1 {
			propName := normalizeIdentifierToken(part[dotIdx+1:])
			if propName != "" {
				properties = append(properties, propName)
			}
		}
	}
	return properties
}

func parsePropertyType(typeName string) (storage.PropertyType, error) {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "STRING":
		return storage.PropertyTypeString, nil
	case "INTEGER", "INT":
		return storage.PropertyTypeInteger, nil
	case "FLOAT":
		return storage.PropertyTypeFloat, nil
	case "BOOLEAN", "BOOL":
		return storage.PropertyTypeBoolean, nil
	case "DATE":
		return storage.PropertyTypeDate, nil
	case "DATETIME", "ZONED DATETIME", "ZONEDDATETIME":
		return storage.PropertyTypeZonedDateTime, nil
	case "LOCAL DATETIME", "LOCALDATETIME":
		return storage.PropertyTypeLocalDateTime, nil
	default:
		return "", fmt.Errorf("unsupported property type: %s", typeName)
	}
}

// executeCreateFulltextIndex handles CREATE FULLTEXT INDEX commands.
//
// Supported syntax:
//
//	CREATE FULLTEXT INDEX index_name [IF NOT EXISTS]
//	FOR (n:Label) ON EACH [n.prop1, n.prop2]
//
//	CREATE FULLTEXT INDEX index_name [IF NOT EXISTS]
//	FOR ()-[r:RelType]-() ON EACH [r.prop1, r.prop2]
//
// The relationship form binds an index to a relationship type (or
// types — Neo4j allows `:R1|R2`); the runtime then scopes
// db.index.fulltext.queryRelationships to those types instead of
// scanning every edge.
func (e *StorageExecutor) executeCreateFulltextIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, fmt.Errorf("schema manager not available")
	}

	parsed, err := e.parseCreateFulltextIndexDDL(cypher)
	if err != nil {
		return nil, err
	}

	if parsed.isRelationship {
		if err := schema.AddFulltextRelationshipIndex(parsed.indexName, parsed.relationshipTypes, parsed.properties); err != nil {
			return nil, fmt.Errorf("failed to add fulltext relationship index: %w", err)
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if err := schema.AddFulltextIndex(parsed.indexName, []string{parsed.label}, parsed.properties); err != nil {
		return nil, fmt.Errorf("failed to add fulltext index: %w", err)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// parseDomainValueList parses a comma-separated list of literal values from a domain constraint.
// Supports strings ('value' or "value"), integers, and floats.
func parseDomainValueList(raw string) ([]interface{}, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var values []interface{}
	// Simple tokenizer: split by comma, handling quoted strings
	for len(raw) > 0 {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			break
		}

		var token string
		if raw[0] == '\'' {
			// Single-quoted string
			end := strings.Index(raw[1:], "'")
			if end < 0 {
				return nil, fmt.Errorf("unterminated string: %s", raw)
			}
			token = raw[1 : end+1]
			values = append(values, token)
			raw = raw[end+2:]
		} else if raw[0] == '"' {
			// Double-quoted string
			end := strings.Index(raw[1:], "\"")
			if end < 0 {
				return nil, fmt.Errorf("unterminated string: %s", raw)
			}
			token = raw[1 : end+1]
			values = append(values, token)
			raw = raw[end+2:]
		} else {
			// Numeric or boolean literal
			commaIdx := strings.Index(raw, ",")
			if commaIdx >= 0 {
				token = strings.TrimSpace(raw[:commaIdx])
				raw = raw[commaIdx:]
			} else {
				token = strings.TrimSpace(raw)
				raw = ""
			}
			if token == "" {
				continue
			}
			// Try integer
			if intVal, err := strconv.ParseInt(token, 10, 64); err == nil {
				values = append(values, intVal)
			} else if floatVal, err := strconv.ParseFloat(token, 64); err == nil {
				values = append(values, floatVal)
			} else if token == "true" {
				values = append(values, true)
			} else if token == "false" {
				values = append(values, false)
			} else {
				// Treat as bare string
				values = append(values, token)
			}
		}

		// Skip comma separator
		raw = strings.TrimSpace(raw)
		if len(raw) > 0 && raw[0] == ',' {
			raw = raw[1:]
		}
	}

	return values, nil
}

func parseFulltextRelationshipTypes(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nerrors.ErrInvalidFulltextRelationshipTypes
	}

	types := make([]string, 0, 2)
	var cur strings.Builder
	inBacktick := false

	flush := func() error {
		part := normalizeIdentifierToken(strings.TrimSpace(cur.String()))
		cur.Reset()
		if part == "" {
			return nerrors.ErrInvalidFulltextRelationshipTypes
		}
		types = append(types, part)
		return nil
	}

	for _, r := range raw {
		switch r {
		case '`':
			inBacktick = !inBacktick
			cur.WriteRune(r)
		case '|':
			if inBacktick {
				cur.WriteRune(r)
				continue
			}
			if err := flush(); err != nil {
				return nil, err
			}
		default:
			cur.WriteRune(r)
		}
	}

	if inBacktick {
		return nil, nerrors.ErrInvalidFulltextRelationshipTypes
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return types, nil
}

func normalizeIdentifierToken(v string) string {
	s := strings.TrimSpace(v)
	if len(s) >= 2 && strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") {
		s = s[1 : len(s)-1]
		s = strings.ReplaceAll(s, "``", "`")
	}
	return strings.TrimSpace(s)
}

// executeCreateVectorIndex handles CREATE VECTOR INDEX commands.
//
// Supported syntax:
//
//	CREATE VECTOR INDEX index_name IF NOT EXISTS
//	FOR (n:Label) ON (n.property)
//	OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}}
func (e *StorageExecutor) executeCreateVectorIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	parsed, err := e.parseCreateIndexDDL(cypher, "CREATE VECTOR INDEX")
	if err != nil {
		return nil, fmt.Errorf("invalid CREATE VECTOR INDEX syntax")
	}
	if len(parsed.properties) != 1 || parsed.indexName == "" ||
		(!parsed.isRelationship && parsed.label == "") ||
		(parsed.isRelationship && parsed.relationshipType == "") {
		return nil, fmt.Errorf("invalid CREATE VECTOR INDEX syntax")
	}

	indexName := parsed.indexName
	label := parsed.label
	if parsed.isRelationship {
		label = parsed.relationshipType
	}
	property := parsed.properties[0]

	// Parse OPTIONS if present - use configured default dimensions
	dimensions := e.GetDefaultEmbeddingDimensions()
	similarityFunc := "cosine" // Default

	dimensions, similarityFunc = parseVectorOptions(parsed.optionsClause, dimensions, similarityFunc)

	// Add vector index. The schema entry is recorded regardless of the
	// vector master switch so SHOW INDEXES still reflects operator intent
	// and DROP INDEX has something to remove. The actual in-memory vector
	// space is only registered when vector search is enabled — otherwise
	// the operator explicitly turned vector indexing off and we must not
	// allocate or warm any vector backend on their behalf.
	entityType := storage.ConstraintEntityNode
	if parsed.isRelationship {
		entityType = storage.ConstraintEntityRelationship
	}
	if err := e.storage.GetSchema().AddVectorIndexForEntity(indexName, label, property, dimensions, similarityFunc, entityType); err != nil {
		return nil, err
	}

	// Relationship vector indexes are consumed by relationship vector procedures
	// directly from relationship properties and schema metadata.
	// registerVectorSpace is node-vector specific; keep relationship indexes out.
	if !parsed.isRelationship && (e.searchService == nil || e.searchService.VectorEnabled()) {
		e.registerVectorSpace(indexName, label, property, dimensions, similarityFunc)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
