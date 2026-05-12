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
	"regexp"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var (
	identifierCompatPattern  = `(?:` + "`" + `[^` + "`" + `]+` + "`" + `|[A-Za-z_][A-Za-z0-9_]*)`
	createIndexLegacyPattern = regexp.MustCompile(
		`(?is)^\s*CREATE\s+INDEX(?:\s+(` + identifierCompatPattern + `))?(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+:(` + identifierCompatPattern + `)\s*\(([^)]+)\)(?:\s+OPTIONS\s+\{.*\})?\s*$`)
	createIndexForCompatPattern = regexp.MustCompile(
		`(?is)^\s*CREATE\s+INDEX(?:\s+(` + identifierCompatPattern + `))?(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + identifierCompatPattern + `)\s*:\s*(` + identifierCompatPattern + `)\s*\)\s+ON\s+(.+?)(?:\s+OPTIONS\s+\{.*\})?\s*$`)
	fulltextIndexCompatPattern = regexp.MustCompile(
		`(?is)^\s*CREATE\s+FULLTEXT\s+INDEX\s+(` + identifierCompatPattern + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(?\s*(` + identifierCompatPattern + `)\s*:\s*(` + identifierCompatPattern + `)\s*\)?\s+ON(?:\s+EACH)?\s+(.+?)(?:\s+OPTIONS\s+\{.*\})?\s*$`)
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

	// NODE KEY constraints (Neo4j 5.x)
	if matches := constraintNamedForRequireNodeKey.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		properties := e.parseConstraintProperties(matches[4])
		if len(properties) == 0 {
			return nil, fmt.Errorf("NODE KEY constraint requires properties")
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireNodeKey.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("NODE KEY constraint requires properties")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_node_key", strings.ToLower(label), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertNodeKey.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("NODE KEY constraint requires properties")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_node_key", strings.ToLower(label), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Temporal no-overlap constraints (NornicDB extension)
	if matches := constraintNamedForRequireTemporal.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		properties := e.parseConstraintProperties(matches[4])
		if len(properties) != 3 {
			return nil, fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireTemporal.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) != 3 {
			return nil, fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_temporal", strings.ToLower(label), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Node domain/enum constraints (NornicDB extension)
	if matches := constraintNodeNamedForRequireDomain.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])
		allowedValues, err := parseDomainValueList(matches[6])
		if err != nil {
			return nil, fmt.Errorf("invalid domain value list: %w", err)
		}
		if len(allowedValues) == 0 {
			return nil, fmt.Errorf("DOMAIN constraint requires at least one allowed value")
		}

		constraint := storage.Constraint{
			Name:          constraintName,
			Type:          storage.ConstraintDomain,
			Label:         label,
			Properties:    []string{property},
			AllowedValues: allowedValues,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintNodeUnnamedForRequireDomain.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		allowedValues, err := parseDomainValueList(matches[5])
		if err != nil {
			return nil, fmt.Errorf("invalid domain value list: %w", err)
		}
		if len(allowedValues) == 0 {
			return nil, fmt.Errorf("DOMAIN constraint requires at least one allowed value")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_domain", strings.ToLower(label), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:          constraintName,
			Type:          storage.ConstraintDomain,
			Label:         label,
			Properties:    []string{property},
			AllowedValues: allowedValues,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// EXISTS / NOT NULL constraints
	if matches := constraintNamedForRequireNotNull.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireNotNull.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_exists", strings.ToLower(label), strings.ToLower(property))
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertExists.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_exists", strings.ToLower(label), strings.ToLower(property))
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertNotNull.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_exists", strings.ToLower(label), strings.ToLower(property))
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Property type constraints
	if matches := constraintNamedForRequireType.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])
		expectedType, err := parsePropertyType(matches[6])
		if err != nil {
			return nil, err
		}
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        label,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraintWithOptions(constraintName, label, property, expectedType, storage.PropertyTypeConstraintOptions{IfNotExists: ifNotExists}); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireType.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		expectedType, err := parsePropertyType(matches[5])
		if err != nil {
			return nil, err
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_type", strings.ToLower(label), strings.ToLower(property))
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        label,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraintWithOptions(constraintName, label, property, expectedType, storage.PropertyTypeConstraintOptions{IfNotExists: ifNotExists}); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertType.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		expectedType, err := parsePropertyType(matches[5])
		if err != nil {
			return nil, err
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_type", strings.ToLower(label), strings.ToLower(property))
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        label,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraintWithOptions(constraintName, label, property, expectedType, storage.PropertyTypeConstraintOptions{IfNotExists: ifNotExists}); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 1 (Neo4j 5.x): CREATE CONSTRAINT name IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		schema := e.storage.GetSchema()
		if err := schema.AddUniqueConstraint(constraintName, label, property, ifNotExists); err != nil {
			return nil, err
		}
		if err := storage.RefreshUniqueConstraintValuesForEngine(e.storage, schema); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 2 (Neo4j 5.x without name): CREATE CONSTRAINT IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s", strings.ToLower(label), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		schema := e.storage.GetSchema()
		if err := schema.AddUniqueConstraint(constraintName, label, property, ifNotExists); err != nil {
			return nil, err
		}
		if err := storage.RefreshUniqueConstraintValuesForEngine(e.storage, schema); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 3 (Neo4j 4.x): CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintOnAssert.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s", strings.ToLower(label), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		schema := e.storage.GetSchema()
		if err := schema.AddUniqueConstraint(constraintName, label, property, ifNotExists); err != nil {
			return nil, err
		}
		if err := storage.RefreshUniqueConstraintValuesForEngine(e.storage, schema); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// =========================================================================
	// Relationship constraint patterns
	// =========================================================================

	// Cardinality constraints — outgoing (NornicDB extension)
	if matches := constraintCardinalityOutNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		maxCount, err := strconv.Atoi(matches[4])
		if err != nil || maxCount < 1 {
			return nil, fmt.Errorf("MAX COUNT must be a positive integer, got %q", matches[4])
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintCardinality,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			MaxCount:   maxCount,
			Direction:  "OUTGOING",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	if matches := constraintCardinalityOutUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		maxCount, err := strconv.Atoi(matches[3])
		if err != nil || maxCount < 1 {
			return nil, fmt.Errorf("MAX COUNT must be a positive integer, got %q", matches[3])
		}
		constraintName := fmt.Sprintf("constraint_%s_max_outgoing_%d", strings.ToLower(relType), maxCount)
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintCardinality,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			MaxCount:   maxCount,
			Direction:  "OUTGOING",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Cardinality constraints — incoming (NornicDB extension)
	if matches := constraintCardinalityInNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		maxCount, err := strconv.Atoi(matches[4])
		if err != nil || maxCount < 1 {
			return nil, fmt.Errorf("MAX COUNT must be a positive integer, got %q", matches[4])
		}
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintCardinality,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			MaxCount:   maxCount,
			Direction:  "INCOMING",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	if matches := constraintCardinalityInUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		maxCount, err := strconv.Atoi(matches[3])
		if err != nil || maxCount < 1 {
			return nil, fmt.Errorf("MAX COUNT must be a positive integer, got %q", matches[3])
		}
		constraintName := fmt.Sprintf("constraint_%s_max_incoming_%d", strings.ToLower(relType), maxCount)
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintCardinality,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			MaxCount:   maxCount,
			Direction:  "INCOMING",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship endpoint policy constraints (NornicDB extension)
	if matches := constraintPolicyAllowedNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		sourceLabel := normalizeIdentifierToken(matches[2])
		relType := normalizeIdentifierToken(matches[4])
		targetLabel := normalizeIdentifierToken(matches[5])
		constraint := storage.Constraint{
			Name:        constraintName,
			Type:        storage.ConstraintPolicy,
			EntityType:  storage.ConstraintEntityRelationship,
			Label:       relType,
			SourceLabel: sourceLabel,
			TargetLabel: targetLabel,
			PolicyMode:  "ALLOWED",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	if matches := constraintPolicyAllowedUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		sourceLabel := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		targetLabel := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_%s_allowed", strings.ToLower(sourceLabel), strings.ToLower(relType), strings.ToLower(targetLabel))
		constraint := storage.Constraint{
			Name:        constraintName,
			Type:        storage.ConstraintPolicy,
			EntityType:  storage.ConstraintEntityRelationship,
			Label:       relType,
			SourceLabel: sourceLabel,
			TargetLabel: targetLabel,
			PolicyMode:  "ALLOWED",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	if matches := constraintPolicyDisallowedNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		sourceLabel := normalizeIdentifierToken(matches[2])
		relType := normalizeIdentifierToken(matches[4])
		targetLabel := normalizeIdentifierToken(matches[5])
		constraint := storage.Constraint{
			Name:        constraintName,
			Type:        storage.ConstraintPolicy,
			EntityType:  storage.ConstraintEntityRelationship,
			Label:       relType,
			SourceLabel: sourceLabel,
			TargetLabel: targetLabel,
			PolicyMode:  "DISALLOWED",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	if matches := constraintPolicyDisallowedUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		sourceLabel := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		targetLabel := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_%s_disallowed", strings.ToLower(sourceLabel), strings.ToLower(relType), strings.ToLower(targetLabel))
		constraint := storage.Constraint{
			Name:        constraintName,
			Type:        storage.ConstraintPolicy,
			EntityType:  storage.ConstraintEntityRelationship,
			Label:       relType,
			SourceLabel: sourceLabel,
			TargetLabel: targetLabel,
			PolicyMode:  "DISALLOWED",
		}
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship domain/enum constraints (NornicDB extension)
	if matches := constraintRelNamedForRequireDomain.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])
		allowedValues, err := parseDomainValueList(matches[6])
		if err != nil {
			return nil, fmt.Errorf("invalid domain value list: %w", err)
		}
		if len(allowedValues) == 0 {
			return nil, fmt.Errorf("DOMAIN constraint requires at least one allowed value")
		}

		constraint := storage.Constraint{
			Name:          constraintName,
			Type:          storage.ConstraintDomain,
			EntityType:    storage.ConstraintEntityRelationship,
			Label:         relType,
			Properties:    []string{property},
			AllowedValues: allowedValues,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequireDomain.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		allowedValues, err := parseDomainValueList(matches[5])
		if err != nil {
			return nil, fmt.Errorf("invalid domain value list: %w", err)
		}
		if len(allowedValues) == 0 {
			return nil, fmt.Errorf("DOMAIN constraint requires at least one allowed value")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_domain", strings.ToLower(relType), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:          constraintName,
			Type:          storage.ConstraintDomain,
			EntityType:    storage.ConstraintEntityRelationship,
			Label:         relType,
			Properties:    []string{property},
			AllowedValues: allowedValues,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship temporal no-overlap constraints (NornicDB extension)
	if matches := constraintRelNamedForRequireTemporal.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		properties := e.parseConstraintProperties(matches[4])
		if len(properties) < 3 {
			return nil, fmt.Errorf("TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)")
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequireTemporal.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) < 3 {
			return nil, fmt.Errorf("TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_temporal", strings.ToLower(relType), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship KEY constraints (composite properties) — must be checked before composite UNIQUE
	if matches := constraintRelNamedForRequireRelKey.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		properties := e.parseConstraintProperties(matches[4])
		if len(properties) == 0 {
			return nil, fmt.Errorf("RELATIONSHIP KEY constraint requires properties")
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintRelationshipKey,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequireRelKey.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("RELATIONSHIP KEY constraint requires properties")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_rel_key", strings.ToLower(relType), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintRelationshipKey,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship single-property KEY
	if matches := constraintRelNamedForRequireSingleRelKey.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintRelationshipKey,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequireSingleRelKey.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_rel_key", strings.ToLower(relType), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintRelationshipKey,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship composite UNIQUE constraints — must be checked before single-property UNIQUE
	if matches := constraintRelNamedForRequireCompositeUnique.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		properties := e.parseConstraintProperties(matches[4])
		if len(properties) == 0 {
			return nil, fmt.Errorf("UNIQUE constraint requires properties")
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequireCompositeUnique.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("UNIQUE constraint requires properties")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_unique", strings.ToLower(relType), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship single-property UNIQUE
	if matches := constraintRelNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_unique", strings.ToLower(relType), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship EXISTS / NOT NULL
	if matches := constraintRelNamedForRequireNotNull.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequireNotNull.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		constraintName := fmt.Sprintf("constraint_%s_%s_exists", strings.ToLower(relType), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			EntityType: storage.ConstraintEntityRelationship,
			Label:      relType,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint, ifNotExists); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Relationship property type constraints
	if matches := constraintRelNamedForRequireType.FindStringSubmatch(cypher); matches != nil {
		constraintName := normalizeIdentifierToken(matches[1])
		relType := normalizeIdentifierToken(matches[3])
		property := normalizeIdentifierToken(matches[5])
		expectedType, err := parsePropertyType(matches[6])
		if err != nil {
			return nil, err
		}
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			EntityType:   storage.ConstraintEntityRelationship,
			Label:        relType,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraintWithOptions(constraintName, relType, property, expectedType, storage.PropertyTypeConstraintOptions{EntityType: storage.ConstraintEntityRelationship, IfNotExists: ifNotExists}); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintRelUnnamedForRequireType.FindStringSubmatch(cypher); matches != nil {
		relType := normalizeIdentifierToken(matches[2])
		property := normalizeIdentifierToken(matches[4])
		expectedType, err := parsePropertyType(matches[5])
		if err != nil {
			return nil, err
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_type", strings.ToLower(relType), strings.ToLower(property))
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			EntityType:   storage.ConstraintEntityRelationship,
			Label:        relType,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraintWithOptions(constraintName, relType, property, expectedType, storage.PropertyTypeConstraintOptions{EntityType: storage.ConstraintEntityRelationship, IfNotExists: ifNotExists}); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

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

	if err := e.storage.GetSchema().DropIndex(name); err != nil {
		if ifExists && strings.Contains(err.Error(), "does not exist") {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
		return nil, err
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
	// Pattern: CREATE INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property[, n.property2, ...])
	// Uses pre-compiled patterns from regex_patterns.go
	if matches := indexNamedFor.FindStringSubmatch(cypher); matches != nil {
		indexName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		propertiesStr := matches[4] // e.g., "n.prop1, n.prop2"

		// Parse properties (single or multiple)
		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}

		// Add index to schema (supports composite indexes)
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}
		if err := e.backfillPropertyIndex(label, properties); err != nil {
			return nil, err
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Try without index name
	if matches := indexUnnamedFor.FindStringSubmatch(cypher); matches != nil {
		label := normalizeIdentifierToken(matches[2])
		propertiesStr := matches[3] // e.g., "n.prop1, n.prop2"

		// Parse properties
		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}

		// Generate index name based on label and properties
		propsJoined := strings.Join(properties, "_")
		indexName := fmt.Sprintf("index_%s_%s", strings.ToLower(label), strings.ToLower(propsJoined))

		// Add index
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}
		if err := e.backfillPropertyIndex(label, properties); err != nil {
			return nil, err
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Neo4j legacy syntax: CREATE INDEX [name] ON :Label(prop[, prop2])
	if matches := createIndexLegacyPattern.FindStringSubmatch(strings.TrimSpace(cypher)); matches != nil {
		indexName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[2])
		properties := e.parseIndexProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}
		if indexName == "" {
			propsJoined := strings.Join(properties, "_")
			indexName = fmt.Sprintf("index_%s_%s", strings.ToLower(label), strings.ToLower(propsJoined))
		}
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}
		if err := e.backfillPropertyIndex(label, properties); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Neo4j 5 compat variant: CREATE INDEX [name] FOR (n:Label) ON n.prop
	if matches := createIndexForCompatPattern.FindStringSubmatch(strings.TrimSpace(cypher)); matches != nil {
		indexName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		onPart := strings.TrimSpace(matches[4])
		if strings.HasPrefix(onPart, "(") && strings.HasSuffix(onPart, ")") {
			onPart = strings.TrimSpace(onPart[1 : len(onPart)-1])
		}
		properties := e.parseQualifiedIndexProperties(onPart)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}
		if indexName == "" {
			propsJoined := strings.Join(properties, "_")
			indexName = fmt.Sprintf("index_%s_%s", strings.ToLower(label), strings.ToLower(propsJoined))
		}
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}
		if err := e.backfillPropertyIndex(label, properties); err != nil {
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
	// Reuse generic CREATE INDEX regexes by normalizing RANGE syntax.
	normalized := strings.Replace(strings.ToUpper(cypher), "CREATE RANGE INDEX", "CREATE INDEX", 1)

	// Pattern: CREATE RANGE INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property)
	// Reuse the standard index pattern - same structure
	if matches := indexNamedFor.FindStringSubmatch(normalized); matches != nil {
		indexName := matches[1]
		label := matches[3]
		propertiesStr := matches[4]

		// Range index only supports single property
		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) != 1 {
			return nil, fmt.Errorf("RANGE INDEX only supports single property, got %d", len(properties))
		}

		err := e.storage.GetSchema().AddRangeIndex(indexName, label, properties[0])
		if err != nil {
			return nil, fmt.Errorf("failed to create range index: %w", err)
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Unnamed range index
	if matches := indexUnnamedFor.FindStringSubmatch(normalized); matches != nil {
		label := matches[2]
		propertiesStr := matches[3]

		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) != 1 {
			return nil, fmt.Errorf("RANGE INDEX only supports single property, got %d", len(properties))
		}

		indexName := fmt.Sprintf("range_idx_%s_%s", strings.ToLower(label), properties[0])
		err := e.storage.GetSchema().AddRangeIndex(indexName, label, properties[0])
		if err != nil {
			return nil, fmt.Errorf("failed to create range index: %w", err)
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE RANGE INDEX syntax")
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
//	CREATE FULLTEXT INDEX index_name IF NOT EXISTS
//	FOR (n:Label) ON EACH [n.prop1, n.prop2]
func (e *StorageExecutor) executeCreateFulltextIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE FULLTEXT INDEX name IF NOT EXISTS FOR (n:Label) ON EACH [n.prop1, n.prop2]
	// Uses pre-compiled pattern from regex_patterns.go
	matches := fulltextIndexPattern.FindStringSubmatch(cypher)

	if matches == nil {
		compat := fulltextIndexCompatPattern.FindStringSubmatch(strings.TrimSpace(cypher))
		if compat == nil {
			return nil, fmt.Errorf("invalid CREATE FULLTEXT INDEX syntax: %s", cypher)
		}

		indexName := normalizeIdentifierToken(compat[1])
		label := normalizeIdentifierToken(compat[3])
		propsRaw := strings.TrimSpace(compat[4])
		if strings.HasPrefix(propsRaw, "[") && strings.HasSuffix(propsRaw, "]") {
			propsRaw = strings.TrimSpace(propsRaw[1 : len(propsRaw)-1])
		}
		properties := e.parseQualifiedIndexProperties(propsRaw)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties found in fulltext index definition")
		}
		schema := e.storage.GetSchema()
		if schema == nil {
			return nil, fmt.Errorf("schema manager not available")
		}
		if err := schema.AddFulltextIndex(indexName, []string{label}, properties); err != nil {
			return nil, fmt.Errorf("failed to add fulltext index: %w", err)
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	indexName := normalizeIdentifierToken(matches[1])
	label := normalizeIdentifierToken(matches[3])
	propertiesStr := matches[4]

	// Parse properties: "n.prop1, n.prop2" -> ["prop1", "prop2"]
	properties := e.parseQualifiedIndexProperties(propertiesStr)

	if len(properties) == 0 {
		return nil, fmt.Errorf("no properties found in fulltext index definition")
	}

	// Add fulltext index
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, fmt.Errorf("schema manager not available")
	}

	if err := schema.AddFulltextIndex(indexName, []string{label}, properties); err != nil {
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
	// Pattern: CREATE VECTOR INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property)
	// Uses pre-compiled patterns from regex_patterns.go
	matches := vectorIndexPattern.FindStringSubmatch(cypher)

	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE VECTOR INDEX syntax")
	}

	indexName := normalizeIdentifierToken(matches[1])
	label := normalizeIdentifierToken(matches[3])
	property := normalizeIdentifierToken(matches[5])

	// Parse OPTIONS if present - use configured default dimensions
	dimensions := e.GetDefaultEmbeddingDimensions()
	similarityFunc := "cosine" // Default

	if strings.Contains(cypher, "OPTIONS") {
		// Extract dimensions using pre-compiled pattern
		if dimMatches := vectorDimensionsPattern.FindStringSubmatch(cypher); dimMatches != nil {
			if dim, err := strconv.Atoi(dimMatches[1]); err == nil {
				dimensions = dim
			}
		}

		// Extract similarity function using pre-compiled pattern
		if simMatches := vectorSimilarityPattern.FindStringSubmatch(cypher); simMatches != nil {
			similarityFunc = simMatches[1]
		}
	}

	// Add vector index
	if err := e.storage.GetSchema().AddVectorIndex(indexName, label, property, dimensions, similarityFunc); err != nil {
		return nil, err
	}

	e.registerVectorSpace(indexName, label, property, dimensions, similarityFunc)

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
